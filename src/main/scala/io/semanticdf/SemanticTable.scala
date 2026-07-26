package io.semanticdf

import io.semanticdf.audit.{AuditEvent, AuditSink, QueryRequest => AuditQueryRequest}

import org.apache.spark.sql.{Column, Dataset, DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._
import scala.jdk.CollectionConverters._

/** Sort-key DSL for [[SemanticTable.orderBy]] / [[SemanticTable.query]] (Phase 5 completion).
  *
  * A bare string is ascending; wrap in [[SortKey.desc]] for descending:
  * {{{
  * st.orderBy("carrier", SortKey.desc("total_passengers"))
  * }}} */
sealed trait SortKey {
  private[semanticdf] def toColumn: Column
}
object SortKey {

  /** Wrap a column name in backticks if it contains characters that
    * Spark's `col(...)` would misinterpret — notably `.` (treated as a
    * table/struct qualifier). Joined dimensions are named `alias.column`
    * (e.g. `customers.signup_date`); without quoting, `col("customers.x")`
    * looks for a nested struct field instead of the literal column.
    * Names already wrapped in backticks (by the caller) are left as-is,
    * so this is backward-compatible with manual `` SortKey.asc(s"`x`") ``.
    * Simple identifiers are returned unchanged. */
  private def quote(name: String): String =
    if (name.startsWith("`")) name
    else if (name.matches("[a-zA-Z_][a-zA-Z0-9_]*")) name
    else s"`$name`"

  private[semanticdf] final case class Asc(name: String)  extends SortKey { def toColumn = col(quote(name)).asc }
  private[semanticdf] final case class Desc(name: String) extends SortKey { def toColumn = col(quote(name)).desc }

  /** Explicit ascending key. */
  def asc(name: String): SortKey = Asc(name)
  /** Explicit descending key. */
  def desc(name: String): SortKey = Desc(name)

  /** Typed ascending key — reads the column name directly from the
    * [[SemanticField]] witness. Works for any field (dimension or measure),
    * so `SortKey.asc(carrier)`, `SortKey.desc(pax)` are both valid.
    *
    * The parameter is the typeclass instance itself (not a `FieldRef`), so
    * `SemanticDimension[F]` / `SemanticMeasure[F]` match by subtyping in
    * Scala's phase-1 overload resolution — no implicit conversion is needed,
    * and this overload is picked over `asc(name: String)` even from
    * cross-package consumer code. */
  def asc(field: SemanticField[_]): SortKey = Asc(field.name)

  /** Typed descending key — see [[asc(field)*]]. */
  def desc(field: SemanticField[_]): SortKey = Desc(field.name)

  /** Read the column-name field of any SortKey (private to avoid exposing the sealed
    * cases to public API). Used by [[SemanticTable.explainSemantic]]. */
  private[semanticdf] def nameOf(k: SortKey): String = k match {
    case Asc(n)  => n
    case Desc(n) => n
    case _       => ""
  }

  /** Implicit `String => SortKey` so `orderBy("carrier", SortKey.desc("x"))` works. */
  implicit def strToSortKey(name: String): SortKey = Asc(name)
}

/** Structured result of a [[SemanticTable.validate]] call.
  *
  * - `errors`   are conditions that would cause `execute()` to throw at runtime.
  * - `warnings` are conditions that are legal but worth surfacing (e.g. a time
  *              dimension with no `smallestTimeGrain` would surprise `atTimeGrain()`).
  *
  * `isValid` is the boolean summary; CI checks use that directly. */
final case class ValidationResult(
    errors: Seq[String],
    warnings: Seq[String],
) {
  def isValid: Boolean = errors.isEmpty
  def hasIssues: Boolean = errors.nonEmpty || warnings.nonEmpty
}

/** Immutable facade over the root of a semantic op tree (DESIGN §4.1).
  *
  * A `SemanticTable` is *not* a Spark `DataFrame`; it is a deferred, source-agnostic
  * definition that compiles to a DataFrame at an execution terminal. The batch terminal
  * is [[SemanticTable.toDataFrame]] / [[SemanticTable.execute]]; the streaming
  * terminal is [[SemanticTable.toStreamingQuery]]. Same definition, different sink,
  * mirroring Spark's own `df.write` vs `df.writeStream`.
  */
final class SemanticTable private[semanticdf] (
    private[semanticdf] val root: SemanticOp,
    private[semanticdf] val postAggPredicates: List[Predicate] = Nil,
    /** Per-model schema version, propagated to MCP/OKF consumers.
      *
      * `0` means "pre-versioning era" — the model declaration did not commit to
      * a version. The library never fails on a mismatch; it just stores and emits
      * the value. Compatibility policy is the consumer's problem (MCP server,
      * agent framework, downstream pipelines).
      *
      * Defaults to 0. Set via the YAML `version:` field or the fluent `.version(n)` setter.
      */
    val version: Int = 0,
    /** Name of the underlying source DataFrame this model was built from, if known.
      *
      * Populated by [[YamlLoader]] from the YAML `table:` field — the name used
      * to resolve the source DataFrame against either a caller-supplied map or
      * the Spark catalog. Unset (None) for models built directly from the Scala
      * DSL ([[io.semanticdf.toSemanticTable]]) where there's no equivalent concept.
      *
      * Used by MCP `describe_model` to expose the origin of a model's data to
      * consumers (LLM agents, BI tools, lineage trackers).
      */
    val sourceTable: Option[String] = None,
    /** Lifecycle status of this model. Surfaced by MCP `describe_model`,
      * the manifest artifact (`SemanticManifest.toJson`), and OKF generation
      * so consumers (LLM agents, BI tools, downstream pipelines) can decide
      * whether to query, warn, or refuse.
      *
      * Defaults to [[ModelStatus.Published]] for backwards compatibility —
      * models built in v0.1.x implicitly were published; carrying that
      * semantics forward keeps existing programs working without change.
      * New models can declare `status: draft` / `published` / `deprecated`
      * in YAML or via the fluent setter [[status(s:ModelStatus)* status]].
      *
      * Lifecycle is purely informational at the library level — the query
      * terminals (`toDataFrame`, `toStreamingQuery`, `execute`) do not
      * consult status. Consumers enforce policy.
      */
    val status: ModelStatus = ModelStatus.Published,
    /** Audit log sink — when set, every `toDataFrame` / `execute` call
      * that traces back to a `query()` invocation emits an
      * [[io.semanticdf.audit.AuditEvent]] describing the request
      * shape and the execution result. Default `None` (no audit) so
      * the audit path is opt-in.
      *
      * Set via the fluent `.withAuditSink(sink)` setter. Survives the
      * fluent chain (`.query(...).limit(...).toDataFrame(...)` keeps
      * the sink) so a single setter call at the model level covers
      * every downstream query. */
    val auditSink: Option[io.semanticdf.audit.AuditSink] = None,
    /** Captured request shape for audit emission. Populated by
      * [[query]] (and the streaming variants); preserved across the
      * fluent chain so the audit event carries the user's original
      * request, not the post-chain op tree.
      *
      * Default `None`. When `auditSink` is also `None`, this field
      * is dormant — no hashing cost. */
    val auditRequest: Option[AuditQueryRequest] = None,
    /** Result cache — when set, every `toDataFrame` / `execute` call
      * that traces back to a `query()` invocation checks the cache
      * first (by a stable SHA-256 of the request shape) and returns
      * the cached rows on hit. On miss, the result is stored before
      * the DataFrame is returned. Default `None` (no cache) so the
      * cache path is opt-in.
      *
      * The cache key is derived from `auditRequest`, so the
      * fluent chain must capture the request via `query(...)` for
      * caching to work — directly-built chains (`groupBy(...).aggregate(...)`)
      * bypass the cache. */
    val resultCache: Option[io.semanticdf.cache.ResultCache] = None,
) extends Serializable with SemanticTableCore with SemanticTableStreaming with SemanticTableMutation with SemanticTableCollection {
}


/** A row-level filter declared on a model via YAML `filters:` block.
  *
  * Read-only value type. The library maintains these internally as op-tree
  * entries (`SemanticRowFilterOp`), and exposes them through this type for
  * catalog consumers: MCP `describe_model.filters` and OKF `# Filters`.
  */
final case class SemanticFilter(
    name: String,
    description: Option[String],
    expr: String,
    metadata: Map[String, String],
)

/** Classification of a measure within a semantic model.
  *
  * - `Base`: aggregates source columns directly (e.g. `sum(amount)`, `count(1)`).
  * - `Calc`: lambda references other declared measures in the same model
  *   (e.g. `total_revenue / event_count`).
  *
  * Surfaced as MCP `describe_model.measures[].kind` so consumers can reason
  * about aggregation costs and dependencies without re-classifying locally. */
sealed trait MeasureKind
object MeasureKind {
  case object Base extends MeasureKind
  case object Calc extends MeasureKind
}

/** Summary of one join in a semantic model — exposed for MCP `describe_model`.
  *
  * Captures the cardinality, side names, grain (join-key) columns, and any
  * dimensions/measures added via `withDimensions` / `withMeasures` after the
  * join. Internalised `SemanticJoinOp` is kept private to the package; this
  * DTO is the stable, MCP-facing shape. */
final case class JoinInfo(
    /** Cardinality as a string ("one" | "many" | "cross") — string not enum so
      * it serializes cleanly to JSON without a sealed-trait encoder. */
    cardinality: String,
    /** Name of the left-side source model (e.g. "orders"). None if anonymous. */
    leftName: Option[String],
    /** Name of the right-side source model (e.g. "customers"). None if anonymous. */
    rightName: Option[String],
    /** Join-key column names — the equi-join keys. Empty for cross joins. */
    keys: Seq[String],
    /** Names of dimensions added via `withDimensions` after this join. */
    extraDimensions: Seq[String],
    /** Names of measures added via `withMeasures` after this join. */
    extraMeasures: Seq[String],
)
