package io.semanticdf.core.engine

import io.semanticdf.core.schema.Field

/** Engine-portable execution-plan ADT — the wrapper around an
  * engine-specific compiled result.
  *
  * Per the multi-engine design's design §2.1 / v0.3.0
  * HIGH-A closure: `ExecutionPlan` is a `sealed trait` that does
  * NOT auto-extend `Product with Serializable`. The reason:
  * `R` can hold a cluster-unsafe handle (Spark `QueryPlan`,
  * Databricks Connect plan). Auto-derived `Serializable` would
  * silently attempt to serialize the handle on any cluster-mode
  * use, producing `NotSerializableException` deep in the driver
  * or — worse — a half-serialized object that NPEs at use.
  *
  * The wire-safe shape is [[ExecutionPlanSummary]] — a `case
  * class extends Product with Serializable` that carries the
  * summary (SQL string, logical plan, capabilities, warnings,
  * schema) for cache, audit, and MCP transport.
  *
  * ==Why a `sealed trait` (not a `case class`)==
  *
  * The trait exposes the inspectable members (`engine`, `native`,
  * `warnings`, `requiredCapabilities`, `normalizedSchema`,
  * `isCacheable`) abstractly. The single concrete impl
  * [[ExecutionPlan.Simple]] is a `case class` with `Serializable`
  * OFF (note: `extends ExecutionPlan[Simple.R]` — not
  * `extends Product with Serializable`). This breaks the
  * design §2.1 hazard.
  *
  * ==Why these three members (`warnings`, `requiredCapabilities`, `normalizedSchema`)==
  *
  * Per design §4.5.4 "Inspectable plans and portable results":
  * every engine adapter must surface (1) the warnings the engine
  * emitted during compile, (2) the capabilities the plan requires
  * (so a downstream engine policy can validate before execute),
  * and (3) the result schema the engine will produce (so the
  * consumer can deserialize rows without a roundtrip).
  *
  * ==Why `R` is unbounded==
  *
  * `R` is the engine's native plan type — Trino uses
  * `ParameterizedSql`, Spark uses `DataFrame`, Databricks uses
  * its Connect plan. Forcing a Serializable bound would prevent
  * non-Serializable native types. The type system doesn't enforce
  * it here — but [[ExecutionPlan.isCacheable]] signals whether
  * the plan survives a wire roundtrip.
  *
  * ==Data-driven mantra compliance==
  *
  * - Pure contract: `sealed trait` with abstract members
  *   (no implementation leakage)
  * - Summary (`ExecutionPlanSummary`) is a `case class extends
  *   Product with Serializable` — the cacheable / wire-safe
  *   shape
  * - Opaque handle ([[SparkPlanHandle]]) is `extends AnyRef` —
  *   deliberately not Serializable
  *
  * ==Boundary contract==
  *
  * Zero Spark imports. Verifiable by:
  * `grep -r 'org.apache.spark' semanticdf-core/src/main/scala/io/semanticdf/core/engine/ExecutionPlan.scala`
  */
sealed trait ExecutionPlan[+R] {

  /** The engine that compiled this plan. */
  def engine: EngineIdentity

  /** The engine-native plan (Trino SQL, Spark op tree, etc.). */
  def native: R

  /** Warnings the engine emitted during compile (per design
    * §4.5.4 "Inspectable plans"). Default = empty list. */
  def warnings: List[EngineWarning] = Nil

  /** Capabilities the plan requires (so a downstream policy can
    * validate before execute). Default = empty set. */
  def requiredCapabilities: Set[Capability] = Set.empty

  /** The result schema the engine will produce. Default = empty
    * schema (engines populate this as they compute it). */
  def normalizedSchema: ResultSchema = ResultSchema(Nil)

  /** True iff the plan is safe to ship across a wire boundary
    * (cache, audit, MCP). Engines whose `R` is Serializable set
    * this to true; engines with non-Serializable `R` (Databricks
    * Connect, future Spark Structured Streaming) set it to false
    * and require [[ExecutionPlanSummary]] for wire transport. */
  def isCacheable: Boolean = false

  /** Convert to a Serializable summary for cache / audit / MCP
    * wire format. Engines should override to include any engine-
    * specific summary fields (e.g. SQL text, logical plan). */
  def toSummary: ExecutionPlanSummary = ExecutionPlanSummary(
    engine               = engine,
    sql                  = None,
    logicalPlan          = None,
    requiredCapabilities = requiredCapabilities,
    warnings             = warnings,
    normalizedSchema     = normalizedSchema,
  )
}

/** The Serializable summary of an [[ExecutionPlan]]. This is the
  * shape that crosses wire boundaries (cache, audit, MCP, REST).
  *
  * Per design §4.5.4: callers can read this summary without
  * holding the original plan. The summary is a pure-data case
  * class; the `native: R` of the original plan is replaced by
  * the optional `sql: Option[String]` and `logicalPlan: Option[String]`
  * fields. */
final case class ExecutionPlanSummary(
    engine:               EngineIdentity,
    sql:                  Option[String],
    logicalPlan:          Option[String],
    requiredCapabilities: Set[Capability],
    warnings:             List[EngineWarning],
    normalizedSchema:     ResultSchema,
) extends Product with Serializable {

  /** Convenience: `true` iff the plan has at least one textual
    * representation. */
  def hasTextualPlan: Boolean = sql.isDefined || logicalPlan.isDefined
}

/** Companion object — smart constructor + standard impls. */
object ExecutionPlan {

  /** The single concrete impl. A `final class` (not `case class`)
    * with explicit `override val` accessors. Per design §4.5.4
    * (cluster-safety boundary): this class does NOT extend
    * `Product with Serializable` (which is what `case class`
    * auto-derives). The `native: R` field is the cluster-safety
    * boundary.
    *
    * ==Why `final class` (not `case class`)==
    *
    * A `case class` in Scala 2.13 auto-extends
    * `Product with Serializable`. That auto-derived Serializable
    * would silently attempt to serialize the `native: R` field
    * on any cluster-mode use, defeating the design §2.1
    * closure. A plain `final class` does NOT auto-extend
    * `Product with Serializable` — the type-level guarantee is
    * enforced.
    *
    * The companion's `apply` factory preserves call-site
    * ergonomics (engines still write
    * `ExecutionPlan(engine, native, ...)`) without inheriting
    * `Product with Serializable`. */
  final class Simple[R](
      override val engine:               EngineIdentity,
      override val native:               R,
      override val warnings:             List[EngineWarning],
      override val requiredCapabilities: Set[Capability],
      override val normalizedSchema:     ResultSchema,
      override val isCacheable:          Boolean,
  ) extends ExecutionPlan[R]

  /** The canonical "compiled" plan shape. Returns a [[Simple]]
    * carrying the engine's native result + the inspectable
    * metadata (warnings, capabilities, schema). Default
    * `cacheable = true` (assumes `R` is Serializable; engines
    * with non-Serializable R pass `cacheable = false` explicitly). */
  def apply[R](
      engine:               EngineIdentity,
      native:               R,
      warnings:             List[EngineWarning] = Nil,
      requiredCapabilities: Set[Capability]   = Set.empty,
      normalizedSchema:     ResultSchema      = ResultSchema(Nil),
      cacheable:            Boolean           = true,
  ): ExecutionPlan[R] = new Simple[R](
    engine               = engine,
    native               = native,
    warnings             = warnings,
    requiredCapabilities = requiredCapabilities,
    normalizedSchema     = normalizedSchema,
    isCacheable          = cacheable,
  )
}

/** Opaque, deliberately-not-Serializable wrapper for an
  * engine-native plan handle that must not cross a wire boundary.
  *
  * Per the design §2.1 closure: a Spark `QueryPlan`
  * or Databricks Connect plan can hold live JVM references that
  * are NOT cluster-safe. Storing one in [[ExecutionPlan.native]]
  * and then trying to ship the plan to a worker would silently
  * attempt serialization (because of `Product with Serializable`
  * on the case class).
  *
  * Engines that produce non-Serializable `R` should wrap the
  * handle in `SparkPlanHandle(handle)` and pass
  * `cacheable = false` to [[ExecutionPlan.apply]]. The
  * MCP/cache/audit paths then go through `toSummary` (which only
  * sees the textual plan, not the handle). */
final class SparkPlanHandle(val handle: AnyRef) extends AnyRef {

  override def toString: String = s"SparkPlanHandle(<opaque>)"
}

object SparkPlanHandle {

  /** Wrap a non-Serializable engine-native plan (e.g. Spark
    * `QueryPlan`). */
  def apply(handle: AnyRef): SparkPlanHandle = new SparkPlanHandle(handle)
}

/** Engine-portable result-schema ADT — the shape the engine
  * promises to produce. Used by [[ExecutionPlan.normalizedSchema]]
  * and (in later PRs) by `PortableQueryResult`.
  *
  * Per design §4.5.4: "ResultSchema and ResultRow are case
  * classes because conformance tests compare them with ==".
  *
  * ==Why `List[Field]` (not `Map[String, String]`)==
  *
  * The previous `ResolvedSchema.fields: Map[String, String]`
  * (in `core.engine.ResolvedSource`) lost the typed `Field`
  * data — the `dataType: SealedDataType` and `nullable:
  * Boolean` were discarded by resolvers. The `List[Field]`
  * shape preserves order, type, and nullability end-to-end. */
final case class ResultSchema(
    fields: List[Field] = Nil,
) extends Product with Serializable {

  /** Number of fields in the schema. */
  def size: Int = fields.size

  /** Look up a field by name. Returns `None` if not found. */
  def field(name: String): Option[Field] = fields.find(_.name == name)

  /** True iff the schema has no fields. */
  def isEmpty: Boolean = fields.isEmpty
}