package io.semanticdf.core.schema

/** Engine-portable schema summary — Phase 2 contract.
  * Mirrors the design doc §4.4.2 "Schema".
  *
  * ==What this is==
  *
  * The `SchemaSummary` is the engine-portable projection of a
  * [[io.semanticdf.core.model.Model]]'s field metadata. Every
  * engine adapter that implements the engine-portable contract
  * can produce this shape from a `Model`.
  *
  * ==Why this mirrors (but is NOT identical to) Spark's `df.schema`==
  *
  * The Spark adapter's `df.schema` (and `SemanticTableCore.schema`)
  * returns a `DataFrame` with 12 columns describing each field —
  * `model_name`, `model_description`, `field_name`, `field_type`,
  * `description`, `metadata_keys`, `metadata_values`, `is_entity`,
  * `is_time_dimension`, `smallest_grain`, `join_source`,
  * `join_cardinality`.
  *
  * Several of those columns (`is_entity`, `is_time_dimension`,
  * `smallest_grain`) are derived from the Spark op tree, which
  * is Spark-specific. Trino (and other engines) don't have an op
  * tree — they compile directly from the engine-portable `Model`.
  *
  * Per scala-data-driven-refacer §1 ("data is data, behavior
  * lives elsewhere"): the portable schema summary stays
  * engine-portable (only fields that are derivable from the
  * `Model` itself). Engine-specific enrichments live in the
  * engine adapter, not in core.
  *
  * ==Why `fieldKind` is a sealed trait (not a String)==
  *
  * Per scala-data-driven-refacer §1 ("typed, not stringly-typed"):
  * `fieldKind` is a sealed trait with four cases. Consumers
  * pattern-match on it; adding a new case is a compile-time
  * error at every consumer (vs. silently passing a misspelled
  * String).
  *
  * ==Why a case class for `SchemaField` (not a tuple)==
  *
  * Per scala-data-driven-refacer §3 ("case class for structured
  * data, not tuples"): `SchemaField` carries named fields that
  * are self-documenting at call sites (`SchemaField(name =
  * "region", fieldKind = SchemaFieldKind.Dimension, dataType =
  * Some(SealedDataType.Varchar))` reads on its own).
  *
  * ==Boundary contract==
  *
  * Zero Spark imports. Verifiable by:
  * `grep -r 'org.apache.spark' semanticdf-core/src/main/scala/io/semanticdf/core/schema/SchemaSummary.scala`
  *
  * ==Data-driven mantra compliance==
  *
  * - Pure data: case classes with `val` fields (no behavior except
  *   accessors and the two small derivation helpers on
  *   `SchemaSummary`)
  * - `extends Product with Serializable` for case-class-equivalent
  *   semantics (Product = auto-generated `equals`/`hashCode`/
  *   `toString`; Serializable = Java-serialization round-trip)
  * - No `Any` fields, no closures, no engine refs
  * - Two small derivations (`rowCount`, `isEmpty`) live on
  *   `SchemaSummary` itself because they're 1-line data projections
  *   from the field list — per scala-data-driven-refacer §1,
  *   "data accessor" methods belong on the data class. */

/** What kind of field a [[SchemaField]] represents.
  *
  * Closed sealed trait — adding a new case is a deliberate,
  * compile-time-visible API change. */
sealed trait SchemaFieldKind
object SchemaFieldKind {

  /** A row-level grouping dimension. Maps to Spark's
    * `SemanticTable.dimensions` and Trino's projected columns
    * (no aggregation). */
  case object Dimension extends SchemaFieldKind

  /** An aggregated measure (e.g. SUM, COUNT). Maps to Spark's
    * `SemanticTable.measures` and Trino's aggregated columns. */
  case object Measure extends SchemaFieldKind

  /** A measure computed from other measures/dimensions via a
    * calculator expression. Maps to Spark's
    * `SemanticTable.calculatedMeasures` and Trino's projected
    * post-aggregation columns. */
  case object CalculatedMeasure extends SchemaFieldKind

  /** A key field exposed by a join — the join's left/right
    * relationship. Maps to Spark's `SemanticTable.joins` and
    * Trino's JOIN ON clauses. */
  case object JoinKey extends SchemaFieldKind
}

/** One field in the [[SchemaSummary]]. */
final case class SchemaField(
    fieldName:   String,
    fieldKind:   SchemaFieldKind,
    description: Option[String]               = None,
    dataType:    Option[SealedDataType]       = None,
) extends Product with Serializable

/** Schema summary for a [[io.semanticdf.core.model.Model]] —
  * the engine-portable projection of the model's field metadata.
  *
  * ==Usage==
  *
  * An engine adapter produces this shape from a model:
  * {{{
  * val summary: Either[EngineError, SchemaSummary] =
  *   trinoEngine.schema(model, ctx)
  * summary match {
  *   case Right(s) => s.fields.foreach(f => println(s"${f.fieldKind}: ${f.fieldName}"))
  *   case Left(err) => println(s"schema error: $err")
  * }
  * }}}
  *
  * ==Mirrors Spark's `df.schema`==
  *
  * Spark's `df.schema` returns a 12-column `DataFrame` describing
  * each field (model name, model description, field name, field
  * type, description, metadata, entity flag, time-dimension flag,
  * smallest grain, join source, join cardinality). This shape is
  * the engine-portable subset — fields that derive purely from the
  * `Model` are here; Spark-op-tree-specific fields (`is_entity`,
  * `is_time_dimension`, `smallest_grain`) live in the Spark
  * adapter's own schema response. */
final case class SchemaSummary(
    modelName:        String,
    modelDescription: Option[String],
    fields:           List[SchemaField],
) extends Product with Serializable {

  /** Number of fields in this summary. Mirrors `df.schema`'s row
    * count for consumers that want a single integer. */
  def rowCount: Int = fields.size

  /** True iff no fields are present (typically: a model with no
    * dimensions, no measures, no calculated measures, and no
    * joins). */
  def isEmpty: Boolean = fields.isEmpty

  /** Filter to fields of a specific kind. Useful for MCP
    * `describe_model` (e.g. "list only dimensions"). */
  def ofKind(kind: SchemaFieldKind): List[SchemaField] =
    fields.filter(_.fieldKind == kind)
}