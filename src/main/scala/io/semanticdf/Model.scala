package io.semanticdf

import org.apache.spark.sql.Column

/** Semantic model records (DESIGN §5.1).
  *
  * Both `Dimension` and `Measure` carry an expression `SemanticScope => Column` plus
  * metadata. They are plain case classes — immutable, structural-equality, the values
  * users pass to `withDimensions` / `withMeasures`.
  */

/** A grouping field. `expr` is evaluated against a [[BaseScope]] over the source table.
  *
  * @param metadata arbitrary key-value pairs for tracking ownership, tags, unit, source
  *                system, sensitivity, etc. e.g. `Map("owner" -> "analytics-team",
  *                "unit" -> "USD", "pii" -> "true")`. Has zero runtime cost — Spark
  *                never sees these values; they flow through to YAML and downstream
  *                catalog tooling only.
  *
  * @param smallestTimeGrain when [[isTimeDimension]] is true, the finest time grain
  *                          this dimension supports. A query requesting a finer grain raises.
  */
final class Dimension(
    val name: String,
    val expr: SemanticScope => Column,
    val description: Option[String] = None,
    val metadata: Map[String, String] = Map.empty,
    val isEntity: Boolean = false,
    val isTimeDimension: Boolean = false,
    val isEventTimestamp: Boolean = false,
    val smallestTimeGrain: Option[String] = None,
    /** Original expression string (e.g. the YAML `expr:` value or the
      * programmatic hint). Carried so consumers like `DescribeModel` can
      * surface a human-readable expression instead of the lambda's
      * opaque `toString`. `None` for dimensions built from a bare lambda
      * with no hint; the DescribeModel fallback then shows the lambda. */
    val exprString: Option[String] = None,
) extends Serializable {

  def copy(
      name: String = this.name,
      expr: SemanticScope => Column = this.expr,
      description: Option[String] = this.description,
      metadata: Map[String, String] = this.metadata,
      isEntity: Boolean = this.isEntity,
      isTimeDimension: Boolean = this.isTimeDimension,
      isEventTimestamp: Boolean = this.isEventTimestamp,
      smallestTimeGrain: Option[String] = this.smallestTimeGrain,
      exprString: Option[String] = this.exprString,
  ): Dimension = new Dimension(name, expr, description, metadata, isEntity, isTimeDimension, isEventTimestamp, smallestTimeGrain, exprString)

  override def equals(that: Any): Boolean = that match {
    case d: Dimension =>
      name == d.name &&
      description == d.description &&
      metadata == d.metadata &&
      isEntity == d.isEntity &&
      isTimeDimension == d.isTimeDimension &&
      isEventTimestamp == d.isEventTimestamp &&
      smallestTimeGrain == d.smallestTimeGrain &&
      exprString == d.exprString &&
      expr.toString == d.expr.toString  // functions have no value equality; compare source
    case _ => false
  }
  override def hashCode(): Int = (name, expr, exprString, description, metadata, isEntity, isTimeDimension, isEventTimestamp, smallestTimeGrain).##
  override def toString: String = s"Dimension($name,${if (description.isDefined) description.get else "_"})$$"
}

/** Companion with ergonomic factories. */
object Dimension {

  def apply(
      name: String,
      expr: SemanticScope => Column,
      description: Option[String] = None,
      metadata: Map[String, String] = Map.empty,
  ): Dimension = new Dimension(name, expr, description, metadata)

  def time(
      name: String,
      expr: SemanticScope => Column,
      smallestTimeGrain: Option[String] = None,
      isEventTimestamp: Boolean = false,
      description: Option[String] = None,
      metadata: Map[String, String] = Map.empty,
  ): Dimension = new Dimension(
    name, expr, description, metadata,
    isEntity = false,
    isTimeDimension = true,
    isEventTimestamp = isEventTimestamp,
    smallestTimeGrain = smallestTimeGrain,
  )

  def entity(
      name: String,
      expr: SemanticScope => Column,
      description: Option[String] = None,
      metadata: Map[String, String] = Map.empty,
  ): Dimension = new Dimension(name, expr, description, metadata, isEntity = true)
}

/** A measure. `expr` is either a *base* aggregate (references base columns, runs in the
  * aggregation) or a *calc* (references other measures by name, runs against the
  * aggregated result). Base vs calc is classified at compile time, not stored here —
  * faithful to BSL's auto-classification (users do not mark measures).
  */
final case class Measure(
    name: String,
    expr: SemanticScope => Column,
    description: Option[String] = None,
    metadata: Map[String, String] = Map.empty,
    /** Original expression string (e.g. the YAML `expr:` value or the
      * programmatic hint). See [[Dimension.exprString]] for the full
      * rationale. `None` for measures built from a bare lambda with
      * no hint; the DescribeModel fallback then shows the lambda. */
    exprString: Option[String] = None,
)

/** A per-row transformation applied to the source data at model-load time.
  *
  * `expr` is evaluated against the [[BaseScope]] of the source DataFrame, with
  * the resulting Column becoming a new column on the source. The output column
  * is referenced by name in subsequent transforms, dimensions, and measures.
  *
  * Transforms correspond to dbt's staging models / LookML's `derived_table` —
  * per-row logic that doesn't fit the `agg()` aggregate context lives here, not
  * in the measure definition. Examples: `datediff(a, b)`, `case when ...`,
  * `row_number() over (...)`.
  *
  * Evaluation order: topologically sorted by column references. If transform B
  * references the column added by transform A, A runs first. The YAML loader
  * handles the sort; the Scala DSL's `withTransforms(...)` accepts transforms
  * in any order and sorts them on apply.
  */
final case class Transform(
    name: String,
    expr: SemanticScope => Column,
    description: Option[String] = None,
)

/** Extension helpers so callers can write `describe(measure, "Total revenue")` and
  * `tag(measure, "finance", "USD")` without constructing Maps by hand. */
object MeasureExtra {
  def describe(m: Measure, description: String): Measure = m.copy(description = Some(description))
  def tag(m: Measure, kvs: (String, String)*): Measure = m.copy(metadata = m.metadata ++ kvs.toMap)
  def owner(m: Measure, o: String): Measure = tag(m, "owner" -> o)
  def unit(m: Measure, u: String): Measure = tag(m, "unit" -> u)
}
