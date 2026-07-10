package io.semantica

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
) {

  def copy(
      name: String = this.name,
      expr: SemanticScope => Column = this.expr,
      description: Option[String] = this.description,
      metadata: Map[String, String] = this.metadata,
      isEntity: Boolean = this.isEntity,
      isTimeDimension: Boolean = this.isTimeDimension,
      isEventTimestamp: Boolean = this.isEventTimestamp,
      smallestTimeGrain: Option[String] = this.smallestTimeGrain,
  ): Dimension = new Dimension(name, expr, description, metadata, isEntity, isTimeDimension, isEventTimestamp, smallestTimeGrain)

  override def equals(that: Any): Boolean = that match {
    case d: Dimension =>
      name == d.name &&
      description == d.description &&
      metadata == d.metadata &&
      isEntity == d.isEntity &&
      isTimeDimension == d.isTimeDimension &&
      isEventTimestamp == d.isEventTimestamp &&
      smallestTimeGrain == d.smallestTimeGrain &&
      expr.toString == d.expr.toString  // functions have no value equality; compare source
    case _ => false
  }
  override def hashCode(): Int = (name, expr, description, metadata, isEntity, isTimeDimension, isEventTimestamp, smallestTimeGrain).##
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
)

/** Extension helpers so callers can write `describe(measure, "Total revenue")` and
  * `tag(measure, "finance", "USD")` without constructing Maps by hand. */
object MeasureExtra {
  def describe(m: Measure, description: String): Measure = m.copy(description = Some(description))
  def tag(m: Measure, kvs: (String, String)*): Measure = m.copy(metadata = m.metadata ++ kvs.toMap)
  def owner(m: Measure, o: String): Measure = tag(m, "owner" -> o)
  def unit(m: Measure, u: String): Measure = tag(m, "unit" -> u)
}
