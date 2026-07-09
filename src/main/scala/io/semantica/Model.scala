package io.semantica

import org.apache.spark.sql.Column

/** Semantic model records (DESIGN §5.1).
  *
  * Both `Dimension` and `Measure` carry an expression `SemanticScope => Column` plus
  * metadata. They are plain case classes — immutable, structural-equality, the values
  * users pass to `withDimensions` / `withMeasures`.
  */

/** A grouping field. `expr` is evaluated against a [[BaseScope]] over the source table. */
final case class Dimension(
    name: String,
    expr: SemanticScope => Column,
    description: Option[String] = None,
    isEntity: Boolean = false,
    isTimeDimension: Boolean = false,
    isEventTimestamp: Boolean = false,
    smallestTimeGrain: Option[String] = None,
)

/** Companion with ergonomic factories (Phase 6).
  *
  * - [[Dimension.apply]] is the general-purpose constructor.
  * - [[Dimension.time]] marks a timestamp dimension and is eligible for `atTimeGrain` /
  *   `query(timeGrain=...)`. `smallestTimeGrain` (short or `TIME_GRAIN_` form) gates how
  *   fine a grain a query may request.
  * - [[Dimension.entity]] marks a join-key identifier dimension (cosmetic for v0.1).
  */
object Dimension {

  /** General-purpose dimension constructor (preferred over the 7-arg case-class apply). */
  def apply(
      name: String,
      expr: SemanticScope => Column,
  ): Dimension = new Dimension(name, expr)

  /** A time/timestamp dimension. Eligible for `atTimeGrain` and `query(timeGrain=...)`.
    *
    * @param smallestTimeGrain optional floor on requested grains, e.g. `"day"` or
    *                          `"TIME_GRAIN_DAY"`. A request finer than this raises.
    * @param isEventTimestamp mark this as the canonical event-timestamp for the table
    *                         (cosmetic for v0.1; point-in-time join semantics deferred). */
  def time(
      name: String,
      expr: SemanticScope => Column,
      smallestTimeGrain: Option[String] = None,
      isEventTimestamp: Boolean = false,
      description: Option[String] = None,
  ): Dimension = new Dimension(
    name, expr, description,
    isEntity = false,
    isTimeDimension = true,
    isEventTimestamp = isEventTimestamp,
    smallestTimeGrain = smallestTimeGrain,
  )

  /** An entity dimension (join-key identifier). Cosmetic for v0.1. */
  def entity(
      name: String,
      expr: SemanticScope => Column,
      description: Option[String] = None,
  ): Dimension = new Dimension(name, expr, description, isEntity = true)
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
