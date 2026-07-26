package io.semanticdf

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
