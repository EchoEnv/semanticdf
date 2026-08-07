package io.semanticdf.core.model

import io.semanticdf.core.expr.Expr
import io.semanticdf.core.rel.{AggregateCall, AggregateFn}

/** Engine-portable aggregate-measure ADT — Phase 2 contract. Mirrors
  * the design doc §4.4.1 "Measure" (concrete case class per the
  * v0.3.0 design finding that pinned `Measure.aggregate(name, fn, expr)` as
  * the canonical smart constructor).
  *
  * A [[Measure]] is a column-level aggregate (e.g. `SUM(amount)`,
  * `COUNT(*)`). The measure carries the name and the
  * engine-portable aggregate call (`AggregateCall`) that produces
  * the value.
  *
  * ==Why a separate type from `CalculatedMeasure`==
  *
  * `Measure` has an `expr: AggregateCall` — the expression is a
  * single aggregate call. `CalculatedMeasure` has an `expr: Expr` —
  * the expression is ANY engine-portable expression (often combining
  * other measures). The two are semantically different shapes.
  *
  * ==Why a separate type from the existing `io.semanticdf.Measure`==
  *
  * The spark-coupled `io.semanticdf.Measure` carries a `SemanticScope
  * => Column` closure (Spark `Column` is engine-specific). The
  * portable `Measure` carries an `AggregateCall` (16 aggregate
  * functions from PR #360, plus optional input + alias + distinct +
  * arguments). The two coexist intentionally.
  *
  * Per karpathy §3 (surgical, no opportunistic refactors): the
  * existing `io.semanticdf.Measure` is untouched.
  *
  * ==Why a smart constructor `Measure.aggregate(name, fn, expr)`==
  *
  * Per scala-data-driven-refactor §2 ("shape/validity separate"):
  * the smart constructor builds the common case (`AggregateCall(fn,
  * Some(expr), name)`) so callers don't have to repeat the boilerplate.
  * The structural constructor `Measure(name, expr: AggregateCall)`
  * is for the less-common case (e.g. `Count(*)`, where `input` is
  * `None` and the alias is just `"*"`).
  *
  * ==Why core (engine-portable)==
  *
  * Aggregate measures are universal across query engines. The
  * engine-specific compile (Spark's `Column = functions.sum(...)`,
  * Trino's SQL `SUM(x) AS total`, etc.) lives in the engine adapter.
  *
  * ==Data-driven mantra compliance==
  *
  * - Pure data: case class (no behavior)
  * - Equality auto-derived
  * - `Product with Serializable`
  *
  * ==Boundary contract==
  *
  * Zero Spark imports. Verifiable by:
  * `grep -r 'org.apache.spark' semanticdf-core/src/main/scala/io/semanticdf/core/model/Measure.scala`
  */
final case class Measure(
    name: String,
    expr: AggregateCall,
) extends Product with Serializable

object Measure {

  /** Construct a single-aggregate measure (the common case).
    *
    * `Measure.aggregate("total", AggregateFn.Sum, Expr.FieldRef("amount"))`
    * is equivalent to
    * `Measure(name = "total", expr = AggregateCall(fn = Sum, input =
    * Some(FieldRef("amount")), alias = "total"))`.
    *
    * Used when the user wants to declare `SUM(amount) AS total` (the
    * common case). For more complex measures (e.g. `Count(DISTINCT x)`,
    * `ApproxPercentile(x, 0.95)`), use the structural constructor
    * `Measure(name, AggregateCall(...))`. */
  def aggregate(name: String, fn: AggregateFn, expr: Expr): Measure =
    Measure(name, AggregateCall(fn, Some(expr), name))
}