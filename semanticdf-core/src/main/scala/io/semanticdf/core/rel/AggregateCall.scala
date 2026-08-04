package io.semanticdf.core.rel

import io.semanticdf.core.expr.{Expr, LiteralValue}

/** Engine-portable aggregate-call ADT — Phase 2 contract. Mirrors
  * the design doc §4.5.2 "AggregateCall" (wraps an `AggregateFn`
  * with its input expression, alias, distinct flag, and literal
  * arguments).
  *
  * ==Why a case class wrapping `AggregateFn`==
  *
  * `AggregateFn` (the function) is one piece of the call; an
  * actual aggregate CALL needs:
  *   - the function (`fn`)
  *   - the input expression (`input: Option[Expr]` — `None` for
  *     `Count(*)` style)
  *   - the alias (`alias: String` — the column name in the result)
  *   - the distinct flag (`distinct: Boolean` — for `Count(DISTINCT x)`)
  *   - the literal arguments (`arguments: List[LiteralValue]` —
  *     e.g. percentile = 0.95 for `ApproxPercentile`)
  *
  * A `RelOp.Aggregate` carries `aggregates: List[AggregateCall]`,
  * one per aggregate in the result.
  *
  * ==Why `input: Option[Expr]`==
  *
  * `Count(*)` has no input expression — it's the count of rows,
  * not the count of a column. `Sum(x)` has `input = Some(FieldRef("x"))`.
  * The `Option` makes the no-input case explicit.
  *
  * ==Why `distinct: Boolean`==
  *
  * `Count(DISTINCT x)` is distinct from `Count(x)`. The `distinct`
    * flag encodes this at the ADT level. Engines that don't support
    * DISTINCT for the function (e.g. `Min(DISTINCT x)` is just
    * `Min(x)`) ignore the flag.
    *
    * ==Why `arguments: List[LiteralValue]` (not `Map[String, LiteralValue]`)==
    *
    * Per scala-data-driven-refactor §3 ("A rule becomes data only
    * when it must change without a deploy"): the argument shape is
    * FIXED at compile time — a percentile is one literal, a custom
    * function takes its own shape. A `Map` would be a downgrade
    * (silent defaulting on typo'd keys). The engine adapter
    * pattern-matches on the `fn` to determine which arguments it
    * expects.
    *
    * ==Why core (engine-portable)==
    *
    * Aggregate calls are universal across query engines. Every
    * SQL engine supports the same call shape: function + input +
    * alias. The engine-specific compile (Spark's
    * `Column = functions.agg(...)`, Trino's SQL aggregation, etc.)
    * lives in the engine adapter.
    *
    * ==Data-driven mantra compliance==
    *
    * - Pure data: case class (no behavior)
    * - Equality auto-derived (case class)
    * - Hash code stable (auto-derived)
    * - `Product with Serializable`
    *
    * ==Boundary contract==
    *
    * Zero Spark imports. Verifiable by:
    * `grep -r 'org.apache.spark' semanticdf-core/src/main/scala/io/semanticdf/core/rel/AggregateCall.scala`
    */
final case class AggregateCall(
    fn:        AggregateFn,
    input:     Option[Expr]          = None,
    alias:     String                = "",
    distinct:  Boolean               = false,
    arguments: List[LiteralValue]    = Nil,
) extends Product with Serializable