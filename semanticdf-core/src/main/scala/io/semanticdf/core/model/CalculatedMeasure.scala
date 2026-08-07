package io.semanticdf.core.model

import io.semanticdf.core.expr.Expr

/** Engine-portable calculated-measure ADT — Phase 2 contract.
  * Mirrors the design doc §4.4.1 "CalculatedMeasure" (concrete,
  * no `TransformSpec` member per design §1.1).
  *
  * A [[CalculatedMeasure]] is a measure whose value is COMPUTED from
  * other measures (or fields). It carries the name + the
  * engine-portable expression that produces the value.
  *
  * ==Why a separate type from `Measure`==
  *
  * `Measure` (the Phase 2 mirror of the spark-coupled aggregate
  * measure) has an `expr: AggregateCall` — the expression is an
  * aggregate call (`SUM(amount)`, `COUNT(*)`, etc.). Calculated
  * measures have an `expr: Expr` — the expression is ANY
  * engine-portable expression (`a + b`, `field_a / field_b`, etc.).
  * The two are semantically different shapes.
  *
  * ==Why `expr: Expr` (not `String`)==
  *
  * A `String` would let callers pass `"a + b"` / `"a/b"` / typos —
  * silent failures at engine-compile time. The `Expr` ADT (PR #359)
  * forces the model validator to check that the expression is well-
  * formed (the field names exist, the operators are valid, etc.).
  *
  * ==Why core (engine-portable)==
  *
  * Calculated measures are universal across query engines. The
  * engine-specific compile (Spark's `Column = expr.fold(...)`,
  * Trino's SQL `SELECT (a + b) AS calc`, etc.) lives in the engine
  * adapter.
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
  * `grep -r 'org.apache.spark' semanticdf-core/src/main/scala/io/semanticdf/core/model/CalculatedMeasure.scala`
  */
final case class CalculatedMeasure(
    name: String,
    expr: Expr,
) extends Product with Serializable