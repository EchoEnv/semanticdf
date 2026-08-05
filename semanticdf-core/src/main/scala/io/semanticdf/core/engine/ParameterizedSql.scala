package io.semanticdf.core.engine

import io.semanticdf.core.expr.LiteralValue

/** Engine-portable parameterized SQL — Phase 2 follow-up to PR #368.
  *
  * `ParameterizedSql` is the engine-portable shape for a SQL query
  * with bound parameters. The `sql` field is the SQL string with
  * positional `?` placeholders; the `parameters` field is the
  * ordered list of values to bind to those placeholders.
  *
  * ==Why a sealed shape (vs. a free-form String)==
  *
  * SQL-injection attacks are prevented when values are bound via
  * the engine's `PreparedStatement`-style mechanism rather than
  * string concatenation. The contract: the engine adapter MUST
  * bind `parameters` to the placeholders in `sql` rather than
  * inlining them.
  *
  * A `String`-only `ExecutionPlan.native` (the pre-PR #371 shape)
  * forced the compiler to inline values. This new shape lets the
  * compiler emit `?` placeholders while preserving the value
  * list for binding.
  *
  * ==Why `parameters: List[LiteralValue]`==
  *
  * `LiteralValue` is the engine-portable value type (16 cases per
  * PR #359: 7 numeric + 1 text + 1 boolean + 1 binary + 2 temporal
  * + 3 nested + 1 special NullValue). Each engine maps `LiteralValue`
  * to its native parameter type (Trino: `setString`, `setLong`,
  * etc.; Spark: Catalyst `Literal`; Databricks: Connect values).
  *
  * The List is ordered: `parameters.head` binds to the first `?`,
  * `parameters(1)` to the second, etc.
  *
  * ==Why core (engine-portable)==
  *
  * The shape is universal — every SQL engine that supports
  * `PreparedStatement`-style binding has a notion of
  * "SQL + ordered parameters". The engine-specific bind
  * (Trino's `Connection.prepareStatement(sql).setXxx(...)`,
  * Spark's `df.selectExpr(...)` with column bindings,
  * Databricks' Connect parameter binding) lives in the engine
  * adapter.
  *
  * Per scala-data-driven-refactor §1 ("data is data, behavior
  * lives elsewhere"): the data (the parameterized SQL + values)
  * lives in core; the behavior (the engine-specific bind) lives
  * in the engine adapter.
  *
  * ==Why this is in `core.engine` (not `core.rel`)==
  *
  * The shape is the OUTPUT of the engine's `compile` step — it
  * lives in the engine boundary package. `core.rel` is for
  * RELATIONAL PLAN NODES (the portable IR); `core.engine` is
  * for engine-adapter boundary types.
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
  * `grep -r 'org.apache.spark' semanticdf-core/src/main/scala/io/semanticdf/core/engine/ParameterizedSql.scala`
  */
final case class ParameterizedSql(
    sql:        String,
    parameters: List[LiteralValue],
) extends Product with Serializable {

  /** The number of parameters. Useful for validation: the SQL
    * should have exactly `parameters.size` `?` placeholders. */
  def parameterCount: Int = parameters.size
}