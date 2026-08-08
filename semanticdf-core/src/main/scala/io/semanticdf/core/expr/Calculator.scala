package io.semanticdf.core.expr

/** Engine-portable expression helper — Phase 2 contract. Mirrors
  * the design doc §4.5.2 "Calculator" (the AST-walking helper).
  *
  * `Calculator` is a PURE-DATA helper: it provides static methods
  * that walk an `Expr` tree and extract the data the rest of the
  * portable model needs. It does NOT execute the expression
  * (that's engine-specific — Spark's `Expr.eval`, Trino's compile,
  * Databricks' Connect). Per the data-driven mantra, the data
  * (the expression) is in core; the behavior (evaluating it) is
  * in the engine adapter.
  *
  * ==Why a calculator that doesn't evaluate?==
  *
  * The `Calculator` is a STATIC ANALYSIS helper. The model validator
  * uses it to:
  *   - extract the set of fields referenced by a calculated
  *     measure's `expr` (for schema validation)
  *   - extract the set of measures referenced (for dependency
  *     graph construction)
  *   - detect cycles (a measure that references itself)
  *
  * These are the model's STATIC analysis, not its runtime behavior.
  * The engine adapter does the runtime analysis. Per the design's
  * risk #4: "predicates are typed" — the validator's analysis
  * runs once at model-load time, then the engine's compile runs
  * per query.
  *
  * ==Why core (engine-portable)==
  *
  * The static analysis is universal across query engines. Each
  * engine uses the same `Calculator.fieldNamesOf(expr)` result
  * to validate against its native schema. Per scala-data-driven-
  * refactor, the analysis logic is data analysis; the engine-
  * specific behavior (the runtime evaluation) is in the engine
  * adapter.
  *
  * ==Data-driven mantra compliance==
  *
  * `Calculator` is an `object` (singleton), not a class. Its methods
  * are pure functions of their inputs. No state, no engine coupling.
  *
  * ==Boundary contract==
  *
  * Zero Spark imports. Verifiable by:
  * `grep -r 'org.apache.spark' semanticdf-core/src/main/scala/io/semanticdf/core/expr/Calculator.scala`
  */
object Calculator {

  /** Extract the set of field names referenced by an expression.
    *
    * Used by the model validator to check that a calculated
    * measure's `expr` only references fields that exist in the
    * resolved schema. Returns a `Set[String]` (de-duplicated, no
    * order) of the field names.
    *
    * Walks the `Expr` tree recursively. Compound expressions
    * (binary ops, boolean ops) recurse into both operands. Function
    * calls recurse into their arguments. Literals and measure
    * references contribute nothing to the field set.
    *
    * Examples:
    *   - `FieldRef("amount")` -> `Set("amount")`
    *   - `Add(FieldRef("a"), FieldRef("b"))` -> `Set("a", "b")`
    *   - `Literal(42, BigInt)` -> `Set.empty`
    *   - `Multiply(FieldRef("price"), Literal(1.08, Double))` -> `Set("price")`
    *   - `FunctionCall("ABS", Seq(FieldRef("temperature")))` -> `Set("temperature")` */
  def fieldNamesOf(e: Expr): Set[String] = e match {
    case Expr.Literal(_, _)                   => Set.empty
    case Expr.FieldRef(name)                   => Set(name)
    case Expr.MeasureRef(name)                => Set.empty
    case Expr.All(name)                       => Set.empty  // All references a measure, not a field (PR #419 fix)
    case Expr.Add(left, right)                 => fieldNamesOf(left) ++ fieldNamesOf(right)
    case Expr.Subtract(left, right)            => fieldNamesOf(left) ++ fieldNamesOf(right)
    case Expr.Multiply(left, right)            => fieldNamesOf(left) ++ fieldNamesOf(right)
    case Expr.Divide(left, right)              => fieldNamesOf(left) ++ fieldNamesOf(right)
    case Expr.Modulo(left, right)              => fieldNamesOf(left) ++ fieldNamesOf(right)
    case Expr.Equal(left, right)               => fieldNamesOf(left) ++ fieldNamesOf(right)
    case Expr.NotEqual(left, right)            => fieldNamesOf(left) ++ fieldNamesOf(right)
    case Expr.LessThan(left, right)            => fieldNamesOf(left) ++ fieldNamesOf(right)
    case Expr.LessOrEqual(left, right)         => fieldNamesOf(left) ++ fieldNamesOf(right)
    case Expr.GreaterThan(left, right)         => fieldNamesOf(left) ++ fieldNamesOf(right)
    case Expr.GreaterOrEqual(left, right)      => fieldNamesOf(left) ++ fieldNamesOf(right)
    case Expr.And(left, right)                 => fieldNamesOf(left) ++ fieldNamesOf(right)
    case Expr.Or(left, right)                  => fieldNamesOf(left) ++ fieldNamesOf(right)
    case Expr.Not(expr)                        => fieldNamesOf(expr)
    case Expr.IsNull(expr)                     => fieldNamesOf(expr)
    case Expr.IsNotNull(expr)                  => fieldNamesOf(expr)
    case Expr.Cast(expr, _)                    => fieldNamesOf(expr)
    case Expr.FunctionCall(_, args)            => args.flatMap(fieldNamesOf).toSet
  }

  /** Extract the set of measure names referenced by an expression.
    *
    * Used by the model validator to construct the calculated
    * measure dependency graph. A calculated measure `total_revenue =
    * sum(revenue) / count(*)` references the base measure `revenue`.
    * The validator builds a graph (measure -> measures it depends on)
    * to detect cycles (a measure that transitively depends on itself).
    *
    * Returns a `Set[String]` (de-duplicated, no order) of the
    * measure names. Only `MeasureRef` nodes contribute.
    *
    * Examples:
    *   - `MeasureRef("revenue")` -> `Set("revenue")`
    *   - `Multiply(MeasureRef("a"), MeasureRef("b"))` -> `Set("a", "b")`
    *   - `FieldRef("amount")` -> `Set.empty` (field refs don't count) */
  def measureNamesOf(e: Expr): Set[String] = e match {
    case Expr.Literal(_, _)                   => Set.empty
    case Expr.FieldRef(_)                     => Set.empty
    case Expr.MeasureRef(name)                 => Set(name)
    case Expr.Add(left, right)                 => measureNamesOf(left) ++ measureNamesOf(right)
    case Expr.Subtract(left, right)            => measureNamesOf(left) ++ measureNamesOf(right)
    case Expr.Multiply(left, right)            => measureNamesOf(left) ++ measureNamesOf(right)
    case Expr.Divide(left, right)              => measureNamesOf(left) ++ measureNamesOf(right)
    case Expr.Modulo(left, right)              => measureNamesOf(left) ++ measureNamesOf(right)
    case Expr.Equal(left, right)               => measureNamesOf(left) ++ measureNamesOf(right)
    case Expr.NotEqual(left, right)            => measureNamesOf(left) ++ measureNamesOf(right)
    case Expr.LessThan(left, right)            => measureNamesOf(left) ++ measureNamesOf(right)
    case Expr.LessOrEqual(left, right)         => measureNamesOf(left) ++ measureNamesOf(right)
    case Expr.GreaterThan(left, right)         => measureNamesOf(left) ++ measureNamesOf(right)
    case Expr.GreaterOrEqual(left, right)      => measureNamesOf(left) ++ measureNamesOf(right)
    case Expr.And(left, right)                 => measureNamesOf(left) ++ measureNamesOf(right)
    case Expr.Or(left, right)                  => measureNamesOf(left) ++ measureNamesOf(right)
    case Expr.Not(expr)                        => measureNamesOf(expr)
    case Expr.IsNull(expr)                     => measureNamesOf(expr)
    case Expr.IsNotNull(expr)                  => measureNamesOf(expr)
    case Expr.Cast(expr, _)                    => measureNamesOf(expr)
    case Expr.FunctionCall(_, args)            => args.flatMap(measureNamesOf).toSet
    case Expr.All(name)                        => Set(name)
  }
}