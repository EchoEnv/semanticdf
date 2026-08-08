package io.semanticdf.spark

import io.semanticdf.core.expr.{Expr, LiteralValue}
import io.semanticdf.core.schema.SealedDataType

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, lit, not => sparkNot}

/** Engine-specific Spark compiler for portable [[Expr]] \u2192 Spark
  * [[Column]]. Per scala-data-driven-refacer \u00a71: behavior in
  * adapters; the portable `Expr` is pure data in `core`.
  *
  * ==v0.3.1 scope (Gap 1 partial closure)==
  *
  * Supports all 18 [[Expr]] cases EXCEPT:
  * - `MeasureRef` \u2014 requires subquery resolution (deferred to v0.4.0
  *   when `t.all` / pct-of-total lands per Gap 2)
  *
  * ==JVM-safety check 1 (Null is a liar)==
  *
  * Spark `Column` operations are null-safe; no `null` checks needed
  * in this layer. The bound values use `lit(null)` for `NullValue`.
  *
  * ==Why a companion object (vs. trait / typeclass)==
  *
  * Pure function: `(Expr) \u2192 Column`. No state, no IO. Per
  * scala-data-driven-refacer \u00a73: a sealed-trait dispatch over
  * match is the cheapest correct shape.
  */
object PortableExprCompiler {

  /** Convert a portable [[Expr]] to a Spark [[Column]]. Throws
    * `UnsupportedOperationException` for `MeasureRef` (deferred). */
  def toColumn(expr: Expr): Column = expr match {
    case Expr.Literal(value, _)    => literalToColumn(value)
    case Expr.FieldRef(name)       => col(name)

    case Expr.Add(l, r)            => toColumn(l) + toColumn(r)
    case Expr.Subtract(l, r)       => toColumn(l) - toColumn(r)
    case Expr.Multiply(l, r)       => toColumn(l) * toColumn(r)
    case Expr.Divide(l, r)         => toColumn(l) / toColumn(r)
    case Expr.Modulo(l, r)         => toColumn(l) % toColumn(r)

    case Expr.Equal(l, r)          => toColumn(l) === toColumn(r)
    case Expr.NotEqual(l, r)       => toColumn(l) =!= toColumn(r)
    case Expr.LessThan(l, r)       => toColumn(l) <  toColumn(r)
    case Expr.LessOrEqual(l, r)    => toColumn(l) <= toColumn(r)
    case Expr.GreaterThan(l, r)    => toColumn(l) >  toColumn(r)
    case Expr.GreaterOrEqual(l, r) => toColumn(l) >= toColumn(r)

    case Expr.And(l, r)            => toColumn(l) && toColumn(r)
    case Expr.Or(l, r)             => toColumn(l) || toColumn(r)
    case Expr.Not(e)               => !toColumn(e)
    case Expr.IsNull(e)            => toColumn(e).isNull
    case Expr.IsNotNull(e)         => toColumn(e).isNotNull

    case Expr.MeasureRef(_) =>
      // Deferred to v0.4.0: requires subquery resolution.
      // MeasureRef("foo") means "the value of measure foo at the
      // aggregation level", which needs a correlated subquery in
      // SQL terms. The portable `Expr` will gain a MeasureRef-resolved
      // path when Gap 2 (Expr.All / pct-of-total) lands.
      throw new UnsupportedOperationException(
        "PortableExprCompiler.toColumn: Expr.MeasureRef is not supported in v0.3.1 (deferred to v0.4.0 with Expr.All / pct-of-total).",
      )

    case Expr.All(name) =>
      // Lowered to a simple column reference. By the time
      // calculated measures are evaluated, `applyAggregations`
      // has already added the referenced measure as a window-
      // aggregated column (when the model uses Expr.All). The
      // column-name resolution is therefore straightforward.
      col(name)

    case Expr.FunctionCall(name, args) =>
      // Engine-agnostic UDF resolution is out of scope for v0.3.1.
      // Future: route through a Spark UDF registry keyed by name.
      throw new UnsupportedOperationException(
        s"PortableExprCompiler.toColumn: Expr.FunctionCall('$name', ...) is not supported in v0.3.1 (UDF resolution deferred to v0.4.0).",
      )
  }

  /** Map a portable [[LiteralValue]] to a Spark `lit(...)` column.
    * Per JVM-safety check 1: handle null at the boundary
    * (`LiteralValue.NullValue` \u2192 `lit(null)`). */
  private def literalToColumn(value: LiteralValue): Column = value match {
    case LiteralValue.NullValue              => lit(null)
    case LiteralValue.BoolValue(b)            => lit(b)
    case LiteralValue.IntValue(n)             => lit(n)
    case LiteralValue.LongValue(n)            => lit(n)
    case LiteralValue.FloatValue(f)           => lit(f)
    case LiteralValue.DoubleValue(d)          => lit(d)
    case LiteralValue.DecimalValue(d)         => lit(d)
    case LiteralValue.StringValue(s)          => lit(s)
    case LiteralValue.TimestampValue(instant) => lit(instant)
    case LiteralValue.DateValue(date)         => lit(date)
  }
}
