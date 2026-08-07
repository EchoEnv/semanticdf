package io.semanticdf.predicate

import io.semanticdf.core.expr.{Expr, LiteralValue}
import io.semanticdf.core.schema.SealedDataType

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Tests for [[PredicateToExprConverter]] \u2014 the boundary adapter
  * that converts legacy `io.semanticdf.predicate.Predicate` (Spark-
  * bearing) to portable `io.semanticdf.core.expr.Expr`.
  *
  * Closes Gap 4 from `docs/design/v0.3.1-feature-parity-backlog.md`.
  *
  * ==Per scala-spark-batch-bugs \u00a73 (schema drift)==
  *
  * The output `Expr` shape must use the portable ADT's binary form
  * (`Expr.Equal(FieldRef, Literal)`, not a stringly-typed compare).
  * These tests pin the literal field-reference + literal value
  * structure so downstream engines can rely on it. */
class PredicateToExprConverterSpec extends AnyFunSuite with Matchers {

  // -- Compare family (supported) --

  test("Eq(field, value) \u2192 Expr.Equal(FieldRef(field), Literal(value, dt))") {
    val p: Predicate = Predicate.Compare.Eq("region", "AA")
    PredicateToExprConverter.toExpr(p) shouldBe Expr.Equal(
      Expr.FieldRef("region"),
      Expr.Literal(LiteralValue.StringValue("AA"), SealedDataType.Varchar),
    )
  }

  test("Ne(field, value) \u2192 Expr.NotEqual(...)") {
    PredicateToExprConverter.toExpr(Predicate.Compare.Ne("region", "us")) shouldBe Expr.NotEqual(
      Expr.FieldRef("region"),
      Expr.Literal(LiteralValue.StringValue("us"), SealedDataType.Varchar),
    )
  }

  test("Lt(field, value) \u2192 Expr.LessThan(...)") {
    PredicateToExprConverter.toExpr(Predicate.Compare.Lt("amount", 100)) shouldBe Expr.LessThan(
      Expr.FieldRef("amount"),
      Expr.Literal(LiteralValue.IntValue(100), SealedDataType.Int),
    )
  }

  test("Le \u2192 Expr.LessOrEqual, Gt \u2192 Expr.GreaterThan, Ge \u2192 Expr.GreaterOrEqual") {
    PredicateToExprConverter.toExpr(Predicate.Compare.Le("x", 1)) shouldBe Expr.LessOrEqual(
      Expr.FieldRef("x"), Expr.Literal(LiteralValue.IntValue(1), SealedDataType.Int))
    PredicateToExprConverter.toExpr(Predicate.Compare.Gt("x", 1)) shouldBe Expr.GreaterThan(
      Expr.FieldRef("x"), Expr.Literal(LiteralValue.IntValue(1), SealedDataType.Int))
    PredicateToExprConverter.toExpr(Predicate.Compare.Ge("x", 1)) shouldBe Expr.GreaterOrEqual(
      Expr.FieldRef("x"), Expr.Literal(LiteralValue.IntValue(1), SealedDataType.Int))
  }

  // -- Literal type inference --

  test("Long literal maps to LiteralValue.LongValue + SealedDataType.Int") {
    val expr = PredicateToExprConverter.toExpr(Predicate.Compare.Eq("n", 100L))
    expr shouldBe Expr.Equal(
      Expr.FieldRef("n"),
      Expr.Literal(LiteralValue.LongValue(100L), SealedDataType.Int),
    )
  }

  test("Double literal maps to LiteralValue.DoubleValue + SealedDataType.Double") {
    val expr = PredicateToExprConverter.toExpr(Predicate.Compare.Gt("ratio", 0.5))
    expr shouldBe Expr.GreaterThan(
      Expr.FieldRef("ratio"),
      Expr.Literal(LiteralValue.DoubleValue(0.5), SealedDataType.Double),
    )
  }

  test("Boolean literal maps to LiteralValue.BoolValue + SealedDataType.Boolean") {
    val expr = PredicateToExprConverter.toExpr(Predicate.Compare.Eq("active", true))
    expr shouldBe Expr.Equal(
      Expr.FieldRef("active"),
      Expr.Literal(LiteralValue.BoolValue(true), SealedDataType.Boolean),
    )
  }

  test("null literal maps to LiteralValue.NullValue") {
    val expr = PredicateToExprConverter.toExpr(Predicate.Compare.Eq("x", null))
    expr shouldBe Expr.Equal(
      Expr.FieldRef("x"),
      Expr.Literal(LiteralValue.NullValue, SealedDataType.Varchar),
    )
  }

  // -- In / IsNull --

  test("In(field, [a, b]) \u2192 Or(Eq(a), Eq(b))") {
    val p = Predicate.In("region", Seq("us", "eu"))
    PredicateToExprConverter.toExpr(p) shouldBe Expr.Or(
      Expr.Equal(Expr.FieldRef("region"), Expr.Literal(LiteralValue.StringValue("us"), SealedDataType.Varchar)),
      Expr.Equal(Expr.FieldRef("region"), Expr.Literal(LiteralValue.StringValue("eu"), SealedDataType.Varchar)),
    )
  }

  test("In(field, [a], negate=true) \u2192 Not(Eq(a))") {
    val p = Predicate.In("region", Seq("us"), negate = true)
    PredicateToExprConverter.toExpr(p) shouldBe Expr.Not(
      Expr.Equal(Expr.FieldRef("region"), Expr.Literal(LiteralValue.StringValue("us"), SealedDataType.Varchar)),
    )
  }

  test("IsNull(field) \u2192 Expr.IsNull(FieldRef(field))") {
    PredicateToExprConverter.toExpr(Predicate.IsNull("deleted_at")) shouldBe Expr.IsNull(
      Expr.FieldRef("deleted_at"),
    )
  }

  test("IsNull(field, negate=true) \u2192 Expr.Not(IsNull(...))") {
    val expr = PredicateToExprConverter.toExpr(Predicate.IsNull("deleted_at", negate = true))
    expr shouldBe Expr.Not(Expr.IsNull(Expr.FieldRef("deleted_at")))
  }

  // -- Compound --

  test("And(Eq, Gt) \u2192 Expr.And(Eq, Gt)") {
    val p = Predicate.And(
      Predicate.Compare.Eq("region", "us"),
      Predicate.Compare.Gt("amount", 100),
    )
    val expr = PredicateToExprConverter.toExpr(p)
    expr shouldBe Expr.And(
      Expr.Equal(Expr.FieldRef("region"), Expr.Literal(LiteralValue.StringValue("us"), SealedDataType.Varchar)),
      Expr.GreaterThan(Expr.FieldRef("amount"), Expr.Literal(LiteralValue.IntValue(100), SealedDataType.Int)),
    )
  }

  test("Or(Eq, Eq) \u2192 Expr.Or(Eq, Eq)") {
    val p = Predicate.Or(
      Predicate.Compare.Eq("region", "us"),
      Predicate.Compare.Eq("region", "eu"),
    )
    PredicateToExprConverter.toExpr(p) shouldBe a [Expr.Or]
  }

  test("Not(Eq) \u2192 Expr.Not(Eq)") {
    val p = Predicate.Not(Predicate.Compare.Eq("region", "us"))
    PredicateToExprConverter.toExpr(p) shouldBe Expr.Not(
      Expr.Equal(Expr.FieldRef("region"), Expr.Literal(LiteralValue.StringValue("us"), SealedDataType.Varchar)),
    )
  }

  // -- Unsupported (deferred to v0.4.0) --

  test("Contains fails loud with UnsupportedOperationException") {
    val ex = intercept[UnsupportedOperationException] {
      PredicateToExprConverter.toExpr(Predicate.Compare.Contains("region", "us"))
    }
    ex.getMessage should include ("Contains")
  }

  test("StartsWith fails loud") {
    val ex = intercept[UnsupportedOperationException] {
      PredicateToExprConverter.toExpr(Predicate.Compare.StartsWith("region", "us"))
    }
    ex.getMessage should include ("StartsWith")
  }

  test("EndsWith fails loud") {
    val ex = intercept[UnsupportedOperationException] {
      PredicateToExprConverter.toExpr(Predicate.Compare.EndsWith("region", "us"))
    }
    ex.getMessage should include ("EndsWith")
  }

  test("ArrayContains fails loud") {
    val ex = intercept[UnsupportedOperationException] {
      PredicateToExprConverter.toExpr(Predicate.Compare.ArrayContains("tags", "vip"))
    }
    ex.getMessage should include ("ArrayContains")
  }

  // -- Determinism (per scala-data-driven-refacer \u00a71: pure data) --

  test("two conversions of the same legacy predicate produce equal portable Expr") {
    val p = Predicate.Compare.Eq("region", "AA")
    PredicateToExprConverter.toExpr(p) shouldBe PredicateToExprConverter.toExpr(p)
  }
}
