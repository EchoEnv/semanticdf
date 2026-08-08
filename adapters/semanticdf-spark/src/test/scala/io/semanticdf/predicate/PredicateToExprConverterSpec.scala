package io.semanticdf.predicate

import io.semanticdf.core.engine.EngineError
import io.semanticdf.core.expr.{Expr, LiteralValue}
import io.semanticdf.core.schema.SealedDataType

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Tests for [[PredicateToExprConverter]] \u2014 the boundary adapter
  * that converts legacy `io.semanticdf.predicate.Predicate` (Spark-
  * bearing) to portable `io.semanticdf.core.expr.Expr`.
  *
  * Per `docs/design/error-handling-style.md`: typed `Either` at the
  * boundary. No throw/catch round-trip. Each test asserts the
  * exact `Right(Expr)` shape (per scala-spark-batch-bugs \u00a73
  * schema-drift) or the typed `Left(EngineError.UnsupportedCapability)`
  * for deferred cases. */
class PredicateToExprConverterSpec extends AnyFunSuite with Matchers {

  // -- helpers for assertion brevity --
  private def to(p: Predicate): Either[EngineError.UnsupportedCapability, Expr] =
    PredicateToExprConverter.toExpr(p)

  // -- Compare family (supported) --

  test("Eq(field, value) -> Right(Expr.Equal(FieldRef(field), Literal(value, dt)))") {
    to(Predicate.Compare.Eq("region", "AA")) shouldBe Right(Expr.Equal(
      Expr.FieldRef("region"),
      Expr.Literal(LiteralValue.StringValue("AA"), SealedDataType.Varchar),
    ))
  }

  test("Ne(field, value) -> Right(Expr.NotEqual(...))") {
    to(Predicate.Compare.Ne("region", "us")) shouldBe Right(Expr.NotEqual(
      Expr.FieldRef("region"),
      Expr.Literal(LiteralValue.StringValue("us"), SealedDataType.Varchar),
    ))
  }

  test("Lt(field, value) -> Right(Expr.LessThan(...))") {
    to(Predicate.Compare.Lt("amount", 100)) shouldBe Right(Expr.LessThan(
      Expr.FieldRef("amount"),
      Expr.Literal(LiteralValue.IntValue(100), SealedDataType.Int),
    ))
  }

  test("Le, Gt, Ge map to the corresponding Expr comparisons") {
    to(Predicate.Compare.Le("x", 1)) shouldBe Right(Expr.LessOrEqual(
      Expr.FieldRef("x"), Expr.Literal(LiteralValue.IntValue(1), SealedDataType.Int)))
    to(Predicate.Compare.Gt("x", 1)) shouldBe Right(Expr.GreaterThan(
      Expr.FieldRef("x"), Expr.Literal(LiteralValue.IntValue(1), SealedDataType.Int)))
    to(Predicate.Compare.Ge("x", 1)) shouldBe Right(Expr.GreaterOrEqual(
      Expr.FieldRef("x"), Expr.Literal(LiteralValue.IntValue(1), SealedDataType.Int)))
  }

  // -- Literal type inference --

  test("Long literal maps to LiteralValue.LongValue + SealedDataType.Int") {
    to(Predicate.Compare.Eq("n", 100L)) shouldBe Right(Expr.Equal(
      Expr.FieldRef("n"),
      Expr.Literal(LiteralValue.LongValue(100L), SealedDataType.Int),
    ))
  }

  test("Double literal maps to LiteralValue.DoubleValue + SealedDataType.Double") {
    to(Predicate.Compare.Gt("ratio", 0.5)) shouldBe Right(Expr.GreaterThan(
      Expr.FieldRef("ratio"),
      Expr.Literal(LiteralValue.DoubleValue(0.5), SealedDataType.Double),
    ))
  }

  test("Boolean literal maps to LiteralValue.BoolValue + SealedDataType.Boolean") {
    to(Predicate.Compare.Eq("active", true)) shouldBe Right(Expr.Equal(
      Expr.FieldRef("active"),
      Expr.Literal(LiteralValue.BoolValue(true), SealedDataType.Boolean),
    ))
  }

  test("null literal maps to LiteralValue.NullValue") {
    to(Predicate.Compare.Eq("x", null)) shouldBe Right(Expr.Equal(
      Expr.FieldRef("x"),
      Expr.Literal(LiteralValue.NullValue, SealedDataType.Varchar),
    ))
  }

  // -- In / IsNull --

  test("In(field, [a, b]) -> Right(Or(Eq(a), Eq(b)))") {
    to(Predicate.In("region", Seq("us", "eu"))) shouldBe Right(Expr.Or(
      Expr.Equal(Expr.FieldRef("region"), Expr.Literal(LiteralValue.StringValue("us"), SealedDataType.Varchar)),
      Expr.Equal(Expr.FieldRef("region"), Expr.Literal(LiteralValue.StringValue("eu"), SealedDataType.Varchar)),
    ))
  }

  test("In(field, [a], negate=true) -> Right(Not(Eq(a)))") {
    to(Predicate.In("region", Seq("us"), negate = true)) shouldBe Right(Expr.Not(
      Expr.Equal(Expr.FieldRef("region"), Expr.Literal(LiteralValue.StringValue("us"), SealedDataType.Varchar)),
    ))
  }

  test("IsNull(field) -> Right(Expr.IsNull(FieldRef(field)))") {
    to(Predicate.IsNull("deleted_at")) shouldBe Right(Expr.IsNull(
      Expr.FieldRef("deleted_at"),
    ))
  }

  test("IsNull(field, negate=true) -> Right(Not(IsNull(...)))") {
    to(Predicate.IsNull("deleted_at", negate = true)) shouldBe Right(Expr.Not(
      Expr.IsNull(Expr.FieldRef("deleted_at"))))
  }

  // -- Compound --

  test("And(Eq, Gt) -> Right(And(Eq, Gt))") {
    to(Predicate.And(
      Predicate.Compare.Eq("region", "us"),
      Predicate.Compare.Gt("amount", 100),
    )) shouldBe Right(Expr.And(
      Expr.Equal(Expr.FieldRef("region"), Expr.Literal(LiteralValue.StringValue("us"), SealedDataType.Varchar)),
      Expr.GreaterThan(Expr.FieldRef("amount"), Expr.Literal(LiteralValue.IntValue(100), SealedDataType.Int)),
    ))
  }

  test("Or(Eq, Eq) -> Right(Expr.Or(...))") {
    to(Predicate.Or(
      Predicate.Compare.Eq("region", "us"),
      Predicate.Compare.Eq("region", "eu"),
    )) shouldBe a [Right[_, _]]
    // stronger: the inner is an Or
    to(Predicate.Or(
      Predicate.Compare.Eq("region", "us"),
      Predicate.Compare.Eq("region", "eu"),
    )).toOption.get shouldBe a [Expr.Or]
  }

  test("Not(Eq) -> Right(Not(Eq))") {
    to(Predicate.Not(Predicate.Compare.Eq("region", "us"))) shouldBe Right(Expr.Not(
      Expr.Equal(Expr.FieldRef("region"), Expr.Literal(LiteralValue.StringValue("us"), SealedDataType.Varchar)),
    ))
  }

  // -- Unsupported (deferred to v0.4.0) --

  test("Contains -> Left(UnsupportedCapability)") {
    to(Predicate.Compare.Contains("region", "us")).swap.toOption.get shouldBe a [EngineError.UnsupportedCapability]
    to(Predicate.Compare.Contains("region", "us")).swap.toOption.get.name should include ("Contains")
  }

  test("StartsWith -> Left(UnsupportedCapability)") {
    val left = to(Predicate.Compare.StartsWith("region", "us")).swap.toOption.get
    left shouldBe a [EngineError.UnsupportedCapability]
    left.name should include ("StartsWith")
  }

  test("EndsWith -> Left(UnsupportedCapability)") {
    val left = to(Predicate.Compare.EndsWith("region", "us")).swap.toOption.get
    left shouldBe a [EngineError.UnsupportedCapability]
    left.name should include ("EndsWith")
  }

  test("ArrayContains -> Left(UnsupportedCapability)") {
    val left = to(Predicate.Compare.ArrayContains("tags", "vip")).swap.toOption.get
    left shouldBe a [EngineError.UnsupportedCapability]
    left.name should include ("ArrayContains")
  }

  // -- Determinism (per scala-data-driven-refacer §1: pure data) --

  test("two conversions of the same legacy predicate produce the same portable Expr") {
    val p = Predicate.Compare.Eq("region", "AA")
    to(p) shouldBe to(p)
  }
}
