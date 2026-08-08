package io.semanticdf.core.expr

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.semanticdf.core.schema.SealedDataType

/** Phase 2 contract: prove `Calculator` (the static-analysis helper)
  * correctly extracts the field and measure names from an `Expr` tree.
  * Per scala-data-driven-refactor, this is pure data analysis; the
  * engine-specific runtime evaluation is in the engine adapter.
  */
class CalculatorSpec extends AnyFunSuite with Matchers {

  // -- Calculator.fieldNamesOf --

  test("fieldNamesOf: FieldRef returns its name") {
    val e = Expr.FieldRef("amount")
    Calculator.fieldNamesOf(e) shouldBe Set("amount")
  }

  test("fieldNamesOf: MeasureRef does NOT contribute (it's a measure, not a field)") {
    val e = Expr.MeasureRef("total_passengers")
    Calculator.fieldNamesOf(e) shouldBe Set.empty
  }

  test("fieldNamesOf: All does NOT contribute (it's a measure reference, not a field)") {
    // Regression guard for PR #419 + #420: Expr.All references a
    // measure (the named one's per-group sum), not a field. Per
    // scala-data-driven-refacer §1, this is data-correct: fieldNamesOf
    // collects FieldRef names only.
    val e = Expr.All("total_passengers")
    Calculator.fieldNamesOf(e) shouldBe Set.empty
  }

  test("fieldNamesOf: nested arithmetic containing All does not crash (PR #420 exhaustiveness)") {
    // Regression guard for the bug surfaced by the v0.3.1 SocratiCode
    // audit: a calculated measure like `amount / All(total)` walked
    // through fieldNamesOf would have raised MatchError before the
    // fix in Calculator.fieldNamesOf (PR #420 follow-up).
    val e = Expr.Divide(Expr.FieldRef("amount"), Expr.All("total"))
    Calculator.fieldNamesOf(e) shouldBe Set("amount")
  }

  test("fieldNamesOf: Literal does NOT contribute") {
    val e = Expr.Literal(LiteralValue.IntValue(42), SealedDataType.Int)
    Calculator.fieldNamesOf(e) shouldBe Set.empty
  }

  test("fieldNamesOf: Add collects names from both operands") {
    val e = Expr.Add(Expr.FieldRef("a"), Expr.FieldRef("b"))
    Calculator.fieldNamesOf(e) shouldBe Set("a", "b")
  }

  test("fieldNamesOf: nested arithmetic collects all field names") {
    val e = Expr.Add(
      Expr.Multiply(Expr.FieldRef("a"), Expr.FieldRef("b")),
      Expr.FieldRef("c"),
    )
    Calculator.fieldNamesOf(e) shouldBe Set("a", "b", "c")
  }

  test("fieldNamesOf: FunctionCall recursively collects from args") {
    val e = Expr.FunctionCall("CONCAT", Seq(Expr.FieldRef("first"), Expr.FieldRef("last")))
    Calculator.fieldNamesOf(e) shouldBe Set("first", "last")
  }

  test("fieldNamesOf: Cast preserves its inner field names") {
    val e = Expr.Cast(Expr.FieldRef("price"), SealedDataType.Double)
    Calculator.fieldNamesOf(e) shouldBe Set("price")
  }

  test("fieldNamesOf: IsNull preserves its inner field names") {
    val e = Expr.IsNull(Expr.FieldRef("a"))
    Calculator.fieldNamesOf(e) shouldBe Set("a")
  }

  test("fieldNamesOf: dedup — same field name appears only once in result") {
    val e = Expr.Add(Expr.FieldRef("a"), Expr.FieldRef("a"))
    Calculator.fieldNamesOf(e) shouldBe Set("a")
  }

  // -- Calculator.measureNamesOf --

  test("measureNamesOf: MeasureRef returns its name") {
    val e = Expr.MeasureRef("total_passengers")
    Calculator.measureNamesOf(e) shouldBe Set("total_passengers")
  }

  test("measureNamesOf: FieldRef does NOT contribute (it's a field, not a measure)") {
    val e = Expr.FieldRef("amount")
    Calculator.measureNamesOf(e) shouldBe Set.empty
  }

  test("measureNamesOf: Add collects measures from both operands") {
    val e = Expr.Add(Expr.MeasureRef("a"), Expr.MeasureRef("b"))
    Calculator.measureNamesOf(e) shouldBe Set("a", "b")
  }

  test("measureNamesOf: realistic calculated measure: sum(revenue) / count(*)") {
    val sum = Expr.MeasureRef("sum")
    val cnt = Expr.MeasureRef("cnt")
    val e = Expr.Divide(sum, cnt)
    Calculator.measureNamesOf(e) shouldBe Set("sum", "cnt")
  }

  test("measureNamesOf: nested measures for dependency graph construction") {
    val e = Expr.Add(
      Expr.MeasureRef("orders_total"),
      Expr.MeasureRef("refunds_total"),
    )
    Calculator.measureNamesOf(e) shouldBe Set("orders_total", "refunds_total")
  }

  test("measureNamesOf: dedup — same measure name appears only once in result") {
    val e = Expr.Add(Expr.MeasureRef("a"), Expr.MeasureRef("a"))
    Calculator.measureNamesOf(e) shouldBe Set("a")
  }

  // -- combined: build a dependency graph from realistic expressions --

  test("realistic calc: total_revenue depends on base measures, not fields") {
    val fieldOnly = Expr.FieldRef("revenue")
    Calculator.measureNamesOf(fieldOnly) shouldBe Set.empty
    Calculator.fieldNamesOf(fieldOnly) shouldBe Set("revenue")

    val measureOnly = Expr.MeasureRef("orders_total")
    Calculator.fieldNamesOf(measureOnly) shouldBe Set.empty
    Calculator.measureNamesOf(measureOnly) shouldBe Set("orders_total")

    val calc = Expr.Add(Expr.FieldRef("revenue"), Expr.MeasureRef("orders_total"))
    Calculator.fieldNamesOf(calc) shouldBe Set("revenue")
    Calculator.measureNamesOf(calc) shouldBe Set("orders_total")
  }
}