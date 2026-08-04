package io.semanticdf.core.model

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.semanticdf.core.expr.{Expr, LiteralValue}
import io.semanticdf.core.schema.SealedDataType

/** Phase 2 contract: prove `CalculatedMeasure` is a usable, Spark-
  * free data record. Per scala-data-driven-refactor, this is pure
  * data: the calc-measure SHAPE is engine-portable; the engine-
  * specific compile is in the engine adapter.
  */
class CalculatedMeasureSpec extends AnyFunSuite with Matchers {

  test("CalculatedMeasure carries name + expr") {
    val cm = CalculatedMeasure(
      name = "avg_per_flight",
      expr = Expr.Divide(
        Expr.MeasureRef("total_distance"),
        Expr.MeasureRef("flight_count"),
      ),
    )
    cm.name shouldBe "avg_per_flight"
    cm.expr shouldBe Expr.Divide(
      Expr.MeasureRef("total_distance"),
      Expr.MeasureRef("flight_count"),
    )
  }

  test("CalculatedMeasure with literal expr") {
    val cm = CalculatedMeasure(
      name = "discounted",
      expr = Expr.Multiply(
        Expr.MeasureRef("total"),
        Expr.Literal(LiteralValue.DoubleValue(0.95), SealedDataType.Double),
      ),
    )
    cm.expr shouldBe a [Expr.Multiply]
  }

  test("CalculatedMeasure with nested Expr (Add + Multiply)") {
    val cm = CalculatedMeasure(
      name = "complex_calc",
      expr = Expr.Add(
        Expr.Multiply(Expr.MeasureRef("a"), Expr.MeasureRef("b")),
        Expr.MeasureRef("c"),
      ),
    )
    cm.expr shouldBe Expr.Add(
      Expr.Multiply(Expr.MeasureRef("a"), Expr.MeasureRef("b")),
      Expr.MeasureRef("c"),
    )
  }

  test("CalculatedMeasure is a value, not a singleton — two with same fields are equal") {
    val a = CalculatedMeasure("c", Expr.MeasureRef("x"))
    val b = CalculatedMeasure("c", Expr.MeasureRef("x"))
    a shouldBe b
  }

  test("CalculatedMeasure with different names are not equal") {
    val a = CalculatedMeasure("a", Expr.MeasureRef("x"))
    val b = CalculatedMeasure("b", Expr.MeasureRef("x"))
    a should not be b
  }

  test("CalculatedMeasure round-trips through Java serialization") {
    val cm = CalculatedMeasure(
      name = "pct_total",
      expr = Expr.Divide(
        Expr.MeasureRef("region_total"),
        Expr.FunctionCall("SUM", Seq(Expr.MeasureRef("region_total"))),
      ),
    )
    val bos = new java.io.ByteArrayOutputStream()
    val oos = new java.io.ObjectOutputStream(bos)
    oos.writeObject(cm)
    oos.close()
    val bis = new java.io.ByteArrayInputStream(bos.toByteArray)
    val ois = new java.io.ObjectInputStream(bis)
    val restored = ois.readObject().asInstanceOf[CalculatedMeasure]
    restored shouldBe cm
  }
}