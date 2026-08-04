package io.semanticdf.core.rel

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.semanticdf.core.expr.{Expr, LiteralValue}
import io.semanticdf.core.schema.SealedDataType

/** Phase 2 contract: prove `AggregateCall` is a usable, Spark-free
  * data record. Per scala-data-driven-refactor, this is pure data:
  * the call SHAPE is engine-portable; the engine-specific compile
  * is in the engine adapter.
  */
class AggregateCallSpec extends AnyFunSuite with Matchers {

  private val sampleInput = Expr.FieldRef("amount")

  test("AggregateCall default fields: input=None, alias=\"\", distinct=false, arguments=Nil") {
    val c = AggregateCall(fn = AggregateFn.Sum)
    c.fn shouldBe AggregateFn.Sum
    c.input shouldBe None
    c.alias shouldBe ""
    c.distinct shouldBe false
    c.arguments shouldBe Nil
  }

  test("AggregateCall with input + alias + distinct") {
    val c = AggregateCall(
      fn       = AggregateFn.Count,
      input    = Some(sampleInput),
      alias    = "row_count",
      distinct = true,
    )
    c.fn shouldBe AggregateFn.Count
    c.input shouldBe Some(sampleInput)
    c.alias shouldBe "row_count"
    c.distinct shouldBe true
  }

  test("AggregateCall with arguments (percentile = 0.95)") {
    val c = AggregateCall(
      fn        = AggregateFn.ApproxPercentile,
      input     = Some(sampleInput),
      alias     = "p95",
      arguments = List(LiteralValue.DoubleValue(0.95)),
    )
    c.arguments.size shouldBe 1
    c.arguments(0) shouldBe LiteralValue.DoubleValue(0.95)
  }

  test("AggregateCall with multiple arguments (median variant)") {
    val c = AggregateCall(
      fn        = AggregateFn.Median,
      input     = Some(sampleInput),
      alias     = "med",
      arguments = List(LiteralValue.IntValue(0), LiteralValue.IntValue(1)),
    )
    c.arguments.size shouldBe 2
  }

  test("AggregateCall is a value, not a singleton — two with same fields are equal") {
    val a = AggregateCall(AggregateFn.Sum, Some(sampleInput), "total")
    val b = AggregateCall(AggregateFn.Sum, Some(sampleInput), "total")
    a shouldBe b
  }

  test("AggregateCall with different fn are not equal") {
    val a = AggregateCall(AggregateFn.Sum, Some(sampleInput), "x")
    val b = AggregateCall(AggregateFn.Avg, Some(sampleInput), "x")
    a should not be b
  }

  test("realistic: Count(*) — input is None, alias is the column name") {
    val c = AggregateCall(
      fn    = AggregateFn.Count,
      alias = "*",
    )
    c.input shouldBe None
    c.alias shouldBe "*"
  }

  test("realistic: Count(DISTINCT user_id)") {
    val c = AggregateCall(
      fn       = AggregateFn.CountDistinct,
      input    = Some(Expr.FieldRef("user_id")),
      alias    = "unique_users",
      distinct = true,
    )
    c.distinct shouldBe true
    c.input shouldBe Some(Expr.FieldRef("user_id"))
  }

  test("AggregateCall round-trips through Java serialization") {
    val c = AggregateCall(
      fn        = AggregateFn.ApproxPercentile,
      input     = Some(sampleInput),
      alias     = "p95",
      distinct  = false,
      arguments = List(LiteralValue.DoubleValue(0.95)),
    )
    val bos = new java.io.ByteArrayOutputStream()
    val oos = new java.io.ObjectOutputStream(bos)
    oos.writeObject(c)
    oos.close()
    val bis = new java.io.ByteArrayInputStream(bos.toByteArray)
    val ois = new java.io.ObjectInputStream(bis)
    val restored = ois.readObject().asInstanceOf[AggregateCall]
    restored shouldBe c
  }
}