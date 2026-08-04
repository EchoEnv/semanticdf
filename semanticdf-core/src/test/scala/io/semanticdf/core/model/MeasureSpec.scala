package io.semanticdf.core.model

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.semanticdf.core.expr.Expr
import io.semanticdf.core.rel.{AggregateCall, AggregateFn}

/** Phase 2 contract: prove `Measure` is a usable, Spark-free data
  * record + the smart constructor `Measure.aggregate` builds the
  * common case correctly. Per scala-data-driven-refactor, this is
  * pure data: the measure SHAPE is engine-portable; the engine-
  * specific compile is in the engine adapter.
  */
class MeasureSpec extends AnyFunSuite with Matchers {

  test("Measure.aggregate smart constructor builds a single-aggregate measure") {
    val m = Measure.aggregate("total", AggregateFn.Sum, Expr.FieldRef("amount"))
    m.name shouldBe "total"
    m.expr.fn shouldBe AggregateFn.Sum
    m.expr.input shouldBe Some(Expr.FieldRef("amount"))
    m.expr.alias shouldBe "total"
  }

  test("Measure.aggregate result equals the structural constructor") {
    val a = Measure.aggregate("total", AggregateFn.Sum, Expr.FieldRef("amount"))
    val b = Measure(
      name = "total",
      expr = AggregateCall(
        fn     = AggregateFn.Sum,
        input  = Some(Expr.FieldRef("amount")),
        alias  = "total",
      ),
    )
    a shouldBe b
  }

  test("Measure with Count(*) — input is None, alias is '*'") {
    val m = Measure(
      name = "row_count",
      expr = AggregateCall(
        fn    = AggregateFn.Count,
        alias = "*",
      ),
    )
    m.expr.input shouldBe None
    m.expr.alias shouldBe "*"
  }

  test("Measure with Count(DISTINCT user_id)") {
    val m = Measure(
      name = "unique_users",
      expr = AggregateCall(
        fn       = AggregateFn.CountDistinct,
        input    = Some(Expr.FieldRef("user_id")),
        alias    = "unique_users",
        distinct = true,
      ),
    )
    m.expr.distinct shouldBe true
  }

  test("Measure with ApproxPercentile(x, 0.95)") {
    import io.semanticdf.core.expr.LiteralValue
    val m = Measure(
      name = "p95",
      expr = AggregateCall(
        fn        = AggregateFn.ApproxPercentile,
        input     = Some(Expr.FieldRef("latency")),
        alias     = "p95",
        arguments = List(LiteralValue.DoubleValue(0.95)),
      ),
    )
    m.expr.arguments.size shouldBe 1
  }

  test("Measure.aggregate with various AggregateFn cases") {
    val cases: Seq[(String, AggregateFn, Expr)] = Seq(
      ("total",         AggregateFn.Sum,         Expr.FieldRef("amount")),
      ("count_rows",    AggregateFn.Count,       Expr.FieldRef("id")),
      ("avg_distance",  AggregateFn.Avg,         Expr.FieldRef("distance")),
      ("min_distance",  AggregateFn.Min,         Expr.FieldRef("distance")),
      ("max_distance",  AggregateFn.Max,         Expr.FieldRef("distance")),
    )
    cases.foreach { case (name, fn, expr) =>
      val m = Measure.aggregate(name, fn, expr)
      m.name shouldBe name
      m.expr.fn shouldBe fn
    }
  }

  test("Measure is a value, not a singleton — two with same fields are equal") {
    val a = Measure.aggregate("total", AggregateFn.Sum, Expr.FieldRef("amount"))
    val b = Measure.aggregate("total", AggregateFn.Sum, Expr.FieldRef("amount"))
    a shouldBe b
  }

  test("Measure round-trips through Java serialization") {
    val m = Measure.aggregate("total", AggregateFn.Sum, Expr.FieldRef("amount"))
    val bos = new java.io.ByteArrayOutputStream()
    val oos = new java.io.ObjectOutputStream(bos)
    oos.writeObject(m)
    oos.close()
    val bis = new java.io.ByteArrayInputStream(bos.toByteArray)
    val ois = new java.io.ObjectInputStream(bis)
    val restored = ois.readObject().asInstanceOf[Measure]
    restored shouldBe m
  }
}