package io.semanticdf.core.model

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.semanticdf.core.rel.AggregateFn

/** Phase 2 contract: prove `RollupMeasureSpec` is a usable, Spark-free
  * data record. Per scala-data-driven-refactor, this is pure data:
  * the rollup-measure spec is engine-portable; the engine-specific
  * compile is in the engine adapter.
  */
class RollupMeasureSpecSpec extends AnyFunSuite with Matchers {

  test("RollupMeasureSpec carries name + aggregator + storageCol") {
    val m = RollupMeasureSpec(
      name       = "total",
      aggregator = AggregateFn.Sum,
      storageCol = "total",
    )
    m.name shouldBe "total"
    m.aggregator shouldBe AggregateFn.Sum
    m.storageCol shouldBe "total"
  }

  test("RollupMeasureSpec supports all AggregateFn cases (Sum, Count, Avg, ...)") {
    val cases: Set[RollupMeasureSpec] = Set(
      RollupMeasureSpec("m_sum",     AggregateFn.Sum,                "sum_col"),
      RollupMeasureSpec("m_count",   AggregateFn.Count,              "count_col"),
      RollupMeasureSpec("m_avg",     AggregateFn.Avg,                "avg_col"),
      RollupMeasureSpec("m_min",     AggregateFn.Min,                "min_col"),
      RollupMeasureSpec("m_max",     AggregateFn.Max,                "max_col"),
      RollupMeasureSpec("m_dist",    AggregateFn.CountDistinct,      "distinct_col"),
    )
    cases.size shouldBe 6
  }

  test("RollupMeasureSpec is a value, not a singleton — two with same fields are equal") {
    val a = RollupMeasureSpec("total", AggregateFn.Sum, "total")
    val b = RollupMeasureSpec("total", AggregateFn.Sum, "total")
    a shouldBe b
  }

  test("RollupMeasureSpec with different aggregators are not equal") {
    val a = RollupMeasureSpec("total", AggregateFn.Sum, "x")
    val b = RollupMeasureSpec("total", AggregateFn.Avg, "x")
    a should not be b
  }

  test("realistic: rollup measure for Sum of `amount` stored in column `sum_amount`") {
    val m = RollupMeasureSpec(
      name       = "total",
      aggregator = AggregateFn.Sum,
      storageCol = "sum_amount",
    )
    m.storageCol shouldBe "sum_amount"
  }

  test("RollupMeasureSpec round-trips through Java serialization") {
    val m = RollupMeasureSpec("p95", AggregateFn.ApproxPercentile, "p95_col")
    val bos = new java.io.ByteArrayOutputStream()
    val oos = new java.io.ObjectOutputStream(bos)
    oos.writeObject(m)
    oos.close()
    val bis = new java.io.ByteArrayInputStream(bos.toByteArray)
    val ois = new java.io.ObjectInputStream(bis)
    val restored = ois.readObject().asInstanceOf[RollupMeasureSpec]
    restored shouldBe m
  }
}