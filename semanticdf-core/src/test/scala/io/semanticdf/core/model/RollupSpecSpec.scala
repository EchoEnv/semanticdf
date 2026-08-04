package io.semanticdf.core.model

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.semanticdf.core.rel.AggregateFn

/** Phase 2 contract: prove `RollupSpec` is a usable, Spark-free data
  * record. Per scala-data-driven-refactor, this is pure data: the
  * rollup spec is engine-portable; the engine-specific registration
  * (which loads the actual rollup source) lives in the engine
  * adapter.
  */
class RollupSpecSpec extends AnyFunSuite with Matchers {

  test("RollupSpec carries name + baseModel + dimensions + measures + freshness") {
    val r = RollupSpec(
      name       = "orders_by_region_category",
      baseModel  = "orders",
      dimensions = List("region", "category"),
      measures   = List(
        RollupMeasureSpec("total", AggregateFn.Sum,   "total"),
        RollupMeasureSpec("count", AggregateFn.Count, "count"),
      ),
      freshness  = RollupFreshnessSpec.Track(
        java.time.Duration.ofMinutes(5),
        OnStalePolicy.FallBackToBase,
      ),
    )
    r.name shouldBe "orders_by_region_category"
    r.baseModel shouldBe "orders"
    r.dimensions.size shouldBe 2
    r.measures.size shouldBe 2
    r.freshness shouldBe a [RollupFreshnessSpec.Track]
  }

  test("RollupSpec with NoTracking freshness (static-fact rollup)") {
    val r = RollupSpec(
      name       = "static_facts",
      baseModel  = "facts",
      dimensions = List("k"),
      measures   = List(RollupMeasureSpec("v", AggregateFn.Sum, "v")),
      freshness  = RollupFreshnessSpec.NoTracking,
    )
    r.freshness shouldBe RollupFreshnessSpec.NoTracking
  }

  test("RollupSpec is a value, not a singleton — two with same fields are equal") {
    val a = RollupSpec(
      "r", "m", List("d"),
      List(RollupMeasureSpec("v", AggregateFn.Sum, "v")),
      RollupFreshnessSpec.NoTracking,
    )
    val b = RollupSpec(
      "r", "m", List("d"),
      List(RollupMeasureSpec("v", AggregateFn.Sum, "v")),
      RollupFreshnessSpec.NoTracking,
    )
    a shouldBe b
  }

  test("RollupSpec with different names are not equal") {
    val a = RollupSpec("r1", "m", Nil, Nil, RollupFreshnessSpec.NoTracking)
    val b = RollupSpec("r2", "m", Nil, Nil, RollupFreshnessSpec.NoTracking)
    a should not be b
  }

  test("realistic: rollup at (region, category) grain for orders model") {
    val r = RollupSpec(
      name       = "orders_by_region_category",
      baseModel  = "orders",
      dimensions = List("region", "category"),
      measures   = List(
        RollupMeasureSpec("total",        AggregateFn.Sum,   "sum_amount"),
        RollupMeasureSpec("count",        AggregateFn.Count, "count_rows"),
        RollupMeasureSpec("unique_users", AggregateFn.CountDistinct, "unique_users"),
      ),
      freshness  = RollupFreshnessSpec.Track(
        maxStaleness = java.time.Duration.ofMinutes(10),
        onStale      = OnStalePolicy.FallBackToBase,
      ),
    )
    r.measures.size shouldBe 3
  }

  test("RollupSpec round-trips through Java serialization") {
    val r = RollupSpec(
      name       = "orders_by_region",
      baseModel  = "orders",
      dimensions = List("region"),
      measures   = List(RollupMeasureSpec("total", AggregateFn.Sum, "sum_amount")),
      freshness  = RollupFreshnessSpec.Track(
        java.time.Duration.ofMinutes(5),
        OnStalePolicy.FallBackToBase,
      ),
    )
    val bos = new java.io.ByteArrayOutputStream()
    val oos = new java.io.ObjectOutputStream(bos)
    oos.writeObject(r)
    oos.close()
    val bis = new java.io.ByteArrayInputStream(bos.toByteArray)
    val ois = new java.io.ObjectInputStream(bis)
    val restored = ois.readObject().asInstanceOf[RollupSpec]
    restored shouldBe r
  }
}