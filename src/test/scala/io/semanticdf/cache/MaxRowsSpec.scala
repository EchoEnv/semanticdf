package io.semanticdf.cache

import io.semanticdf.predicate._
import Predicate._  // brings the implicit strToField conversion into scope
                   // so `"carrier" === "AA"` produces a Predicate.

import io.semanticdf.{Dimension, FlightsFixture, Measure, SparkSessionFixture, toSemanticTable}
import io.semanticdf.audit.PredicateHasher

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{count, lit}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

/** Tests for the driver-memory safety cap on the cache-miss collect path.
  *
  * Before this fix, `toDataFrame()` on a cache miss called `df.collect()` with
  * no upper bound — a 10M-row query OOMs the driver. The fix mirrors the
  * platform's `CacheBridge.executeQuery` row cap: a `maxRows: Int` field on
  * [[io.semanticdf.SemanticTable]] defaults to
  * [[CacheBridge.DefaultMaxRows]] (100,000) and applies `df.limit(maxRows)`
  * before `collect()` on cache miss.
  *
  * These tests pin the cap:
  *   1. Default behaviour unchanged for typical workloads (30-row fixture).
  *   2. `withMaxRows(n)` actually caps the result to n rows on cache miss.
  *   3. `maxRows == 0` disables the cap (escape hatch).
  *   4. Chainable methods (`.where`, `.groupBy`) preserve `maxRows`.
  *   5. Cache hits return the cached rows unchanged regardless of maxRows
  *      (the cap only affects the miss path; cached rows are already bounded
  *      by whatever the producer cached).
  *   6. Two queries with different maxRows produce different cache keys,
  *      so a tighter cap on the second call gets a fresh miss path
  *      (regression guard for the cache-key bug fixed post-#294).
  *   7. `withMaxRows(-1)` throws `IllegalArgumentException`.
  */
class MaxRowsSpec extends AnyFunSuite with SparkSessionFixture with FlightsFixture {

  /** A model that returns 3 rows (one per distinct carrier: AA, UA, DL). */
  private def perCarrierModel: io.semanticdf.SemanticTable =
    toSemanticTable(flightsDf, name = Some("flights"))
      .withDimensions(Dimension("carrier", t => t("carrier")))
      .withMeasures(Measure("flight_count", t => count(lit(1))))

  // ----------------------------------------------------------------
  // Default behaviour
  // ----------------------------------------------------------------

  test("default maxRows = CacheBridge.DefaultMaxRows (100_000)") {
    val t = perCarrierModel.withResultCache(ResultCache.inMemory(maxEntries = 4))
    t.maxRows shouldBe CacheBridge.DefaultMaxRows
  }

  test("default cap does not affect the 30-row flights fixture") {
    val cache = ResultCache.inMemory(maxEntries = 4)
    val q = perCarrierModel.withResultCache(cache)
      .query(measures = Seq("flight_count"), dimensions = Seq("carrier"))
    val rows = q.toDataFrame(spark).collect()
    rows.length shouldBe 3  // AA, UA, DL — uncapped
  }

  // ----------------------------------------------------------------
  // Cap actually applies
  // ----------------------------------------------------------------

  test("withMaxRows(2) caps the cache-miss collect to 2 rows") {
    // Use a fresh cache so the first call is a miss (the test verifies
    // the cache-miss path, which is where the cap lives).
    val cache = ResultCache.inMemory(maxEntries = 4)
    val q = perCarrierModel.withResultCache(cache).withMaxRows(2)
      .query(measures = Seq("flight_count"), dimensions = Seq("carrier"))
    val rows = q.toDataFrame(spark).collect()
    rows.length shouldBe 2  // 3 carriers exist, cap is 2
  }

  test("withMaxRows(0) disables the cap (escape hatch)") {
    val cache = ResultCache.inMemory(maxEntries = 4)
    val q = perCarrierModel.withResultCache(cache).withMaxRows(0)
      .query(measures = Seq("flight_count"), dimensions = Seq("carrier"))
    val rows = q.toDataFrame(spark).collect()
    rows.length shouldBe 3  // all 3 carriers
  }

  // ----------------------------------------------------------------
  // Chain preservation
  // ----------------------------------------------------------------

  test("chainable methods preserve maxRows (where / groupBy / aggregate)") {
    val cache = ResultCache.inMemory(maxEntries = 4)
    val base = perCarrierModel.withResultCache(cache).withMaxRows(2)
    val chained = base
      .where(("carrier" === "AA") or ("carrier" === "UA") or ("carrier" === "DL"))
      .query(measures = Seq("flight_count"), dimensions = Seq("carrier"))
    chained.maxRows shouldBe 2
    val rows = chained.toDataFrame(spark).collect()
    rows.length shouldBe 2  // cap applies through the chain
  }

  // ----------------------------------------------------------------
  // Cache hit / cache key regression guards
  // ----------------------------------------------------------------

  test("same maxRows produces a cache hit on a repeated query") {
    // Two identical queries (same shape, same maxRows) share a cache entry.
    // First call: cache miss (3 carriers → all 3 rows cached).
    // Second call: cache hit — the cached 3 rows are returned unchanged.
    val cache = ResultCache.inMemory(maxEntries = 4)
    val producer = perCarrierModel.withResultCache(cache)
      .query(measures = Seq("flight_count"), dimensions = Seq("carrier"))
    producer.toDataFrame(spark).collect()  // miss, populates cache with 3 rows
    // Second call with the same shape: cache hit, 3 rows returned.
    val second = perCarrierModel.withResultCache(cache)
      .query(measures = Seq("flight_count"), dimensions = Seq("carrier"))
    val rows = second.toDataFrame(spark).collect()
    rows.length shouldBe 3  // cache hit returns the producer's full result
  }

  test("tighter maxRows produces a cache miss with the cap applied (not a stale hit)") {
    // After the post-#294 cache-key fix, two queries with different
    // maxRows produce different cache keys. A subsequent call with a
    // tighter cap must run the miss path (with the cap applied), NOT
    // return the producer's uncapped result. This is the regression
    // guard for the bug where the cache key omitted maxRows.
    val cache = ResultCache.inMemory(maxEntries = 4)
    val producer = perCarrierModel.withResultCache(cache)
      .query(measures = Seq("flight_count"), dimensions = Seq("carrier"))
    producer.toDataFrame(spark).collect()  // miss, 3 rows cached
    val tighter = perCarrierModel.withResultCache(cache).withMaxRows(1)
      .query(measures = Seq("flight_count"), dimensions = Seq("carrier"))
    val rows = tighter.toDataFrame(spark).collect()
    rows.length shouldBe 1  // cache miss with cap=1, not stale cache hit
  }

  test("different maxRows produces a different cache key (regression guard for post-#294 bug)") {
    // A user who calls query() twice with different maxRows must get two
    // separate cache entries, not a hit on the higher-cap row. Pre-fix, the
    // cache key omitted maxRows, so the second call (with a tighter cap)
    // would silently receive the producer's full uncapped result.
    val cache = ResultCache.inMemory(maxEntries = 4)
    val producer = perCarrierModel.withResultCache(cache)
      .query(measures = Seq("flight_count"), dimensions = Seq("carrier"))
    producer.toDataFrame(spark).collect()  // miss, populates cache
    val producerKey = CacheKey.forRequest(
      io.semanticdf.audit.QueryRequest(
        model = "flights", measures = Seq("flight_count"), dimensions = Seq("carrier")),
      CacheBridge.DefaultMaxRows)
    val tighterKey = CacheKey.forRequest(
      io.semanticdf.audit.QueryRequest(
        model = "flights", measures = Seq("flight_count"), dimensions = Seq("carrier")),
      2)
    // Different maxRows → different cache keys. Plain assertion (the
    // Matchers DSL `should not equal` resolves ambiguously here).
    assert(producerKey != tighterKey, "different maxRows must produce different cache keys")
    assert(producerKey.isDefined)
    assert(tighterKey.isDefined)
  }

  // ----------------------------------------------------------------
  // Validation
  // ----------------------------------------------------------------

  test("withMaxRows(-1) throws IllegalArgumentException") {
    val ex = intercept[IllegalArgumentException] {
      perCarrierModel.withMaxRows(-1)
    }
    ex.getMessage should include("withMaxRows")
    ex.getMessage should include("-1")
  }
}
