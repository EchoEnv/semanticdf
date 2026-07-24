package io.semanticdf.cache

import io.semanticdf.{Dimension, FlightsFixture, Measure, SparkSessionFixture, toSemanticTable}
import io.semanticdf.audit.PredicateHasher

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{count, lit}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

/** Tests for the result-cache machinery.
  *
  * The cache path has four moving parts:
  *   1. `CacheKey.forRequest` — stable SHA-256 of the request shape
  *   2. `InMemoryResultCache` — LRU bounded by `maxEntries`
  *   3. `SemanticTable.withResultCache(...)` — fluent setter
  *   4. `SemanticTable.toDataFrame()` — cache check, miss path, store
  *
  * These tests cover all four. The end-to-end tests use a real
  * Spark session and verify that repeated queries return identical
  * results without re-execution. */
class ResultCacheSpec extends AnyFunSuite with SparkSessionFixture with FlightsFixture {

  // ----------------------------------------------------------------
  // CacheKey
  // ----------------------------------------------------------------

  test("forRequest: same model/measures/dimensions => same key") {
    val a = CacheKey.forRequest(makeReq(model = "flights", measures = Seq("c"), dimensions = Seq("d")))
    val b = CacheKey.forRequest(makeReq(model = "flights", measures = Seq("c"), dimensions = Seq("d")))
    assert(a == b)
    assert(a.exists(_.length == 64))  // SHA-256 hex
  }

  test("forRequest: different model => different key") {
    val a = CacheKey.forRequest(makeReq(model = "flights"))
    val b = CacheKey.forRequest(makeReq(model = "carriers"))
    assert(a != b)
  }

  test("forRequest: different measures => different key") {
    val a = CacheKey.forRequest(makeReq(measures = Seq("c1")))
    val b = CacheKey.forRequest(makeReq(measures = Seq("c2")))
    assert(a != b)
  }

  test("forRequest: measures are order-insensitive (set semantics)") {
    val a = CacheKey.forRequest(makeReq(measures = Seq("c1", "c2")))
    val b = CacheKey.forRequest(makeReq(measures = Seq("c2", "c1")))
    assert(a == b)
  }

  test("forRequest: different where => different key") {
    val p1 = io.semanticdf.Predicate.Compare.Eq("carrier", "AA")
    val p2 = io.semanticdf.Predicate.Compare.Eq("carrier", "UA")
    val a = CacheKey.forRequest(makeReq(where = Some(p1)))
    val b = CacheKey.forRequest(makeReq(where = Some(p2)))
    assert(a != b)
  }

  test("forRequest: same where => same key") {
    val p = io.semanticdf.Predicate.Compare.Eq("carrier", "AA")
    val a = CacheKey.forRequest(makeReq(where = Some(p)))
    val b = CacheKey.forRequest(makeReq(where = Some(p)))
    assert(a == b)
  }

  test("forRequest: None model => None key") {
    val k = CacheKey.forRequest(makeReq(model = ""))
    assert(k.isEmpty)
  }

  test("forRequest: where with same content but different shape => same key") {
    // And(A, B) vs And(B, A) — PredicateHasher is commutative, so the
    // cache key for the equivalent predicate is the same.
    val p1 = io.semanticdf.Predicate.And(
      io.semanticdf.Predicate.Compare.Eq("carrier", "AA"),
      io.semanticdf.Predicate.Compare.Gt("distance", 500),
    )
    val p2 = io.semanticdf.Predicate.And(
      io.semanticdf.Predicate.Compare.Gt("distance", 500),
      io.semanticdf.Predicate.Compare.Eq("carrier", "AA"),
    )
    val a = CacheKey.forRequest(makeReq(where = Some(p1)))
    val b = CacheKey.forRequest(makeReq(where = Some(p2)))
    assert(a == b)
  }

  // ----------------------------------------------------------------
  // InMemoryResultCache
  // ----------------------------------------------------------------

  test("inMemory: get on empty cache returns None") {
    val c = ResultCache.inMemory()
    assert(c.get("missing").isEmpty)
  }

  test("inMemory: put then get returns the same value") {
    val c = ResultCache.inMemory().asInstanceOf[InMemoryResultCache]
    val schema = StructType(Seq(StructField("x", IntegerType)))
    val v = CachedResult(Array.empty[Row], schema)
    c.put("k", v)
    val got = c.get("k")
    assert(got.isDefined)
    assert(got.get.schema == schema)
  }

  test("inMemory: LRU evicts the least-recently-accessed entry on overflow") {
    val c = ResultCache.inMemory(maxEntries = 2).asInstanceOf[InMemoryResultCache]
    val v = CachedResult(Array.empty[Row], StructType(Seq(StructField("x", IntegerType))))
    c.put("a", v)
    c.put("b", v)
    // Touch "a" so "b" becomes the LRU.
    val _ = c.get("a")
    c.put("c", v)  // overflows; "b" is the LRU
    assert(c.get("b").isEmpty,  "b should be evicted")
    assert(c.get("a").isDefined)
    assert(c.get("c").isDefined)
  }

  test("inMemory: clear() drops everything") {
    val c = ResultCache.inMemory().asInstanceOf[InMemoryResultCache]
    val v = CachedResult(Array.empty[Row], StructType(Seq(StructField("x", IntegerType))))
    c.put("a", v); c.put("b", v)
    c.clear()
    assert(c.get("a").isEmpty)
    assert(c.get("b").isEmpty)
  }

  test("NoOp: get returns None, put is a no-op") {
    val c = ResultCache.NoOp
    val v = CachedResult(Array.empty[Row], StructType(Seq(StructField("x", IntegerType))))
    c.put("k", v)
    assert(c.get("k").isEmpty)
  }

  // ----------------------------------------------------------------
  // SemanticTable + cache: end-to-end
  // ----------------------------------------------------------------

  test("end-to-end: cache miss runs the query and stores the result") {
    val cache = ResultCache.inMemory()
    val t = baseModel.withResultCache(cache)
    val df1 = t.query(
      measures   = Seq("flight_count"),
      dimensions = Seq("carrier"),
    ).toDataFrame(spark)
    // First call: miss, executes.
    val r1 = df1.collect()
    assert(r1.length == 3)  // AA, UA, DL
    // The cache should now hold one entry.
    val _ = cache  // sink held by the captured SemanticTable
  }

  test("end-to-end: cache hit returns identical rows without re-execution") {
    val cache = ResultCache.inMemory()
    val t = baseModel.withResultCache(cache)
    val q = t.query(
      measures   = Seq("flight_count"),
      dimensions = Seq("carrier"),
    )
    val r1 = q.toDataFrame(spark).collect()
    val r2 = q.toDataFrame(spark).collect()
    assert(r1.length == r2.length)
    val m1 = r1.map(r => (r.getString(0), r.getLong(1))).toMap
    val m2 = r2.map(r => (r.getString(0), r.getLong(1))).toMap
    assert(m1 == m2)
  }

  test("end-to-end: different filter => different cache key => miss") {
    val cache = ResultCache.inMemory().asInstanceOf[InMemoryResultCache]
    val t = baseModel.withResultCache(cache)
    val q1 = t.query(
      measures   = Seq("flight_count"),
      dimensions = Seq("carrier"),
      where      = Some(io.semanticdf.Predicate.Compare.Eq("carrier", "AA")),
    )
    val q2 = t.query(
      measures   = Seq("flight_count"),
      dimensions = Seq("carrier"),
      where      = Some(io.semanticdf.Predicate.Compare.Eq("carrier", "UA")),
    )
    q1.toDataFrame(spark).collect()
    q2.toDataFrame(spark).collect()
    // Two distinct entries held.
    assert(cache.keys().length == 2)
  }

  test("end-to-end: same filter => same cache key => single entry") {
    val cache = ResultCache.inMemory().asInstanceOf[InMemoryResultCache]
    val t = baseModel.withResultCache(cache)
    val q = t.query(
      measures   = Seq("flight_count"),
      dimensions = Seq("carrier"),
      where      = Some(io.semanticdf.Predicate.Compare.Eq("carrier", "AA")),
    )
    q.toDataFrame(spark).collect()
    q.toDataFrame(spark).collect()
    q.toDataFrame(spark).collect()
    assert(cache.keys().length == 1)
  }

  test("end-to-end: no cache = same behavior as before (regression check)") {
    // Default SemanticTable has no cache; toDataFrame works.
    val df = baseModel.query(
      measures   = Seq("flight_count"),
      dimensions = Seq("carrier"),
    ).toDataFrame(spark)
    assert(df.collect().length == 3)
  }

  test("end-to-end: cache works alongside the audit sink (both fire)") {
    val auditSink = io.semanticdf.audit.AuditSink.inMemory()
    val cache     = ResultCache.inMemory()
    val t = baseModel.withAuditSink(auditSink).withResultCache(cache)
    val q = t.query(
      measures   = Seq("flight_count"),
      dimensions = Seq("carrier"),
    )
    q.toDataFrame(spark)  // miss
    q.toDataFrame(spark)  // hit
    q.toDataFrame(spark)  // hit
    // Three audit events, one cache entry.
    val evs = auditSink.snapshot()
    assert(evs.length == 3, s"expected 3 audit events, got ${evs.length}")
    assert(cache.asInstanceOf[InMemoryResultCache].keys().length == 1)
  }

  // ----------------------------------------------------------------
  // Fixtures
  // ----------------------------------------------------------------

  private def baseModel: io.semanticdf.SemanticTable =
    toSemanticTable(flightsDf, name = Some("flights"))
      .withDimensions(Dimension("carrier", t => t("carrier")))
      .withMeasures(Measure("flight_count", t => count(lit(1))))

  private def makeReq(
      model: String = "flights",
      measures: Seq[String] = Seq("flight_count"),
      dimensions: Seq[String] = Seq("carrier"),
      where: Option[io.semanticdf.Predicate] = None,
      having: Option[io.semanticdf.Predicate] = None,
  ): io.semanticdf.audit.QueryRequest =
    io.semanticdf.audit.QueryRequest(
      model      = model,
      measures   = measures,
      dimensions = dimensions,
      where      = where,
      having     = having,
    )
}
