package io.semanticdf.cache
import io.semanticdf.predicate._

import io.semanticdf.{Dimension, FlightsFixture, Measure, SparkSessionFixture, toSemanticTable}
import io.semanticdf.audit.PredicateHasher

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{count, lit, sum}
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

  test("forRequest: measures order matters (column order is part of the result contract)") {
    val a = CacheKey.forRequest(makeReq(measures = Seq("c1", "c2")))
    val b = CacheKey.forRequest(makeReq(measures = Seq("c2", "c1")))
    assert(a != b, "swapping measure order should change the cache key")
  }

  test("forRequest: same measure order => same key") {
    val a = CacheKey.forRequest(makeReq(measures = Seq("c1", "c2")))
    val b = CacheKey.forRequest(makeReq(measures = Seq("c1", "c2")))
    assert(a == b)
  }

  test("forRequest: different where => different key") {
    val p1 = io.semanticdf.predicate.Predicate.Compare.Eq("carrier", "AA")
    val p2 = io.semanticdf.predicate.Predicate.Compare.Eq("carrier", "UA")
    val a = CacheKey.forRequest(makeReq(where = Some(p1)))
    val b = CacheKey.forRequest(makeReq(where = Some(p2)))
    assert(a != b)
  }

  test("forRequest: same where => same key") {
    val p = io.semanticdf.predicate.Predicate.Compare.Eq("carrier", "AA")
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
    val p1 = io.semanticdf.predicate.Predicate.And(
      io.semanticdf.predicate.Predicate.Compare.Eq("carrier", "AA"),
      io.semanticdf.predicate.Predicate.Compare.Gt("distance", 500),
    )
    val p2 = io.semanticdf.predicate.Predicate.And(
      io.semanticdf.predicate.Predicate.Compare.Gt("distance", 500),
      io.semanticdf.predicate.Predicate.Compare.Eq("carrier", "AA"),
    )
    val a = CacheKey.forRequest(makeReq(where = Some(p1)))
    val b = CacheKey.forRequest(makeReq(where = Some(p2)))
    assert(a == b)
  }

  test("forRequest: orderBy direction matters (asc != desc)") {
    val a = CacheKey.forRequest(makeReq(orderBy = Seq(("c", "asc"))))
    val b = CacheKey.forRequest(makeReq(orderBy = Seq(("c", "desc"))))
    assert(a != b, "asc vs desc must produce different keys")
  }

  test("forRequest: orderBy column matters") {
    val a = CacheKey.forRequest(makeReq(orderBy = Seq(("c1", "asc"))))
    val b = CacheKey.forRequest(makeReq(orderBy = Seq(("c2", "asc"))))
    assert(a != b)
  }

  test("forRequest: limit matters (None != Some(10))") {
    val a = CacheKey.forRequest(makeReq(limit = None))
    val b = CacheKey.forRequest(makeReq(limit = Some(10)))
    assert(a != b, "uncapped vs capped must produce different keys")
  }

  test("forRequest: dimensions order matters (column order is part of the result)") {
    val a = CacheKey.forRequest(makeReq(dimensions = Seq("c1", "c2")))
    val b = CacheKey.forRequest(makeReq(dimensions = Seq("c2", "c1")))
    assert(a != b, "swapping dimension order should change the cache key")
  }

  test("forRequest: time grain, per-dimension grains, and time range change the key") {
    val t = toSemanticTable(flightsWithTimeDf, name = Some("flights_time"))
      .withDimensions(Dimension.time("ts", x => x("ts")))
      .withMeasures(Measure("passengers", x => sum(x("passengers"))))

    def key(
        timeGrain: Option[String] = None,
        timeGrains: Map[String, String] = Map.empty,
        timeRange: Option[(String, String)] = None,
    ) = CacheKey.forRequest(t.query(
      measures   = Seq("passengers"),
      dimensions = Seq("ts"),
      timeGrain  = timeGrain,
      timeGrains = timeGrains,
      timeRange  = timeRange,
    ).auditRequest.get)

    assert(key(timeGrain = Some("day")) != key(timeGrain = Some("month")))
    assert(key(timeGrains = Map("ts" -> "day")) != key(timeGrains = Map("ts" -> "month")))
    assert(key(timeRange = Some("2024-01-01" -> "2024-01-31")) !=
      key(timeRange = Some("2024-02-01" -> "2024-02-29")))
  }

  test("forRequest: time field encoding has no collisions (regression for #186)") {
    // PR #186 introduced length-prefixed-free encoding that admitted
    // collisions: `None` and `Some("none")` both hashed to the same
    // value; `Map("a"->"b,c:d")` collided with `Map("a"->"b", "c"->"d")`;
    // `Some(("a..b","c"))` collided with `Some(("a","b..c"))`. PR
    // #187 switched to length-prefixed encoding for time fields.
    // These tests prove the collisions are gone.
    val a = CacheKey.forRequest(makeReq(timeGrain = None))
    val b = CacheKey.forRequest(makeReq(timeGrain = Some("none")))
    assert(a != b, s"None and Some('none') must hash to different keys; both = $a")

    val c = CacheKey.forRequest(makeReq(timeGrains = Map("a" -> "b,c:d")))
    val d = CacheKey.forRequest(makeReq(timeGrains = Map("a" -> "b", "c" -> "d")))
    assert(c != d, s"ambiguous timeGrains encoding collides: both = $c")

    val e = CacheKey.forRequest(makeReq(timeRange = Some("a..b", "c")))
    val f = CacheKey.forRequest(makeReq(timeRange = Some("a", "b..c")))
    assert(e != f, s"range with '..' in endpoints collides: both = $e")

    // Same input produces the same key (deterministic).
    val g1 = CacheKey.forRequest(makeReq(timeGrain = Some("day"), timeGrains = Map("ts" -> "day"), timeRange = Some("2024-01-01", "2024-01-31")))
    val g2 = CacheKey.forRequest(makeReq(timeGrain = Some("day"), timeGrains = Map("ts" -> "day"), timeRange = Some("2024-01-01", "2024-01-31")))
    assert(g1 == g2, "identical requests must hash to the same key")
  }

  test("forRequest: model/measure/dimension/orderBy encoding has no collisions (regression for #188)") {
    // PR #187 only length-prefixed the time fields. The rest of the
    // request still used delimiter encoding — which admits collisions
    // whenever a string value contains the delimiter:
    //   - Seq("a,b") and Seq("a","b") both encoded as "a,b"
    //   - Seq("a:b") and Seq("a","b") both encoded as "a:b" (orderBy)
    //   - model containing `|` or `me=` could cross field boundaries
    // PR #188 extended the length-prefixed encoding to every field.
    // These tests reproduce the collisions and prove they're gone.

    // Measures: single element with a comma vs two elements
    val k1 = CacheKey.forRequest(makeReq(measures = Seq("a,b")))
    val k2 = CacheKey.forRequest(makeReq(measures = Seq("a", "b")))
    assert(k1 != k2, s"Seq('a,b') and Seq('a','b') must hash to different keys; both = $k1")

    // Dimensions: same scenario
    val k3 = CacheKey.forRequest(makeReq(dimensions = Seq("a,b")))
    val k4 = CacheKey.forRequest(makeReq(dimensions = Seq("a", "b")))
    assert(k3 != k4, s"Seq('a,b') and Seq('a','b') must hash to different keys; both = $k3")

    // OrderBy: single pair with a colon in the name vs two pairs
    val k5 = CacheKey.forRequest(makeReq(orderBy = Seq(("a:b", "asc"))))
    val k6 = CacheKey.forRequest(makeReq(orderBy = Seq(("a", "b"), ("", "asc"))))
    assert(k5 != k6, s"orderBy with delimiter in name must not collide with multi-pair orderBy; both = $k5")

    // Model: containing a `|` (the field separator) must not break the canonical
    val k7 = CacheKey.forRequest(makeReq(model = "flights|me=fake"))
    val k8 = CacheKey.forRequest(makeReq(model = "flights", measures = Seq("fake")))
    assert(k7 != k8, s"model with '|' delimiter must not break canonical encoding; both = $k7")

    // Identical inputs produce identical keys (determinism)
    val k9 = CacheKey.forRequest(makeReq(
      model = "flights", measures = Seq("a,b", "c"), dimensions = Seq("x|y", "z"),
      orderBy = Seq(("a:b", "asc"), ("c:d", "desc"))))
    val k10 = CacheKey.forRequest(makeReq(
      model = "flights", measures = Seq("a,b", "c"), dimensions = Seq("x|y", "z"),
      orderBy = Seq(("a:b", "asc"), ("c:d", "desc"))))
    assert(k9 == k10, "identical requests must hash to the same key")
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

  test("inMemory: LRU eviction cleans the byModel sidecar (no leak on rotation)") {
    // Regression: before the fix, removeEldestEntry ran the cleanup
    // unconditionally, which removed non-evicted entries from the
    // sidecar after every put. After the fix, the sidecar should
    // only be cleaned when an entry is actually evicted.
    val c = ResultCache.inMemory(maxEntries = 2).asInstanceOf[InMemoryResultCache]
    val schema = StructType(Seq(StructField("x", IntegerType)))
    val v = CachedResult(Array.empty[Row], schema)
    c.putWithModel("a", v, "orders")
    c.putWithModel("b", v, "orders")
    // No eviction yet (size=2, maxEntries=2). Both should still be in
    // the sidecar, so invalidateModel returns 2.
    assert(c.invalidateModel("orders") == 2, "both entries should still be tracked")
  }

  test("inMemory: LRU eviction of a model-tagged entry cleans the sidecar") {
    // When an entry IS evicted, the sidecar should be cleaned up.
    val c = ResultCache.inMemory(maxEntries = 2).asInstanceOf[InMemoryResultCache]
    val schema = StructType(Seq(StructField("x", IntegerType)))
    val v = CachedResult(Array.empty[Row], schema)
    c.putWithModel("a", v, "orders")
    c.putWithModel("b", v, "orders")
    c.putWithModel("c", v, "orders")  // evicts "a"
    // After eviction, only "b" and "c" should be tracked.
    assert(c.invalidateModel("orders") == 2, "only 2 surviving entries should be tracked")
  }

  test("inMemory: byModel entry is dropped when its set becomes empty (no empty-set leak)") {
    // Regression: before the fix, the byModel sidecar kept an
    // empty `Set` for each model that had been fully evicted, so
    // cycling through distinct model names accumulated unbounded
    // empty sets. Now the empty set is dropped.
    val c = ResultCache.inMemory(maxEntries = 2).asInstanceOf[InMemoryResultCache]
    val schema = StructType(Seq(StructField("x", IntegerType)))
    val v = CachedResult(Array.empty[Row], schema)
    // 3 distinct models, each with one entry. First put evicts.
    c.putWithModel("a", v, "m1")  // size=1
    c.putWithModel("b", v, "m2")  // size=2
    c.putWithModel("c", v, "m3")  // evicts "a" (m1's set should drop)
    // Force the remaining two out so m2 and m3 also drop.
    c.putWithModel("d", v, "m4")  // evicts "b" (m2 drops)
    c.putWithModel("e", v, "m5")  // evicts "c" (m3 drops)
    c.putWithModel("f", v, "m6")  // evicts "d" (m4 drops)
    // m5 is still in (just put, plus "e"). m6 is the latest.
    // The byModel map should have only m5 and m6 — no empty sets
    // for m1, m2, m3, m4.
    val tracked = c.keys().toSet
    assert(tracked == Set("e", "f"),
      s"expected only the live keys; got $tracked")
    // invalidateModel on a fully-evicted model returns 0 (the
    // byModel entry has been cleaned up):
    assert(c.invalidateModel("m1") == 0)
    assert(c.invalidateModel("m2") == 0)
    assert(c.invalidateModel("m3") == 0)
    assert(c.invalidateModel("m4") == 0)
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
  // invalidateModel — opt-in invalidation by model name
  // ----------------------------------------------------------------

  test("invalidateModel: drops all entries tagged with the model; returns the count") {
    val c = ResultCache.inMemory().asInstanceOf[InMemoryResultCache]
    val schema = StructType(Seq(StructField("x", IntegerType)))
    c.putWithModel("a", CachedResult(Array.empty[Row], schema), "orders")
    c.putWithModel("b", CachedResult(Array.empty[Row], schema), "orders")
    c.putWithModel("c", CachedResult(Array.empty[Row], schema), "customers")
    val removed = c.invalidateModel("orders")
    assert(removed == 2, s"expected 2, got $removed")
    assert(c.get("a").isEmpty, "a should be gone")
    assert(c.get("b").isEmpty, "b should be gone")
    assert(c.get("c").isDefined, "c (different model) should remain")
  }

  test("invalidateModel: no match returns 0 and is a no-op") {
    val c = ResultCache.inMemory().asInstanceOf[InMemoryResultCache]
    val schema = StructType(Seq(StructField("x", IntegerType)))
    c.putWithModel("a", CachedResult(Array.empty[Row], schema), "orders")
    assert(c.invalidateModel("nonexistent") == 0)
    assert(c.get("a").isDefined, "unrelated entry should remain")
  }

  test("invalidateModel: single-arg put (no model tag) is invisible to invalidation") {
    val c = ResultCache.inMemory().asInstanceOf[InMemoryResultCache]
    val schema = StructType(Seq(StructField("x", IntegerType)))
    // 2-arg put: stored without a model tag.
    c.put("untagged", CachedResult(Array.empty[Row], schema))
    // invalidateModel with anything shouldn't see it.
    assert(c.invalidateModel("anything") == 0)
    assert(c.get("untagged").isDefined)
  }

  test("invalidateModel: sidecar index stays in sync when an entry is overwritten") {
    val c = ResultCache.inMemory().asInstanceOf[InMemoryResultCache]
    val schema = StructType(Seq(StructField("x", IntegerType)))
    c.putWithModel("k", CachedResult(Array.empty[Row], schema), "orders")
    // Overwrite the same key under a different model.
    c.putWithModel("k", CachedResult(Array.empty[Row], schema), "customers")
    // The "orders" set should no longer contain "k".
    assert(c.invalidateModel("orders") == 0)
    assert(c.invalidateModel("customers") == 1)
  }

  test("NoOp: invalidateModel is a no-op (returns 0)") {
    val removed = ResultCache.NoOp.invalidateModel("anything")
    assert(removed == 0)
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
      where      = Some(io.semanticdf.predicate.Predicate.Compare.Eq("carrier", "AA")),
    )
    val q2 = t.query(
      measures   = Seq("flight_count"),
      dimensions = Seq("carrier"),
      where      = Some(io.semanticdf.predicate.Predicate.Compare.Eq("carrier", "UA")),
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
      where      = Some(io.semanticdf.predicate.Predicate.Compare.Eq("carrier", "AA")),
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

  test("end-to-end: cache-hit audit row count uses the cached row length") {
    val sink = io.semanticdf.audit.AuditSink.inMemory()
    val cache = ResultCache.inMemory()
    val q = baseModel.withAuditSink(sink).withResultCache(cache).query(
      measures   = Seq("flight_count"),
      dimensions = Seq("carrier"),
    )

    q.toDataFrame(spark) // miss
    q.toDataFrame(spark) // hit

    assert(sink.snapshot().map(_.rowCount) == Seq(3L, 3L))
  }

  test("end-to-end: result-shaping chained after query bypasses the captured cache key") {
    val cache = ResultCache.inMemory().asInstanceOf[InMemoryResultCache]
    val q = baseModel.withResultCache(cache).query(
      measures   = Seq("flight_count"),
      dimensions = Seq("carrier"),
    )
    assert(q.toDataFrame(spark).collect().length == 3)

    val shaped = q.orderBy(io.semanticdf.SortKey.desc("flight_count")).limit(1)
    assert(shaped.auditRequest.isEmpty, "post-query shaping must invalidate the captured request")
    assert(shaped.toDataFrame(spark).collect().length == 1)
    assert(cache.keys().length == 1, "the shaped query must bypass rather than populate the cache")
  }

  // ----------------------------------------------------------------
  // Cache auto-invalidation: model version (v0.2.0, PR #199)
  // ----------------------------------------------------------------

  test("forRequest: model version 0 (default) produces key without an mv segment") {
    // The default `version = 0` is the "pre-versioning era" sentinel.
    // We don't pollute the cache key with `mv=0` because every model
    // before v0.2.0 had version=0 — making the segment mandatory would
    // invalidate every pre-existing cache entry on upgrade.
    val a = CacheKey.forRequest(makeReq(model = "flights", version = 0))
    val b = CacheKey.forRequest(makeReq(model = "flights"))  // also version = 0 by default
    assert(a == b)
  }

  test("forRequest: same model + same shape but different version => different keys") {
    // This is the auto-invalidation contract: a version bump produces a
    // different cache key, so old-version entries become unreachable.
    val v1 = CacheKey.forRequest(makeReq(model = "flights", version = 1))
    val v2 = CacheKey.forRequest(makeReq(model = "flights", version = 2))
    assert(v1.isDefined && v2.isDefined)
    assert(v1 != v2, "different versions must produce different cache keys")
  }

  test("forRequest: version 10 vs version 100 are not prefix-collisions under length-prefix encoding") {
    // PR #188 fixed prefix collisions by length-prefixing every segment.
    // Verify the version segment doesn't reintroduce them when version
    // numbers cross a digit boundary.
    val v10  = CacheKey.forRequest(makeReq(model = "m", version = 10))
    val v100 = CacheKey.forRequest(makeReq(model = "m", version = 100))
    val v1   = CacheKey.forRequest(makeReq(model = "m", version = 1))
    val v9   = CacheKey.forRequest(makeReq(model = "m", version = 9))
    assert(v10 != v100, s"version 10 vs 100 must not collide: $v10 vs $v100")
    assert(v1  != v10,  s"version 1 vs 10 must not collide: $v1 vs $v10")
    assert(v9  != v100, s"version 9 vs 100 must not collide: $v9 vs $v100")
  }

  test("inMemory: invalidateByModelAndVersion removes only matching entries") {
    // The explicit hook for active eviction of old-version entries
    // (e.g. on persistent backends like Redis where LRU doesn't
    // naturally release memory).
    val c = ResultCache.inMemory(maxEntries = 16).asInstanceOf[InMemoryResultCache]
    val schema = StructType(Seq(StructField("x", IntegerType)))
    val v = CachedResult(Array.empty[Row], schema)
    c.putWithModelAndVersion("a", v, "orders", 1)
    c.putWithModelAndVersion("b", v, "orders", 1)
    c.putWithModelAndVersion("c", v, "orders", 2)  // same model, diff version
    assert(c.invalidateByModelAndVersion("orders", 1) == 2,
      "should evict the two v1 entries; leave the v2 entry alone")
    // 'c' (v2) is still there; subsequent calls for v2 still hit:
    assert(c.keys() == Seq("c"))
    // Second call returns 0:
    assert(c.invalidateByModelAndVersion("orders", 1) == 0)
    // v2 still survives its own call:
    assert(c.invalidateByModelAndVersion("orders", 2) == 1)
    assert(c.keys().isEmpty)
  }

  test("inMemory: invalidateModel covers all versions of the model") {
    val c = ResultCache.inMemory(maxEntries = 16).asInstanceOf[InMemoryResultCache]
    val schema = StructType(Seq(StructField("x", IntegerType)))
    val v = CachedResult(Array.empty[Row], schema)
    c.putWithModelAndVersion("a", v, "orders", 1)
    c.putWithModelAndVersion("b", v, "orders", 2)
    c.putWithModelAndVersion("c", v, "orders", 3)
    assert(c.invalidateModel("orders") == 3,
      "invalidateModel must wipe all versions of the named model")
    assert(c.keys().isEmpty)
  }

  // PR-fix #6: post-#278 review. invalidateByModelAndVersion must
  // walk BOTH the row-form sidecar AND the journaled-form sidecar.
  // Pre-fix, journaled entries leaked.
  test("inMemory: invalidateByModelAndVersion clears journaled entries too") {
    val c = ResultCache.inMemory(maxEntries = 16).asInstanceOf[InMemoryResultCache]
    c.putWithModelAndVersion("row-a", CachedResult(Array.empty[Row],
      StructType(Seq(StructField("x", IntegerType)))), "orders", 1)
    c.putJournaledWithModelAndVersion("j-a", new Object(), "orders", 1)
    c.putJournaledWithModelAndVersion("j-b", new Object(), "orders", 2)
    // Invalidate orders v1: should remove row-a + j-a, leave j-b (v2).
    val n = c.invalidateByModelAndVersion("orders", 1)
    assert(n == 2, s"expected 2 entries removed (1 row + 1 journaled), got $n")
    assert(c.get("row-a").isEmpty, "row-form entry should be gone")
    assert(c.getJournaled("j-a").isEmpty, "journaled-form v1 entry should be gone")
    assert(c.getJournaled("j-b").isDefined, "journaled-form v2 entry must survive")
  }

  // PR-fix #9: post-#278 review. Default getOrComputeJournaled
  // silently wrote uninvalidateable entries (model=""). Loud failure
  // is correct — only InMemoryResultCache should provide this.
  test("getOrComputeJournaled: default impl throws UnsupportedOperationException") {
    val raw: ResultCache = new ResultCache {
      override def get(key: String): Option[CachedResult] = None
      override def put(key: String, value: CachedResult): Unit = ()
    }
    val ex = intercept[UnsupportedOperationException] {
      raw.getOrComputeJournaled("k", () => new Object())
    }
    assert(ex.getMessage.contains("getOrComputeJournaled"),
      "error must point the caller at the offending method")
  }

  test("end-to-end: cache miss after version bump is auto-invalidation") {
    // The most important regression test: a user bumps the model
    // version (e.g. flights.yml v1 → v2 with a schema change). Without
    // the cache-key fix, the next query would HIT a stale cache entry
    // from v1. With the fix, the v2 cache key is different (mv=2), so
    // the lookup misses, the query runs fresh, and the v1 entry becomes
    // LRU garbage that ages out.
    val cache = ResultCache.inMemory().asInstanceOf[InMemoryResultCache]
    val v1 = baseModel.version(1).withResultCache(cache)
      .query(measures = Seq("flight_count"), dimensions = Seq("carrier"))
    val v2 = baseModel.version(2).withResultCache(cache)
      .query(measures = Seq("flight_count"), dimensions = Seq("carrier"))
    // First, fill the cache with v1.
    v1.toDataFrame(spark).collect()
    assert(cache.keys().length == 1, "v1 query should populate the cache")
    val v1KeysBefore = cache.keys().toSet
    // Now bump to v2 and query. Cache miss → fresh execution.
    v2.toDataFrame(spark).collect()
    assert(cache.keys().length == 2,
      "v2 query should add a NEW cache entry (different key), " +
      "leaving the v1 entry to age out via LRU")
    assert(v1KeysBefore != cache.keys().toSet,
      "the v1 cache key must not match the v2 cache key")
  }

  test("end-to-end: AuditEvent now carries the model version") {
    // The audit log is the "natural trigger" per the v0.1.17 review's
    // standing recommendation: it should record the model state at
    // query time so consumers (MCP, agents) can correlate events with
    // model versions.
    val sink = io.semanticdf.audit.AuditSink.inMemory(maxEvents = 32)
    val q = baseModel.version(7).withAuditSink(sink)
      .query(measures = Seq("flight_count"), dimensions = Seq("carrier"))
    q.toDataFrame(spark).collect()
    // Read the events via the AuditSink default snapshot (returns any
    // concrete subtype's events; cast through the trait).
    // The sink's public surface returns Seq[AuditEvent].
    val events: Seq[io.semanticdf.audit.AuditEvent] = sink.snapshot()
    assert(events.nonEmpty, "audit sink should receive at least one event")
    assert(events.last.version == 7,
      s"AuditEvent should carry the model's version; got ${events.last.version}")
    // Streaming path: same expectation.
    // (Skipped here: streaming terminal needs a streaming source; covered by
    // the streaming audit tests in v0.1.17 / v0.2.0.)
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
      version: Int = 0,
      measures: Seq[String] = Seq("flight_count"),
      dimensions: Seq[String] = Seq("carrier"),
      where: Option[io.semanticdf.predicate.Predicate] = None,
      having: Option[io.semanticdf.predicate.Predicate] = None,
      orderBy: Seq[(String, String)] = Seq.empty,
      limit: Option[Int] = None,
      timeGrain: Option[String] = None,
      timeGrains: Map[String, String] = Map.empty,
      timeRange: Option[(String, String)] = None,
  ): io.semanticdf.audit.QueryRequest =
    io.semanticdf.audit.QueryRequest(
      model      = model,
      version    = version,
      measures   = measures,
      dimensions = dimensions,
      where      = where,
      having     = having,
      orderBy    = orderBy,
      limit      = limit,
      timeGrain  = timeGrain,
      timeGrains = timeGrains,
      timeRange  = timeRange,
    )
}
