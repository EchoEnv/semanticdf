package io.semanticdf.perf

import io.semanticdf.{Dimension, FlightsFixture, Measure, Predicate, SparkSessionFixture, toSemanticTable}
import io.semanticdf.audit.PredicateHasher
import io.semanticdf.cache.{ResultCache, CachedResult}

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{count, lit}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite

/** Performance baseline for the post-v0.1.16 hot paths.
  *
  * These tests are **observational, not gates**: a slow day in CI
  * doesn't fail the build. Each test publishes its median latency
  * via `info(...)` so the number lands in the surefire-reports and
  * can be tracked over time. A future PR that doubles the latency
  * shows up as a doubling of the published number — visible in
  * trends, not blocking individual PRs.
  *
  * The first run on a fresh machine establishes the baseline. See
  * `docs/design/perf-baseline.md` for the v0.1.17 numbers.
  *
  * What we measure (one representative number per path):
  *   1. Library: median `toDataFrame()` time on a 30-row flights table
  *   2. Library: cache-hit rebuild time (no Spark plan) vs cache-miss time
  *   3. Library: predicate-hash throughput (small/medium/large predicates)
  *   4. Library: `whereHash` per-query (audit-log path)
  *
  * What we don't measure (out of scope):
  *   - Spark SQL planner details (delegated to Spark)
  *   - Network / disk IO (we use an in-memory DataFrame)
  *   - GC pauses (delegated to JVM; not the library's concern)
  *
  * To run locally: `mvn -o test -Dsuites='io.semanticdf.perf.PerfBaselineSpec'`
  * The output is verbose (info lines on every test) so the numbers
  * are easy to grep. */
class PerfBaselineSpec extends AnyFunSuite with SparkSessionFixture with FlightsFixture {

  /** Measure the median of N runs of `f`. Drops the first run as a
    * warmup. Returns the median in milliseconds. */
  private def medianMs(f: => Unit, runs: Int = 11): Long = {
    // Warmup
    f
    val samples = (0 until runs).map { _ =>
      val t0 = System.nanoTime()
      f
      (System.nanoTime() - t0) / 1000000L
    }
    val sorted = samples.sorted
    sorted(sorted.size / 2)
  }

  private def baseModel: io.semanticdf.SemanticTable =
    toSemanticTable(flightsDf, name = Some("flights"))
      .withDimensions(Dimension("carrier", t => t("carrier")))
      .withMeasures(Measure("flight_count", t => count(lit(1))))

  test("perf: toDataFrame on a 30-row flights table") {
    val t = baseModel
    val q = t.query(
      measures   = Seq("flight_count"),
      dimensions = Seq("carrier"),
    )
    val median = medianMs { q.toDataFrame(spark).collect() }
    info(s"[perf] toDataFrame (no audit, no cache): median=${median}ms")
  }

  test("perf: cache miss vs cache hit") {
    val cache = ResultCache.inMemory(maxEntries = 4)
    val t = baseModel.withResultCache(cache)
    val q = t.query(
      measures   = Seq("flight_count"),
      dimensions = Seq("carrier"),
    )
    val missMedian = medianMs { q.toDataFrame(spark).collect() }
    val hitMedian  = medianMs { q.toDataFrame(spark).collect() }
    info(s"[perf] cache miss: median=${missMedian}ms; cache hit: median=${hitMedian}ms")
    // The cache hit should be measurably faster, but we don't assert
    // a ratio — that would make this test flaky on shared CI hardware.
    // The published numbers are the value.
  }

  test("perf: cache hit with audit sink (write + rebuild)") {
    val auditSink = io.semanticdf.audit.AuditSink.inMemory()
    val cache     = ResultCache.inMemory(maxEntries = 4)
    val t = baseModel.withAuditSink(auditSink).withResultCache(cache)
    val q = t.query(
      measures   = Seq("flight_count"),
      dimensions = Seq("carrier"),
    )
    // Warm the cache (miss) so the second call is a hit.
    q.toDataFrame(spark).collect()
    val hitMedian = medianMs { q.toDataFrame(spark).collect() }
    info(s"[perf] cache hit + audit: median=${hitMedian}ms")
  }

  test("perf: predicate hashing on small/medium/large predicates") {
    val small = Predicate.Compare.Eq("carrier", "AA")
    val medium = Predicate.And(
      Predicate.Compare.Eq("carrier", "AA"),
      Predicate.Compare.Gt("distance", 500),
    )
    val large = (0 until 20).foldLeft[Predicate](medium) { (p, i) =>
      Predicate.And(p, Predicate.Compare.Eq(s"dim_$i", "x"))
    }
    val smallMedian = medianMs { for (_ <- 0 until 10000) PredicateHasher.hash(small) }
    val mediumMedian = medianMs { for (_ <- 0 until 10000) PredicateHasher.hash(medium) }
    val largeMedian = medianMs { for (_ <- 0 until 1000) PredicateHasher.hash(large) }
    info(s"[perf] predicate hash: small=${smallMedian}ms/10k; medium=${mediumMedian}ms/10k; large=${largeMedian}ms/1k")
  }

  test("perf: cache key construction (key derivation per query)") {
    val t = baseModel.withResultCache(ResultCache.inMemory(maxEntries = 4))
    val q = t.query(
      measures   = Seq("flight_count"),
      dimensions = Seq("carrier"),
      where      = Some(Predicate.Compare.Eq("carrier", "AA")),
    )
    // Warmup (miss + store)
    q.toDataFrame(spark)
    val median = medianMs { q.toDataFrame(spark).collect() }
    info(s"[perf] full cache hit (key compute + lookup + rebuild): median=${median}ms")
  }

  test("perf: in-memory cache write throughput (200 distinct keys, cap 100)") {
    val cache = ResultCache.inMemory(maxEntries = 100)
    val schema = StructType(Seq(StructField("x", IntegerType)))
    val rows   = Array.empty[Row]
    val median = medianMs {
      for (i <- 0 until 200) {
        cache.put(s"k$i", CachedResult(rows, schema))
      }
    }
    info(s"[perf] 200 puts with eviction (cap=100): median=${median}ms")
  }
}
