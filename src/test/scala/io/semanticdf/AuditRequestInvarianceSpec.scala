package io.semanticdf

import io.semanticdf.audit.AuditSink
import io.semanticdf.cache.ResultCache
import io.semanticdf.predicate.Predicate
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.lit

/** Pins the `auditRequest` co-invariance with `auditSink` /
  * `resultCache`: both systems derive their keys/hashes from the
  * request shape captured by `query()`. The library enforces the
  * invariant by (a) throwing `IllegalStateException` at
  * `toDataFrame` when either is set but `query()` was never called,
  * and (b) clearing `resultCache` whenever `auditRequest` is
  * invalidated by a post-query shape-changer (the Scaladoc on
  * `invalidateAuditRequest` documents the pairing).
  */
class AuditRequestInvarianceSpec extends AnyFunSuite with Matchers with SparkSessionFixture {

  private def baseModel(spark: SparkSession): SemanticTable =
    toSemanticTable(spark.range(10).toDF("k"), name = Some("m"))
      .withDimensions(Dimension("k", t => t("k")))
      .withMeasures(Measure("n", t => count(lit(1))))

  test("withResultCache + toDataFrame without query() throws IllegalStateException") {
    val m = baseModel(spark).withResultCache(ResultCache.inMemory())
    val ex = intercept[IllegalStateException](m.toDataFrame(spark))
    ex.getMessage should include ("resultCache")
    ex.getMessage should include ("query()")
    ex.getMessage should include ("query(measures = Seq")
    ex.getMessage should include ("dimensions = Seq")
  }

  test("withAuditSink + toDataFrame without query() throws IllegalStateException") {
    val m = baseModel(spark).withAuditSink(AuditSink.inMemory())
    val ex = intercept[IllegalStateException](m.toDataFrame(spark))
    ex.getMessage should include ("auditSink")
    ex.getMessage should include ("query()")
  }

  test("auditSink + resultCache + toDataFrame without query(): error names both offenders") {
    val m = baseModel(spark)
      .withAuditSink(AuditSink.inMemory())
      .withResultCache(ResultCache.inMemory())
    val ex = intercept[IllegalStateException](m.toDataFrame(spark))
    ex.getMessage should include ("auditSink and resultCache")
    ex.getMessage should include ("are")
  }

  test("query().where() clears resultCache (post-query shape-changer invalidation)") {
    val m = baseModel(spark)
      .withResultCache(ResultCache.inMemory())
      .query(measures = Seq("n"), dimensions = Seq("k"))
    val reshaped = m.where(Predicate.Compare("gt", "k", 0))
    reshaped.resultCache shouldBe None
  }

  test("query().orderBy().limit() clears resultCache") {
    val m = baseModel(spark)
      .withResultCache(ResultCache.inMemory())
      .query(measures = Seq("n"), dimensions = Seq("k"))
    val reshaped = m.orderBy(SortKey.desc("n")).limit(10)
    reshaped.resultCache shouldBe None
  }

  test("query().having() clears resultCache") {
    val m = baseModel(spark)
      .withResultCache(ResultCache.inMemory())
      .query(measures = Seq("n"), dimensions = Seq("k"))
    val reshaped = m.having(Predicate.Compare("gt", "n", 0))
    reshaped.resultCache shouldBe None
  }

  test("query().withDimensions(...) clears resultCache (shape-changer invalidation)") {
    // Adding or replacing dimensions changes the result shape; the
    // cached key would no longer match a later identical request.
    // Pre-fix, withDimensions preserved both auditRequest and
    // resultCache, exposing a stale-hit risk on cache.
    val cache = ResultCache.inMemory()
    val m = baseModel(spark)
      .withResultCache(cache)
      .query(measures = Seq("n"), dimensions = Seq("k"))
    val reshaped = m.withDimensions(Dimension("v", t => t("v")))
    reshaped.resultCache shouldBe None
    reshaped.auditRequest shouldBe None
  }

  test("query().atTimeGrain() clears resultCache (regression for H1)") {
    val cache = ResultCache.inMemory()
    val df = spark.range(10).toDF("k").withColumn("ts", org.apache.spark.sql.functions.lit("2024-01-01"))
    val m = toSemanticTable(df, name = Some("m"))
      .withDimensions(
        Dimension("k", t => t("k")),
        Dimension.time("ts", t => t("ts"), smallestTimeGrain = Some("day")),
      )
      .withMeasures(Measure("n", t => count(lit(1))))
      .withResultCache(cache)
      .query(measures = Seq("n"), dimensions = Seq("k"))
    val reshaped = m.atTimeGrain("ts", "day")
    reshaped.resultCache shouldBe None
    reshaped.auditRequest shouldBe None
  }

  test("query().toDataFrame(): resultCache survives (happy path)") {
    val cache = ResultCache.inMemory()
    val m = baseModel(spark).withResultCache(cache)
      .query(measures = Seq("n"), dimensions = Seq("k"))
    m.resultCache shouldBe Some(cache)
    m.toDataFrame(spark).count() shouldBe 10
  }
}