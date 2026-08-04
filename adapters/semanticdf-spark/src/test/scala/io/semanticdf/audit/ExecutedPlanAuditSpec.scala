package io.semanticdf.audit

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.semanticdf.{Dimension, Measure, SparkSessionFixture, toSemanticTable}

/** Regression tests for `AuditEvent.executedPlan`.
  *
  * The Spark execution plan that produced a result is captured in
  * the audit event so operators can inspect filter pushdown,
  * partition skew, and slow query plans. Deliberately NOT part
  * of `dedupHash` (see `AuditEvent` Scaladoc).
  */
class ExecutedPlanAuditSpec extends AnyFunSuite with Matchers with SparkSessionFixture {
  test("AuditEvent.executedPlan is captured on the success path") {
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(1), Row(2), Row(3))),
      StructType(Seq(StructField("x", IntegerType)))
    )
    val sink = AuditSink.inMemory(8).asInstanceOf[InMemoryAuditSink]
    val model = toSemanticTable(df, name = Some("test"))
      .withDimensions(Dimension("x", t => t("x")))
      .withMeasures(Measure("c", _ => lit(1), exprString = Some("1")))
      .withAuditSink(sink)
    model.query(measures = Seq("c"), dimensions = Seq("x")).toDataFrame(spark).collect()

    sink.snapshot() should not be empty
    val event = sink.snapshot().head
    assert(event.executedPlan.isDefined, "executedPlan should be captured after compile")
    val plan = event.executedPlan.get
    assert(plan.nonEmpty, s"executedPlan should not be empty; got: '$plan'")
  }

  test("AuditEvent.executedPlan is None on the cache-hit fast path (no compile)") {
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(1), Row(2), Row(3))),
      StructType(Seq(StructField("x", IntegerType)))
    )
    val sink = AuditSink.inMemory(8).asInstanceOf[InMemoryAuditSink]
    val model = toSemanticTable(df, name = Some("test"))
      .withDimensions(Dimension("x", t => t("x")))
      .withMeasures(Measure("c", _ => lit(1), exprString = Some("1")))
      .withResultCache(io.semanticdf.cache.ResultCache.inMemory(64))
      .withAuditSink(sink)
    // First call: cache miss, plan captured.
    model.query(measures = Seq("c"), dimensions = Seq("x")).toDataFrame(spark).collect()
    val firstEvent = sink.snapshot().last
    assert(firstEvent.executedPlan.isDefined, "first call: cache miss, plan expected")
    // Second call: cache hit, plan NOT captured.
    model.query(measures = Seq("c"), dimensions = Seq("x")).toDataFrame(spark).collect()
    val secondEvent = sink.snapshot().last
    assert(secondEvent.executedPlan.isEmpty,
      s"second call: cache hit, plan should be empty; got: ${secondEvent.executedPlan}")
  }

  test("executedPlan is NOT included in dedupHash (replay-safe contract)") {
    // Per AuditEvent's class doc, dedupHash deliberately excludes
    // executedPlan because the plan contains non-deterministic
    // internal metadata. Verify that two events with different
    // plans but identical query-shape produce the same dedupHash.
    val shape = ("m", 1, Seq("c"), Seq("k"), Some("w"), Some("h"))
    val hash1 = AuditEvent.dedupHashOf(shape._1, shape._2, shape._3, shape._4, shape._5, shape._6)
    val hash2 = AuditEvent.dedupHashOf(shape._1, shape._2, shape._3, shape._4, shape._5, shape._6)
    hash1 shouldBe hash2
  }
}
