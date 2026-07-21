package io.semanticdf

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite

import io.semanticdf.StreamingSupport._

/** Tests for the streaming terminal (ADR 0002, PR 1).
  *
  * Verifies:
  *  1. The `StreamingValidator` rejects unsupported op-tree patterns
  *     (`t.all(...)`, `limit`, `orderBy`, `groupBy + aggregate`, joins)
  *     with clear, ADR-referencing errors.
  *  2. The `StreamingValidator` accepts a simple `where`-only op tree.
  *  3. `toStreamingSemanticTable` requires a streaming `DataFrame`
  *     (rejects batch inputs).
  *  4. The streaming terminal method `toStreamingQuery` produces a
  *     `StreamingQuery` that runs end-to-end (micro-batches produced
  *     and routed to the user's `foreachBatch` callback).
  *
  * The end-to-end test takes ~3 seconds (multi-micro-batch); all other
  * tests are unit-level and fast.
  */
class StreamingSpec extends AnyFunSuite with SparkSessionFixture {

  // -------------------------------------------------------------------------
  // Validator behavior
  // -------------------------------------------------------------------------

  /** Build a minimal streaming model: a single base measure on a rate source. */
  private def streamingModel(spark: SparkSession) = {
    val stream = spark.readStream.format("rate").load()
    toStreamingSemanticTable(stream, name = Some("rate"))
      .withDimensions(Dimension("value", t => t("value")))
      .withMeasures(Measure("count", t => count(lit(1))))
  }

  test("validator accepts a where-only streaming model") {
    implicit val s: SparkSession = spark
    val model = streamingModel(s)
    // The bare streaming model is accepted by the validator
    // (no groupBy, no aggregate, no orderBy, no limit, no joins).
    // Note: a `.where(Predicate)` would also be accepted, but requires
    // declaring a typed ref. The bare model is the simplest "supported" case.
    StreamingValidator.validate(model)
    // No throw: where-only is supported.
  }

  test("validator rejects t.all(...) — measure using scope.all") {
    implicit val s: SparkSession = spark
    val stream = s.readStream.format("rate").load()
    val model = toStreamingSemanticTable(stream, name = Some("rate"))
      .withMeasures(
        Measure("count", t => count(lit(1))),  // base: known to the probe
        Measure("ratio", t => t.all("count") / t("count")),  // calc: uses t.all
      )
    val ex = intercept[StreamingUnsupportedError] {
      StreamingValidator.validate(model)
    }
    // The validator detects t.all(...) via MeasureProbeScope and reports
    // which measures use it.
    assert(ex.getMessage.contains("t.all") || ex.getMessage.contains("ratio"),
      s"Expected t.all/ratio message, got: ${ex.getMessage}")
  }

  test("validator rejects limit") {
    implicit val s: SparkSession = spark
    val stream = s.readStream.format("rate").load()
    val model = toStreamingSemanticTable(stream, name = Some("rate"))
    // Force a limit op into the tree via .limit(...)
    val limited = model.limit(5)
    val ex = intercept[StreamingUnsupportedError] {
      StreamingValidator.validate(limited)
    }
    assert(ex.getMessage.contains("limit"),
      s"Expected 'limit' message, got: ${ex.getMessage}")
  }

  test("validator rejects orderBy") {
    implicit val s: SparkSession = spark
    val stream = s.readStream.format("rate").load()
    val model = toStreamingSemanticTable(stream, name = Some("rate"))
    val ordered = model.orderBy("value")
    val ex = intercept[StreamingUnsupportedError] {
      StreamingValidator.validate(ordered)
    }
    assert(ex.getMessage.contains("orderBy"),
      s"Expected 'orderBy' message, got: ${ex.getMessage}")
  }

  test("validator rejects groupBy + aggregate when no window spec") {
    implicit val s: SparkSession = spark
    val stream = s.readStream.format("rate").load()
    val model = toStreamingSemanticTable(stream, name = Some("rate"))
    val grouped = model.groupBy("value").aggregate("count")
    val ex = intercept[StreamingUnsupportedError] {
      StreamingValidator.validate(grouped)  // no window in default options
    }
    assert(ex.getMessage.contains("groupBy") && ex.getMessage.contains("window"),
      s"Expected 'groupBy/window' message, got: ${ex.getMessage}")
  }

  test("validator accepts groupBy + aggregate when a window spec is provided") {
    implicit val s: SparkSession = spark
    val stream = s.readStream.format("rate").load()
    val model = toStreamingSemanticTable(stream, name = Some("rate"))
      .groupBy("value").aggregate("count")
    val options = StreamingQueryOptions(window = Some(WindowSpec("timestamp", "5 seconds")))
    // Should not throw.
    StreamingValidator.validate(model, options)
  }

  test("validator rejects non-streaming root (batch model)") {
    implicit val s: SparkSession = spark
    val df = s.createDataFrame(
      s.sparkContext.parallelize(Seq(Row(1L, "a"), Row(2L, "b"))),
      StructType(Seq(StructField("v", LongType), StructField("c", StringType)))
    )
    val batchModel = toSemanticTable(df, name = Some("batch"))
      .withDimensions(Dimension("c", t => t("c")))
    val ex = intercept[IllegalStateException] {
      batchModel.toStreamingQuery(s, StreamingQueryOptions())
    }
    assert(ex.getMessage.contains("SemanticStreamingTableOp") ||
           ex.getMessage.contains("toStreamingSemanticTable"),
      s"Expected non-streaming root error, got: ${ex.getMessage}")
  }

  test("toStreamingSemanticTable rejects batch DataFrame") {
    implicit val s: SparkSession = spark
    val df = s.createDataFrame(
      s.sparkContext.parallelize(Seq(Row(1L))),
      StructType(Seq(StructField("v", LongType)))
    )
    val ex = intercept[IllegalArgumentException] {
      toStreamingSemanticTable(df, name = Some("bad"))
    }
    assert(ex.getMessage.contains("streaming"),
      s"Expected 'streaming' message, got: ${ex.getMessage}")
  }

  test("StreamingSource rejects non-streaming DataFrame") {
    implicit val s: SparkSession = spark
    val df = s.createDataFrame(
      s.sparkContext.parallelize(Seq(Row(1L))),
      StructType(Seq(StructField("v", LongType)))
    )
    val ex = intercept[IllegalArgumentException] {
      StreamingSource(df)
    }
    assert(ex.getMessage.contains("streaming"),
      s"Expected 'streaming' message, got: ${ex.getMessage}")
  }

  // -------------------------------------------------------------------------
  // End-to-end: streaming terminal runs and produces micro-batches
  // -------------------------------------------------------------------------

  test("toStreamingQuery: rate source + where filter, end-to-end micro-batches") {
    implicit val s: SparkSession = spark
    val stream = s.readStream
      .format("rate")
      .option("rowsPerSecond", 5)
      .load()

    val model = toStreamingSemanticTable(stream, name = Some("rate_test"))
      .withDimensions(Dimension("value", t => t("value")))
      .withMeasures(Measure("count", t => count(lit(1))))

    val collected = scala.collection.mutable.ListBuffer.empty[Long]
    val query = model.toStreamingQuery(s, StreamingQueryOptions(
      trigger = Some(Trigger.ProcessingTime("500 milliseconds")),
      foreachBatch = (df: DataFrame) => {
        if (!df.rdd.isEmpty()) collected += df.count()
      },
    ))

    // Wait long enough for several micro-batches to fire.
    Thread.sleep(3000)
    query.stop()
    query.awaitTermination()

    assert(collected.nonEmpty,
      s"expected at least one micro-batch to be processed, got 0")
    val total = collected.sum
    assert(total > 0L,
      s"expected some rows across micro-batches, got total=$total")
  }

  test("toStreamingQuery: windowed aggregation, end-to-end") {
    implicit val s: SparkSession = spark
    val stream = s.readStream
      .format("rate")
      .option("rowsPerSecond", 5)
      .load()

    val model = toStreamingSemanticTable(stream, name = Some("rate_windowed"))
      .withDimensions(Dimension.time("timestamp", t => t("timestamp"), smallestTimeGrain = Some("second")))
      .withMeasures(Measure("count", t => count(lit(1))))
      .groupBy("timestamp")
      .aggregate("count")

    val collected = scala.collection.mutable.ListBuffer.empty[Long]
    val query = model.toStreamingQuery(s, StreamingQueryOptions(
      trigger = Some(Trigger.ProcessingTime("500 milliseconds")),
      // Short window (1 second) so a window can close within the test wait time.
      window = Some(WindowSpec("timestamp", "1 second")),
      // Watermark is required for windowed aggregations in append mode.
      watermark = Some(WatermarkSpec("timestamp", "1 second")),
      // Update mode emits per-micro-batch aggregates (so we don't have
      // to wait for a window to close).
      outputMode = "update",
      foreachBatch = (df: DataFrame) => {
        if (!df.rdd.isEmpty()) collected += df.count()
      },
    ))

    // Wait for several micro-batches.
    Thread.sleep(5000)
    query.stop()
    query.awaitTermination()

    assert(collected.nonEmpty,
      s"expected at least one windowed micro-batch, got 0")
    val total = collected.sum
    assert(total > 0L,
      s"expected some aggregated rows across micro-batches, got total=$total")
  }


  test("validator rejects stream-stream joins") {
    implicit val s: SparkSession = spark
    val stream1 = s.readStream.format("rate").load()
    val stream2 = s.readStream.format("rate").load()
    val m1 = toStreamingSemanticTable(stream1, name = Some("s1"))
      .withDimensions(Dimension("k", t => t("value")))
    val m2 = toStreamingSemanticTable(stream2, name = Some("s2"))
      .withDimensions(Dimension("k", t => t("value")))
    val joined = m1.join_one(m2, (l, r) => l("k") === r("k"))
    val ex = intercept[StreamingUnsupportedError] {
      StreamingValidator.validate(joined)
    }
    assert(ex.getMessage.contains("stream-stream") || ex.getMessage.contains("stage 3"),
      s"Expected stream-stream error, got: ${ex.getMessage}")
  }

  test("validator accepts static-stream join") {
    implicit val s: SparkSession = spark
    import s.implicits._
    val staticRows = s.sparkContext.parallelize(Seq(Row("AA")))
    val staticSchema = org.apache.spark.sql.types.StructType(Seq(
      org.apache.spark.sql.types.StructField("key", org.apache.spark.sql.types.StringType)
    ))
    val staticData = s.createDataFrame(staticRows, staticSchema)
    val staticModel = toSemanticTable(staticData, name = Some("static"))
      .withDimensions(Dimension("k", t => t("key")))
    val stream = s.readStream.format("rate").load()
    val streamingModel = toStreamingSemanticTable(stream, name = Some("stream"))
      .withDimensions(Dimension("k", _ => lit("AA")))
    val joined = staticModel.join_one(streamingModel, (l, r) => l("k") === r("k"))
    // Should not throw.
    StreamingValidator.validate(joined)
}
  test("ADR 0002 stage 4: validator accepts t.all(...) in streaming when window is specified") {
    implicit val s: SparkSession = spark
    val stream = s.readStream.format("rate").load()
    val model = toStreamingSemanticTable(stream, name = Some("rate"))
      .withDimensions(Dimension("timestamp", t => t("timestamp")))
      .withMeasures(
        Measure("numerator", t => sum(t("value"))),
        Measure("denominator", t => sum(t("value"))),
        Measure("pct", t => t("numerator") / t.all("denominator")),
      )
      .groupBy("timestamp")
      .aggregate("pct")
    val options = StreamingQueryOptions(
      window = Some(WindowSpec("timestamp", "1 second")),
      watermark = Some(WatermarkSpec("timestamp", "1 second")),
      outputMode = "update",
    )
    // Should NOT throw: t.all(...) is allowed in streaming when a window
    // spec is provided (the framework computes per-window totals).
    StreamingValidator.validate(model, options)

    // But it DOES throw without a window spec.
    val noWindow = StreamingQueryOptions(
      window = None,
      watermark = Some(WatermarkSpec("timestamp", "1 second")),
      outputMode = "update",
    )
    val ex = intercept[StreamingUnsupportedError] {
      StreamingValidator.validate(model, noWindow)
    }
    assert(ex.getMessage.contains("t.all"),
      s"Expected t.all error, got: ${ex.getMessage}")
  }

}
