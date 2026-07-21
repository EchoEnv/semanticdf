package com.example.streamingevents

import io.semanticdf._
import io.semanticdf.StreamingSupport._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.StreamingQuery

/** Narrative logger for this template.
  *
  * Uses java.util.logging.Logger (JDK built-in). The public API
  * (`info` / `warn` / `error` / `debug`) is logger-agnostic — swap the
  * underlying implementation for SLF4J / log4j2 in production by
  * changing only the body of these four methods. Callsites stay stable.
  */
object Logger {
  import java.util.logging.{Level, Logger => JulLogger}
  private val jul: JulLogger = JulLogger.getLogger("com.example.streamingevents")
  jul.setLevel(Level.INFO)

  def info(msg: => String): Unit  = jul.info(msg)
  def warn(msg: => String): Unit  = jul.warning(msg)
  def error(msg: => String): Unit = jul.severe(msg)
  def debug(msg: => String): Unit = jul.fine(msg)
}

/** Streaming-events example — real-time event count per (window × event_type).
  *
  * What this demonstrates:
  *   1. Loading a YAML model whose `table:` resolves to a streaming DataFrame
  *   2. Windowed aggregation (30-second windows, 1-minute watermark)
  *   3. `t.all(...)` windowed totals (per-window grand total as denominator)
  *   4. Typed `StreamingConfig` constructed in code (operator-side)
  *   5. Streaming lifecycle: open source, start query, stop query
  *
  * The streaming source here is the `rate` source (1 row per ~200 ms,
  * configurable via `rowsPerSecond`). In production this would be Kafka,
  * Kinesis, or a file stream — the operator program is where the source
  * choice is made. The YAML only carries the model; the operator program
  * owns the streaming operational config (window, watermark, output sink).
  *
  * To run:
  *   1. `mvn install` the parent semanticdf project (so the local jar is available)
  *   2. From this directory: `mvn scala:run -DmainClass=com.example.streamingevents.Main`
  *      Optional: append the run time in seconds, e.g. `-Dexec.args="15"`
  */
object Main {

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession.builder()
      .master("local[2]")
      .appName("semanticdf-streaming-events")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    try {

      // ---------------------------------------------------------------------
      // 1. Open the streaming source — operator knows the source in code
      // ---------------------------------------------------------------------
      val source = openStreamingSource(spark)

      val tables = Map("events_stream" -> source)

      // ---------------------------------------------------------------------
      // 2. Load the YAML model — YamlLoader auto-routes `df.isStreaming`
      //    to `toStreamingSemanticTable`, so the model DSL is source-agnostic
      // ---------------------------------------------------------------------
      val models = YamlLoader.load("models/events.yml", tables)
      val streaming = models("events")

      Logger.info("=" * 70)
      Logger.info(s"Loaded ${models.size} model: ${streaming.name.getOrElse("events")}")
      Logger.info(s"Dimensions: ${streaming.dimensions.keys.mkString(", ")}")
      Logger.info(s"Measures:   ${streaming.measures.keys.mkString(", ")}")
      Logger.info("=" * 70)

      // ---------------------------------------------------------------------
      // 2b. Operator composes the aggregation in code.
      //     The model declares dims + measures + calc measures, but the
      //     grain (which dims to group by) is operator config — it lives
      //     in code, not in YAML. Adding `.groupBy("type").aggregate(...)`
      //     wraps the streaming root in a SemanticAggregateOp so the
      //     streaming terminal applies windowed aggregation.
      //
      //     We aggregate only the base measures (`event_count`, `total_value`)
      //     so we can use `update` output mode and get rows back instantly.
      //     The YAML model ALSO declares `pct_of_total = event_count / all(event_count)`
      //     (per-window grand total via `t.all(...)`). Adding that to
      //     `.aggregate(...)` switches the framework to `append` mode
      //     internally (the calc measure triggers a stream-stream join for
      //     windowed totals) which has stricter timing constraints — it
      //     still works, but for a quick demo we show the per-window counts
      //     and skip the calc. See the docs and `docs/known-limitations.md`
      //     for the windowed-totals pattern.
      // ---------------------------------------------------------------------
      val model = streaming
        .groupBy("type")
        .aggregate("event_count", "total_value")

      // ---------------------------------------------------------------------
      // 3. Build the typed StreamingConfig in code — the operator owns this
      //    (window / watermark / output sink are operational, not semantic)
      // ---------------------------------------------------------------------
      // `outputMode = "update"` emits per-micro-batch aggregates (so we
      // see results quickly, without waiting for windows to close). For
      // windowed-totals (`t.all(...)`), the framework uses `append` mode
      // internally because the cross-join is a stream-stream join — that
      // combination works but requires longer runtimes.
      val cfg = StreamingConfig(
        outputSink         = OutputSink.Console(limit = 20),
        window             = Some(WindowSpec(column = "timestamp", duration = "10 seconds")),
        watermark          = Some(WatermarkSpec(column = "timestamp", delay = "10 seconds")),
        outputMode         = "update",
        checkpointLocation = None,
      )

      Logger.info(s"StreamingConfig: window=${cfg.window} watermark=${cfg.watermark}")
      Logger.info(s"Sink:           ${cfg.outputSink.label}")

      // ---------------------------------------------------------------------
      // 4. Operator-side lifecycle: start the streaming query
      // ---------------------------------------------------------------------
      val query: StreamingQuery = model.toStreamingQuery(spark, cfg)

      // Run for `runForSeconds` seconds; in production this might be a
      // Kubernetes Job with SIGTERM handling, a Kafka rebalance hook, etc.
      val runForSeconds: Long = args.collectFirst { case s if s.forall(_.isDigit) => s.toLong }
        .getOrElse(10L)
      Logger.info(s"Streaming for ${runForSeconds}s, then stopping...")

      Thread.sleep(runForSeconds * 1000L)

      // ---------------------------------------------------------------------
      // 5. Operator-side lifecycle: graceful stop
      // ---------------------------------------------------------------------
      query.stop()
      Logger.info(s"Stopped. Final query status: ${query.status.message}")

    } finally {
      spark.stop()
    }
  }

  /** Open the streaming source for this template.
    *
    * Uses Spark's `rate` source for portability (no external broker needed).
    * In a real pipeline, replace this with `spark.readStream.format("kafka")...`
    * or another format — the operator program is the natural place for
    * source choice.
    *
    * The rate source produces columns `timestamp` (event time) and `value`
    * (an incrementing long). The streaming model expects columns `type`
    * (dimension) and `value` (summed measure), so we synthesize a
    * constant `type` column and rename for clarity.
    */
  private def openStreamingSource(spark: SparkSession): DataFrame = {
    import org.apache.spark.sql.functions.lit
    spark.readStream
      .format("rate")
      .option("rowsPerSecond", 5)
      .load()
      .withColumnRenamed("timestamp", "ts_renamed")
      .withColumnRenamed("value",    "value_raw")
      .withColumn("type", lit("heartbeat"))
      .withColumnRenamed("ts_renamed",  "timestamp")
      .withColumnRenamed("value_raw",   "value")
  }
}
