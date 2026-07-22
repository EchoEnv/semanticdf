package com.example.streamingmanifestload

import io.semanticdf.SemanticManifest

import scala.io.Source

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.types.TimestampType

/** Demo: load a pre-built `SemanticManifest` JSON artifact for a streaming
  * model and run a streaming query against it.
  *
  * This is the streaming analog of the existing `manifest-load` example
  * (PR #139). The checked-in artifact at `manifests/events.json` was
  * built from `examples/streaming-events/models/events.yml`. It carries:
  *   - 2 dimensions: `event_type`, `timestamp_bucket` (time)
  *   - 2 base measures: `event_count`, `total_value`
  *   - 1 pre-aggregate filter: `require_known_event_type` (drops null `type`)
  *   - `digest.isStreaming: true` — the manifest's signal that this is a
  *     streaming model. The runtime uses it to dispatch to
  *     `SemanticStreamingTableOp` at `fromJson` time.
  *
  * The streaming source here is the `rate` source enriched with a `type`
  * column so it matches the model's expected schema. In production this
  * would be Kafka, Kinesis, or a file stream — the operator program is
  * the natural place for the source choice.
  *
  * To run:
  *   1. `mvn install` the parent semanticdf project
  *   2. From this directory: `mvn scala:run -DmainClass=com.example.streamingmanifestload.Main`
  *
  * What this demonstrates:
  *   - Loading a streaming manifest from disk via `SemanticManifest.fromJson`
  *     with a `DataStream` (not a `DataFrame`) as the source
  *   - Inspecting the loaded model via `ManifestMeta` (the `isStreaming`
  *     flag tells the operator which terminal to use)
  *   - Building a `StreamingConfig` and calling `toStreamingQuery` —
  *     the operator-side `StreamingConfig` is NOT in the manifest
  *     (PR #124 boundary; the manifest carries the model, the operator
  *     carries the runtime config)
  *   - Windowed aggregation with watermark + update mode
  *   - Lifecycle surfacing via `ManifestMeta.status` */
object Main {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("semanticdf-streaming-manifest-load")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try {
      // -- 1. Load the manifest JSON from the classpath (manifests/) ------
      val manifestJson = Source.fromResource("manifests/events.json").mkString

      // -- 2. Inspect the manifest metadata (no Spark needed) --------------
      val meta = SemanticManifest.parseMeta(manifestJson)
      println(s"Manifest schema: ${meta.schemaVersion}, kind: ${meta.kind}")
      println(s"Model: ${meta.modelName.getOrElse("?")}  v${meta.version}  status=${meta.status}")
      println(s"Digest: ${meta.dimensions} dims, ${meta.measures} measures, " +
        s"${meta.filters} filters, streaming=${meta.isStreaming}")
      if (!meta.isStreaming) {
        System.err.println("FATAL: this manifest is not a streaming model — use the batch manifest-load example instead")
        sys.exit(1)
      }
      println()

      // -- 3. Open the streaming source ----------------------------------
      // The manifest is source-agnostic — it only carries the model's
      // static definition. The source is operator-side, by design
      // (PR #124 boundary). For this self-contained example we use the
      // `rate` source enriched with a `type` column so it matches the
      // events.yml schema (timestamp, type, value).
      val source = openStreamingSource(spark)

      // -- 4. Reconstruct the streaming SemanticTable from the manifest ----
      // `fromJson` checks the `isStreaming` digest flag and produces a
      // SemanticStreamingTableOp (not a SemanticTableOp). The source
      // type is the only difference vs the batch manifest-load example.
      val model = SemanticManifest.fromJson(manifestJson, source)

      // -- 5. Run the streaming query -----------------------------------
      // Explicit groupBy + aggregate. The aggregation path in
      // `toStreamingQuery` matches only a root SemanticStreamingTableOp;
      // without an explicit aggregation the query falls through to the
      // raw-batch path (see the recipe's BLOCK feedback in
      // docs/design/REVIEW-FEEDBACK.md).
      val query: StreamingQuery = model
        // The streaming terminal's groupBy uses SOURCE column names,
        // not dim names. The events model's `event_type` dim has
        // `expr: type`, so we group by `type` (the source column).
        // This mirrors what the streaming-events example does.
        .groupBy("type")
        .aggregate("event_count", "total_value")
        .toStreamingQuery(
          spark,
          io.semanticdf.StreamingSupport.StreamingQueryOptions(
            trigger = Some(Trigger.ProcessingTime("2 seconds")),
            outputMode = "update",
            // The streaming validator requires a WindowSpec when the
            // model uses groupBy + aggregate. The 10s window matches
            // the rate source's row rate (5 rows/s ≈ 50 rows/window) and
            // the example's 12s runtime.
            window = Some(io.semanticdf.StreamingSupport.WindowSpec("timestamp", "10 seconds")),
            // Watermark bounds event-time lateness; 5s matches the
            // 2s trigger comfortably without dropping valid data.
            watermark = Some(io.semanticdf.StreamingSupport.WatermarkSpec("timestamp", "5 seconds")),
          ),
        )

      // Run long enough that at least one micro-batch completes (the
      // `rate` source emits 5 rows/s, and the trigger fires every 2s).
      // 12 seconds is enough to see multiple batches reliably.
      println("Streaming query running for 12 seconds...")
      try {
        query.awaitTermination(12000)
      } catch {
        case _: org.apache.spark.sql.streaming.StreamingQueryException =>
          // Expected — awaitTermination throws when the query is stopped
          // (a clean exit via .stop() raises this).
      }
      println()

      // -- 6. Print the most recent batch's progress ---------------------
      println("Most recent batch:")
      Option(query.lastProgress).foreach { p =>
        println(s"  batchId=${p.batchId}  numInputRows=${p.numInputRows}  " +
          s"batchDurationMs=${p.batchDuration}")
      }
    } finally {
      // Stop the streaming query cleanly.
      spark.streams.active.foreach(_.stop())
      spark.stop()
    }
  }

  /** Open the streaming source for this example. Mirrors the pattern in
    * `examples/streaming-events/Main.scala` — the `rate` source enriched
    * to match the events.yml schema. In a real pipeline, replace this
    * with `spark.readStream.format("kafka")...` or another format. */
  private def openStreamingSource(spark: SparkSession): DataFrame = {
    import org.apache.spark.sql.functions.lit
    spark.readStream
      .format("rate")
      .option("rowsPerSecond", 5)
      .load()
      // `rate` produces `timestamp, value`. The events model expects
      // a `type` column. We add one with a constant value so the
      // streaming source matches the manifest's schema.
      .withColumn("type", lit("heartbeat"))
      // Cast the timestamp to a proper TimestampType for the time dim
      // (the events model has `timestamp_bucket` with smallest_time_grain=day).
      .withColumn("timestamp", org.apache.spark.sql.functions.col("timestamp").cast(TimestampType))
  }
}