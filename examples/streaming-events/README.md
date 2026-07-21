# semanticdf streaming-events example

A complete, working streaming example of [semanticdf](https://github.com/earendil/semanticdf) — a declarative semantic layer on Apache Spark.

This template shows how to define a streaming model in **YAML** (no Scala required for the model itself) and run it from a regular Spark program. Windowed aggregation, watermarks, and percent-of-total are expressed in the model; lifecycle (when to start / how long to run / graceful stop) is owned by the operator program.

---

## What you get

```
semanticdf-streaming-events/
├── README.md                                     ← you are here
├── pom.xml                                       ← Maven build (depends on semanticdf)
├── models/
│   └── events.yml                                ← streaming model (windowed, percent-of-total)
└── src/main/scala/com/example/streamingevents/
    └── Main.scala                                ← the app: opens stream, loads model, runs query
```

---

## Run it (5 minutes)

### Prerequisites

- JDK 17 (semanticdf requires Java 17+)
- Maven 3.9+
- Apache Spark 3.5.x or 4.x (pulled in automatically by Maven)

### Step 1: install semanticdf locally

The example depends on semanticdf, which (for now) isn't on Maven Central. Build and install it to your local Maven repo from the parent project:

```bash
cd /path/to/semanticdf
mvn install -DskipTests
```

This puts `io.semanticdf:semanticdf_2.13:0.1.8` in `~/.m2/repository`.

### Step 2: run the example

```bash
cd /path/to/semanticdf/examples/streaming-events
mvn scala:run -DmainClass=com.example.streamingevents.Main
# Optional: tell it how long to run
mvn scala:run -DmainClass=com.example.streamingevents.Main -Dexec.args="15"
```

You should see output like (one batch print every 6 seconds, 6 rows each):

```
======================================================================
Loaded 1 model: events
Dimensions: event_type, timestamp_bucket
Measures:   event_count, total_value
======================================================================
StreamingConfig: window=Some(WindowSpec(timestamp,30 seconds)) watermark=Some(WatermarkSpec(timestamp,1 minute))
Sink:           Console(limit=6)
Streaming for 10s, then stopping...
-------------------------------------------
Batch: 0
-------------------------------------------
+----+----------+--------+------------+
|type|window    |event_ct|total_value |
+----+----------+--------+------------+
|heartbeat|[28s,58s]|3       |42          |
|heartbeat|[28s,58s]|3       |42          |
...

Stopped. Final query status: Stopped
```

---

## What the queries show

For streaming, the "queries" are the lifecycle steps that the operator program walks through:

| Step | What it demonstrates |
|---|---|
| **Q1** | Open the streaming source — operator owns `readStream` format choice (here: `rate`; in production: Kafka, Kinesis, file stream, etc.) |
| **Q2** | Load the YAML model — `YamlLoader.load` auto-routes `df.isStreaming == true` to `toStreamingSemanticTable` (same loader API as batch) |
| **Q3** | Operator composes the aggregation in code — `.groupBy("type").aggregate("event_count", "total_value")` wraps the streaming root in a `SemanticAggregateOp` so the streaming terminal applies windowed aggregation. The group keys live in operator code (not the model) — that's the boundary. |
| **Q4** | Build the typed `StreamingConfig` — window / watermark / output sink in code |
| **Q5** | Operator lifecycle — start the streaming query, run for N seconds |
| **Q6** | Operator lifecycle — graceful `.stop()` |
| Output | Windowed aggregation: one row per (window, event_type), emitting live in `update` mode |

See [Calculated measures with `t.all(...)`](#calculated-measures-with-tall) below for the windowed-totals pattern (per-window grand total as denominator).

---

## Customize it

### Change the source

Replace the `rate` source in `Main.openStreamingSource` with a Kafka topic, Kinesis stream, or file source. As long as the resulting DataFrame exposes the columns the YAML references (`timestamp`, `type`, `value`), no model changes are needed.

```scala
private def openStreamingSource(spark: SparkSession): DataFrame = {
  spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker:9092")
    .option("subscribe", "events")
    .load()
    // map Kafka columns to your model schema here
}
```

### Change the window

Edit `Main.scala`'s `StreamingConfig` — windows live in code, not YAML:

```scala
val cfg = StreamingConfig(
  outputSink = OutputSink.Console(limit = 20),
  window     = Some(WindowSpec(column = "timestamp", duration = "5 minutes")),
  watermark  = Some(WatermarkSpec(column = "timestamp", delay = "15 minutes")),
  outputMode = "update",
)
```

### Add a measure

Edit `models/events.yml`:

```yaml
measures:
  event_count:
    expr: "count(1)"
    description: "Number of events (rows) arriving in the window"
    metadata:
      owner: streaming-team
      unit: count

  # New measure:
  distinct_types:
    expr: "count(distinct type)"
    description: "Number of distinct event types seen in this window"
    metadata:
      owner: streaming-team
      unit: count
```

The new measure becomes queryable immediately.

---

## Model files: what they produce

`models/events.yml` declares one model named `events`:

- **table**: `events_stream` — the name your operator program registers the streaming DataFrame under
- **dimensions**: `event_type` (categorical), `timestamp_bucket` (time dimension, second-grain)
- **measures**: `event_count`, `total_value` (base aggregations)

> For a `calculated_measures` example with `t.all(...)` (windowed grand-total denominator), see [Calculated measures with `t.all(...)`](#calculated-measures-with-tall) below.

The streaming-side adaption (`isStreaming == true` → `toStreamingSemanticTable`) happens automatically inside `YamlLoader.load`. Streaming and batch share the same DSL — only the factory differs.

---

## The boundary: semantic layer vs operator lifecycle

This template makes a **deliberate distinction**:

| What | Where | Why |
|---|---|---|
| Dimensions, measures, calc measures | **YAML** (the model) | Semantic logic — owned by the data team |
| Pre-join filters, metadata, descriptions | **YAML** | Semantic hygiene — owned by the data team |
| Window, watermark, output sink | **Operator code** (Scala) | Operational config — owned by the runtime |
| When the stream runs, how long, how it's stopped | **Operator code** | Lifecycle — owned by the platform / deployment |

The semantic layer doesn't try to control the stream. The operator program doesn't try to redefine the model. Each layer keeps what it owns.

The `streaming:` block in YAML is intentionally **not** supported — it would blur this boundary by putting operational config in the semantic file. If you find yourself wanting it, that's a signal that the config belongs in code (or in a different file layered on top of the YAML).

## Calculated measures with `t.all(...)`

Streaming supports the same `t.all(...)` pattern as batch — a calc measure that
re-evaluates a base measure at zero grain (per-window grand total). Example
declaration:

```yaml
calculated_measures:
  pct_of_total:
    expr: "event_count / all(event_count)"
    description: "Fraction of all events in this window that fell into this event_type"
```

Aggregating `pct_of_total` switches the framework to a windowed-totals
cross-join under the hood, which:

- Forces `outputMode = "append"` (Spark only allows stream-stream joins in append mode)
- Times how quickly results emit: a row appears after its window's watermark has passed (≈ `window.duration + watermark.delay`)

For the example runtime (default 10 seconds) you won't see rows when
aggregating `pct_of_total` — the windows never close. To run the
windowed-totals pattern, set:

```scala
val cfg = StreamingConfig(
  outputSink         = OutputSink.Console(limit = 20),
  window             = Some(WindowSpec("timestamp", "10 seconds")),
  watermark          = Some(WatermarkSpec("timestamp", "10 seconds")),
  outputMode         = "append",           // required for windowed-totals
  checkpointLocation = None,
)
val model = streaming
  .groupBy("type")
  .aggregate("event_count", "total_value", "pct_of_total")  // include the calc
```

...and run for `~25+` seconds (`-Dexec.args="30"`) so windows close and emit.

---

## Going to production

The `OutputSink.Console` sink in this example is for visibility while running. Real deployments use:

```scala
OutputSink.Parquet("s3://bucket/events/agg/")            // batch-style sink
OutputSink.Csv("hdfs:///events/agg/", checkpoint = true) // file sink
OutputSink.Custom("kafka", df => /* your writer */)      // any sink you want
```

In production the operator program is typically a Spark Structured Streaming job, a Kubernetes `Deployment`, or a serverless function — same `model.toStreamingQuery(spark, cfg)` call, different lifecycle wrapper.

Checkpointing default is a per-query temp dir; set `checkpointLocation = Some("s3://bucket/checkpoints/events/")` for durable restart offsets.

---

## What's next

- Add the same model to the MCP server and query it from an LLM agent with `list_models` / `describe_model` / `query_model` tools — semanticdf's `MCP` surface works the same on streaming models.
- See [`examples/starter`](../starter/README.md) for batch-side fundamentals (dims / measures / calcs / joins).
- See [`examples/hospital`](../hospital/README.md) for a worked batch example with multiple models and joins.
- See [`examples/window-analytics`](../window-analytics/README.md) for batch window functions.

## Get help

- [Main semanticdf docs](../../docs/guide.md)
- [Known limitations](../../docs/known-limitations.md) — what's supported in streaming vs not
- [GLOSSARY](../../docs/GLOSSARY.md) — `SemanticStreamingTableOp`, `toStreamingQuery`, `StreamingConfig` etc.
- File an issue: https://github.com/EchoEnv/semanticdf/issues
