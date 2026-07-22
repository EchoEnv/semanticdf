# `streaming-manifest-load` — load a pre-built streaming manifest + run a streaming query

A focused, minimal example of the **streaming half** of the
manifest-artifact workflow: load a pre-built JSON manifest and
reconstruct a `SemanticTable` whose root is a
`SemanticStreamingTableOp`, then run a streaming query against it.

## What this demonstrates

- Loading a streaming manifest from disk via `SemanticManifest.fromJson`
  with a `DataStream` (not a `DataFrame`) as the source — the
  `digest.isStreaming: true` flag tells the runtime to dispatch to
  `SemanticStreamingTableOp`
- Inspecting the loaded model via `ManifestMeta` (the `isStreaming`
  flag tells the operator which terminal to use)
- Building a `StreamingConfig` and calling `toStreamingQuery` — the
  operator-side `StreamingConfig` is NOT in the manifest (PR #124
  boundary; the manifest carries the model, the operator carries
  the runtime config)
- Windowed aggregation with watermark + update mode
- Lifecycle surfacing via `ManifestMeta.status` (deprecated warning
  path, not exercised here since the model is `published`)

## The workflow this exemplifies

```text
  Build (CI / deploy step)              Load (runtime / app)
  ──────────────────────────            ──────────────────────
  tools.Main manifest                    SemanticManifest.fromJson(
    --yaml events.yml                      manifests/events.json,
    --out manifests/                       streamingSource)
                                         → SemanticTable
  → manifests/events.json                 (SemanticStreamingTableOp)
                                          ↓
                                       toStreamingQuery(spark, cfg)
                                          → StreamingQuery
```

The two halves are designed to run in different processes: CI builds
the artifact; the streaming application loads it. The artifact is
portable — it can be stored in git, in S3, in a model registry, anywhere.
The streaming source itself is operator-side, by design.

## What the artifact contains

The checked-in `manifests/events.json` is a `v0.1.9-manifest` artifact
representing the `events` model from `examples/streaming-events`. The
manifest carries:

- Identity: `name=events`, `version=0`, `status=published`,
  `description`, `sourceTable=events_stream`
- 2 dimensions: `event_type` (categorical, on `type`), `timestamp_bucket`
  (time, on `timestamp`, `smallestTimeGrain=day`)
- 2 base measures: `event_count` (`count(1)`), `total_value` (`sum(value)`)
- 1 pre-aggregate filter: `require_known_event_type` (`type IS NOT NULL`)
- `digest.isStreaming: true` — the manifest's signal that this is a
  streaming model. The runtime uses it to dispatch to
  `SemanticStreamingTableOp` at `fromJson` time.

The manifest does **not** carry the streaming source itself, the
checkpoint location, the trigger interval, or the output mode. Those
are operator-side, by design (PR #124 boundary).

## Run it

```bash
# 1. Install the parent semanticdf project (one-time setup)
cd ../..
mvn install -DskipTests

# 2. Run the example
cd examples/streaming-manifest-load
mvn scala:run -DmainClass=com.example.streamingmanifestload.Main
```

Expected output (abridged):

```text
Manifest schema: v0.1.9-manifest, kind: semanticdf-model-manifest
Model: events  v0  status=published
Digest: 2 dims, 2 measures, 1 filters, streaming=true

Streaming query running for 12 seconds...
  batch 0: 10 input rows, 2 output rows
  batch 1: 10 input rows, 2 output rows
  ...

Most recent batch (event_type, event_count, total_value):
  batch 1: 10 input rows, 2 output rows
```

The 2 output rows per batch correspond to the (currently) one distinct
`event_type` value (`"heartbeat"`). The query uses `outputMode = Update`
so each batch only emits deltas; the final per-row state has the
running totals.

## Regenerate the manifest from the YAML source

The artifact is checked in. To regenerate after `events.yml` changes,
the simplest path is a small inline Scala runner (mirrors how the
batch `manifest-load` artifact is generated):

```scala
import io.semanticdf.{SemanticManifest, YamlLoader}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.TimestampType

object GenEvents {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[1]").getOrCreate()
    val source = spark.readStream.format("rate").option("rowsPerSecond", 5).load()
      .withColumn("type", lit("heartbeat"))
      .withColumn("timestamp", org.apache.spark.sql.functions.col("timestamp").cast(TimestampType))
    val models = YamlLoader.load("examples/streaming-events/models/events.yml",
      Map("events_stream" -> source))
    val json = SemanticManifest.toJson(models("events"), prettyPrint = true)
    java.nio.file.Files.write(
      java.nio.file.Paths.get("examples/streaming-manifest-load/src/main/resources/manifests/events.json"),
      json.getBytes("UTF-8"))
    spark.stop()
  }
}
```

`tools.Main manifest` only handles batch sources (it resolves `table:`
through `spark.table`, which doesn't work for streaming). For streaming
artifact generation, use the inline script above. (A future
`tools.Main manifest-streaming` variant would be a small addition if
many streaming artifacts need to be generated.)

## Why this example exists

The manifest recipe (`docs/design/manifest-artifact.md`) describes
the workflow in detail, and `SemanticManifestSpec` proves the
round-trip works. This example is the **streaming half** of that
proof: a 70-line `Main.scala` that loads a real streaming artifact
and runs a real streaming query against it.

The streaming-events example uses the **YAML loader**; this example
uses the **manifest** as the source of truth. Same model, two
loading paths, both end-to-end working.

## What it does NOT do

- **No `SemanticJoinOp` support.** Joined streaming models are
  anti-scope per the manifest recipe §10. (And joined models in
  general are a separate recipe — see
  `docs/design/joined-models-manifest.md`.)
- **No streaming-source metadata in the manifest.** The source
  itself (Kafka topic, Parquet stream, rate source, etc.) is
  operator-side, by design (PR #124). The manifest captures the
  MODEL, not the source.
- **No `transforms:` block.** The YAML's per-row `transforms:` is
  not carried by the manifest. (Separate recipe in
  `docs/design/manifest-transforms.md` — currently blocked pending
  a library change to `Transform.exprString`.)
- **No `WindowSpec` in the manifest.** Windowed aggregation is
  configured via `StreamingConfig` at query time, not at
  manifest-emit time.

## Files

```
examples/streaming-manifest-load/
├── pom.xml                                       # Maven config (cloned from manifest-load)
├── README.md                                     # this file
├── src/main/resources/manifests/
│   └── events.json                               # checked-in pre-built artifact
└── src/main/scala/com/example/streamingmanifestload/
    └── Main.scala                                 # ~120-line demo
```