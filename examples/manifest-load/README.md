# `manifest-load` — load a pre-built SemanticManifest JSON artifact

A focused, minimal example of the manifest-artifact workflow's **second
half**: load a pre-built JSON manifest and reconstruct a `SemanticTable`
at runtime, ready to query.

## What this demonstrates

- Loading a manifest JSON from disk via `SemanticManifest.fromJson`
- Inspecting the manifest's identity + digest header via `ManifestMeta`
  (no Spark needed — fast pre-flight check)
- Reconstructing a `SemanticTable` from the manifest + a source `DataFrame`
- Running a base-measure query against the reconstructed model
- Lifecycle surfacing: the manifest's `status` field is exposed so
  consumers can route on `Draft` / `Published` / `Deprecated`

  The example uses base measures (`order_count`, `order_amount`).
  Calc measures (`avg_ship_days`, `on_time_rate`) are loaded as
  metadata but throw on query — see "What it does NOT do" below.

## The workflow this exemplifies

```text
  Build (CI / deploy step)              Load (runtime / app)
  ──────────────────────────            ─────────────────────
  tools.Main manifest                    SemanticManifest.fromJson(
    --yaml orders.yml                       manifests/orders.json,
    --out manifests/                        sourceDataFrame)
                                          → SemanticTable
  → manifests/orders.json
```

The two halves are designed to run in different processes: CI builds
the artifact; the application loads it. The artifact is portable —
it can be stored in git, in S3, in a model registry, anywhere. The
application does not need access to the YAML loader or the original
source-of-truth.

## What the artifact contains

The checked-in `manifests/orders.json` is a `v0.1.9-manifest` artifact
representing the `orders` model from `examples/operations-analytics`.
The manifest carries:

- Identity: `name`, `version`, `status`, `description`, `sourceTable`
- 4 dimensions (`order_id`, `customer_id`, `order_date` (time), `status`)
- 6 measures (4 base, 2 calc — `on_time_rate` and `avg_ship_days`)
- 0 joins, 0 filters (single-table model — the manifest's anti-scope
  is joined models, per the recipe §10)

The manifest does **not** carry computed output. The data lives in
the operator's pipeline (here, a CSV; in production, a Kafka topic or
Parquet table).

## Run it

```bash
# 1. Install the parent semanticdf project (one-time setup)
cd ../..
mvn install -DskipTests

# 2. Run the example
cd examples/manifest-load
mvn scala:run -DmainClass=com.example.manifestload.Main
```

Expected output (abridged):

```text
Manifest schema: v0.1.9-manifest, kind: semanticdf-model-manifest
Model: orders  v0  status=published
Digest: 4 dims, 6 measures (2 calc), 0 filters, joins=0

Q1 — order count + total amount by status:
| status  |order_count|order_amount        |
| shipped |35         |3057.3199999999983  |

Q2 — dim-only projection by order_date (top 5):
+----------+-----------+
|order_date|order_count|
+----------+-----------+
|2024-01-05|1          |
|2024-01-06|1          |
|2024-01-07|1          |
|2024-01-08|1          |
|2024-01-10|1          |
+----------+-----------+
```

## Regenerate the manifest from the YAML source

If `examples/operations-analytics/models/orders.yml` changes, regenerate
the checked-in artifact:

```bash
# From the project root, with the library installed:
cd ../..
mvn -o exec:java -Dexec.mainClass=io.semanticdf.tools.Main \
  -Dexec.jvmArgs="<the standard Spark JVM flags>" \
  -Dexec.args="manifest --yaml examples/operations-analytics/models/orders.yml --out examples/manifest-load/manifests/"
```

(The `-Dexec.jvmArgs="..."` carries the same `--add-opens` flags as the
other example projects — required for Spark on JDK 17.)

Or just call `SemanticManifest.toJson` programmatically and write the
output — the artifact is plain JSON.

## Why this example exists

The manifest recipe (`docs/design/manifest-artifact.md`) describes the
workflow in detail, and `SemanticManifestSpec` proves the round-trip
works. This example is the **human-readable proof**: a 70-line
`Main.scala` that loads a real artifact and runs real queries against
it. Reading the code is enough; no setup beyond `mvn install` is needed
to see the full pipeline in action.

The operations-analytics model is a no-join single-table model — the
simplest shape the manifest supports. Joined models are explicitly out
of scope (recipe §10); that constraint exists so the manifest can stay
inspection-only, without encoding the operator's data-lifecycle decisions.

## What it does NOT do

- **No `SemanticJoinOp` support.** The recipe documents this as the
  manifest's primary anti-scope. Joined models throw at `toJson` time.
- **No streaming.** The manifest is for batch models. Streaming
  lifecycle is operator-side (per PR #124).
- **No manifest validation.** The manifest is inspected, not
  validated. Use `tools.Main validate-manifest` for the
  closed-error-list validation pass.
- **No calc-measure execution.** The manifest round-trips calc-measure
  *metadata* (name, expr, dependsOn) for inspection but does not
  reconstruct the lambda body. Calc measures (`avg_ship_days`,
  `on_time_rate`) work when loaded from YAML but throw a loud error
  when invoked on a manifest-loaded model. Consumers needing calc
  behavior must re-load from YAML. This is documented in the recipe §5
  ("metadata is in-process-only data consumed by the library's
  classifier / OKF / inspection tools").

## Files

```
examples/manifest-load/
├── pom.xml                                       # Maven config (sourced from starter)
├── README.md                                     # this file
├── manifests/
│   └── orders.json                               # checked-in pre-built artifact
└── src/main/scala/com/example/manifestload/
    └── Main.scala                                 # 70-line demo
```