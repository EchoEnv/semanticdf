# `manifest-load` — load a pre-built SemanticManifest JSON artifact

A focused, minimal example of the manifest-artifact workflow's **second
half**: load a pre-built JSON manifest and reconstruct a `SemanticTable`
at runtime, ready to query.

## What this demonstrates

- Loading a manifest JSON from disk via `SemanticManifest.fromJson`
- Inspecting the manifest's identity + digest header via `ManifestMeta`
  (no Spark needed — fast pre-flight check)
- Reconstructing a `SemanticTable` from the manifest + a source `DataFrame`
- Running base-measure queries (Q1) and dim + measure queries (Q3)
- Running a **calc-measure query** (Q2) — `revenue_per_event =
  total_revenue / event_count` round-trips through the manifest because
  the loader uses `CalcExpr` to substitute measure references through
  the post-aggregation `MeasureScope`
- Lifecycle surfacing: the manifest's `status` field is exposed so
  consumers can route on `Draft` / `Published` / `Deprecated`

## The workflow this exemplifies

```text
  Build (CI / deploy step)              Load (runtime / app)
  ──────────────────────────            ─────────────────────
  tools.Main manifest                    SemanticManifest.fromJson(
    --yaml usage.yml                        manifests/usage.json,
    --out manifests/                         sourceDataFrame)
                                          → SemanticTable
  → manifests/usage.json
```

The two halves are designed to run in different processes: CI builds
the artifact; the application loads it. The artifact is portable —
it can be stored in git, in S3, in a model registry, anywhere. The
application does not need access to the YAML loader or the original
source-of-truth.

## What the artifact contains

The checked-in `manifests/usage.json` is a `v0.1.9-manifest` artifact
representing the `usage` model from `examples/telco-analytics`. The
manifest carries:

- Identity: `name`, `version`, `status`, `description`, `sourceTable`
- 8 dimensions (`usage_id`, `promo_code`, `plan_name`, `is_roaming`,
  `plan_id`, `event_type`, `customer_id`, `customer_name` + 1 time
  dim `event_date`)
- 5 measures: 4 base (`event_count`, `total_revenue`,
  `total_roaming_revenue`, `avg_event_amount`) + 1 calc
  (`revenue_per_event`)
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
Model: usage  v0  status=published
Digest: 9 dims, 5 measures (1 calc), 0 filters, joins=0

Q1 — event_count + total_revenue (base measures):
+-----------+-------------+
|event_count|total_revenue|
+-----------+-------------+
|40         |143.35       |
+-----------+-------------+

Q2 — revenue_per_event (calc measure = total_revenue / event_count):
+-------------+-----------+------------------+
|total_revenue|event_count|revenue_per_event |
+-------------+-----------+------------------+
|143.35       |40         |3.5837499999999998|
+-------------+-----------+------------------+

Q3 — total_revenue by event_type (dim + measure):
+----------+-------------+
|event_type|total_revenue|
+----------+-------------+
|call      |85.0         |
|data      |58.0         |
+----------+-------------+
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
- **No `transforms:` block.** The YAML's per-row `transforms:` block
  (e.g. `ship_days = datediff(shipped_at, order_date)`) is not carried
  by the manifest. Models that depend on transform-produced columns
  throw `UNRESOLVED_COLUMN` at query time on the raw source DF.
  Consumers needing transforms must re-load from YAML.

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