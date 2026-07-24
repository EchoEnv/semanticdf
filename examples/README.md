# SemanticDF examples

Every example below is a runnable Maven project under `examples/`. Each
one demonstrates a real workload on top of SemanticDF; the descriptions
below say which to open first based on what you're trying to do.

## Start here

The [`starter/`](starter/) example is intentionally the smallest. Read
its README end-to-end, then run it:

```bash
cd examples/starter
mvn scala:run -DmainClass=com.example.starter.Main
```

After `starter/`, pick your next example from the journeys below. None
of them require reading every other example first — each is
self-contained once you've done `starter/`.

## By journey — recommended order

### "I want to evaluate SemanticDF quickly"
1. [`starter/`](starter/) — YAML model, fastest eval
2. [`pipeline/`](pipeline/) — full ETL lifecycle (raw CSV → parquet → semantic)
3. [`window-analytics/`](window-analytics/) — top-N, period-over-period, running total

### "I'm building analytics for CRM / marketing"
1. `starter/`
2. [`customer-analytics/`](customer-analytics/) — RFM (recency / frequency / monetary), cohort activity

### "I'm building ops / supply-chain analytics"
1. `starter/`
2. [`operations-analytics/`](operations-analytics/) — fulfillment time, on-time rate, anomaly detection

### "I have messy source data"
1. `starter/`
2. [`hospital/`](hospital/) — the **cleansing step**. Most examples load clean data; this one starts dirty and cleans in Scala before loading.

### "I'm building an LLM agent or external client"
1. `starter/`
2. [`cli-consumer/`](cli-consumer/) — `sdf` CLI on top of the REST API. Lightweight, no Spark, no SemanticDF dependencies of its own.
3. (`semanticdf-mcp/`) — the MCP server itself, for MCP-protocol-aware clients (Claude Desktop, Cursor, Continue).

### "Telco-specific patterns"
1. `starter/`
2. [`telco-analytics/`](telco-analytics/) — carriers, plans, promotions

### "I want to load a pre-built model artifact (CI / version-pinning)"
1. `starter/`
2. [`manifest-load/`](manifest-load/) — `SemanticManifest.fromJson` end-to-end. Companion to the [manifest-artifact recipe](../docs/design/manifest-artifact.md); shows the runtime half of the build/load workflow.
3. [`streaming-manifest-load/`](streaming-manifest-load/) — the streaming analog: load a streaming manifest, build a `StreamingConfig`, run a streaming query.

### "I'm wiring joined-manifest into a build/load pipeline"
1. `starter/`
2. [`joined-manifest/`](joined-manifest/) — the canonical joined-manifest wire shape. Round-trips a joined manifest in a single process.
3. [`joined-manifest-e2e/`](joined-manifest-e2e/) — the **artifact-on-disk** workflow for joined manifests. Build phase emits the JSON; Query phase loads from disk and runs analytics. Demonstrates the asymmetric-key (v0.1.14) path with real clinical data. The gap that no other example closes: prove a joined manifest survives across process boundaries.
4. [`joined-manifest-split/`](joined-manifest-split/) — historical / pre-v0.1.11 reference. Kept for legacy consumers; new code should use `joined-manifest/`.

## What each example shows

| Example | Demonstrates |
|---|---|
| [`starter/`](starter/) | Simplest YAML model + typed queries |
| [`pipeline/`](pipeline/) | ETL + semantic lifecycle: raw CSV → parquet → declarative queries |
| [`window-analytics/`](window-analytics/) | Top-N per group, period-over-period, running total |
| [`customer-analytics/`](customer-analytics/) | RFM (recency / frequency / monetary), cohort activity |
| [`operations-analytics/`](operations-analytics/) | Fulfillment time, on-time rate, anomaly detection |
| [`hospital/`](hospital/) | Data-quality workflow: messy source → clean schema (with sidecar OKF docs in `hospital-ok/`) |
| [`telco-analytics/`](telco-analytics/) | Telco domain: carriers, plans, promotions |
| [`cli-consumer/`](cli-consumer/) | Standalone CLI client (`sdf`) for the REST API — uses no Spark, no SemanticDF dep |
| [`manifest-load/`](manifest-load/) | Load a pre-built `SemanticManifest` JSON artifact and reconstruct a `SemanticTable`. Companion to the [manifest-artifact recipe](../docs/design/manifest-artifact.md) |
| [`streaming-manifest-load/`](streaming-manifest-load/) | Load a pre-built streaming `SemanticManifest` and run a streaming query. Companion to the [streaming-manifest recipe](../docs/design/streaming-manifest.md) |
| [`joined-manifest/`](joined-manifest/) | Emit a joined-manifest via `SemanticManifest.toJoinedJson` (per-side metadata + alias-prefixed dims via `extra_dimensions[]`, `leftPrefix` / `rightPrefix` on the join block, structured `predicate_ast` for non-equi / OR, asymmetric-key support) and round-trip via `fromJoinedJson`. The canonical example for the joined-manifest wire shape. |
| [`joined-manifest-e2e/`](joined-manifest-e2e/) | End-to-end artifact workflow on real data. Phase 1 (`Build`) emits a joined manifest to `target/*.json`; phase 2 (`Query`) loads from disk and runs analytics. Demonstrates the asymmetric-key (v0.1.14) path with clinical data. |
| [`joined-manifest-split/`](joined-manifest-split/) | Legacy / pre-v0.1.11 reference: the hand-rolled per-side emit + manually-composed joined envelope pattern. Kept for consumers on pre-v0.1.11 versions; use [`joined-manifest/`](joined-manifest/) for new code. |

## Prerequisites

For any example to run:

- **JDK 17** and **Maven 3.9+**.
- The parent library installed locally before the first run of any example:
  ```bash
  # from the repo root
  mvn install -DskipTests
  ```
- **Spark is `provided`** — every example starts a local `SparkSession`
  for you when you `mvn scala:run`, so you don't need a Spark cluster.

## Running an example

Each example is a self-contained Maven project. The typical workflow:

```bash
cd examples/<name>
mvn scala:run -DmainClass=<Main class name, listed in that example's README>
```

The exact main class name and command are in each example's own README.
Some examples (like `cli-consumer/`) use `bin/sdf` or a custom
executable — read the example README first.

## When the YAML files aren't enough

If you're building a real product and the `*.yml` models from these
examples are too simple for your data, see:

- [`DESIGN.md`](../DESIGN.md) §4 (architecture) and §6 (the hard problems) for what the library guarantees
- [`docs/runtime-quickstart.md`](../docs/runtime-quickstart.md) for the toolchain quirks
- [`examples/hospital/hospital-ok/`](../examples/hospital/hospital-ok/) for a richer real-world model surface (encounters + diagnoses + patients with joins, time dims, and OKF)

## What "complementary" means

Every example README ends with a *complements* note pointing at the
other examples that fill in adjacent concerns. They form a graph, not a
hierarchy — pick the entry point above that matches your goal, then
follow those cross-links.
