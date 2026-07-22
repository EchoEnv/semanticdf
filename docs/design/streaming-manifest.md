# Design Recipe: Streaming-Manifest Worked Example

**Status:** SHIPPED (recipe was WORKING DRAFT pending tactical resolution of the BLOCK feedback)
**Library version that emits this shape:** `0.1.11-streaming-example`
**Scope:** Docs + example only. **No library API change.** Closes the "no worked example for the streaming manifest read path" gap that the audit surfaced. The streaming manifest is already supported by the library (`isStreaming: true` is already recorded in the digest; `fromJson` produces a `SemanticStreamingTableOp` for streaming sources). What's missing is a worked example showing the read path end-to-end.

## 1. What this is (and what it isn't)

A worked example that demonstrates the **runtime half** of the streaming-manifest workflow:

```
  Build (CI / deploy step)              Load (runtime / app)
  ──────────────────────────            ──────────────────────
  tools.Main manifest                    SemanticManifest.fromJson(
    --yaml events.yml                      manifests/events.json,
    --out manifests/                       streamingSource)
                                         → SemanticTable
  → manifests/events.json                 ↓
                                       toStreamingQuery(spark, cfg)
                                         → StreamingQuery
```

This is the streaming analog of the existing `manifest-load` example. The example file lives at `examples/streaming-manifest-load/` (or extends the existing `streaming-events` example.

**What it is NOT:**
- ❌ A code change to `SemanticManifest` or the streaming terminal
- ❌ A new manifest schema kind
- ❌ A change to how the streaming terminal works (operator-side)

## 2. The gap this fills

The streaming terminal (terminal boundary) requires:
- A `StreamingConfig` (operator-side, NOT in the manifest)
- A source `DataStream` (operator-side, NOT in the manifest)
- The static model definition (THIS is what the manifest carries)

The manifest's `digest.isStreaming = true` flag records that the model is a streaming one. `fromJson` produces a `SemanticStreamingTableOp` (the streaming equivalent of `SemanticTableOp`). The user then wires the streaming source + a `StreamingConfig` and calls `toStreamingQuery` — exactly the same flow as the YAML loader's `streaming-events` example.

**The gap:** there's no worked example showing this end-to-end with a manifest. The streaming-events example uses the YAML loader, not the manifest. Users reading the manifest documentation might not realize the streaming path works.

## 3. Proposed example shape

A new directory `examples/streaming-manifest-load/` mirroring the existing `manifest-load` example:

```
examples/streaming-manifest-load/
├── pom.xml                                       # same template as manifest-load
├── README.md                                     # streaming-specific notes
├── src/main/resources/manifests/
│   └── events.json                               # pre-built streaming manifest
└── src/main/scala/com/example/streamingmanifestload/
    └── Main.scala                                 # load + toStreamingQuery
```

The example would:
1. Load `events.json` from the classpath
2. Build a `DataStream` from `spark.readStream.format("rate")` (or any streaming source)
3. `SemanticManifest.fromJson(json, sourceDataStream)` — note: streaming source, not a regular DataFrame
4. Print the loaded `ManifestMeta` (digest, isStreaming flag)
5. Build a `StreamingConfig` programmatically
6. `toStreamingQuery(spark, cfg)` and run for a few seconds
7. Stop the query cleanly

The example is self-contained: pre-built manifest + a streaming source that anyone can run locally. The streaming source doesn't have to match the production source — it's a stand-in.

## 4. Decisions locked

| Decision | Choice | Rationale |
|---|---|---|
| New example directory vs extend existing | **New `examples/streaming-manifest-load/`** | Same separation as `manifest-load` (v0.1.11) vs the rest. Each example has one job: one manifest artifact, one query. |
| Streaming source for the example | **`spark.readStream.format("rate")`** | Self-contained, no external dependencies (no Kafka, no Parquet, no external infrastructure). The rate source gives a `timestamp, value` schema that demonstrates streaming query without needing a real source. |
| Manifest artifact | **Generated from `examples/streaming-events/models/events.yml`** (re-generated to `events.json`) | Reuses an existing model. The streaming-events example already demonstrates the same model via the YAML loader, so the manifest is just an alternate view. |
| Build via the new example? **Yes** — show the build (CI) half, then the load (runtime) half | Mirrors the manifest-load example's "Regenerate the manifest" section. Users see the full workflow in one place. |
| Streaming source / model mismatch | **Documented** | The example uses a `rate` source whose schema (`timestamp, value`) doesn't match the `events.yml` model's dims (`timestamp_bucket`, `type`, `value`). The example only inspects metadata, not queries the stream; if it did query, the schema mismatch would surface as a clear error. |
| `toStreamingQuery` duration | **5 seconds** | Long enough to demonstrate "stream is running", short enough to not slow CI / local test runs. Uses `awaitTermination` with a finite timeout. |
| Operator-side `StreamingConfig` | **Built in the example, documented as "this is what YOU write in your app"** | Mirrors the terminal layer's documented boundary: the manifest captures the model, the operator captures the runtime config. |

## 5. Files

| File | Purpose |
|---|---|
| `examples/streaming-manifest-load/pom.xml` | Maven config (clone of `manifest-load/pom.xml`) |
| `examples/streaming-manifest-load/README.md` | Workflow + run + streaming-source caveats |
| `examples/streaming-manifest-load/src/main/resources/manifests/events.json` | Pre-built artifact from `streaming-events/models/events.yml` |
| `examples/streaming-manifest-load/src/main/scala/.../Main.scala` | ~70-line demo: load manifest, build source, `toStreamingQuery`, print a few rows, stop |

The `events.json` is generated once (manually, by running `tools.Main manifest --yaml events.yml --out events.json` against the streaming source's schema) and committed. Future regeneration instructions in the README.

## 6. Test plan

This recipe is **docs + example only**, so it has no library tests. The example itself is verified manually:

| Verification | Asserts |
|---|---|
| `mvn -pl examples/streaming-manifest-load compile` | Compiles against the installed `semanticdf_2.13` jar |
| `mvn -pl examples/streaming-manifest-load scala:run ...` | Prints the manifest meta, runs the streaming query for ~5 seconds, prints some rows, stops cleanly |
| Manifest fields match `examples/streaming-events/models/events.yml` | `digest.dimensions` matches the YAML's dims, `digest.isStreaming` is `true`, etc. |

If we want CI coverage, a follow-up PR can add `mvn -pl examples/streaming-manifest-load scala:run` to the matrix. For this recipe, manual verification is fine — it matches the pattern of the existing `streaming-events` example (which has no automated test either).

## 7. Diff estimate

| File | LOC delta |
|---|---|
| `examples/streaming-manifest-load/pom.xml` | ~50 (clone of `manifest-load`'s) |
| `examples/streaming-manifest-load/README.md` | ~80 (workflow + streaming-specific notes) |
| `examples/streaming-manifest-load/src/main/resources/manifests/events.json` | ~50 lines (re-generated from existing YAML) |
| `examples/streaming-manifest-load/src/main/scala/.../Main.scala` | ~70 |
| `examples/README.md` (add the new example) | +5 |

**Total: ~250 LOC across 5 files.** Single PR. **No library code change.**

## 8. Anti-scope (deliberately)

- ❌ A code change to `SemanticManifest` or the streaming terminal — recipe §10 is the boundary
- ❌ A new manifest schema kind for streaming — the existing `kind: "semanticdf-model-manifest"` with `isStreaming: true` is sufficient
- ❌ A reference to streaming-source metadata in the manifest — the source itself is operator-side, by design
- ❌ Checkpoint location, trigger interval, output mode — all in `StreamingConfig`, operator-side
- ❌ Replaying the streaming source's schema in the manifest — the manifest captures the MODEL's schema (dims, measures), not the source's schema

## 9. Migration / backwards compatibility

| Concern | Resolution |
|---|---|
| `SemanticManifest` API | Unchanged |
| `examples/README.md` index | One new row in the "What each example shows" table |
| Streaming-events example | Unchanged; the new example complements it (manifest view vs YAML view) |
| CI matrix | Optional follow-up: add `mvn -pl examples/streaming-manifest-load scala:run` for CI coverage |

## 10. Open questions for review

| # | Question | Current proposal |
|---|---|---|
| Q1 | `rate` source vs a real streaming source (Kafka, Parquet stream)? | **`rate`** for self-containment. Real sources would require infrastructure the example can't assume. |
| Q2 | Generate the manifest at build-time (in pom.xml) vs commit pre-built? | **Commit pre-built** — the manifest is checked in, like the existing `manifest-load` example's `orders.json`. Regeneration is documented in the README. |
| Q3 | Should this be a separate example directory, or a section in the existing `streaming-events` README? | **Separate directory** — same shape as `manifest-load`. The streaming-events example is YAML-focused; mixing in the manifest path would muddy its message. |
| Q4 | Show the streaming query in the manifest-load example? | **No** — manifest-load is batch-only. Streaming is a separate workflow with its own operator-side config. Two examples, two workflows. |