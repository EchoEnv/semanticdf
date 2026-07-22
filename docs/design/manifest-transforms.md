# Design Recipe: `transforms:` Block in `SemanticManifest`

**Status:** DRAFT (BLOCK from senior-engineer review 2026-07-22; Transform model lacks `exprString` field — see `docs/design/REVIEW-FEEDBACK.md` for details)
**Library version that would emit this shape:** `0.1.11-transforms`
**Scope:** Single, additive feature. Extends the manifest schema with a `transforms: [...]` field paralleling `dimensions` and `measures`. Replays transforms on `fromJson`. No breaking wire changes.

## 1. What this is (and what it isn't)

The current manifest (PR #132, refined in #139 and #140) supports per-row `transforms:` from the YAML loader side, but does NOT carry the transform expressions across the serialization boundary. Models that depend on transform-produced columns (like `operations-analytics/orders` with its `ship_days` and `on_time_flag`) cannot be loaded from a manifest without first re-applying the transforms to the source DF.

This recipe adds a `transforms: [...]` field to the manifest. `fromJson` replays the per-row computations on the source DF before constructing the model.

**What it is:** a single new field in the manifest schema, plus a replay step in `fromJson`.

**What it is NOT:**
- ❌ A change to how transforms are EVALUATED — they continue to use Spark `F.expr` per row, the same as the YAML loader does today
- ❌ A change to the model definition (the `SemanticTransformsOp` already exists in the op tree)
- ❌ A way to write transforms without a source DF — the transforms run during load, just like the YAML loader

## 2. The gap this fixes

```
operations-analytics/models/orders.yml:
  orders:
    table: orders_csv
    transforms:
      ship_days:
        expr: "datediff(shipped_at, order_date)"
      on_time_flag:
        expr: "case when datediff(shipped_at, order_date) <= 2 then 1 else 0 end"
    measures:
      total_ship_days: "sum(ship_days)"            # depends on transform
      total_on_time:  "sum(on_time_flag)"         # depends on transform
    calculated_measures:
      avg_ship_days:  "total_ship_days / order_count"
      on_time_rate:   "total_on_time / order_count"
```

`SemanticManifest.toJson(ordersModel)` works — the manifest records 4 base + 2 calc measures, with `dependsOn: [order_count, total_ship_days]` for `avg_ship_days`. `SemanticManifest.fromJson(json, raw_orders_csv)` partially works post-PR-#140: `order_count` (the base measure that references a raw column) evaluates correctly, but `total_ship_days` (which references `ship_days` from the transform) throws `UNRESOLVED_COLUMN`.

The `manifest-load` example had to swap to `telco-analytics/usage.yml` (which has no transforms) to demonstrate a clean round-trip. With this fix, the `operations-analytics/orders` model can round-trip too.

## 3. Proposed schema

The manifest gains a new field paralleling `dimensions` and `measures`:

```json
{
  "schemaVersion": "v0.1.11-manifest",
  "kind": "semanticdf-model-manifest",
  "compiledAt": "2026-07-25T12:00:00Z",

  "model": {
    "name": "orders",
    "version": 1,
    "status": "published",
    "description": "Per-order data with fulfillment timestamps",
    "sourceTable": "orders_csv"
  },

  "transforms": [
    {"name": "ship_days",     "expr": "datediff(shipped_at, order_date)"},
    {"name": "on_time_flag",  "expr": "case when datediff(shipped_at, order_date) <= 2 then 1 else 0 end"}
  ],

  "dimensions": [...],
  "measures": [...],

  "digest": { "dimensions": 4, "measures": 6, "calcMeasures": 2, "transforms": 2, ... },
  "warnings": []
}
```

`digest.transforms` is added (count of transform entries). Other fields unchanged.

## 4. Decisions locked

| Decision | Choice | Rationale |
|---|---|---|
| Add new field vs. extend existing | **New `transforms: []` field** | Parallel to `dimensions` and `measures` — same shape (`{name, expr}`), same place in the JSON. Writer reads them once and passes them to the new `withTransforms` op. |
| Backwards compat | **Existing manifests without `transforms:` parse unchanged** | The field is optional. `fromJson` checks for its presence; if absent, no transforms are applied. The reader is tolerant (tolerated-extra-fields pattern, established in PR #140). |
| Replay mechanism | **In `fromJson`, before constructing the model** | Mirrors the YAML loader: the source DF is `withColumn`-ed for each transform, then the transformed DF becomes the model's root. The order of transforms is preserved (manifest is an array). |
| Transform expression semantics | **`F.expr` per transform, evaluated on the source DF** | Exactly the same as `YamlLoader`'s current behavior (which uses `SemanticTransformsOp`). No semantic divergence between YAML and manifest. |
| Streaming transforms | **Allowed for batch only in v1** | A streaming model can't have transforms in the YAML loader today (`SemanticTransformsOp` requires a `Dataset`, not a `DataStream`). The manifest should refuse transforms on a streaming source: `toJson` throws a clear `IllegalArgumentException`. |
| Joined-model transforms | **Each side carries its own transforms** | When the joined-manifest recipe lands, each embedded side is a full single-table manifest (with its own `transforms` array). The runtime applies each side's transforms before composing the join. |
| `digest.transforms` count | Yes, new field | Useful for `parseMeta` callers that want to know "is this a model with transforms?" without parsing the full `transforms` array. |

## 5. API surface

```scala
// Existing API: unchanged
def SemanticManifest.toJson(model: SemanticTable, prettyPrint: Boolean = true): String
def SemanticManifest.fromJson(text: String, source: DataFrame): SemanticTable
def SemanticManifest.parseMeta(text: String): ManifestMeta

// New: ManifestMeta gets a `transforms` field
final case class ManifestMeta(
    schemaVersion: String,
    kind: String,
    modelName: Option[String],
    version: Int,
    description: Option[String],
    sourceTable: Option[String],
    status: String,
    dimensions: Int,
    measures: Int,
    calcMeasures: Int,
    joins: Int,
    filters: Int,
    isStreaming: Boolean,
    usesTAll: Boolean,
    transforms: Int,    // NEW: count of transform entries; 0 for transform-less models
)

// No new read/write methods needed — `toJson` and `fromJson` handle the new
// field transparently. The only public surface change is the new
// `ManifestMeta.transforms` field.
```

## 6. Implementation outline

`toJson`:
1. Walk `model.transforms` (the `Seq[Transform]` from the model root)
2. Emit each as `{"name": ..., "expr": ...}` in the `transforms: []` array
3. Add `transforms: <count>` to the digest

`fromJson`:
1. Parse `transforms: []` if present (tolerate absence)
2. For each transform entry, do `source.withColumn(name, F.expr(expr))`
3. The transformed DF becomes the source for the model's root
4. Existing dim/measure/calc reconstruction proceeds unchanged

Internally, the transformed DF gets wrapped in `SemanticTransformsOp` so the rest of the model machinery (dims, measures, filters) sees it via the op tree. This mirrors the YAML loader's behavior.

## 7. Test plan

| Test | Asserts |
|---|---|
| `transforms-round-trip-via-fromJson` | Build a model with transforms via Scala DSL → `toJson` → `fromJson(json, raw_source)` → query a transform-dependent measure (e.g. `sum(ship_days)`) returns the correct value |
| `transforms-applied-in-manifest-order` | Transforms with col-name dependencies (e.g. `b` depends on `a`) apply in the persisted order — same as the YAML loader |
| `transforms-absent-in-manifest-is-OK` | A model without transforms round-trips unchanged (digest.transforms == 0) |
| `streaming-with-transforms-rejected-at-toJson` | Calling `toJson` on a streaming model with transforms throws `IllegalArgumentException` naming the transform and the streaming root |
| `parseMeta-includes-transforms-count` | `ManifestMeta.transforms` returns the correct count for both transform-having and transform-less manifests |
| `transforms-with-bad-expr-fails-loudly` | A transform whose `expr` doesn't parse as Spark SQL throws `AnalysisException` with the offending expr in the message |
| `transforms-round-trip-preserves-count-and-names` | `toJson` then `parseMeta` shows the same transform count; `fromJson` produces a model where `root.transforms` has the same names |
| `manifest-load-example-restored-to-operations-analytics` | The `manifest-load` example's `Main.scala` runs end-to-end against `operations-analytics/orders.yml`'s manifest, producing the same numbers as the original example |

8 tests across `SemanticManifestSpec`. The `manifest-load` example reverts from `usage.json` to a fresh `orders.json` (or a new fixture); the README is updated to reflect the example.

## 8. Diff estimate

| File | LOC delta |
|---|---|
| `src/main/scala/io/semanticdf/SemanticManifest.scala` | +60 (writeTransforms, readTransforms, replay logic) |
| `src/test/scala/io/semanticdf/SemanticManifestSpec.scala` | +120 (7 new tests) |
| `examples/manifest-load/src/main/resources/manifests/orders.json` (re-generated) | (re-generated) |
| `examples/manifest-load/src/main/scala/.../Main.scala` (swap orders.json back) | -10 / +10 |
| `examples/manifest-load/README.md` (update example model) | +5 / -5 |

**Total: ~190 LOC across 5 files.** Single PR.

## 9. Anti-scope (deliberately)

- ❌ Non-Spark-SQL transform expressions — `F.expr` only parses standard Spark SQL
- ❌ Streaming-source transforms — `SemanticTransformsOp` requires a `Dataset`; streaming uses `DataStream`. Out of scope.
- ❌ Transform metadata (description, owner) — the manifest stores only the name + expr (matches dimensions/measures)
- ❌ Cross-model transform dependencies — each manifest is self-contained

## 10. Migration / backwards compatibility

| Concern | Resolution |
|---|---|
| Existing manifests without `transforms:` | Parse unchanged. `ManifestMeta.transforms` returns 0. |
| Existing `toJson` callers | No API change. Models with transforms now round-trip the transforms too. |
| Existing `fromJson` callers | No API change. The transforms are applied internally; consumers see the same model shape. |
| Wire-stable strings | The schemaVersion bumps to `v0.1.11-manifest`. Consumers that gate on version get an error if they're old. PR documents this. |

## 11. Open questions for review

| # | Question | Current proposal |
|---|---|---|
| Q1 | Should the manifest carry `transforms:` for streaming sources? | **No** — `toJson` rejects streaming+transforms with a clear error. (Streaming transforms aren't supported by the runtime op-tree; the manifest is honest about that.) |
| Q2 | `digest.transforms` count or a more detailed sub-digest (e.g. list of names)? | **Count only** — matches the existing digest pattern (counts of dims/measures/etc). Names are available in the full `transforms` array. |
| Q3 | `withColumn` for transform replay vs `select`? | **`withColumn`** — preserves original column order and matches what the YAML loader does in `SemanticTransformsOp` |
| Q4 | Should the writer refuse transforms when there's only one row of source data (single-row sources)? | **No** — the writer trusts the YAML loader's behavior. If the YAML loader accepts it, the manifest does too. |
| Q5 | What about calc measures that use transform-produced columns? | **Already work post-PR-#140** — the calc-measure fix is independent. With this recipe, the transforms are applied first, so `avg_ship_days` (= `total_ship_days / order_count`) evaluates correctly. |