# Design Recipe: Joined Models in `SemanticManifest`

**Status:** DRAFT
**Library version that would emit this shape:** `0.1.11-joined-manifest`
**Scope:** Single, additive feature. Extends the manifest schema with a new `kind: "semanticdf-joined-manifest"`. No library API changes for single-table models. No breaking wire changes (existing manifests parse unchanged).

## 1. What this is (and what it isn't)

The current manifest (PR #132) is intentionally single-table — recipe §10 documents that joined models throw at `toJson` time. This recipe proposes a separate `kind` for joined models so they can round-trip alongside the single-table form.

**What it is:** a manifest schema for joined models, with both sides inlined as single-table manifests plus a `join` block. Carries enough information for a runtime with access to two source DataFrames to reconstruct the joined `SemanticTable` and query it.

**What it is NOT:**
- ❌ A change to the existing single-table manifest schema (which is rock-solid for its envelope)
- ❌ A redesign of how joins work at runtime — `SemanticJoinOp` is unchanged
- ❌ A streaming-manifest format (handled separately by the streaming-manifest recipe)
- ❌ A composition-of-N-models format — for v1 this is **2-way joins only**; N-way joins stay in YAML

## 2. The asymmetry this fixes

| Path | What it has | What it produces |
|---|---|---|
| `YamlLoader.load("flights.yml", tables)` | The YAML file + a `Map[String, DataFrame]` with the source CSVs already loaded | A live `SemanticTable` rooted at `SemanticJoinOp` with the joined dimensions/measures merged into a `MergedSemanticModel` |
| `SemanticManifest.toJson(joinedModel)` | Just the model object, no source CSVs, no join condition | Throws: recipe §10 anti-scope |

A joined model's query behavior depends on things the manifest alone can't carry: both source DataFrames, the join condition, and field-prefix rules. The current single-table manifest format only captures the last piece (dimensions, measures, table).

**Why not just fix the single-table manifest to carry joins?** Because that breaks its core property: portable, inspectable, no-runtime-context-needed. A joined manifest needs to either:
- Reference other artifacts (filesystem coupling)
- Inline both sides' definitions (duplicates data if both sides are also used standalone)

Both are bigger design decisions than a tweak. Hence a separate `kind` for joined models.

## 3. Proposed schema

```json
{
  "schemaVersion": "v0.1.11-joined-manifest",
  "kind": "semanticdf-joined-manifest",
  "compiledAt": "2026-07-25T12:00:00Z",

  "model": {
    "name": "flights",
    "version": 1,
    "status": "published",
    "description": "Flights enriched with carrier details",
    "left":  { ...embedded single-table manifest... },
    "right": { ...embedded single-table manifest... },

    "join": {
      "cardinality": "one",
      "leftKeys":  ["carrier"],
      "rightKeys": ["carrier"],
      "leftPrefix":  "left_",   // optional, default ""
      "rightPrefix": "right_"   // optional, default ""
    }
  },

  "digest": {
    "leftDimensions": 4,
    "rightDimensions": 3,
    "mergedDimensions": 6,
    "leftMeasures": 4,
    "rightMeasures": 2,
    "mergedMeasures": 5,
    "joins": 1,
    "isStreaming": false
  },

  "warnings": []
}
```

The two embedded `left` / `right` blocks are **the same shape as the existing single-table manifest** (the kind="semanticdf-model-manifest" JSON object). The library validates that they parse as valid single-table manifests before joining them. This means existing manifest-reading code can be reused unchanged for each side.

## 4. Decisions locked

| Decision | Choice | Rationale |
|---|---|---|
| Single kind vs separate | **Separate kind: `semanticdf-joined-manifest`** | Existing single-table manifests are unchanged; the new shape is opt-in. `toJson` dispatches on root op kind: `SemanticTableOp` → single-table, `SemanticStreamingTableOp` → streaming single-table, `SemanticJoinOp` → joined. |
| Inlined vs referenced sides | **Inlined** | Single-file artifact, no external coupling, fully inspectable. Duplication is acceptable because the joined manifest is the COMPOSITION — refresh-in-place happens at the joined-manifest level, not at the side level. (A future recipe could add a `references` form for cases where refresh at the side level matters.) |
| 2-way joins only | **Yes** | N-way joins can be expressed as 2-way chained, but the manifest should match the runtime's natural unit. `SemanticJoinOp` is 2-way; chained joins stay in YAML. |
| `leftKeys` / `rightKeys` schema | **String arrays, ordered, equi-join only** | Mirrors the existing `SemanticJoinOp.on` (which is `Seq[(String, String)]`). Non-equi joins are out of scope. |
| Prefix strategy | **Optional `leftPrefix` / `rightPrefix`, default `""`** | `SemanticJoinOp` has no prefix support today; consumers pick prefixes that avoid column collisions. When a prefix is needed, it's recorded in the manifest. The default of `""` matches the existing in-memory behavior. |
| Joined-model `toJson` failure | **Throws `IllegalStateException`** | Same as the current recipe §10. The error message names the dispatch rule: `"semanticdf-joined-manifest is the joined-model kind; if you want a joined manifest, you must call SemanticManifest.toJoinedJson(joined, ...) which is not yet implemented. PR #141."` |
| Backwards compat | **Existing single-table manifests parse unchanged** | The `kind` field is the discriminator. A consumer seeing `kind: "semanticdf-model-manifest"` reads the existing fields; seeing `semanticdf-joined-manifest` reads the new fields. |

## 5. API surface

```scala
// New: emit a joined manifest
def SemanticManifest.toJoinedJson(
    model: SemanticTable,                    // rooted at SemanticJoinOp
    prettyPrint: Boolean = true,
    pathResolver: Path => String = Path.toString  // serializes the embedded sides
): String

// New: reconstruct a joined SemanticTable from a manifest + two source DFs
def SemanticManifest.fromJoinedJson(
    text: String,
    leftSource: DataFrame,
    rightSource: DataFrame,
): SemanticTable

// New: identity-only header for joined manifests (no source DFs needed)
def SemanticManifest.parseJoinedMeta(text: String): JoinedManifestMeta

case class JoinedManifestMeta(
    schemaVersion: String,
    kind: String,
    modelName: Option[String],
    version: Int,
    description: Option[String],
    leftDigest:  SubDigest,    // mirrors single-table digest
    rightDigest: SubDigest,
    cardinality: String,      // "one" | "many" | "cross"
    isStreaming: Boolean,
)
```

The `fromJoinedJson(text, leftSource, rightSource)` reconstructs the same shape `YamlLoader.load` would: it calls `fromJson(text.left, leftSource)` and `fromJson(text.right, rightSource)`, then chains them with the join condition via `SemanticJoinOp` construction.

## 6. Test plan (the guarantees we make)

| Test | Asserts |
|---|---|
| `joined-yaml-joined-manifest-round-trip` | Build a joined model via Scala DSL → `toJoinedJson` → `fromJoinedJson` with two source DFs → `groupBy().aggregate(...).execute(spark)` produces the same result as the original |
| `joined-manifest-cardinality-round-trips` | `cardinality: "one"` and `cardinality: "many"` and `cardinality: "cross"` all parse and produce the right `SemanticJoinOp` |
| `joined-manifest-prefix-applied` | When `leftPrefix: "left_"` is set, the right-side columns are aliased and the query resolves them via the prefix |
| `joined-manifest-key-mismatch-throws-loud` | `leftKeys = ["carrier"]` but `rightKeys = ["code"]` throws a clear `IllegalArgumentException` naming the mismatch |
| `joined-manifest-cardinality-must-be-valid` | An invalid cardinality string (`"maybe"`) throws at parse time |
| `joined-manifest-missing-side-throws` | `left` or `right` missing from the JSON throws a clear error |
| `joined-manifest-rejects-single-table-input` | Calling `toJoinedJson` on a `SemanticTableOp`-rooted model throws `IllegalArgumentException` |
| `joined-manifest-rejects-streaming-input` | Calling `toJoinedJson` on a streaming model throws (or warns + serializes as single-table; decision needed) |
| `parseJoinedMeta-source-free` | `parseJoinedMeta` works without any source DFs; returns the identity + digest header only |
| `joined-manifest-warnings-collected` | Warnings on each side are merged into a top-level `warnings` array (de-duped) |

10 tests across a new `JoinedManifestSpec`.

## 7. Diff estimate (concrete LOC)

| File | LOC delta |
|---|---|
| `src/main/scala/io/semanticdf/SemanticManifest.scala` | +120 |
| `src/main/scala/io/semanticdf/ManifestSpec.scala` (new test) | +150 |
| `docs/agents/mcp-contract.md` (mention joined manifest) | +10 |
| `examples/manifest-load/README.md` (note the new kind) | +10 |

**Total: ~290 LOC across 4 files.** Single PR. No library API changes for single-table models.

## 8. Anti-scope (deliberately)

- ❌ N-way joins — chain them in YAML
- ❌ Streaming joined models — separate recipe (`streaming-manifest`)
- ❌ Non-equi joins — outside `SemanticJoinOp`'s capability
- ❌ Cross-source joins (Kafka + S3) — single-source per side for v1; cross-source is a runtime concern
- ❌ Refresh of side manifests independently — the joined manifest is the unit; the embedded sides are versioned together

## 9. Migration / backwards compatibility

| Concern | Resolution |
|---|---|
| Existing single-table manifests | Parsed unchanged — `kind: "semanticdf-model-manifest"` is the existing path |
| Existing `toJson` callers (single-table only) | Unaffected — `toJson` only dispatches for single-table roots |
| New `toJoinedJson` callers | Add at the model level; the existing `toJson` continues to throw for joined roots (recipe §10 anti-scope unchanged) |
| MCP server | The `describe_model` tool reads the kind field; the new shape surfaces `cardinality`, prefix info, and the digest as a sub-header. Wire-stable JSON. |
| OKF gen | No change — the joined manifest is a separate artifact from OKF. If we want joined OKF, that's a follow-up recipe. |

## 10. Open questions for review

| # | Question | Current proposal |
|---|---|---|
| Q1 | Inlined vs referenced sides? | **Inlined** (above). If the reviewer disagrees, the referenced form is a small addition. |
| Q2 | `cardinality` as a string vs sealed enum? | **String** for wire-stable forward compat (matches `SemanticJoinOp.cardinality` which is a String). |
| Q3 | `leftKeys` / `rightKeys` as a list of strings, OR as a list of `{left, right}` pairs (allowing non-symmetric naming)? | **String arrays** (simpler, matches `SemanticJoinOp.on`). The `{left, right}` pair form is overkill for the common equi-join case. |
| Q4 | Should the joined-manifest embed full single-table manifests, OR a slimmed-down view (dimensions + table + measures, no `digest`)? | **Full embedded single-table manifests** (the existing format is small enough; ~600 bytes per side). Slimming loses `digest` for the side, which is useful for `parseJoinedMeta`. |
| Q5 | Streaming joined models? | **Out of v1** — the recipe is silent on streaming joined roots. `toJoinedJson` should refuse streaming roots (throw with a clear message). Future recipe: `streaming-joined-manifest` if there's real demand. |
| Q6 | Should the joined manifest support `transforms` on the embedded sides? | **Yes** — the embedded sides are full single-table manifests, so they carry their own transforms. The runtime applies each side's transforms before composing the join. (Once the `manifest-transforms` recipe lands, this comes for free.) |
| Q7 | What if the left and right sides have the same source-table name (e.g. both `flights`)? | The manifest's `model.sourceTable` is recorded per side. Conflicts are caught at query time by Spark (ambiguous column). The manifest doesn't pre-check. |