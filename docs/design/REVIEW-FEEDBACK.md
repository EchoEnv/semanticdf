# Reviewer Feedback: Three Recipes (2026-07-22)

Senior-engineer subagent reviews of three recipes landed in the v0.1.11 review cycle
returned 🔴 **BLOCK** on all three. The recipes need significant rework,
not just nits. This file documents the feedback so the recipes can be
re-designed or de-prioritised.

## Resolution status (added 2026-07-22, end of v0.1.11 cycle)

The BLOCK verdicts in §§1–3 below were resolved (or worked around) by
the v0.1.11 implementation cycle, summarised below. The per-finding
status is also annotated inline under each finding (✅ resolved, ❌
still BLOCK, ⚠️ partial).

| Recipe | Original BLOCK | Resolved in | Status |
|---|---|---|---|
| `manifest-transforms.md` | "Transform lacks `exprString`" | PR #149 | ✅ Shipped |
| `joined-models-manifest.md` | 5 fatal issues | PRs #150 + #151 | ⚠️ Partially shipped (BLOCK #1 `on` reconstruction deferred) |
| `streaming-manifest.md` | 3 tactical issues | PR #143 | ✅ Shipped (working draft resolved tactically) |

See the implementation notes in each recipe's status block for details.


## 1. `joined-models-manifest.md` — BLOCK

**Fatal issues:**

1. **`SemanticJoinOp` doesn't carry the metadata the recipe assumes.**
   The op retains each side's op/root but not side `version` / `status` /
   `sourceTable`. The joined root's facade metadata defaults (`name`,
   `description` = None). `toJoinedJson(joined)` cannot produce the
   full side envelopes promised in §3.

2. **The schema drops essential merged-model state.** The runtime's
   merged-dimensions are `right ++ left ++ extraDimensions/extraMeasures`
   (where extras include arbitrary post-join dims/measures and YamlLoader's
   alias-prefixed dimensions). Neither embedded side carries them, and
   the `join` block cannot recreate them. A YAML query like
   `groupBy("carriers.name")` won't round-trip.

3. **Prefixes don't match runtime semantics.** `SemanticJoinOp` has
   no `leftPrefix`/`rightPrefix` fields or column-renaming stage.
   YamlLoader adds semantic-dimension aliases via `alias.dimension`,
   not `left_`/`right_` prefixes on DataFrame columns. The proposed
   prefix test (`leftPrefix: "left_"`) is internally inconsistent.

4. **The key model is inaccurate.** `SemanticJoinOp.on` is a function
   `(JoinSide, JoinSide) => Column`, not `Seq[(String, String)]`.
   Compilation requires identical, sorted key-name sets, so asymmetric
   `leftKeys` / `rightKeys` are unsupported.

5. **API / tests / decisions remain unresolved.** §4 says `toJson`
   dispatches; §9 says it continues throwing — contradicts itself. The
   `pathResolver` parameter is undefined. `SubDigest`, side warnings,
   and streaming-joined behavior are undecided.

**To unblock:**
- Either retain side `version` / `status` / `sourceTable` in the
  runtime op (a library change), OR change the API to require the
  caller to pass the original side models alongside the joined one.
- Replace prefixes with explicit alias rules.
- Use a single `keys` array (empty for cross joins) or scope a
  runtime change for asymmetric key names.
- Re-design the merged-model extras: how are `extraDimensions` and
  `extraMeasures` serialized in the joined manifest?

## 2. `manifest-transforms.md` — BLOCK

**Fatal issues:**

1. **`Transform` has no `exprString` field.** `Model.scala:345-349`
   defines `Transform(name, expr: SemanticScope => Column, description)`
   — no expression string. `YamlLoader.scala:586-588` discards
   `exprStr` after creating `_ => F.expr(exprStr)`, so the SQL is
   immediately lost. `toJson` cannot emit the required SQL.

2. **`source.withColumn` replay loses the transform op-tree.** Using
   just `withColumn` makes one query work but loses the transform
   nodes; wrapping the already-transformed DF in `SemanticTransformsOp`
   would apply them twice. The recipe needs a `withTransforms` call
   similar to YamlLoader's, plus an `exprString` field on `Transform`.

3. **Streaming+transforms isn't actually rejected.** `SemanticTransformsOp`
   accepts a `DataFrame` (streaming data is also a `DataFrame`).
   `StreamingValidator` explicitly treats transforms as allowed
   (`StreamingSupport.scala:239`). The recipe's "toJson throws on
   streaming+transforms" claim is incorrect without an explicit
   `model.streaming == true` check in `withTransforms` or similar.

4. **Version bump breaks backwards compat with v0.1.9 manifests.**
   `v0.1.11` strict parsing would reject older manifests that don't
   carry the new `transforms` field. Need either lenient parsing
   (default `transforms = []`) or a separate version bump.

**To unblock:**
- Add `exprString: Option[String]` to the `Transform` case class; have
  `YamlLoader` retain it alongside the lambda.
- Have `toJson` emit the `exprString` if present; have `fromJson`
  prefer `exprString` for replay (matching the manifest design).
- Reject streaming+transforms at the runtime op level
  (`withTransforms` checks `model.streaming`).
- Either keep `CurrentSchemaVersion = "v0.1.9-manifest"` (no version
  bump; new field is optional) or document the v0.1.10 manifest
  compat shim.

## 3. `streaming-manifest.md` — BLOCK

**Fatal issues:**

1. **`rate` source schema-incompatible with `events.yml`.** The rate
   source exposes `timestamp, value`. The model expects a `type` column
   (filter and `event_type` dimension). `SemanticManifest.readManifest`
   doesn't validate the source schema against model expressions; the
   mismatch surfaces later at query time.

2. **Without explicit aggregation, the example bypasses the manifest.**
   `toStreamingQuery`'s aggregation branch matches only a root
   `SemanticStreamingTableOp`. Without an explicit aggregation
   command, execution falls through to the raw-batch path — the
   example may print rate rows while bypassing the manifest's
   dimensions/measures.

3. **`tools.Main manifest` can't generate `isStreaming:true` from
   this YAML** — it uses the batch YamlLoader, which resolves
   `events_stream` through `spark.table`, not `readStream`. The
   example-side manifest must be hand-generated (or a separate
   streaming-aware generator is needed).

4. **5-second duration is flaky.** `StreamingConfig` uses a 5-second
   trigger; the example runs for 5 seconds → may print an empty
   batch. Should be 10–12 seconds.

5. **Verification command is wrong.** The root POM is not a reactor
   (no `<modules>`). Use `cd examples/streaming-manifest-load && mvn compile`.

**To unblock:**
- Enrich the streaming source with a `type` column (similar to how
  `streaming-events/Main.scala` does).
- Have the example explicitly call `loaded.groupBy("type").aggregate("event_count", "total_value")` before `toStreamingQuery`.
- Use 10–12 second duration.
- Document how to generate `events.json` (manual script or a
  `tools.Main manifest-streaming` variant).
- Note that dims are `event_type` / `timestamp_bucket`, not the
  `type`/`value`/`timestamp` claimed in §1.

## Path forward

Three options for the BLOCK verdicts:

**A. Redesign all three recipes (significant work).** Each recipe
needs ~half a day to a day of redesign. Then re-run the senior-engineer
review. Then implement. Probably 1–2 weeks to ship all three.

**B. Ship v0.1.10 without these features (close out the v0.1.10
cohort now).** 11 PRs already shipped; the manifest is feature-complete
for its actual envelope (single-table, base + calc, filters, time dims,
status). Joined-models, transforms, and streaming-example are real
gaps but each is its own design effort. Defer them to v0.2 alongside
lineage (which is also a composition feature).

**C. Pick the easiest to fix and ship that.** Streaming-manifest is
the smallest of the three BLOCKs (~1 day of fixes vs ~3-5 days for
the others). The blockers are tactical: enrich the rate source, fix
the verification command, use a longer duration. Ship that in v0.1.10.

**My pick: option C (streaming-manifest only) for v0.1.10, options A
(joined-models) for v0.2, option A (manifest-transforms) deferred
indefinitely.**

The other two recipes need more substantive design work than a
single PR. The transforms recipe needs a library change
(`Transform.exprString`) which is a breaking-ish change to `Model.scala`.
The joined-models recipe is fundamentally a composition-layer design
that pairs naturally with the planned v0.2 lineage work.

Pick your poison.