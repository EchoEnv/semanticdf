# Design Recipe: Manifest Artifact (`SemanticManifest.toJson` / `fromJson`)

**Status:** SHIPPED (recipe was ACCEPTED; implementation landed in PR #132 for v0.1.9 — extended in PR #140 for calc measures and PR #132's example in PR #139)
**Library version that emits this shape:** `0.1.9-manifest` (extended in `v0.1.11-manifest`)
**Scope:** Single, additive feature (`SemanticManifest` object + `tools.Main manifest` route + tests). No op-tree changes. No new framework dependencies.

> **Implementation note (carried from PR #132):** The recipe originally
> said "no new external dependencies." The implementation took a small
> deviation: it uses **`jackson-databind` + `jackson-module-scala_2.13`**
> (already a project dep via `semanticdf-mcp`, pinned to 2.15.2 to match
> Spark's bundled Jackson) instead of a hand-rolled JSON serializer. The
> trade-off was ~250 LOC of hand-rolled parse/serialize + edge-case
> handling vs. one explicit library-pom dep declaration. The dep itself
> is unchanged in semanticdf-mcp; only the library pom now declares it
> explicitly instead of relying on Spark's transitive `provided` scope.

## 1. What this is (and what it isn't)

A **manifest** is the semantic model's *static definition* serialized as JSON. It carries everything needed to reconstruct a `SemanticTable` without re-parsing the YAML — the model identity, dimensions, measures, joins, filters, and a small "at-a-glance" digest header. It does NOT carry the model's *computed output* (data); the operator program owns lifecycle and persistence of data, as per the v0.1.9 boundary (PRs #117, #124, #129).

**Why** (real use cases — not speculative):

1. **Version-pinning without git.** A consumer can pin to `flights.v1.4.2.json` instead of `models/flights.yml`. Rollback is "swap the JSON."
2. **Cross-tool consumption without Spark.** Agents, BI tools, ad-hoc scripts can read the manifest JSON without loading the library or running YAML through SnakeYAML.
3. **Audit / observability.** "What was the actual compiled model at deploy time?" is `cat flights.v1.4.2.json | jq`.
4. **Load-time perf for query apps.** `SemanticManifest.fromJson(json)` is faster than `YamlLoader.load(path, ...)` for hot-path startup. (Not currently measured to be hot, but the JSON form is cheaper to parse.)

**What this is NOT** (anti-scope, per karpathy "minimum code that solves the problem"):

- ❌ Catalog publishing (Unity/Glue/Polaris). Operator-side.
- ❌ Materialization orchestration (incremental / view / table / cache modes). Operator-side.
- ❌ Cross-model references / dependency graphs. We don't have cross-model joins yet.
- ❌ Sign or verify. We don't sign anything else; nobody asked.
- ❌ Schema version negotiation (v1 ↔ v2). Same library parses both formats; no current demand for negotiation.
- ❌ Compression / canonicalization. Premature.
- ❌ Embedded computed views (`explainSemantic` narrative, full lineage graph). These get stale the moment the artifact is written; can be regenerated from the manifest at any time.

## 2. JSON schema (concrete example for `examples/streaming-events/models/events.yml`)

```json
{
  "schemaVersion": "v0.1.9-manifest",
  "kind": "semanticdf-model-manifest",
  "compiledAt": "2026-07-21T16:30:00Z",

  "model": {
    "name": "events",
    "version": 9,
    "status": "published",
    "description": "Real-time events arriving on the events topic.",
    "sourceTable": "events_stream"
  },

  "digest": {
    "dimensions": 2,
    "timeDimensions": 1,
    "derivedTimeDimensions": 0,
    "measures": 2,
    "calcMeasures": 0,
    "joins": 0,
    "filters": 1,
    "isStreaming": true,
    "usesTAll": false
  },

  "dimensions": [
    {
      "name": "event_type",
      "kind": "categorical",
      "expr": "type",
      "isTimeDimension": false,
      "isEntity": false,
      "smallestTimeGrain": null,
      "isDerived": false
    },
    {
      "name": "timestamp_bucket",
      "kind": "time",
      "expr": "timestamp",
      "isTimeDimension": true,
      "isEntity": false,
      "smallestTimeGrain": "second",
      "isDerived": false
    }
  ],

  "measures": [
    {
      "name": "event_count",
      "kind": "base",
      "expr": "count(1)",
      "dependsOn": []
    },
    {
      "name": "total_value",
      "kind": "base",
      "expr": "sum(value)",
      "dependsOn": []
    }
  ],

  "joins": [],

  "filters": [
    {
      "name": "require_known_event_type",
      "expr": "type IS NOT NULL",
      "appliedAt": "pre_aggregate"
    }
  ]
}
```

**Why flat (no nested arrays of "all the things"):** JSON-friendly; `jq .measures[].name`, `jq '.dimensions | map(select(.isTimeDimension))'` work without tooling.

**Why a `digest`:** JSON consumers (agents, scripts, dashboards) can answer "what's in this model?" by reading 5–10 cheap fields without parsing the rest. `cat foo.json | jq .digest`. No library, no Spark.

## 3. Field-by-field picks (the ones you can push back on)

| Pick | Choice | Why | The question to push back on |
|---|---|---|---|
| `schemaVersion` string | `"v0.1.9-manifest"` (library version + shape) | Tracks "what library version produced this artifact" | Rename to semver-style? `0.1.9-manifest.v1`? |
| `kind` field | `"semanticdf-model-manifest"` | Future-proof: if we add a different artifact (e.g., manifest-diff), `kind` discriminates | Should we drop and only use `schemaVersion`? |
| `version` (single `Int`) | Matches `SemanticTable.version: Int` exactly | Round-trip is exact (no lossy coercion) | Couldn't we have `versionMajor/Minor/Patch`? Future semver split — requires both API + manifest changes, defer |
| `status` field | `"draft" \| "published" \| "deprecated"` | Matches the proposal + governance gap | Default `"published"`; `"deprecated"` causes a loud failure at load time; `"draft"` is informational. PICK: I implement this as a separate PR (status field on `Model.status`). The manifest references it but doesn't introduce it. |
| `digest` block | Top-level cheap counters | `jq` consumers want a one-look summary | Reasonable design. Should `digest` be a separate file (`flights.json + flights.digest.json`)? NO — one artifact, more atomic. |
| `dimensions[].kind` field | `"time" \| "entity" \| "categorical" \| "derived-time"` | Distinguishes 4 cases | Simpler as 2 fields (`isTimeDimension: bool`, `isEntity: bool`, `isDerived: bool`)? PICK: `kind` is cleaner for non-tooling consumers; if you reject `kind`, the 3 bools default. |
| `dimensions[].expr` | String form only | Lambda isn't serializable | Lossy — round-trip can't reconstruct the original lambda, only the string. See §5 (round-trip policy). |
| `dimensions[].isDerived` | New flag for derived-time dims (added in PR #129) | Distinguishes auto-derived from user-declared | If you have no use for it, omit. I include it because today's code already has the field. |
| `measures[].dependsOn` | `Seq[String]` of base-measure names | Quick-glance dependency closure for calc measures | Computed at serialize time from the calc measure's AST — cheap. |
| `filters[].appliedAt` | `"pre_aggregate" \| "post_aggregate"` | Currently we have `SemanticRowFilterOp` (pre) and `SemanticFilterOp` (post) | PICK: just 2 values. Future filter types would add new enum values. |
| `joins[].cardinality` | `"many_to_one" \| "one_to_one" \| ...` | Already in the existing op | Pre-existing. |
| Empty `joins: []` | Allowed | Some models don't join | Empty array. NOT omitted. |
| `compiledAt` | ISO-8601 string | When was this artifact emitted | Trivial; includes time-of-compile for audit. |

**If you disagree with any of these 11 picks**, push back. I'll update the recipe and re-review BEFORE any code.

## 4. API surface

```scala
package io.semanticdf

object SemanticManifest {

  /** The current schema version. Bumped only on breaking changes to
    * the JSON shape (renamed fields, removed fields, type changes).
    * Adding optional fields is non-breaking. */
  val CurrentSchemaVersion: String = "v0.1.9-manifest"

  /** Pretty-print by default (auditing artifact; humans read it).
    * Set `prettyPrint = false` for hot-path machine-to-machine. */
  def toJson(
      model: SemanticTable,
      prettyPrint: Boolean = true,
  ): String

  /** Parse a manifest JSON into a SemanticTable.
    *
    * Throws on:
    *   - malformed JSON
    *   - missing required fields
    *   - schemaVersion != [[CurrentSchemaVersion]] (no negotiation;
    *     a different library must upgrade to emit the supported version)
    *   - type mismatches in known fields
    *
    * Tolerates unknown fields (forward compat). */
  def fromJson(text: String): SemanticTable
}

case class ManifestParsingException(msg: String)
  extends RuntimeException(s"SemanticManifest: $msg")
```

That's it. Two methods. The `case class` for the exception makes catch-and-report clean.

## 5. Round-trip policy (this is the gotcha)

`SemanticManifest.toJson` is **lossy** in one important dimension:

- **Lose:** `Dimension.expr: SemanticScope => Column` (the lambda). Lambdas aren't serializable.
- **Lose:** `Dimension.metadata: Map[String,String]` and `Measure.metadata: Map[String,String]`. Same reason — the metadata map is consumed by in-process tools (`Introspector`, `DescribeModel`, OKF gen) that already have direct access to the `SemanticTable`. The manifest is the cross-tool schema-only artifact; metadata is in-process-only.
- **Preserve:** `Dimension.exprString: Option[String]` — the YAML/DSL string form.

**Streaming detection rule.** The `digest.isStreaming: bool` flag walks the op tree the same way `SemanticTable.toStreamingQuery`'s `findStream` helper does (`SemanticTable.scala:96–108`): it descends through `SemanticAggregateOp` / `SemanticFilterOp` / `SemanticRowFilterOp` / `SemanticOrderByOp` / `SemanticLimitOp` / `SemanticHintOp` / `SemanticTransformsOp` until it hits a `SemanticStreamingTableOp` (true) or a `SemanticTableOp` (false). Without this rule, the digest would falsely report `false` for the common case where a streaming-root model is wrapped in `.groupBy(...).aggregate(...)`. Pinned by test `streaming-source-walks-aggregate-wrapper` below.

**The rule:**
- If `exprString` is `Some(s)`, the manifest carries `s`. The user can round-trip.
- If `exprString` is `None` (a bare-lambda dimension built without an `expr:` string), the manifest records `"<lambda>"` as a sentinel. **Round-trip does NOT produce an identical dimension.**

Test:
```scala
test("round-trip preserves exprString when present, sentinel when absent") {
  val withString = toSemanticTable(df).withDimensions(
    Dimension("carrier", t => t("carrier"), exprString = Some("carrier"))
  )
  val rt = SemanticManifest.fromJson(SemanticManifest.toJson(withString))
  rt.dimensions("carrier").exprString shouldBe Some("carrier")

  val withoutString = toSemanticTable(df).withDimensions(
    Dimension("carriers_hub", t => t("hub"))  // exprString = None
  )
  val rt2 = SemanticManifest.fromJson(SemanticManifest.toJson(withoutString))
  rt2.dimensions("carriers_hub").exprString shouldBe Some("<lambda>")
  // exprString here is a sentinel; the original lambda is NOT recoverable.
  // Tests assert this is acknowledged, not hidden.
}
```

This matches the project's existing `exprString` discipline — which `DescribeModel` already uses for human-readable output.

## 6. CLI surface (`tools.Main`)

```text
$ mvn scala:run -Dexec.mainClass=io.semanticdf.tools.Main \
    -Dexec.args="manifest --yaml models/flights.yml --out artifacts/flights.v1.4.2.json"

$ mvn scala:run -Dexec.mainClass=io.semanticdf.tools.Main \
    -Dexec.args="validate-manifest --file artifacts/flights.v1.4.2.json"
OK: model=flights, dimensions=8, measures=6, joins=1
```

Routes in `tools.Main` (adding 2 new cases):

```scala
case "manifest" :: "--yaml" :: yml :: "--out" :: out :: rest =>
  // Uses existing `tables` resolution (parquet/csv/json paths).
  // Reads the YAML at `yml`, builds the SemanticTable, writes the
  // manifest to `out`. Errors are loud with the file + reason.
  val mt = YamlLoader.load(yml, tables)
  writeFile(out, SemanticManifest.toJson(mt))

case "validate-manifest" :: "--file" :: f :: rest =>
  val mt = SemanticManifest.fromJson(readFile(f))
  println(s"OK: model=${mt.name.get}, dimensions=${mt.dimensions.size}, ...")
```

That's it. **No new CLI verb beyond `manifest` and `validate-manifest`.** No flags like `--pretty=false` — `SemanticManifest.toJson(prettyPrint=false)` is the call site for that; we don't expose it to CLI yet.

## 7. Tests (the guarantees we make)

| Test | Asserts |
|---|---|
| `round-trip-yaml-manifest-yaml-parity` | YAML → SemanticTable → JSON → SemanticTable → query; results match YAML → SemanticTable → query (same ops, same data) |
| `round-trip-preserves-exprString` | YAML dim with explicit `expr: foo` round-trips its exprString intact |
| `round-trip-lambda-sentinel` | Bare-lambda dim (no exprString) round-trips with sentinel `"<lambda>"` and a clear test assertion that the original lambda is not recoverable |
| `digest-accurate` | After round-trip, digest counts match `model.dimensions.size`, etc. |
| `digest-flags-accurate` | `isStreaming` reflects root op type; `usesTAll` reflects measure closure; `timeDimensions` counts `Dimension.isTimeDimension`-true dims; `derivedTimeDimensions` counts time dims with `derived` non-empty |
| `unknown-fields-tolerated` | Adding a new field to a v0.2-manifest does NOT break v0.1.9-manifest parsing (forward compat) |
| `missing-required-fields-error` | Parsing a JSON without `model.name` throws `ManifestParsingException` with a clear message |
| `schema-mismatch-error` | Parsing a JSON with `"schemaVersion": "v0.2.0-manifest"` (hypothetical future) throws `ManifestParsingException` |
| `streaming-source-recorded` | `SemanticStreamingTableOp` root → manifest records `sourceTable` + `isStreaming: true` |
| `streaming-source-walks-aggregate-wrapper` | `SemanticAggregateOp(SemanticStreamingTableOp(...))` root → manifest's `digest.isStreaming` correctly walks through `SemanticAggregateOp` to find the streaming op (rule from §5) |
| `calc-measure-dependsOn-recorded` | A `t.all("total")` calc measure has `dependsOn: ["total"]` |

These 10 tests run as part of the library's normal `mvn test` cycle.

## 8. Diff estimate (concrete LOC)

| File | Change | LOC |
|---|---|---|
| `src/main/scala/io/semanticdf/SemanticManifest.scala` (new) | Object with `toJson`, `fromJson`, exception class | ~110 |
| `src/main/scala/io/semanticdf/tools/Main.scala` (existing) | 2 new routes in the dispatcher | ~25 |
| `src/test/scala/io/semanticdf/SemanticManifestSpec.scala` (new) | 10 tests above | ~150 |

**Total: ~285 LOC.** No external deps added (uses existing SnakeYAML + Spark JSON; if Spark JSON is too heavy for the test-only path, we use a hand-rolled tiny JSON serializer for the artifact, ~30 LOC more).

## 9. Open questions (genuinely undecided — push back if you have a preference)

1. **`status` field timing.** The manifest references `model.status`, but `Model.status` doesn't exist yet. Should the manifest PR also add it? **My pick:** separate PR (status field + manifest can each land independently, smaller diff per PR, status is a governance feature worth its own commit message).
2. **`schemaVersion` semantics.** We bump on breaking changes. What's "breaking"? Renamed fields, removed fields, type changes. Adding optional fields is non-breaking. **Acknowledge my pick in the recipe doc.**
3. **What goes in `filters[].appliedAt` for new filter kinds?** Currently 2 values. Future filter types would add new values (e.g., `streaming_watermark`). **My pick:** add new string values when filter kinds appear; no schema bump required (it's a free-form string in JSON).
4. **Should `manifest --yaml` also accept a `.semantic-manifest.json` from disk?** (i.e., a no-op passthrough or chainable tool). **My pick:** no. It's `yaml → json`; the opposite direction isn't built yet. If a user wants that, they can fork.
5. **Schema version embedded in the file name or only in the JSON body?** **My pick:** body only. The library has stable identities (path resolved by user); the schema version is metadata inside the artifact.
6. **`fromJson` should reject unknown `kind`?** Currently plan accepts `"semanticdf-model-manifest"` only. Anything else → error. **My pick:** reject mismatched kind (no future-proofing here).
7. **`SemanticTable.version: Int` vs future semver.** For v0.1.9 the manifest emits a single `version: int` matching the existing API. A future semver split (`versionMajor/Minor/Patch`) requires coordinated change on both the API surface AND the manifest; defer until a real consumer needs three-part versioning. Today, recipe choice #3 is the conservative single-Int pick — section 5's lossier rules cover it without a manifest break.

## 10. Anti-scope (reaffirmed)

This PR does NOT include:

- **Single-table models only.** `SemanticJoinOp`-rooted tables (`examples/pipeline/models/*.yml` joined shape) are out of scope for this PR. The manifest's `model.sourceTable` + `model.name` shape doesn't fit a `SemanticJoinOp` root, and we have no consumer asking for joined-model persistence yet. Joined models serialize as a separate, additive follow-up when a real consumer emerges.
- Schema evolution / negotiation
- Cross-model references in the manifest
- Embedded computed views (`explainSemantic` output, full lineage graph)
- Sign / verify / checksums
- Compression
- Embedded streaming-config blocks
- Catalog publishing
- An MCP tool that emits manifests — wiring that goes through a separate PR if there's a real consumer
- A `models.v1.json` "manifest-of-manifests" multi-model index — out of scope for v0.2; comes later if needed

## 11. Test path (when implementation lands)

```bash
# Unit (already green after this PR):
mvn -o test
mvn -o test -Dspark.version=4.1.1 -Dscala.version=2.13.18

# Smoke (manual one-liner):
mvn scala:run -Dexec.mainClass=io.semanticdf.tools.Main \
  -Dexec.args="manifest --yaml examples/streaming-events/models/events.yml \
                --out /tmp/events.json"
cat /tmp/events.json | jq .digest
# Expect: digest with dimensions=2, measures=2, isStreaming=true

# Verify round-trip:
mvn scala:run -Dexec.mainClass=io.semanticdf.tools.Main \
  -Dexec.args="validate-manifest --file /tmp/events.json"
# Expect: "OK: model=events, dimensions=2, ..."
```

OK with these picks? Reply with **"ship the recipe"** and I open a PR for THIS DOC (no code yet), then you confirm and I open the implementation PR. Or push back on any pick first.
