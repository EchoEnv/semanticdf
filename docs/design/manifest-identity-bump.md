# Design Recipe: Manifest Identity + Governance Bump

**Status:** DRAFT
**Library version that would emit this shape:** `v0.1.11-manifest`
**Scope:** Single, additive feature. Extends the existing single-table manifest schema with 5 new optional fields (id, namespace, metadata, $schema, manifestVersion). **No change** to dimensions, measures, filters, or digest content. **No breaking change** to existing manifests. The joined-manifest recipe (separate, BLOCKED) will use the new `id` field for cross-referencing.

## 1. What this is (and what it isn't)

The current single-table manifest (PR #132, refined in #139 / #140 / #144) records a model's static definition. What's missing is the **identity + governance** layer that lets manifests reference each other and be tracked across environments. This recipe adds 5 optional top-level fields that are **metadata, not content** — the dim/measure/filter shape is untouched.

**What it is:** 5 new optional fields, all backwards-compatible. Every existing manifest parses unchanged. The recipe is the foundation for:
- The joined-manifest recipe (BLOCKED) — uses the new `id` field to reference side manifests by FQN
- Future lineage work — uses `id` to track model versions

**What it is NOT:**
- ❌ A change to the `dimensions` / `measures` / `filters` content
- ❌ A breaking change to the existing `kind: "semanticdf-model-manifest"` shape
- ❌ A new `kind` for single-table models — single-table stays at `kind: "semanticdf-model-manifest"`
- ❌ A new joined-manifest shape (separate recipe)
- ❌ A `transforms[]` field (separate, BLOCKED recipe — orthogonal to identity)

## 2. The gap this fills

| Need | Current state | After this recipe |
|---|---|---|
| Stable cross-references between manifests | None — manifests have no `id`, joined models can only reference by file path | `id` (FQN) on every manifest; `relationships[].left/right` reference by FQN |
| Schema validation | None — `schemaVersion` is a free-form string; tooling can't validate | `$schema` URL + `manifestVersion` semver; tooling can fetch and validate |
| Multi-env deployment | None — no way to mark a manifest as `dev` vs `prod` | `namespace` field; consumer filters by namespace |
| Audit metadata | Only `compiledAt` timestamp | `metadata: { author, license, tags, ... }` for full audit chain |
| Versioning of the manifest spec itself | Implicit (`schemaVersion: "v0.1.9-manifest"`) | Explicit `manifestVersion: "1.0.0"` semver |

The 5 new fields are a small, focused bump. The schema is otherwise untouched.

## 3. Proposed schema

The v0.1.11 single-table manifest shape (existing fields shown for context, new fields marked **NEW**):

```json
{
  "schemaVersion": "v0.1.11-manifest",        // bumped from v0.1.9
  "kind": "semanticdf-model-manifest",
  "compiledAt": "2026-07-22T05:00:00Z",

  "manifestVersion": "1.0.0",               // NEW: semver of the manifest spec
  "$schema": "https://github.com/EchoEnv/semanticdf/schemas/manifest.schema.json",  // NEW
  "id": "io.semanticdf.examples.starter.flights",  // NEW: FQN
  "namespace": "default",                     // NEW: multi-env scoping
  "metadata": {                              // NEW
    "author": "data-platform-team",
    "license": "Apache-2.0",
    "tags": ["flights", "airline"]
  },

  "model": {
    "name": "flights",
    "version": 0,
    "status": "published",
    "description": "...",
    "sourceTable": "flights_csv"
    // optional: "id": "..." overrides top-level id if the model needs a
    // different FQN than the artifact
  },

  "dimensions": [...],                        // unchanged
  "measures": [...],                          // unchanged
  "joins": [],                                 // unchanged
  "filters": [],                               // unchanged

  "digest": {...},                            // unchanged
  "warnings": []                              // unchanged
}
```

The 5 new fields are at the **top level** (siblings of `schemaVersion` / `kind` / `model`). They're not nested under `model` because they're properties of the **artifact** (the JSON file), not of the model itself. A model can have an `id` in the joined-manifest recipe's `relationships[].left/right` sense, but the artifact's `id` is the canonical handle for the JSON file.

The joined-manifest shape (separate recipe) uses the same top-level fields plus a `relationships[]` array — this recipe defines the fields the joined recipe can rely on.

## 4. Decisions locked

| Decision | Choice | Rationale |
|---|---|---|
| Are the 5 new fields required? | **Optional** | Backwards compat — every existing manifest parses unchanged. The `schemaVersion` string bumps to `v0.1.11-manifest` so consumers can gate on it; new fields default to absent. |
| `id` format | **Reverse-DNS FQN string** (e.g. `io.semanticdf.examples.starter.flights`) | Matches the example's pattern. Stable, hierarchical, no spaces. Tools can derive a path from it (or vice versa) for the joined-manifest recipe. |
| `id` uniqueness scope | **Globally unique** | A manifest is a portable artifact; its `id` must be globally stable so cross-references resolve. Mirrors the example's `com.yourorg.semantic-module.orders` pattern. |
| `id` per model vs per artifact | **One `id` per artifact** (top-level) | The artifact IS the model for single-table; a future joined-manifest can have a different `id` per side. If the model needs a distinct FQN from the artifact, an optional `model.id` overrides the top-level — not implemented in v1. |
| `manifestVersion` semver | **Major.minor.patch** (e.g. `1.0.0`) | The example uses `1.0.0`. Tracks the manifest-spec format itself, separate from the `schemaVersion` (which tracks the library version that produced it). |
| `$schema` value | **GitHub-hosted JSON Schema URL** | The example's `$schema` points to a tool's JSON Schema. We host a JSON Schema file at `https://raw.githubusercontent.com/EchoEnv/semanticdf/main/schemas/manifest.schema.json` (a new file, but small and version-controlled). |
| `metadata` shape | **Free-form object** with conventional keys | The example uses `{author, license, created, tags, ...}` — no schema, just conventional keys. Tools can read any keys; no required field beyond what's in the example. |
| `namespace` format | **Free-form string** (e.g. `default`, `dev`, `prod`, `team-X`) | Single field, simple, multi-purpose. Consumer code filters by namespace as needed. |
| `tools.Main manifest` regenerates the new fields | **YES** — `toJson` adds the new top-level fields from the `SemanticTable` and emission context | The writer is the source of truth for `id` and `metadata`. The reader parses them all. |
| Backwards compat for old readers | **YES** — old `tools.Main validate-manifest` ignores unknown fields (tolerated-extra-fields pattern, established in PR #140) | Adding new fields doesn't break old readers; they just don't see the new info. |
| `metadata.license` default | **Apache-2.0** if not specified | Match the project's actual license. |

## 5. API surface

```scala
// Existing API: unchanged
def SemanticManifest.toJson(model: SemanticTable, prettyPrint: Boolean = true): String
def SemanticManifest.fromJson(text: String, source: DataFrame): SemanticTable
def SemanticManifest.parseMeta(text: String): ManifestMeta

// New: ManifestMeta gets 4 new fields, all optional
final case class ManifestMeta(
    schemaVersion:    String,
    kind:             String,
    compiledAt:       String,
    // NEW in v0.1.11:
    manifestVersion:  Option[String] = None,    // semver of the spec
    id:               Option[String] = None,    // FQN
    namespace:        Option[String] = None,    // multi-env
    metadata:         Map[String, String] = Map.empty,  // free-form
    // Existing:
    modelName:        Option[String],
    version:          Int,
    description:      Option[String],
    sourceTable:      Option[String],
    status:           String,
    dimensions:       Int,
    measures:         Int,
    calcMeasures:     Int,
    joins:            Int,
    filters:          Int,
    isStreaming:      Boolean,
    usesTAll:         Boolean,
    transforms:       Int = 0,                  // added in v0.1.10 (BLOCKED)
)
```

`$schema` is **not** exposed via `ManifestMeta` — it's a tooling-only URL, not a runtime field. Tools that need to validate can fetch the URL and validate the JSON; `SemanticManifest` itself doesn't need it.

The `tools.Main manifest` writer pulls `id` and `namespace` from the writer's environment (CLI flags or pom-derived defaults). `metadata` is built from a small config file (`.sdf-manifest.yml`) in the project root.

## 6. Writer-side changes (small)

`tools.Main manifest` already pulls the `SemanticTable` from the YAML and emits the JSON. The new fields are filled in by the writer from the CLI invocation context:

```
$ mvn exec:java -Dexec.mainClass=io.semanticdf.tools.Main \
  -Dexec.args="manifest \
    --yaml models/orders.yml \
    --out manifests/ \
    --id com.yourorg.orders \
    --namespace dev \
    --metadata-file .sdf-manifest.yml"
```

`.sdf-manifest.yml` (new, optional) is a small YAML file at the project root:
```yaml
author: data-platform-team
license: Apache-2.0
tags: [orders, fulfillment]
```

If `--id` / `--namespace` / `--metadata-file` are absent, the writer falls back to:
- `id` = derived from the YAML path (e.g. `models/orders.yml` → `io.semanticdf.<groupId>.<artifactId>.orders` if a parent pom provides groupId/artifactId; else a content-based hash)
- `namespace` = `"default"`
- `metadata` = empty

This way the writer "just works" without ceremony, and explicit overrides are available for production use.

## 7. Test plan

| Test | Asserts |
|---|---|
| `id-round-trips` | `SemanticManifest.toJson` emits `id`; `fromJson` + `parseMeta` reads it back. |
| `id-absent-omits-field` | A manifest without `id` parses unchanged; `parseMeta.id == None`. |
| `namespace-round-trips` | Same for `namespace`. |
| `metadata-round-trips` | `metadata: {author: "...", license: "..."}` round-trips through `parseMeta.metadata`. |
| `metadata-empty-default` | Manifest without `metadata` field parses to `parseMeta.metadata == Map.empty`. |
| `manifestVersion-round-trips` | `manifestVersion: "1.0.0"` parses to `parseMeta.manifestVersion == Some("1.0.0")`. |
| `$schema-field-present-in-emit` | `toJson` emits `$schema` pointing to the GitHub URL. |
| `$schema-field-not-required` | Reader ignores `$schema` (tolerated-extra-fields). |
| `old-manifest-still-parses` | A v0.1.9 manifest without the new fields parses to a valid `ManifestMeta` (all new fields `None` / empty). |
| `parseMeta-source-free` | `parseMeta` works on a manifest without any source DF (it's metadata-only). |
| `tools.Main manifest -writes-new-fields` | CLI smoke test: `manifest --yaml X --out Y` produces a file with all 5 new fields populated when CLI flags are given. |
| `tools.Main manifest -defaults-id` | CLI smoke test: when no `--id` is given, the writer falls back to a derived default. |

12 tests across `SemanticManifestSpec` (8) and a new `ManifestWriteSpec` (4) for the CLI smoke tests.

## 8. Diff estimate

| File | LOC delta |
|---|---|
| `src/main/scala/io/semanticdf/SemanticManifest.scala` | +40 (writeMetadata; parseMeta updates) |
| `src/main/scala/io/semanticdf/tools/Main.scala` | +30 (new CLI flags; metadata-file parsing) |
| `src/test/scala/io/semanticdf/SemanticManifestSpec.scala` | +120 (8 new tests) |
| `src/test/scala/io/semanticdf/tools/ManifestWriteSpec.scala` (new) | +60 (4 smoke tests) |
| `schemas/manifest.schema.json` (new, in repo) | +50 (small JSON Schema; first commit) |
| `docs/agents/mcp-contract.md` (mention the new fields) | +10 |

**Total: ~310 LOC across 6 files.** Single PR. **No breaking change** to existing single-table models.

## 9. Anti-scope (deliberately)

- ❌ A new `kind` for single-table models — single-table stays at `kind: "semanticdf-model-manifest"`
- ❌ The joined-manifest shape (`relationships[]` array, `sides{}` block) — separate recipe
- ❌ A `transforms[]` field — separate BLOCKED recipe
- ❌ A `versioning` field on individual dimensions/measures — already covered by `model.version`
- ❌ A `permissions` / `extensionPoints` block (the example's extensibility layer) — out of scope; access control is operator-side
- ❌ A new `kind: "semanticdf-joined-manifest"` — that comes in the joined-manifest recipe
- ❌ Per-model `id` override (`model.id` distinct from top-level `id`) — defer until a real consumer needs it

## 10. Migration / backwards compatibility

| Concern | Resolution |
|---|---|
| Existing v0.1.9 / v0.1.10 single-table manifests | Parse unchanged. All 5 new fields are absent → `parseMeta` returns `None` / empty for them. |
| Existing `tools.Main manifest` callers | No API change. The new CLI flags are optional; absence falls back to sensible defaults. |
| Existing `tools.Main validate-manifest` callers | No change. The reader tolerates unknown fields (extra-fields pattern). |
| MCP `describe_model` | No change in v0.1.11. The new fields surface via `ManifestMeta` but don't appear in `describe_model.data` until a follow-up wires them through. (One-line change in `handlers/DescribeModel.scala`; defer to keep this recipe focused.) |
| OKF gen (`make okfgen`) | No change. The OKF docs don't currently surface the manifest's id / metadata; defer to a follow-up. |
| Wire-stable strings | The `schemaVersion` bumps to `v0.1.11-manifest`. Consumers that gate on the version string will need to update. |

## 11. Open questions for review

| # | Question | Current proposal |
|---|---|---|
| Q1 | Where does the `id` come from when `--id` is absent? | **Derived from the pom's groupId/artifactId + the model's name** (e.g. `io.semanticdf.examples.starter.flights`). If no parent pom, fall back to `<groupId>.<model-name>` derived from the manifest's `model.name`. |
| Q2 | Should `id` be required for new manifests emitted by `tools.Main manifest`, or always optional? | **Always optional** at the schema level (backwards compat), but the writer emits it (derives a default if not given). Existing readers don't break; new writers always populate it. |
| Q3 | Does `metadata.license` need a validation pattern (SPDX identifiers)? | **No** — free-form string. The example's metadata is `{author, license, created, ...}` with conventional keys, no schema. We follow the same pattern. |
| Q4 | Does `tools.Main validate-manifest` need to validate the new fields? | **YES** — the writer is the source of truth. The validator should at least check that `id` is present (warning, not error, for back-compat) and that `$schema` matches the supported URL. |
| Q5 | Should the joined-manifest recipe use the new `id` for cross-referencing, or file paths? | **FQN (`id`)** — the whole point of this recipe. File paths are still a valid alternative (the joined-manifest recipe covers both shapes). |
| Q6 | Where does `manifestVersion` get bumped? | **On backwards-incompatible schema changes only.** A new optional field is a MINOR bump. Removing or renaming a field is MAJOR. The `schemaVersion` (v0.1.11-manifest) tracks the library version, separate from `manifestVersion` (1.0.0). |
| Q7 | Should `metadata` allow nested objects (e.g. `metadata.contact: {email: ...}`)? | **YES, but flatten for `parseMeta.metadata`** — the manifest's `metadata` is a free-form object; `parseMeta` flattens to `Map[String, String]` for ergonomic access. Nested objects are accessible via the raw JSON. |
| Q8 | Is this a breaking change for `tools.Main manifest`? | **No** — same CLI surface; new flags are optional. Default behavior (no flags) produces a manifest with the new fields auto-filled. |