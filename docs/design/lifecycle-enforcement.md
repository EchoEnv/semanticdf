# Design Recipe: Lifecycle Enforcement on `Deprecated` / `Draft` Models

**Status:** SHIPPED (recipe was ACCEPTED)
**Library version that emits this shape:** `0.1.10`
**Scope:** Single, additive feature (3 MCP handler edits + 1 schema field + 3 spec edits + 1 doc edit). No library API changes. No new error codes.

## 1. What this is (and what it isn't)

`SemanticTable.status` carries lifecycle state — `Draft` / `Published` / `Deprecated` — and surfaces in `describe_model.data.status` and the manifest artifact. The library terminals (`toDataFrame`, `toStreamingQuery`, `execute`) stay permissive: the lifecycle marker is informational at the library level; **consumers enforce policy**.

This recipe adds the **MCP-server-side enforcement** layer: when a model is `Deprecated` or `Draft`, the relevant MCP tool responses carry a warning in the existing `Envelope.warnings: List[String]` field. No new error code. No refusing queries. Agents see the warning and choose how to act.

**Critical contract note:** lifecycle states produce **success envelopes with warnings**, not error envelopes. The closed MCP error-code list in `docs/agents/mcp-contract.md` §"Error codes" is **unchanged**. This is an additive, opt-in signal, not a refusal path.

**Why warn, not refuse:**

1. **Additive contract change.** Adding a `warnings: ["..."]` to existing `Envelope.ok(...)` responses is wire-additive; clients that ignore the new field keep working. Refusing with a new error code (e.g. `MODEL_DEPRECATED`) is a breaking change for every existing agent that doesn't check status before querying.
2. **Agents are the right place to enforce.** An LLM agent that sees `"model 'flights' is deprecated"` can choose to skip, ask the user, or query anyway. A server-side refusal hides information.
3. **Matches the existing intent.** `ModelStatus`'s docstring (Model.scala:397) explicitly says *"Consumers SHOULD warn before serving results; future work will add explicit refusal."* — `SHOULD`, not `MUST`.

**What this is NOT** (anti-scope):

- ❌ Library-level refusal — `toDataFrame` / `toStreamingQuery` / `execute` stay permissive
- ❌ New MCP error codes — `MODEL_DEPRECATED`, `MODEL_DRAFT` would be breaking
- ❌ `SemanticManifest` schema changes — manifest stays pure inspection (status is already there)
- ❌ `OkfGen` policy — OKF remains documentation, generates for any status
- ❌ `tools.Main` CLI tools — `manifest` / `validate-manifest` already pass status through; no policy needed
- ❌ Per-server configuration of warning behavior — single policy, no opt-in/opt-out

## 2. Decisions locked

| Decision | Choice | Rationale |
|---|---|---|
| Warn vs refuse for `Deprecated` | **Always warn, never refuse** | Additive wire change; matches docstring intent; agents are the right enforcement point |
| Same treatment for `Draft` | **Same as Deprecated** (different wording) | Both are lifecycle signals agents should know about; Draft means "results may change" |
| Warning string format | **Lowercase human-readable** | Goes into `Envelope.warnings: List[String]` for LLM agent consumption, not error-code routing. Existing model: `Introspect` populates `data.warnings` with lowercase strings like `"field 'foo' was skipped"`. |
| Per-server policy config | **None** | Single behavior; no opt-in/opt-out. Future work can add `MCP_LIFECYCLE_POLICY=warn|refuse|ignore` if real demand. |
| `list_models` surface | **`status: String` in `ModelSummary`** | Agents can pre-filter without a follow-up `describe_model` |

## 3. Hook points (the site map)

All hook points already have the `Envelope.warnings` field infrastructure; none require a new envelope shape.

| # | File:line | Current | Change |
|---|---|---|---|
| 1 | `semanticdf-mcp/.../handlers/Query.scala:90-92` (`handle`) | `Envelope.ok(data, warnings = Nil, meta = ...)` | Build warnings from `t.status`; pass `warnings = ...` |
| 2 | `semanticdf-mcp/.../handlers/Query.scala:113` (`explain`) | `Envelope.ok(planText, meta = ...)` | Same warning builder applied |
| 3 | `semanticdf-mcp/.../handlers/ListModels.scala:38-53` (`handle`) | `Envelope.ok(data)` | Build warnings (one per Deprecated/Draft model); add `status` field to `ModelSummary` |
| 4 | `semanticdf-mcp/.../handlers/DescribeModel.scala:108-141` (`handle`) | `Envelope.ok(data)` | Build single warning when `t.status != Published` |

The builder is a single helper, hoisted to the `Handlers` companion in `Json.scala:63` alongside `modelNameSchema`. **Signature: `Handlers.lifecycleWarnings(modelName: String, status: ModelStatus): List[String]`** — takes the registry key (canonical name the agent called the model with), not `t.name`, because `t.name` may be empty for anonymous models or differ from the registry key. The caller passes both — the handler already has `t.status` and the `request.model` (the registry key).

## 4. Warning strings (wire format)

| Status | Warning string | Examples |
|---|---|---|
| `Deprecated` | `"model '<name>' is deprecated"` | `"model 'flights' is deprecated"` |
| `Draft` | `"model '<name>' is in draft; shape may change"` | `"model 'flights' is in draft; shape may change"` |
| `Published` | *(none)* | — |

These are **wire-stable strings**. Renaming is a breaking change to consumers that pattern-match on them. The strings are lowercase + human-readable so LLMs can render them naturally to end users.

## 5. `ModelSummary` schema change (`list_models`)

**Current** (ListModels.scala:33):
```scala
final case class ModelSummary(
    name: String,
    description: String,
)
```

**New**:
```scala
final case class ModelSummary(
    name: String,
    description: String,
    status: String,    // "draft" | "published" | "deprecated" — wire format
)
```

The `status` field is required (not `Option`) because every model has a status (defaults to `Published`).

**Backwards compatibility caveat:** adding a required field is *additive* for tolerant JSON clients that ignore unknown fields (the common case — all LLM-agent SDKs do this), but it IS a breaking change for strict schema validators, exhaustive decoders, snapshot tests, and positional consumers. Library users with custom MCP adapters will need to add a `status` field to their own `ModelSummary` shape — documented in the PR migration notes.

## 6. Out of scope (deliberately)

- **Refusal mode.** Documented in `ModelStatus`'s docstring as "future work." Adding it requires either a config flag (breaking if default changes) or a new error code (breaking either way). The warning path gives consumers enough signal today.
- **Per-call acknowledgment.** Some servers implement `?acknowledge_deprecated=true` query params. Out of scope — keep the contract minimal.
- **Audit log of warnings.** A separate feature for ops visibility. Not in scope.
- **`Model.status` for library builders.** Library terminals stay permissive. Documented in `Model.scala:386`.

## 7. Tests (the guarantees we make)

| Test | Asserts |
|---|---|
| `query-warns-deprecated-model` | `Query.handle()` against a Deprecated model returns `Envelope[Data]` with `warnings` containing `"model '<name>' is deprecated"` |
| `query-warns-draft-model` | Same, for `Draft` status, with the draft-specific wording |
| `query-does-not-warn-published` | `Query.handle()` against a Published model returns `warnings == Nil` |
| `query-explains-warns-deprecated` | `Query.explain()` follows the same warning policy as `handle()` |
| `list-models-carries-status` | `list_models` `data.models` entries each have a `status: String` field |
| `list-models-warns-deprecated` | `list_models` envelope warnings include one entry per Deprecated/Draft model |
| `list-models-warning-order-deterministic` | Warnings appear in alphabetical model-name order, matching the alphabetical `models` list (helps snapshot tests) |
| `list-models-no-duplicate-warnings` | A model is never warned twice (one entry per model regardless of how many times the helper runs) |
| `list-models-does-not-warn-published` | `list_models` against an all-Published registry returns `warnings == Nil` |
| `describe-warns-deprecated` | `describe_model` of a Deprecated model returns `warnings` containing the deprecation string |
| `describe-warns-draft` | `describe_model` of a Draft model returns the draft-specific wording |
| `describe-does-not-warn-published` | `describe_model` of a Published model returns `warnings == Nil` |
| `draft-warning-text-distinct-from-deprecated` | Both warnings present and not equal; agent can distinguish |
| `transport-warnings-and-status-serialize-to-json` | Round-trip via the JSON mapper confirms `warnings` and `status` appear in the wire output (not just in-memory shape) |
| `error-envelope-codes-unchanged` | Regression: the closed error-code list (`MODEL_NOT_FOUND`, `RESULT_TOO_LARGE`, etc.) is unaffected — lifecycle states produce success envelopes with warnings, not error envelopes |

15 tests across `QuerySpec`, `ListModelsSpec`, `DescribeModelSpec`, plus one transport test in `RestServerSpec` and one error-code regression in `QuerySpec`.

**Note:** warning strings are **display text**, not error identifiers. Consumers SHOULD render them to end-users / pass to LLMs verbatim, but SHOULD NOT pattern-match on exact substrings (use the structured `data.status` field for any routing logic). The wire-format lowercasing and human-readability are for LLM consumption.

## 8. Diff estimate (concrete LOC)

| File | LOC delta |
|---|---|
| `semanticdf-mcp/src/main/scala/io/semanticdf/mcp/handlers/Query.scala` | +12 |
| `semanticdf-mcp/src/main/scala/io/semanticdf/mcp/handlers/ListModels.scala` | +8 |
| `semanticdf-mcp/src/main/scala/io/semanticdf/mcp/handlers/DescribeModel.scala` | +6 |
| `semanticdf-mcp/src/main/scala/io/semanticdf/mcp/Json.scala` (helpers in `Handlers` companion) | +20 |
| `semanticdf-mcp/src/test/scala/io/semanticdf/mcp/handlers/QuerySpec.scala` | +25 |
| `semanticdf-mcp/src/test/scala/io/semanticdf/mcp/handlers/ListModelsSpec.scala` | +30 |
| `semanticdf-mcp/src/test/scala/io/semanticdf/mcp/handlers/DescribeModelSpec.scala` | +25 |
| `docs/agents/mcp-contract.md` (warning contract + updated response example for `list_models` showing `status` field, + a "Lifecycle warnings" subsection) | +60 |

**Total: ~185 LOC across 8 files.** Single PR. No library changes.

The mcp-contract.md update includes both prose AND a refreshed `list_models` example response shape (with `status` per model) — so the published contract stays consistent with the implementation. A stale example in the contract is a bug source.

## 9. Anti-scope contract

This recipe does NOT change:

- `SemanticTable.status` semantics 
- `SemanticManifest` schema
- `YamlLoader.parseStatus` 
- `OkfGen` frontmatter (v0.1.10+)
- Library terminals (`toDataFrame` / `toStreamingQuery` / `execute`)
- MCP error-code closed list (no new codes)
- `tools.Main` CLI behavior

## 10. Migration / backwards compatibility

| Concern | Resolution |
|---|---|
| Existing clients that ignore `warnings` | Unaffected — wire-additive only |
| Existing clients that don't read `ModelSummary.status` | Unaffected — wire-additive only |
| Library users with their own MCP adapters | They'll need to adopt the same pattern; documented in the PR |
| Existing OKF reference bundles | Regenerate via `make okfgen`; `okf_mapping.md` frontmatter unchanged |
| Pre-status models (built before v0.1.10+) | Default to `Published`; no warning emitted |

## 11. Resolved questions (from senior-engineer review)

| # | Question | Resolution |
|---|---|---|
| Q1 | Should `Draft` warnings be emitted on `list_models` (per-model) or only `describe_model` (per-query)? | **Per-model on `list_models`** — agents pre-filter before even calling describe |
| Q2 | Helper signature | **`Handlers.lifecycleWarnings(modelName: String, status: ModelStatus): List[String]`** — takes the registry key (canonical name the agent called the model with), not `t.name`; lives in the `Handlers` companion object in `Json.scala:63` |
| Q3 | Author in Draft warning | **Just the lifecycle state** — author info isn't on `SemanticTable` |
| Q4 | `explain` warnings | **Yes** — `explain` is the planning companion to `handle`; agents planning a query on a deprecated model should know |
| Q5 | Helper duplication | **Single helper, three call sites** — `Query.handle`, `Query.explain`, `ListModels.handle`, `DescribeModel.handle` all share the same `Handlers.lifecycleWarnings(...)` |
| Q6 | Wire format punctuation (em dash vs ASCII) | **ASCII** — "shape may change" without the em dash |
| Q7 | Determinism of `list_models` warning order | **Alphabetical** — matches the alphabetical model list, helps snapshot tests |
| Q8 | Closed error-code list affected? | **No** — explicitly stated in §1 and pinned by `error-envelope-codes-unchanged` test |