# Deterministic-Purity Audit — `semanticdf` Library

**Status:** Audit complete. Findings reported. Fix plan presented for approval.

This document is the output of a senior-data-engineer + senior-software-architect review of the `semanticdf` library, against the deterministic-purity requirement for the Restate-native platform ([`docs/design/platform-architecture.md`](./platform-architecture.md)). The audit target is `src/main/scala/io/semanticdf/` only. `semanticdf-mcp/`, `semanticdf-platform/`, and `examples/` are out of scope (they are callers, not emitters).

## 1. The requirement

Per the platform-architecture design, when a handler calls `semanticdf.of(spark, model)` inside a `Restate.run` block, the result is journaled. On replay, Restate re-runs the handler; the result must be the same as the original execution. **Any non-deterministic call in the library breaks this contract.**

The library must be a **pure function of its inputs** from the journal's perspective. Any wall-clock / random / environment-driven call must either:
- Be removed (Fix C), OR
- Be supplied by the caller as a parameter (Fix A), OR
- Be wrapped in a `Restate.run` block by the call site (Fix B)

## 2. Call sites found

The audit found **7 non-deterministic call sites** in the library. The two senior subagents agreed on the 5 wall-clock sites and added the streaming `createTempFile` (DE) and the `System.nanoTime()` sites (DE + Architect).

| # | Site | File:Line | What's called | Crosses Restate boundary? |
|---|---|---|---|---|
| 1 | Audit `ts` (ok path) | `SemanticTableCore.scala:178` | `Instant.now()` | **Yes** — inside the `try` block that wraps `toDataFrame` |
| 2 | Audit `ts` (error path) | `SemanticTableCore.scala:196` | `Instant.now()` | **Yes** — same `try` block |
| 3 | Streaming audit `ts` | `SemanticTableStreaming.scala:470` | `Instant.now()` | **No** — `foreachBatch` is a Spark driver callback, not a Restate handler. Per-batch, not per-replay. |
| 4 | Manifest `compiledAt` (joined) | `SemanticManifest.scala:444` | `Instant.now()` via `DateTimeFormatter.ISO_INSTANT.format(...)` | **Yes** — manifest write is in the platform's "model-record-at-this-version" path |
| 5 | Manifest `compiledAt` (single-table) | `SemanticManifest.scala:861` | `Instant.now()` | **Yes** |
| 6 | `elapsedMs` (3 sites) | `SemanticTableCore.scala:172,194` and `SemanticTableStreaming.scala:464` | `System.nanoTime()` (monotonic, NOT wall-clock) | Yes (batch), No (streaming). On replay, `nanoTime()` re-measures and gets a different value. |
| 7 | `createTempFile` checkpoint default | `SemanticTableStreaming.scala:264` | `java.io.File.createTempFile(...)` | **Yes** — and worse than #6: checkpoint path includes a per-JVM random suffix. On replay, path differs → Spark's checkpoint store loses continuity → either stream reset or silent replay-of-history. |

## 3. Severity-ranked findings

The two subagents' findings reconciled:

| Severity | Site | Replay consequence | Fix |
|---|---|---|---|
| **Critical** | #7 `createTempFile` checkpoint | Stream reset / double-processing of history | **A** (mandatory): platform must require `checkpointLocation`; library can stay as-is with a louder warning, or refuse to start without one. |
| **Critical** | #1, #2 `AuditEvent.ts` | Replay creates a new audit row with a different `ts`. The `whereHash`/`havingHash` are identical, so consumers deduplicating by query-shape see two events. | **A**: pass `clock: () => Instant` from the platform. Library becomes a pure function of `(request, now)`. |
| **High** | #4, #5 manifest `compiledAt` | Manifest overwritten on replay with new timestamp. Third-party consumers may use `compiledAt` for change detection. | **A**: pass `clock` to `toJson` / `toJoinedJson`. |
| **Medium** | #6 `elapsedMs` | `elapsedMs` differs by a few ms across replays. Observability noise, not correctness. | **A** (preferred): pass `clock` and compute from clock. Or **C** (drop field entirely — the platform records handler duration in its own metrics). |
| **Low** | #3 streaming audit `ts` | None visible — per-batch, not per-replay. | **D**: accept, document. |
| **Low** | streaming `t0` closure capture | None — closed-over `val`, replay-stable. | none. |

## 4. The single most important question

> **What is the platform's `dedupHash` contract for `AuditEvent`?**

The platform's `AuditService.append` (`semanticdf-platform/.../AuditService.java:34`) dedupes on `LAST_DEDUP_HASH`. The platform computes the hash from the `AuditEvent`. **If the hash includes `ts` or `elapsedMs`, every replay creates a new audit row** — no amount of library-side purity fixes this; the platform's hash function is the actual contract.

`AuditEvent` carries 7 hashable fields: `model`, `version`, `measures`, `dimensions`, `whereHash`, `havingHash`, `rowCount`. Of those, `rowCount` *might* differ on replay (only if there's genuine Spark non-determinism). The dangerous fields are `ts` and `elapsedMs`. **The hash must include only the query-shape fields (`model`, `version`, `measures`, `dimensions`, `whereHash`, `havingHash`).**

This is a **needs-confirmation** question per the `clean-architecture-refactor` skill: the answer pivots between Option A (preserve the field, hash only the shape) and Option C (drop the field, the platform's Postgres `received_at` column owns the timestamp).

**Recommendation: add an explicit `dedupHash: String` field to `AuditEvent`** so the library owns the contract rather than leaving it implicit. The platform uses the field; the library's hash is deterministic (over the query-shape fields).

## 5. Prioritized fix plan (severity-ordered, per `clean-architecture-refactor`)

| # | Finding | Fix | Verification | Risk |
|---|---|---|---|---|
| 1 | #7 `createTempFile` checkpoint | **Library**: change default to fail-fast with a clear error. **Platform**: `StreamingService.run` requires `checkpointLocation` from caller. | Unit test: instantiating the streaming config without `checkpointLocation` throws. | Needs-confirmation (platform discipline). |
| 2 | #1, #2 `AuditEvent.ts` | **A**: add `clock: () => Instant = () => Instant.now()` to `toDataFrame`'s audit path. | `DeterministicPuritySpec`: inject frozen clock, assert `ts == clock()`. Replay-equivalence test: two consecutive calls with same clock produce same `ts`. | Auto-safe. |
| 3 | #4, #5 manifest `compiledAt` | **A**: add `clock: () => Instant = () => Instant.now()` to `toJson` / `toJoinedJson` / `buildJsonTree`. | `DeterministicPuritySpec`: two consecutive `toJson` calls with same clock produce byte-identical output. | Auto-safe. |
| 4 | **Define `dedupHash` contract** | Add `dedupHash: String` field to `AuditEvent`. Library's `PredicateHasher` produces the hash from the query-shape fields only. | New test: `PredicateHasher.dedupHash` is stable across the same query, different across different queries. **Needs team confirmation** (MCP wire change). | Needs-confirmation. |
| 5 | #6 `elapsedMs` | **A** (preferred): pass `clock`, compute from clock. **C** (alternative): drop field, platform records handler duration in its own metrics. | `DeterministicPuritySpec`: `elapsedMs` is replay-stable (within a ms) when computed from clock. | Auto-safe; **C** is wire-breaking. |
| 6 | #3 streaming audit `ts` | **D**: accept. Document. | Code comment at `SemanticTableStreaming.scala:457`. | Auto-safe. |

## 6. The data semantics of the dedup question (per `clean-architecture-refactor`)

**Why this is a needs-confirmation gate:**

The fix's data semantics depend on the platform's Postgres `audit_events` table key. Two scenarios:

- If the table is keyed by `(model, version, query-shape-hash)`: replay creates one row (dedup works). The current `ts` is fine.
- If the table is keyed by `(model, ts)`: replay creates a new row each time. Bug.

The library's `dedupHash` field is the contract that prevents the bug. The team must commit to a contract BEFORE any `Restate.run` → `semanticdf.compile` wiring lands. Per the `clean-architecture-refactor` skill's "data semantics" rule: **fix order follows severity, not convenience; data-semantics changes always get explicit confirmation.**

**Recommendation: add `dedupHash: String` to `AuditEvent` as a separate commit, before any other library changes.** This locks the contract; subsequent fixes are mechanical.

## 7. Test strategy for the audit

- **Unit (library):** `DeterministicPuritySpec` — injects a frozen `() => Instant` and asserts:
  1. `AuditEvent.ts` equals the injected value
  2. Two consecutive `toJson` calls with the same clock produce byte-identical output
  3. `elapsedMs` from the same clock is stable across runs
- **Integration (platform):** `ModelService.register` is invoked twice through `dev.restate.sdk.testing.TestRunner`. The second invocation forces a journal replay. The Postgres `models` row's `compiled_at` must be unchanged across the replay.

## 8. Exit criterion

The audit is complete when:
- All Critical / High findings have a documented fix (auto-safe or needs-confirmation)
- `DeterministicPuritySpec` is green on both Spark 3.5.8 and 4.1.1
- One full Restate-replay test in the platform integration suite passes
- The `dedupHash` contract is pinned (either as a new field, or by team confirmation that the existing hash is query-shape-only)

## 9. Reconciliation between the two subagents

The DE subagent and Architect subagent agreed on:
- 5 of 7 call sites (the same severity, same fix recommendation)
- The Option-A fix pattern (pass the clock in)
- The replay-safe pattern (use `Restate.instantNow()` outside the run block)

They diverged on:
- The `createTempFile` checkpoint (DE flagged as Critical; Architect missed it). I verified the code at `SemanticTableStreaming.scala:264` — DE is correct. This is the most consequential finding because it can cause silent data loss.
- The severity of `elapsedMs` (DE: Medium; Architect: Medium too — actually they agreed).
- The streaming audit `ts` severity (DE: High; Architect: Low). Architect is correct — `foreachBatch` is a Spark driver callback, not a Restate handler. The per-batch emit is per-batch, not per-replay.

**The reconciliation: the `createTempFile` issue is the most important finding** (DE caught it; Architect missed it). The streaming audit `ts` is Low, not High (Architect is correct).

## 10. What this audit does NOT cover

- **The platform's own non-determinism** (the `Restate.instantNow()` call in `PlatformApplication.java` is replay-stable; that's the point of using it). Out of scope.
- **`semanticdf-mcp/handlers/Lineage.scala`** (the MCP lineage handlers from PR #213). Their determinism is governed by the platform's Restate rules, not the library's. Out of scope here.
- **The `examples/`** directory. Out of scope.

## 11. Recommended next step

The first commit to land is **Step 4 (the `dedupHash` contract)** — it's the contract gate. The library changes (Steps 2, 3, 5) follow once the contract is pinned.

This audit is the **review deliverable** of the deterministic-purity check. The actual code changes are follow-up PRs.
