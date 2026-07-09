# ADR 0002 — Structured Streaming: batch-first, streaming-shaped (deferred execution terminal)

- **Status:** Accepted (streaming execution deferred; interface unification accepted now)
- **Date:** 2026-07-08
- **Context:** `DESIGN.md` §4.5. The question was whether semantica should support
  Structured Streaming for big-volume data, and if so whether batch and streaming should
  share one DSL/interface or be separate modes.

## TL;DR

- **One DSL, one op tree, two terminals.** Batch and streaming share the *entire*
  model-construction surface (`toSemanticTable`, `withDimensions`, `withMeasures`,
  `groupBy`, `aggregate`, `filter`, `join_one`, …). They diverge only at the *execution
  terminal*: `.toDataFrame()` (batch) vs `.toStreamingQuery()` (streaming).
  This mirrors Spark's own `df.write` vs `df.writeStream` — one Dataset, two sinks.
- **v0.1 ships the batch terminal only.** The streaming terminal is deferred behind an
  explicit trigger. But because the DSL and op tree are **source-agnostic by design**
  (`DESIGN.md` §4.1), enabling streaming later adds a terminal + a validation pass, **not
  a second API**. The unification is structural, not retrofitted.
- **The flagship feature (`t.all(...)` percent-of-total) is architecturally incompatible
  with unbounded streams.** That is a semantic fact, not an implementation gap —
  resolved below.

## Why streaming needs more than the batch model

Structured Streaming looks API-compatible (same `DataFrame`/`Column` types) but imposes
constraints that semantica's core operations violate. The op tree is **mostly**
mode-agnostic; the violations are localized to a few operations and to the percent-of-total
mechanism.

| semantica feature | Streaming verdict | Reason |
|---|---|---|
| `t.all(...)` percent-of-total | ❌ unsupported (as designed) | Zero-grain aggregation over the *entire* stream + `crossJoin`. Streaming forbids unbounded aggregation without a window (state store grows forever → the memory leak) and `crossJoin` between streams is not supported. |
| Cross-join totals (`DESIGN.md` §6.2) | ❌ unsupported | The mechanism for percent-of-total; no streaming equivalent. |
| `groupBy(dim).aggregate(measure)` w/o window | ⚠️ needs window+watermark | "Group all rows by origin" on an infinite stream must declare a window + watermark + output mode, else state grows forever. |
| `join_many` pre-aggregation (`§6.3`) | ⚠️ stream-static only | Pre-aggregating a streaming "many" side needs windowing; stream-stream joins require time bounds + watermarks. |
| `limit`, unbounded `orderBy` | ❌ unsupported in streaming | Structured Streaming limits these to within-batch/watermark scope. |
| `join_one` lookup | ✅ ports cleanly | Stream-static joins are first-class in Spark. |
| Stateless `filter` | ✅ ports cleanly | — |

Net: the parts that make semantica *semantic* (totals, unbounded group-by, multi-fact
grain correction) collide with streaming's bounded-state requirement. That collision is
the reason streaming needs a distinct *execution* path — but not a distinct *construction*
path.

## Decision — the unification portal

**Construction (shared, source-agnostic):** `SemanticTable` is built identically for
batch and streaming. The op tree never branches on mode; `compile(spark)` returns a
`DataFrame` whose flavor (batch or streaming) is determined by the **leaf source** the
user supplied. A measure lambda `t => sum(t("distance"))` is the same lambda whether
resolved against a batch or streaming DataFrame.

**Execution (the portal — where mode diverges):**

```
SemanticTable (one definition)
        ├── .toDataFrame(spark)            →  DataFrame         (batch, v0.1)
        └── .toStreamingQuery(spark, opts) →  StreamingQuery     (streaming, deferred)
```

- The **batch terminal** requires a non-streaming source and returns a batch `DataFrame`.
  It is the v0.1 path and carries no streaming-specific code.
- The **streaming terminal** validates streaming constraints **late** (at terminal call,
  not at construction) and then returns a `StreamingQuery`. Late validation means the
  batch path never pays for streaming checks.

**Streaming constraint validation (at the streaming terminal):**

- window-less aggregation → rejected (requires a time window + watermark);
- `t.all(...)` cross-join totals → rejected, *or* limited to a **windowed-totals** scope
  (totals *within this window* — a future enhancement, see Open Questions);
- stream×stream `join_many` pre-aggregation → rejected unless time-bounded;
- `limit` / unbounded `orderBy` → rejected unless within-watermark.

Rejected operations raise a typed error (e.g. a `StreamingUnsupportedError`) naming the
node and the constraint violated — loud, not silent.

## Big-volume / performance considerations (the motivating concern)

Streaming amplifies the §4.4 invariants and adds one new failure mode:

1. **State store growth is the real memory leak.** Any stateful aggregation (most of a
   semantic layer) without a watermark/TTL retains state across the stream's lifetime
   and OOMs executors after hours on big volume. This is **not fixable in semantica's
   code** — it requires *enforcing windowing* in the model, which is why the streaming
   terminal rejects window-less aggregation. The §4.4 "no cached internals" invariant is
   *mandatory* here (caching a streaming intermediate pins shuffle output across
   micro-batches).
2. **Per-micro-batch plan cost.** Every batch re-analyzes and re-optimizes the Catalyst
   plan, so deep op trees and the calc `select` depth (§6.1) are paid *per batch*, not
   once. High throughput = analysis overhead becomes a measurable fraction of wall-time.
   The single-`select` calc compilation (§6.1, A1) and minimal op tree (§4.1) keep this
   bounded; the streaming terminal is where it would first be benchmarked.
3. **Explicit broadcast for the (windowed) totals path.** Whenever a windowed-totals
   feature is added, it inherits the §6.2 broadcast-hint + 1-row guard + totals-pruning
   rules verbatim — never rely on auto-broadcast (a stray multi-row totals side
   degrades `crossJoin` into O(N·M) `CartesianProduct` and hangs the job).

## What is accepted now vs deferred

**Accepted now (built into v0.1 at no extra cost):**

- The op tree is source-agnostic (`DESIGN.md` §4.1) — no branching on mode.
- `toDataFrame()` is named as the *batch* terminal; a `toStreamingQuery()` slot is
  defined as its sibling (`DESIGN.md` §4.5).
- The §4.4 non-functional invariants (no cached internals; analysis-only introspection;
  no session held in the tree) are stated — they are necessary for batch and
  *necessary-but-not-sufficient* for streaming.

**Deferred (behind the trigger below):**

- The streaming terminal implementation (`toStreamingQuery`) and its constraint
  validator.
- `window(...)` semantics in the model (time dimension maps to a streaming window).
- Windowed-totals scope for `t.all(...)` (so percent-of-total works *within a window*).
- Checkpoint-location + default-watermark policy on streaming models.

## Revive trigger

Implement the streaming terminal when **a concrete windowed-aggregation use case**
appears — i.e., a consumer who needs to run a semantic model over a `readStream` source
with defined windows. Do not implement it speculatively for "big data" throughput; batch
Spark already handles big volume, and a streaming terminal built without a real
windowing requirement would guess at semantics (per-window vs running vs forbidden
totals) that should be a product decision, not an engineering default.

When revived, implement in this order: (1) terminal + constraint validator rejecting
forbidden ops; (2) windowed aggregation via time dimensions; (3) stream-static
`join_one`; (4) windowed-totals `t.all(...)`; (5) checkpoint/watermark defaults.

## Alternatives considered

1. **Two separate APIs (batch DSL + streaming DSL).** Rejected — doubles the surface,
   forces users to rewrite models when moving batch→stream, and contradicts the
   unification goal. Spark itself doesn't do this.
2. **Make v0.1 streaming-first with enforced windowing everywhere.** Rejected —
   speculative (no confirmed streaming use case), and it would force every batch user
   to declare windows they don't need. Violates ADR 0001 (no speculative abstraction).
3. **Support streaming by silently relaxing constraints** (e.g., let `t.all(...)` run
   on a stream and hope). Rejected — silent state growth / wrong results; the loud
   `StreamingUnsupportedError` is the contract.
4. **Drop streaming entirely from the roadmap.** Rejected as a *design* stance — the
   source-agnostic op tree is cheap to preserve now and keeps the door open; only
   *execution* is deferred.

## Consequences

- v0.1 ships batch-only execution, but its model definition is already streaming-shaped.
- A future streaming terminal is an *addition* (one new method + a validator), not a
  rewrite — no batch user's code changes.
- The flagship percent-of-total remains batch-only until windowed-totals semantics are
  decided; this is documented, not hidden.
