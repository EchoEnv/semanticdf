# ADR 0003 — Re-sequence: prove calc-measure compilation before Phase 1 breadth

- **Status:** Accepted
- **Date:** 2026-07-08
- **Context:** `DESIGN.md` §8 (phased plan). Phase 0 (skeleton) is green. The question
  was what to build next.

## Decision

Split the original Phase 1 into **1a** (minimal core model + golden group-by) and **1b**
(calc-measure proof slice), pulling the port's riskiest assumption to the front. Then
resume the plan as written (full analyzer in Phase 2, percent-of-total in Phase 3, …).

- **Phase 1a** — `Dimension`, `Measure`, `SemanticScope`/`BaseScope`, the `aggregate` op
  node, `withDimensions`/`withMeasures`/`groupBy`/`aggregate`/`execute`. Exit: golden
  group-by reproducing BSL `test_query.py`'s exact rows (`{AA→550, UA→775, DL→1050}`).
- **Phase 1b** — `MeasureScope` (by-name resolution), a trivial base/calc classifier, and
  single-`select` calc compilation (DESIGN §6.1 invariant A1). Exit: one calc measure
  `avg_distance_per_flight = total_distance / flight_count` resolves **by name** against
  the aggregated DataFrame → `{AA→125, UA→225, DL→325}`.

## Rationale

The single biggest architectural risk in the port is DESIGN §6.1's claim — *"Spark
`Column` expressions are re-runnable against any DataFrame having the named columns, so
name identity is sufficient; we avoid op-graph substitution entirely. This is the single
biggest simplification of the port."* If that claim is wrong, calc-measure compilation
needs a Catalyst-tree-walking/substitution approach (as BSL does with Ibis), which
changes the design.

Breadth-first execution (full Phase 1 — schema introspection, all op nodes — before any
calc work) delays the moment we learn whether the bet holds, and writes more code before
the riskiest assumption is tested. Hard-path-first execution tests the bet with the
minimum code that could falsify it.

## What is deferred vs de-scoped

- **Deferred (still in the plan, just later):** schema introspection (`schema`/`values`
  properties), `orderBy`/`limit`/`filter` op nodes, the `query()` parameter API — folded
  into the phases that consume them (5, 6). No feature is dropped.
- **De-scoped from Phase 1:** the earlier "end-to-end percent-of-total smoke test" exit
  criterion. It was impossible in Phase 1 — percent-of-total needs calc measures (Phase 2)
  + the totals mechanism (Phase 3). It now lives (correctly) in Phase 3's exit criterion.

## Scope discipline for 1b (not the full analyzer)

Phase 1b proves **name-based compilation**, not classification. The base/calc split uses
a trivially simple classifier for 1b: a scope that records which known-measure names a
lambda touches (base-column-wins on collision). The *full* analyzer — structural
classification (`pushable` / `has_window` / `post_agg_only` / `references_AllOf` /
`depends_on`) and calc-of-calc topological ordering — is Phase 2. This keeps 1b small
(a vertical slice) while still falsifying the load-bearing claim.

## Consequences

- If the bet holds (expected): Phase 2 builds the analyzer on a proven compilation core.
- If the bet fails: we learn it after writing ~1a + a thin slice, not after a full
  Phase 1, and the design pivot is cheaper.
- The Phase 0 round-trip test is preserved (regression guard for the leaf node).
