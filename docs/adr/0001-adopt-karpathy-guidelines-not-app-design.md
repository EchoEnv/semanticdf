# ADR 0001 — Adopt karpathy-guidelines; do not adopt karpathy-app-design

- **Status:** Accepted
- **Date:** 2026-07-08
- **Context:** `DESIGN.md` (semantica — BSL → Spark/Scala port). Two Karpathy skills are
  installed: `karpathy-guidelines` (4 behavioural rules) and `karpathy-app-design`
  (Core + Extension Points + Plugin/Extension/SDK/Registry/Portal architecture). The
  question was whether to use one of them as semantica's governing design.

## Decision

1. **Adopt `karpathy-guidelines`** (Think Before Coding · Simplicity First · Surgical
   Changes · Goal-Driven Execution) as the standing engineering discipline for building
   semantica. They govern *how* we write code; they prescribe no architecture.
2. **Do NOT adopt `karpathy-app-design`.** `DESIGN.md` remains the architecture of record.

## Rationale — why not app-design

`karpathy-app-design`'s own gate rules exclude us:

> When NOT to use: no third-party extensions → skip the Portal; internal / single-team →
> skip Extensions; unsure → don't add it yet. Karpathy: no abstractions for single-use code.

Semantica fails all three toward "Portal not needed":

- It is a **library** embedded in users' Spark jobs, not an application with a request
  lifecycle. The skill's shape (PreHook → Core → Transformer → Sink, exemplified by the
  `metric-diff` parse→diff→summarize→emit pipeline) does not map to a semantic-layer
  library.
- **No extension authors** for v0.1. Auto-discovery via `META-INF/services/` +
  `ServiceLoader` + Registry + an SDK stability promise is pure carrying cost until a
  real external consumer exists.
- Adopting it would **violate the guideline both skills share**: no speculative
  extensibility.

The one thing worth borrowing from it — *name a one-sentence Core, freeze it after v1* —
is already captured by `DESIGN.md` §1/§2 (the two intellectual cores: calc-measure
compilation and grain-correct joins) and needs no Portal to express. Semantica's genuine
swappable points (chart backends, LLM-agent backends, profile/connection providers,
custom filter operators) will use **idiomatic Scala `trait`s + constructor/implicit
injection** — the JVM's native extensibility — not a Registry.

## Simplicity audit — scope decisions recorded (from auditing DESIGN.md against the guidelines)

These are **deferred**, each behind a concrete trigger (no schedule; pulled in only when
the trigger fires):

| Deferred | Trigger to revive |
|---|---|
| Multi-package layout (`expr/ ops/ …`) | A flat `io.semantica` package grows a 3+ file cluster |
| Second DSL style (tuple-builder) | A user genuinely finds record-varargs insufficient |
| `compare_periods`, `index()`, `unnest()` | A concrete model needs the feature |
| Dependency-graph introspection | A consumer asks for field-level lineage |
| `t.all(expr: => Column)` (inline-reduction totals) | A real measure needs totals of an inline reduction |
| Sum-special-case windowed totals fast path | A benchmark shows cross-join totals is too slow |
| YAML config + expr mini-DSL parser | A user wants declarative config over the Scala API |
| Cross-build Scala 2.12 + 2.13 | A consumer is pinned to Spark < 3.5 |
| MiMa binary compatibility | There is a downstream user to not break |
| MCP / HTTP tool server, chart JSON emitter | A consumer needs LLM-tool or viz integration |

## Open assumptions to confirm before Phase 0 (guideline #1)

These were assumed in `DESIGN.md §7` and must be confirmed with the user before any code:

- Target **Spark version** (3.3 / 3.5 / 4.0) and **Scala version** (2.12 / 2.13 / 3) —
  drives almost every downstream choice.
- **Deployment model** — library (`% "provided"`) vs standalone app — drives dependency
  scope and packaging.
- **Justification of the op tree** — §4.1 picks "own sealed-trait op tree" over
  "subclass Catalyst", but never tests the third option ("no op tree; emit DataFrames
  directly"). Confirm the op tree is required by a concrete feature (graph introspection /
  `asTable()` re-derivation / pre-agg join re-planning) before committing to its full
  shape, and size it to that requirement.

## Goal-Driven Execution — success criteria sharpened (guideline #4)

Each phase's exit criterion is verifiable only if it cites a concrete BSL test whose
output it reproduces. Target mapping (to be finalized at Phase 0):

- Phase 1 (core model + base agg) ← `test_query.py` basic group_by/aggregate
- Phase 2 (calc measures) ← `test_calc_compiler.py`
- Phase 3 (percent-of-total) ← `test_percent_of_total.py`
- Phase 4 (joins, fan-out) ← `test_bi_traps.py`

Phase 1 should additionally ship a **single end-to-end golden test**
(flights → dims/measures → group_by → agg → percent-of-total) that exercises the
architecturally load-bearing path before any breadth is built.

## Consequences

- v0.1 contract is smaller and every deferral has an explicit revive-trigger (table above).
- `DESIGN.md` is *not* rewritten here (Surgical Changes); the prune is recorded in this
  ADR and will be applied to `DESIGN.md` only when the user confirms the assumptions above.
- No Portal / Registry / SDK-stability machinery is introduced. If a real extension
  ecosystem emerges, revisit this ADR.
