# Current scope & guardrails

This document explains what SemanticDF **does today** (v0.1.10) and the
guardrails worth knowing before you adopt it. Each item pairs the
*current behavior* with a *workaround* and a *roadmap hint* — so you
can plan around what's here now and what's coming.

Think of it as a scoping doc, not a defect list. The workarounds are
production-shaped; the roadmap hints point at where each gap closes.

For the narrative walkthrough of how the library works, see
[`docs/guide.md`](guide.md). For calc-measure authoring pitfalls
specifically, see [`docs/calc-author-guide.md`](calc-author-guide.md).

---

## At a glance

| Area | Status today | Roadmap |
|---|---|---|
| **Execution model** | Batch `DataFrame` + streaming terminals (same op tree) | Stream-stream joins (only static-stream supported today) |
| **Security model** | Per Spark session (no in-library row-level security) | Pluggable authn/authz adapter (gated on consumer demand) |
| **Schema evolution** | Caller's responsibility | Drift detection on the wishlist |
| **Join chains** | `join_one` / `join_many` / `join_cross`, tested up to 2 tables | Multi-hop join testing + collision policy |
| **Time dimensions** | `atTimeGrain()` truncation, or `derive = Seq("year", "month", "day")` for sibling dims | — (correct as-is) |
| **NULL semantics** | SQL-correct (null-propagating); `safeDivide` for dashboard zeros | — (correct as-is) |
| **YAML joins** | Symmetric keys (same name both sides) | Asymmetric-key support |
| **YAML calc measures** | Arithmetic + `all()` | Function calls (`abs`, `round`, …) |

---

## Scope

### Streaming terminal — what's supported and what isn't

The streaming terminal (`model.toStreamingQuery(spark, opts)`) accepts
the same model DSL as the batch terminal. Supported patterns:

- `where` filters on the streaming source
- `groupBy(...) + aggregate(...)` when a `WindowSpec` is provided
- Static-stream `join_one` (one batch side, one streaming side)
- `t.all(...)` windowed-totals when a `WindowSpec` is provided
- Default watermark + checkpoint location when not specified

Not yet supported in streaming:

- **Stream-stream joins** — both sides would have to be streaming;
  only static-stream `join_one` works today.
- **`orderBy` + `limit`** in the streaming pipeline (use the batch
  pipeline for those, or filter after the fact in `foreachBatch`).
- **Calc measures referencing measure names not in the source** — the
  streaming aggregation translates base measures against the
  watermarked source; a calc measure that references a non-base
  measure (like `pct = numerator / denominator`) works *within a
  window* via the windowed-totals cross-join, but only when a
  window spec is set.

**Workaround:** Drop back to the batch terminal (`.toDataFrame(spark)`)
for unsupported patterns, or run the streaming query per-batch in
`foreachBatch` against a batch-compiled slice.

### Per-session security model

There's no row-level security, column masking, or tenant isolation in
the library itself. Anyone with access to the Spark session sees all
the data the session sees. This is consistent with Spark's own model
— security belongs upstream of the analytical layer.

**Workaround today:** Filter data before passing it to
`toSemanticTable`, or enforce authz at the Spark catalog / table level.
**Roadmap:** Pluggable authn/authz adapter (no concrete PR yet; gated
on consumer demand).

### Schema stability is the caller's responsibility

If the source DataFrame's schema changes (column renamed, type changed,
column dropped), existing `Dimension` and `Measure` definitions may
silently fail or produce wrong results. There's no invalidation or
migration system today.

**Workaround today:** Validate schemas explicitly before constructing
semantic models. The `introspect` CLI tool ([see `docs/runtime-quickstart.md`](runtime-quickstart.md))
generates a starter YAML from an existing DataFrame and is a good
sanity check when schemas shift.
**Roadmap:** Schema-drift detection is on the wishlist.

### Join cardinality: one / many / cross (multi-hop untested)

`join_one`, `join_many`, and `join_cross` are all shipped and tested.
Join chains beyond two tables have not been verified for edge cases
(name-collision resolution, cross-join behavior under composition).

**Workaround today:** Use at most two tables in a join chain. If you
need a third, chain `join_one`/`join_many` results and verify outputs
against a known-good query.
**Roadmap:** Multi-hop join testing + a documented collision policy.

### Time dimensions: derive year/month/day from a single declaration

`Dimension.time("ts", ..., derive = Seq("year", "month", "day"))`
auto-materializes sibling dims using Spark date-part functions on the
source column. The YAML equivalent:

```yaml
ts:
  type: time
  expr: ts
  smallest_time_grain: day
  derived_dimensions: [year, month, day]
```

`atTimeGrain()` truncation remains available for ad-hoc groupings.

### NULL semantics in calc formulas

Calc formulas (e.g. `t("a") / t("b")`) use Spark's NULL semantics:
`null / 0 = null`, `null / null = null`, `x / null = null`. This is
correct SQL behavior, but dashboards that expect `0` will show nulls.

**Workaround today:** Use `CalcHelpers.safeDivide(num, denom, defaultValue = 0.0)`:

```scala
Measure("pct", t => safeDivide(t("total"), t.all("total"), defaultValue = 0.0))
```

**Status:** Correct as-is. `safeDivide` is the dashboard-friendly escape hatch.

---

## Behavioral notes (easy to trip on, not bugs)

### Predicate DSL: `===` and `.not`, not `==` and `!`

Scala's `==` is `final` on `Any` and can't be overridden to return a
deferred `Predicate`. Use `===` for equality, `=!=` for inequality,
and `.not` (or the `not(...)` wrapper) for negation:

```scala
st.where("carrier" === "AA")              // ✓
st.where("carrier" ==  "AA")              // ✗ returns Boolean, not Predicate

st.where(("carrier" === "AA").not)        // ✓
st.where(!("carrier" === "AA"))           // ✗ won't compile
```

### Lambda purity required

Measure and dimension lambdas must not have side effects. They run
**twice**: once at classification (to determine dependencies), once at
compile (to generate Spark expressions). They also capture their
enclosing scope for the model's lifetime.

```scala
// Don't do this:
Measure("bad", t => { println("side effect!"); sum(t("x")) })  // runs twice
```

### Spark 3 vs Spark 4 divide-by-zero

- **Spark 3.5.x:** `/` returns `null` on divide-by-zero (correct SQL semantics).
- **Spark 4.x:** ANSI mode is on by default; `/` **throws**
  `SparkArithmeticException` on divide-by-zero unless
  `spark.sql.ansi.enabled = false`.

The test suite runs with `ansi.enabled = false` for consistency across
versions. On Spark 4 in production with ANSI mode on, use
`CalcHelpers.safeDivide`.

### Measure-name collisions with base columns (after joins)

After a `join_many`, the joined DataFrame contains pre-aggregated
columns (e.g. `order_amount`). If you call `.withMeasures()` on the
joined result and reuse an existing column name, the framework
classifies it as a calc dependency and replaces it with a literal.

```scala
// order_amount is ALREADY a column in the joined result (pre-aggregated) — DON'T:
joined.withMeasures(Measure("order_amount", t => sum(t("order_amount"))))  // WRONG

// Use a DIFFERENT name for post-join measures — DO:
joined.withMeasures(Measure("total_orders", t => sum(t("order_amount"))))  // OK
```

---

## Performance notes

- **Wide joins (many columns):** Each side is pre-aggregated at the
  join-key grain. This is correct but not free — pre-aggregation has a
  cost. For very large fact tables, benchmark before assuming it's fast
  enough.
- **Percent-of-total with many `t.all()` calls:** A single pruned
  totals row is built per aggregate compile, containing only the
  measures referenced by any `t.all()` in that query. All `t.all()`
  calls in the same query share the same cross-joined totals row. Keep
  `t.all()` usage moderate — the single-row cross-join is cheap but the
  pruning is per-query, so very heavy `t.all()` workloads still pay for
  the broadcast.
- **`explain()` does not run the query.** `explain(spark)` compiles and
  explains the plan; `explain()` (no args) shows the SemanticDF op tree
  without compiling. See [`docs/guide.md` → How a query compiles](guide.md#how-a-query-compiles).

---

## YAML authoring notes

### Join keys must be symmetric (same column name on both sides)

The YAML loader requires `left_on == right_on` — the join key must
have the same column name on both tables. This matches SemanticDF's
equi-join engine.

```yaml
# DON'T — different names on each side → error
joins:
  carriers:
    left_on: carrier   # flights.carrier
    right_on: code      # carriers.code — DIFFERENT name

# DO — rename the column in one table so both use the same key name
carriers:
  table: carriers_tbl
  dimensions:
    carrier: carrier   # renamed from 'code' to 'carrier' to match flights
```

**Roadmap:** Asymmetric-key support (either by relaxing the join
engine's symmetric-key constraint or via column-rename preprocessing).

### Base measures are Spark SQL expressions

YAML `measures:` are Spark SQL aggregate strings (`sum(distance)`,
`count(distinct user_id)`). Typos in column references are caught at
model-load time by `ExpressionValidator` (visible column set: source +
transforms + previously-declared measures). The Scala DSL
(`t => sum(t("distance"))`) catches typos even earlier via scope
resolution, and the typed `withMeasures(measure, expr)` overload
witnesses the measure at compile time.

### Calc measures: arithmetic only (no function calls)

YAML `calculated_measures:` support `+`, `-`, `*`, `/`, parentheses,
numeric literals, measure-name references, and `all(name)` for
percent-of-total. They do **not** support function calls (e.g.
`abs(x)`, `round(x, n)`). For those, use the Scala DSL.

**Roadmap:** A richer `calculated_measures:` grammar (function calls)
is on the wishlist — gated on consumer demand.

### Dimension-name collisions across joined tables

If two joined tables both declare a dimension with the same non-key
name (e.g. both have `shared`), SemanticDF detects this at join time
and throws a clear error:

```
Dimension name collision across joined tables: 'shared' exists on both the
left and right sides of the join. Reference the right-side copy via its
prefixed name ("right.shared"), or rename one side to a unique name before
joining.
```

**Workaround (one of):**
- Reference the right-side copy via its prefixed name: `"right.shared"`
- Rename one side to a unique name before joining

---

## Roadmap summary

The items above marked "Roadmap" are the active deferral list. They
share two characteristics:

1. **The interface is shaped for them.** Streaming has an ADR; calc
   function calls have a parser extension point; asymmetric joins have
   a documented engine constraint. None of these require API breaks.
2. **They're gated on consumer signal.** SemanticDF ships
   production-shaped workarounds for each, so adoption isn't blocked.
   If you hit one of these and the workaround doesn't fit, file an
   issue — that's the signal that moves a roadmap item into a PR.

For the full feature roadmap with priorities, see
[`docs/feature-roadmap.md`](feature-roadmap.md).
For the architectural decisions behind these deferrals, see
[`docs/adr/`](adr/).

---

*This document is updated each release. Last updated: v0.1.10.*
