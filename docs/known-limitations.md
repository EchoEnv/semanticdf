# Known Limitations

This document lists features that do not work in v0.1. Read this before starting — it will save you from hitting obvious gaps without context.

---

## Not supported

### Streaming sources
**Streaming is not implemented.** `toSemanticTable` accepts a batch `DataFrame`. Structured Streaming sources will fail. ADR 0002 describes the design for streaming; it is not yet built.

**Workaround:** Use only batch DataFrames for now.

### External customers / multi-tenant security
There is no row-level security, column masking, or tenant isolation. All users with access to the Spark session can see all data.

**Workaround:** Filter data before passing it to `toSemanticTable`.

### Schema evolution
If the source DataFrame's schema changes (column renamed, type changed, column dropped), existing `Dimension` and `Measure` definitions may silently fail or produce wrong results. There is no invalidation or migration system.

**Workaround:** Validate schemas explicitly before constructing semantic models.

### Multi-hop join chains (3+ tables)
Join chains beyond two tables have not been tested. `joined.join_one(third, ...)` may work but has not been verified for edge cases (name collision resolution, cross-join behavior).

**Workaround:** Use at most two tables in a join. Chain `join_one`/`join_many` results with caution and verify results.

### Derived time dimensions
`Dimension.time(..., derived_dimensions = Seq("year", "month", "day"))` — auto-generating year/month/day from a timestamp — is not implemented. Only `atTimeGrain()` truncation is supported.

**Workaround:** Declare each time dimension explicitly:
```scala
.withDimensions(
  Dimension.time("ts",        t => t("ts"), smallestTimeGrain = Some("day")),
  Dimension("year",  t => year(t("ts"))),
  Dimension("month", t => month(t("ts"))),
)
```

### NULL handling in calc formulas
Calc formulas (e.g. `t("a") / t("b")`) use Spark's NULL semantics: `null / 0 = null`, `null / null = null`, `x / null = null`. This is correct SQL behavior, but dashboards that expect `0` will show nulls.

**Workaround:** Use `CalcHelpers.safeDivide(num, denom, defaultValue = 0.0)`:
```scala
Measure("pct", t => safeDivide(t("total"), t.all("total"), defaultValue = 0.0))
```

---

## Behavioral quirks to know

### Comparison operators: `===` not `==`

Scala's `==` is final on `Any` and cannot be overridden to return `Predicate`. Use `===` for equality and `=!=` for inequality:
```scala
st.where("carrier" === "AA")      // ✓
st.where("carrier" == "AA")      // ✗ wrong — returns Boolean, not Predicate
```

### NOT: `.not` method, not `!`

`!` only works on `Boolean` in Scala. `Predicate` is not `Boolean`. Use:
```scala
st.where(("carrier" === "AA").not)        // ✓
st.where(not("carrier" === "AA"))         // ✓
st.where(!("carrier" === "AA"))           // ✗ won't compile
```

### Lambda purity required

Measure and dimension lambdas must not have side effects. They are executed twice: once at classification (to determine dependencies) and once at compile (to generate Spark expressions). They capture their enclosing scope for the model's lifetime.

**Don't do this:**
```scala
Measure("bad", t => { println("side effect!"); sum(t("x")) })  // side effects run twice
```

### Division by zero: Spark 3 vs Spark 4

- **Spark 3.5.x:** `/` returns `null` on divide-by-zero (correct SQL semantics)
- **Spark 4.x:** ANSI mode is enabled by default. `/` **throws** `SparkArithmeticException` on divide-by-zero unless `spark.sql.ansi.enabled = false`

The test suite runs with `ansi.enabled = false` for consistency. In production on Spark 4 with ANSI mode enabled, use `CalcHelpers.safeDivide`.

### Measure name collision with base columns (after joins)

After a `join_many`, the joined DataFrame contains pre-aggregated columns (e.g. `order_amount`). If you call `.withMeasures()` on the joined result and use the same name as an existing column, the framework classifies it as a calc dependency and replaces it with a literal.

**Don't do this:**
```scala
// order_amount is ALREADY a column in the joined result (pre-aggregated)
joined
  .withMeasures(Measure("order_amount", t => sum(t("order_amount"))))  // WRONG
  .groupBy("customer_id")
  .aggregate("order_amount")
```

**Do this instead:**
```scala
// Use DIFFERENT names for post-join measures
joined
  .withMeasures(Measure("total_orders", t => sum(t("order_amount"))))  // OK
  .groupBy("customer_id")
  .aggregate("total_orders")
```

---

## Performance notes

- **Wide joins (many columns):** Each side is pre-aggregated at the join key grain. This is correct but not free — pre-aggregation has a cost. For very large fact tables, benchmark before assuming it's fast enough.
- **Percent-of-total with many `t.all()` calls:** Each `t.all()` builds a separate 1-row totals table and cross-joins it. Many `t.all()` calls in one query = many cross-joins. Keep `t.all()` usage moderate.
- **explain() does not run the query.** `explain(spark)` compiles and explains the plan. `explain()` shows the semanticdf op tree without compiling.

---

## YAML loader limitations

### Join keys must be symmetric (same column name on both sides)

The YAML loader requires `left_on == right_on` — the join key must have the same column name on both tables. This matches semanticdf's equi-join engine.

**Don't do this:**
```yaml
joins:
  carriers:
    left_on: carrier   # flights.carrier
    right_on: code      # carriers.code — DIFFERENT name → error
```

**Do this:**
```yaml
# Rename the column in one table so both use the same key name
carriers:
  table: carriers_tbl
  dimensions:
    carrier: carrier   # renamed from 'code' to 'carrier' to match flights
```

Asymmetric-key support is a future enhancement (requires either relaxing the
join engine's symmetric-key constraint or column-rename preprocessing).

### Base measures use Spark SQL expressions

YAML `measures:` are Spark SQL aggregate strings (`sum(distance)`, `count(distinct user_id)`).
These are NOT type-checked at compile time — typos surface at runtime. The Scala DSL
(`t => sum(t("distance"))`) catches column-name typos earlier via scope resolution.

### Calc measures: arithmetic only

YAML `calculated_measures:` support `+`, `-`, `*`, `/`, parentheses, numeric literals,
measure-name references, and `all(name)` for percent-of-total. They do NOT support
function calls (e.g. `abs(x)`, `round(x, n)`). For those, use the Scala DSL.

### Dimension name collision across joined tables

If two joined tables both declare a dimension with the same non-key name
(e.g. both have `shared`), semanticdf detects this at join time and throws
a clear error:

```
Dimension name collision across joined tables: 'shared' exists on both the
left and right sides of the join. Reference the right-side copy via its
prefixed name ("right.shared"), or rename one side to a unique name before
joining.
```

**Workaround (one of):**
- Reference the right-side copy via its prefixed name: `"right.shared"`
- Rename one side to a unique name before joining
- Use the Scala DSL's `.alias(name)` (future feature)

### orderBy with dotted dimension names

`orderBy("carriers.name")` is parsed by Spark as `catalog.table.column`, not a literal
dotted column name. Group-by and aggregation on dotted names work fine; only `orderBy`
is affected. Sort results in Scala as a workaround.

---

*This document is updated after each production soak cycle. Last updated: v0.1 + Phase E (partial, typeclass + sealed Compare ADT) + okfgen (Phase F1).*
