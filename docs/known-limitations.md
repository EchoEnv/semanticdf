# Known Limitations

This document lists features that do not work in v0.1. Read this before starting — it will save you from hitting obvious gaps without context.

---

## Not supported

### Streaming sources
**Streaming is not implemented.** `toSemanticTable` accepts a batch `DataFrame`. Structured Streaming sources will fail. ADR 0002 describes the design for streaming; it is not yet built.

**Workaround:** Use only batch DataFrames for now.

### Metastore / view registration
Semantic tables cannot be registered as Spark views (`createTempView`, `createOrReplaceGlobalTempView`). You cannot query them with Spark SQL directly.

**Workaround:** Call `.toDataFrame(spark)` and register the resulting DataFrame manually:
```scala
st.toDataFrame(spark).createOrReplaceTempView("my_view")
spark.sql("SELECT * FROM my_view")
```

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
- **explain() does not run the query.** `explain(spark)` compiles and explains the plan. `explain()` shows the semantica op tree without compiling.

---

*This document is updated after each production soak cycle. Last updated: v0.1 Phase C.*
