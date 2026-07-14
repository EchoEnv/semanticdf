# Calc Author Guide

A guide for analysts and engineers who define calc measures in semanticdf.

---

## What is a calc measure?

A calc measure is a measure that **derives its value from other measures**, rather than directly aggregating a column. Instead of aggregating a raw column, it applies a formula.

```scala
// Base measure: aggregates a column
Measure("total_distance", t => sum(t("distance")))

// Calc measure: derives from other measures
Measure("avg_distance",   t => t("total_distance") / t("flight_count"))
```

The framework **automatically classifies** which measures are base and which are calc. It also **automatically pulls in dependencies** — if you request `avg_distance`, the framework also evaluates `total_distance` and `flight_count`.

---

## The golden rule: reference measures by name

Inside a calc lambda, always reference other measures by their **string name**:
```scala
Measure("avg", t => t("total") / t("count"))
```
Never reference a raw column inside a calc:
```scala
// WRONG — this creates a calc that depends on nothing and bypasses aggregation
Measure("bad_avg", t => sum(t("distance")) / count(lit(1)))
```

---

## Common patterns

### Ratio (average)
```scala
Measure("avg_passengers", t => t("total_passengers") / t("flight_count"))
// AA: 550/7 = 78.6 passengers per flight
```

### Percentage of total
```scala
Measure("pct_of_total", t => t("total_passengers") / t.all("total_passengers"))
// Every carrier's pct sums to 1.0 (by construction)
// t.all() returns the grand total, recomputed at zero grain
```

### Ratio with zero-guard
```scala
// Using safeDivide: returns 0.0 when denominator is 0 or null
// Using plain /: returns null on Spark 3, throws on Spark 4 (ANSI mode)
import io.semanticdf.CalcHelpers._
Measure("safe_pct", t => safeDivide(t("total"), t.all("total"), defaultValue = 0.0))
```

### Difference (variance from average)
```scala
Measure("avg_passengers",   t => t("total_passengers") / t("flight_count"))
Measure("variance_from_avg", t => t("total_passengers") - t("avg_passengers") * t("flight_count"))
```

### Year-over-year (requires time dimension)
```scala
// Requires Dimension.time("year", ...) in your model
Measure("pct_change", t =>
  safeDivide(t("total_this_year") - t("total_last_year"),
             t("total_last_year"),
             defaultValue = 0.0) * lit(100.0))
```

---

## Dependency chains

Calc measures can depend on other calc measures — the framework handles this via topological ordering:

```scala
Measure("total_distance",   t => sum(t("distance")))           // layer 1
Measure("total_passengers", t => sum(t("passengers")))          // layer 1
Measure("avg_distance",    t => t("total_distance") / t("flight_count"))  // layer 2
Measure("avg_passengers",   t => t("total_passengers") / t("flight_count")) // layer 2
Measure("efficiency",      t => t("avg_passengers") / t("avg_distance"))   // layer 3
```

The framework evaluates them in order: layer 1 → layer 2 → layer 3. You don't need to declare dependencies.

---

## Calc-of-calc with t.all()

When a calc uses `t.all()`, the formula is **re-evaluated at zero grain**:
```scala
Measure("avg_passengers",     t => t("total_passengers") / t("flight_count"))
Measure("pct_of_avg_passengers", t => t("avg_passengers") / t.all("avg_passengers"))
```
`avg_passengers` at zero grain = grand_total_passengers / grand_flight_count = 2375/23 = 103.3
**NOT** the sum of per-group averages (which would be wrong).

---

## Division by zero

Two options depending on your desired behavior:

```scala
// Option 1: Spark default (null on div-by-zero)
Measure("ratio", t => t("a") / t("b"))
// 10 / 0 = null

// Option 2: explicit default
Measure("ratio", t => safeDivide(t("a"), t("b"), defaultValue = 0.0))
// 10 / 0 = 0.0

// Option 3: NaN marker
Measure("ratio", t => safeDivide(t("a"), t("b"), defaultValue = Double.NaN))
// 10 / 0 = NaN
```

Choose based on how your downstream consumers handle null/zero/NaN.

---

## What NOT to do in a calc lambda

### No side effects
```scala
// BAD — side effects run twice (classification + compile)
Measure("bad", t => { println("side effect!"); sum(t("x")) })

// GOOD
Measure("good", t => sum(t("x")))
```

### No aggregation functions inside calcs
```scala
// BAD — sum() inside a calc is double-aggregation
Measure("bad", t => sum(t("total")) / t("count"))

// GOOD — calc references OTHER MEASURES by name
Measure("good", t => t("total") / t("count"))
```

### No capture of external state
```scala
// BAD — captures external mutable state
var counter = 0
Measure("bad", t => { counter += 1; sum(t("x")) })

// GOOD — pure function
Measure("good", t => sum(t("x")))
```

---

## Performance tips

1. **Prefer fewer calc layers** — each topological layer adds a `select`. For most queries this is negligible, but deep chains (5+) on wide tables add overhead.

2. **`t.all()` costs a cross-join** — each `t.all()` in a query builds a 1-row totals table and cross-joins it. For 1–3 `t.all()` calls this is fine. For 10+, consider pre-computing totals separately.

3. **Avoid calcs inside `where()` predicates** — `st.where(t("ratio") > 1.0)` is a post-aggregation filter, not a calc. If you need a calc in a filter, compute it first:
   ```scala
   st.groupBy("carrier").aggregate("total", "ratio").where("ratio > 1.0")
   ```

---

## Error messages

Common errors and what they mean:

| Error | Cause | Fix |
|---|---|---|
| `Unknown measure 'x'` | Referenced a measure name that doesn't exist | Check the measure name is correct and it's been added to `.withMeasures()` |
| `Calc dependency cycle among: a, b` | Two calcs depend on each other (a→b→a) | Rename one of them. A→B→C is fine; A↔B is a cycle. |
| `A calc must bottom out in at least one base measure` | All requested measures are calcs, but they form a cycle or depend on an unknown measure | Add the base measure they reference |
| `'x' is not a known column or measure` | Referenced a name that isn't a dimension or measure | Check the name is correct |

---

*For the full technical design, see `DESIGN.md`. For known limitations, see `docs/known-limitations.md`.*
