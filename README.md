# semantica

A **semantic layer for Apache Spark** (JVM/Scala), adapted from the
[Boring Semantic Layer](https://github.com/boringdata/boring-semantic-layer) (Python/Ibis).

A `SemanticTable` is a deferred, source-agnostic definition that compiles to a Spark
`DataFrame` at a batch terminal (`.toDataFrame(spark)` / `.execute(spark)`). It is *not*
a `DataFrame` itself — it captures *what* you want (dimensions, measures, joins, filters,
grains) so the engine can decide *how* to compute it. A future streaming terminal would
reuse the same definition against a different sink (ADR 0002).

**Status:** v0.1 — core capabilities complete. **34/34 tests green** under Spark 3.5.8
(default), 4.0.0, and 4.1.1. See [`DESIGN.md`](DESIGN.md) for the architecture of record
and [`docs/adr/`](docs/adr/) for recorded decisions.

## Build

Requires **JDK 17** and **Maven 3.9+**. Spark is on the classpath as `provided` (it comes
from your cluster/runtime).

```bash
mvn test                      # Spark 3.5.8 (default)
mvn -Pspark4 test             # Spark 4.1.1 (latest stable)
```

## Quick start

```scala
import io.semantica._
import io.semantica.Predicate._    // "field" === value, "field" > value, SortKey.desc, ...
import org.apache.spark.sql.functions.{count, lit, sum}

val flights = toSemanticTable(flightsDf, name = Some("flights"))
  .withDimensions(
    Dimension("carrier", t => t("carrier")),
  )
  .withMeasures(
    Measure("flight_count",   t => count(lit(1))),
    Measure("total_distance", t => sum(t("distance"))),
    // calc measure: references other measures BY NAME — resolved against the
    // aggregated DataFrame, no expression-tree surgery (DESIGN §6.1).
    Measure("avg_distance_per_flight", t => t("total_distance") / t("flight_count")),
  )

flights.groupBy("carrier").aggregate("avg_distance_per_flight").execute(spark)
// {AA → 125, UA → 225, DL → 325}
```

## Capabilities

### Calc measures (name-based compilation)

A calc measure references *other measures* by name. The compiler classifies base vs calc
automatically, pulls transitive dependencies, and applies calcs in topological layers.

```scala
// Request only a leaf calc — its deps (avg → total_distance + flight_count) are pulled.
flights.groupBy("carrier").aggregate("avg_distance_per_flight").execute(spark)
```

- Calc-of-calc chains resolve by name across layers; cycles raise a clear error.
- Typos give a "did you mean?" suggestion instead of a crash.

### Percent-of-total (`t.all`)

`t.all("measure")` resolves to the grand total — the same measure aggregated with no group
keys, cross-joined into the result. **The formula is recomputed at zero grain**, so
non-sum totals are correct:

```scala
.withMeasures(
  Measure("total_passengers", t => sum(t("passengers"))),
  // pct sums to 1.0 by construction:
  Measure("pct_of_total", t => t("total_passengers") / t.all("total_passengers")),
)
```

`t.all("avg_distance_per_flight")` returns **225** (grand avg = 6750/30), *not* 675 (the
sum of per-group averages). That's the classic BI trap, fixed.

> **Division by zero:** Spark's `/` returns `null` on zero/missing denominators (correct SQL
> semantics). If you want an explicit default (e.g. `0.0` instead of `null`), use
> `CalcHelpers.safeDivide(num, denom, defaultValue = 0.0)`.

### Joins (`join_one` / `join_many` / `join_cross`)

```scala
val orders  = toSemanticTable(ordersDf, name = Some("orders"))
val items   = toSemanticTable(lineItemsDf, name = Some("line_items"))

// join_many pre-aggregates each side at the join-key grain to prevent fan-out inflation.
val joined = orders.join_many(items, on = "order_id")
  .withMeasures(
    Measure("orders.total_qty", t => sum(t("line_items.qty"))),
  )
```

- `join_one` — one-to-one / parent-child (post-agg safe).
- `join_many` — one-to-many; **both sides pre-aggregated** at join-key grain before joining
  to prevent fact inflation.
- `join_cross` — Cartesian product.
- Merged model uses left-precedence; prefixed names (`"orders.total_qty"`) resolve correctly.

### Filters — WHERE/HAVING auto-routing

```scala
flights.where("carrier" === "AA")                      // dimension → WHERE (pre-agg)
       .where("total_passengers" > 600)                 // measure   → HAVING (post-agg)
       .where(("carrier" === "AA") and ("total" > 100)) // AND-split: WHERE + HAVING
       .where(("carrier" === "AA") or ("total" > 800))  // OR-whole (can't split)
```

- `where(pred)` routes automatically: dimension predicates → pre-agg, measure predicates →
  post-agg. `And` compounds split per-condition; `Or`/`Not` mixing dim+measure stay whole.
- `having(pred)` forces post-agg.
- DSL: `===` `=!=` `>` `>=` `<` `<=` `in` `notIn` `isNull` `isNotNull`, plus `and`/`or`/`.not`.
  (Standard `==`/`!=` are `final` on `Any` and return `Boolean` — unusable for a deferred DSL.)

### Order, limit, and one-shot `query()`

```scala
// Fluent chain:
flights.groupBy("carrier").aggregate("total_passengers")
  .orderBy(SortKey.desc("total_passengers")).limit(10)
  .execute(spark)   // top-10 carriers

// Or a one-shot bundle:
flights.query(
  measures   = Seq("total_passengers"),
  dimensions = Seq("carrier"),
  having     = Some("total_passengers" > 600),
  orderBy    = Seq(SortKey.desc("total_passengers")),
  limit      = Some(10),
).execute(spark)
```

### Time semantics

```scala
val st = toSemanticTable(flightsWithTimeDf, name = Some("flights"))
  .withDimensions(
    Dimension.time("ts", t => t("ts"), smallestTimeGrain = Some("day")),
  )
  .withMeasures(Measure("total_passengers", t => sum(t("passengers"))))

st.atTimeGrain("ts", "month").groupBy("ts").aggregate("total_passengers").execute(spark)
// groups by truncated month (date_trunc)

// Or via query():
st.query(
  measures   = Seq("total_passengers"),
  dimensions = Seq("ts"),
  timeGrain  = Some("month"),
  timeRange  = Some(("2024-01-01", "2024-02-28")),  // filters raw ts, pre-truncation
).execute(spark)
```

- `Dimension.time(...)` marks a timestamp dimension; `smallestTimeGrain` floors requests.
- `atTimeGrain(dim, "month")` overrides the dimension's expr with `date_trunc`.
- Grain too fine (e.g. `"hour"` when `smallestTimeGrain = "day"`) raises a clear error.
- `time_range` filters the raw column; `time_grain` affects only grouping.

## API reference

| Method | Description |
|---|---|
| `toSemanticTable(df, name?)` | Construct a semantic model from a base `DataFrame`. |
| `.withDimensions(...)` / `.withMeasures(...)` | Immutable model extension. |
| `.join_one(other, on)` / `.join_many(other, on)` / `.join_cross(other)` | Joins. |
| `.where(pred)` / `.having(pred)` | Filters (auto-routed WHERE/HAVING). |
| `.groupBy(keys...).aggregate(measures...)` | Group-by + aggregate. |
| `.atTimeGrain(dim, grain)` | Truncate a time dimension for grouping. |
| `.orderBy(keys...)` / `.limit(n)` | Terminal ordering / top-N. |
| `.query(measures, dimensions?, where?, having?, orderBy?, limit?, timeGrain?, timeGrains?, timeRange?)` | One-shot bundle. |
| `.toDataFrame(spark)` / `.execute(spark)` | Batch terminal (compile to `DataFrame`). |
| `.explain()` | Print the semantica op-tree summary (no Spark compile). |
| `.explain(spark)` | Run the full query and print Spark's physical plan via `explain`. | |

`Dimension.time(...)` / `Dimension.entity(...)` are ergonomic factories. `Predicate._`
brings the filter DSL into scope. `SortKey.desc(...)` / bare `String` (ascending) drive
`orderBy`. `CalcHelpers.safeDivide(num, denom, defaultValue)` guards zero/missing denominators
with an explicit default (Spark `/` returns null on div-by-zero — correct SQL semantics;
use `safeDivide` only when null is undesirable).

## Runnable examples

Five complete, runnable examples are in `src/main/scala/io/semantica/examples/`.
Compile once with `mvn compile`, then run any example:

```bash
mvn compile -q

mvn scala:run -DmainClass=io.semantica.examples.FlightsBasic
mvn scala:run -DmainClass=io.semantica.examples.FlightsPctTotals
mvn scala:run -DmainClass=io.semantica.examples.OrdersJoinMany
mvn scala:run -DmainClass=io.semantica.examples.FiltersRouting
mvn scala:run -DmainClass=io.semantica.examples.TimeSeries
mvn scala:run -DmainClass=io.semantica.examples.Benchmark
```

Or submit as a Spark app:
```bash
mvn package -q
spark-submit --class io.semantica.examples.FlightsBasic target/semantica_2.13-*.jar
```

## Cross-version compatibility

Verified green on all three lines (76 tests each, Phase D + integration + YAML loader):

| Spark | Scala | Status |
|---|---|---|
| 3.5.8 (default) | 2.13.14 | ✅ |
| 4.0.0 (`-Pspark4`) | 2.13.14 | ✅ |
| 4.1.1 (latest, profile default) | 2.13.14 | ✅ |

No code shims are needed — the codebase uses only Spark APIs stable across 3.5→4.x.

## Design & decisions

- **[`DESIGN.md`](DESIGN.md)** — architecture of record: op tree, scopes, calc compilation,
  joins, percent-of-total, filters, time semantics, invariants.
- **[`docs/known-limitations.md`](docs/known-limitations.md)** — what doesn't work in v0.1 (streaming, metastore registration, multi-hop joins, etc.). Read before first consumer.
- **[`docs/calc-author-guide.md`](docs/calc-author-guide.md)** — how to write correct calc measures: ratio, pct-of-total, calc-of-calc, `safeDivide`.
- **[`docs/first-consumer-plan.md`](docs/first-consumer-plan.md)** — 3-week structured soak test plan with criteria for go/no-go.
- **[`docs/adr/`](docs/adr/)** — recorded decisions:
  - [0001](docs/adr/0001-adopt-karpathy-guidelines-not-app-design.md) — karpathy guidelines adopted (think-before-coding, simplicity-first, surgical changes, goal-driven execution); app-design plugin/portal rejected.
  - [0002](docs/adr/0002-streaming-batch-first-streaming-shaped.md) — batch-first, streaming-shaped (DSL/source-agnostic op tree; batch terminal now, streaming terminal deferred).
  - [0003](docs/adr/0003-re-sequence-calc-proof-first.md) — re-sequence to prove name-based calc compilation early.
