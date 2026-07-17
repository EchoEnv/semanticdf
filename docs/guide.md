# SemanticDF — A reader's guide

A narrative walkthrough for new readers. Pairs with `DESIGN.md` (the
architecture of record) the way a guidebook pairs with a map: this
explains *how it works* and *why you'd reach for it*; DESIGN.md
records *the decisions that produced it*.

If you want to *use* SemanticDF, read this. If you want to *contribute*
to it, read this, then read `DESIGN.md`.

---

## The 5-minute mental model

A `SemanticTable` is an **immutable description of a slice of your
business**: dimensions, measures, joins, filters, time grains. It is
not a DataFrame; it's *what* you want, separate from *how* it's computed.
The framework compiles the description into a Spark `DataFrame` on
demand, at the terminal you call.

**Worked example — the same model, three terminals:**

```scala
import io.semanticdf._
import org.apache.spark.sql.functions._

val flights = toSemanticTable(flightsDf, name = Some("flights"))
  .withDimensions(
    Dimension("carrier", t => t("carrier")),
  )
  .withMeasures(
    Measure("flight_count",   t => count(lit(1))),
    Measure("total_distance", t => sum(t("distance"))),
    Measure("avg_distance_per_flight",
            t => t("total_distance") / t("flight_count")),  // calc
  )

// Terminal 1: returns a Spark DataFrame
flights.groupBy("carrier").aggregate("avg_distance_per_flight").execute(spark)
// → DataFrame[(carrier: String, avg_distance_per_flight: Double)]

// Terminal 2: returns a typed Seq (PR #52 + PR #64)
case class Row(carrier: String, avg_distance_per_flight: Double)
implicit val dec: ResultDecoder[Row] = ResultDecoder.derive[Row]
flights.collectAs[Row](spark)
// → Seq(Row("AA", 125.0), Row("UA", 225.0), Row("DL", 325.0))

// Terminal 3: just the compiled DataFrame, for further Spark use
val compiledDf = flights.toDataFrame(spark)
compiledDf.createOrReplaceTempView("flights_view")
spark.sql("SELECT carrier, avg_distance_per_flight FROM flights_view")
```

Between *description* (the model) and *DataFrame* (the compiled output)
sits an **op tree** — a sealed-trait algebra of nodes, one per
operation. Nothing runs until a terminal asks for the DataFrame.

**Three things every model has:**
1. **Dimensions** — groupable / filterable attributes (`carrier`, `region`)
2. **Measures** — numeric aggregates (`flight_count`, `total_passengers`)
3. **Source** — the underlying Spark `DataFrame` the model was built from

Many models also have **joins** (other models attached), **filters**
(pre-join hygiene on the source), and **transforms** (per-row
computations at model-load).

---

## The op tree in 60 seconds

Every operation on a `SemanticTable` — `groupBy`, `aggregate`,
`join`, `filter`, `orderBy`, `limit`, `withMeasures`, `withTransforms` —
adds a node to the op tree and returns a new `SemanticTable`. The
tree is immutable, so each operation creates a *new* tree (with the
same root shared with sibling views).

**Worked example — five views, one shared source:**

```scala
val base = toSemanticTable(df, "flights")

val grouped = base.groupBy("carrier")           // adds a GroupBy node
val agg     = grouped.aggregate("flight_count") // adds an Aggregate node
val sorted  = agg.orderBy("flight_count")        // adds an OrderBy node
val top10   = sorted.limit(10)                  // adds a Limit node

// base / grouped / agg / sorted / top10 are five distinct SemanticTables.
// All five share an underlying source; only one — top10 — has all the
// operations applied. They each compile to a different DataFrame at
// execute-time.
```

You can inspect the tree without running anything: `model.explain()`
prints the op-tree shape (no Spark compilation). This is the cheap
"is my model built right?" check — see *Explaining a query* below for
the full picture.

The op tree is what's stored in `SemanticOp`. The terminal walks the
tree, calling each node's `compile(spark)` in dependency order, and
returns the resulting DataFrame.

**Why a tree?** Three things become possible that wouldn't be in a
direct-builder API:
1. *Deferred compilation* — one model, many queries
2. *Introspection* — `dimensions` / `measures` / `joins` accessors
3. *Join re-planning* — `join_many` can pre-aggregate the "many" side
   before joining

More on each in `DESIGN.md` §4.1.

---

## How a query compiles

A query on a `SemanticTable` walks the op tree, collects the
required base dimensions and measures, and produces a Spark plan in
roughly these steps.

### Step 1 — Classify

For each requested measure, probe the lambda against the base
DataFrame. Lambdas that touch only base columns are *base* measures;
lambdas that reference other measure names are *calc* measures.

```scala
// The measure lambda is probed against a scope that knows about base
// columns AND other measures on the model. Probing determines the
// dependency graph.

val total_distance    = Measure("total_distance",  t => sum(t("distance")))   // touches base col → Base
val flight_count      = Measure("flight_count",    t => count(lit(1)))       // touches nothing real → Base
val avg_per_flight    = Measure("avg_per_flight",  t => t("total_distance") / t("flight_count"))
//    └── ↑ references two measure NAMES, not base columns → Calc
```

### Step 2 — Pull transitive dependencies

If you ask for a leaf calc, its base deps are pulled automatically.

```scala
flights.aggregate("avg_per_flight").execute(spark)
// Internally the engine pulled:
//   avg_per_flight (Calc) → total_distance (Base) + flight_count (Base)
// You didn't need to ask for the bases.
```

### Step 3 — Compile base measures into an aggregated DataFrame

```scala
val baseAgg = df
  .groupBy("carrier")            // the GroupBy keys
  .agg(
    sum("distance").alias("total_distance"),
    count(lit(1)).alias("flight_count"),
  )
```

### Step 4 — Layer calc measures on top

One `select` per topological layer of the calc DAG.

```scala
val withCalcs = baseAgg.select(
  $"carrier",
  $"total_distance",
  $"flight_count",
  // layer 1: leaf calcs (avg_per_flight references only base columns)
  ($"total_distance" / $"flight_count").alias("avg_per_flight"),
  // (a calc-of-calc would go in layer 2 here, after its own dependencies
  //  had been materialized in layer 1)
)
```

A 50-measure-wide model with depth-2 calcs is **two** `select`s,
not 50 chained `withColumn`s.

### Step 5 — WHERE/HAVING auto-routing

A query-time filter on a dimension compiles to a `WHERE` clause
(before the aggregate); a filter on a measure compiles to `HAVING`
(after). OR compounds touching any measure stay whole (post-agg).
AND compounds split condition-by-condition. See the *Filters*
section below for the full mechanics.

### ResultDecoder on the way out

The `ResultDecoder.derive[T]` macro (PR `#64`) does the same kind of
"compile from structure" at the *result* side: a `case class
CarrierCount(carrier: String, count: Long)` becomes a typed decoder
that reads `row.getString(0)` and `row.getLong(1)`. The decoder is
generated at compile time, not at runtime.

### Explaining a query

Three flavours of plan inspection, each for a different debugging
need:

```scala
model.explain()                // op tree shape (no Spark compilation)
model.explain(spark)           // Catalyst physical plan
model.explainSemantic(spark)   // WHY: where each filter routed,
                               //     transitively-pulled measures,
                               //     join strategies, warnings, Spark plan
```

`explainSemantic` is the one a developer usually wants. Sample
output (abridged):

```
Model: flights @ version 0
Source: flights_csv

Operations:
  GroupBy(carrier)
  Aggregate(flight_count)              # Base
  OrderBy(flight_count DESC)
  Limit(10)

Transitively-pulled measures:
  flight_count                   (Base, depends on: —)

Spark plan (post-compile):
  == Physical Plan ==
  *(2) Sort [flight_count DESC NULLS LAST], ...
  +- Exchange SinglePartition, ...
  +- *(1) HashAggregate(...)           ← the aggregate

Filter routing:
  (no filters in this query)

Warnings: (none)
```

Use `explain()` (no args) before talking to Spark — it costs
microseconds and answers most "is my model built right?" questions.
Use `explainSemantic(spark)` once you have a question about *why*
something isn't behaving the way you expected. Use
`explain(spark)` when you want the literal Catalyst plan.

---

## Dimensions, measures, and transforms

Two records with the same shape:

```scala
Dimension("carrier", t => t("carrier"))
Measure  ("flight_count", t => count(lit(1)))
```

Both are value classes. The `t => ...` lambda is what each layer of
the framework calls to produce a Spark `Column`. The compiler probes
the lambda once at construction time to determine dependencies; the
same lambda runs again at compile time to produce the actual Spark
expression. **No column reflection at runtime** — the lambda is a
function, not metadata.

A `Dimension` carries extra optional flags for time dimensions
(`isTimeDimension`, `isEventTimestamp`, `smallestTimeGrain`), entity
flags (`isEntity`), and metadata. A `Measure` carries an optional
aggregator kind (`Base` vs `Calc`); the framework classifies
automatically by probing the lambda against a scope.

### Transforms (per-row computations, applied at model-load)

Per-row logic — `datediff(...)`, `case when ...`, `row_number()`
over `Window.partitionBy(...)` — doesn't fit a measure's aggregate
context. Use the `transforms:` block (YAML) or `withTransforms(...)`
(Scala DSL) to apply such logic to the source data at model-load
time. Transforms correspond to [dbt staging models](https://docs.getdbt.com/docs/build/staging-models)
and [LookML `derived_table`](https://cloud.google.com/looker/docs/derived-tables)
— the canonical place for per-row data prep.

```yaml
# YAML
orders:
  table: orders_csv
  transforms:
    ship_days:
      expr: "datediff(shipped_at, order_date)"
      description: "Days from order placement to shipment (per-row)"
    on_time_flag:
      expr: "case when datediff(shipped_at, order_date) <= 2 then 1 else 0 end"
      description: "1 if shipped within 2 days, else 0 (per-row)"
  measures:
    total_ship_days: "sum(ship_days)"
  calculated_measures:
    avg_ship_days: "total_ship_days / count(1)"
```

```scala
// Scala DSL — equivalent
val orders = toSemanticTable(ordersCsv, name = Some("orders"))
  .withTransforms(
    Transform("ship_days",      t => datediff(t("shipped_at"), t("order_date"))),
    Transform("on_time_flag",   t => when(datediff(t("shipped_at"), t("order_date")) <= 2, lit(1)).otherwise(lit(0))),
  )
  .withMeasures(Measure("total_ship_days", t => sum(t("ship_days"))))
```

**Single source of truth:** YAML `transforms:` and Scala
`withTransforms(...)` produce equivalent results — the same per-row
logic, expressed in either. Transformed columns become part of the
source DataFrame; downstream measures see them as regular columns
and aggregate them freely.

**Two things to know:**
- **Order matters.** Transforms apply in declaration order. If
  transform B references a column added by transform A, declare A
  first. There is no automatic topological sort — you are responsible
  for ordering the dependencies correctly.
- **Transforms run before filters and joins.** Once a transform
  materialises a column, it's visible to row filters (which run
  *after* transforms, *before* joins) — that's why `SparkFilterValidator`
  recognises transform outputs when checking filter expressions.

Typed field references (`SemanticField[T]` phantom types) wrap these
into a typeclass so the compiler catches dimension-vs-measure
confusion at the call site. Optional, additive — the string-based API
is unchanged.

---

## Calc measures — name-based compilation, not expression-tree surgery

The classic semantic-layer problem: how do you compile a formula like

```scala
Measure("avg_passengers", t => t("total_passengers") / t.all("total_passengers"))
```

where `t.all(...)` references a grand-total row that doesn't exist
in the source data? The "obvious" approach is to parse the Spark
expression and rewrite it (inserting a cross-joined totals column).
That's the **expression-tree surgery** approach; it's painful, fragile,
and hard to debug.

SemanticDF sidesteps this. The compiler resolves `t("total_passengers")`
*by name* against the aggregated DataFrame that already exists by that
name. Spark's `Column` API is re-runnable against any DataFrame that
has the named columns — no rewriting needed.

### Worked example — calc-of-calc

```yaml
# YAML model
flights:
  table: flights_csv
  dimensions:
    carrier: carrier
  measures:
    flight_count: "count(1)"
    total_distance: "sum(distance)"
  calculated_measures:
    # Layer 1 (leaves) → base
    avg_distance: "total_distance / flight_count"
    # Layer 2 (calc-of-calc) → references avg_distance from layer 1
    pct_of_avg_distance: "avg_distance / all(avg_distance)"
```

You ask for `pct_of_avg_distance` only. The engine pulls:
- `pct_of_avg_distance` (calc, depends on `avg_distance` and `all(avg_distance)`)
- `avg_distance` (calc, depends on `total_distance` and `flight_count`)
- `total_distance` (base, depends on `distance`)
- `flight_count` (base, depends on nothing)

Compilation:
1. **Base layer** (one `groupBy().agg(...)`):
   `groupBy("carrier").agg(sum("distance").alias("total_distance"), count(lit(1)).alias("flight_count"))`
2. **Layer 1** (one `select`):
   `select("carrier", "total_distance", "flight_count", (`total_distance` / `flight_count`).alias("avg_distance"))`
3. **Totals layer** (one `groupBy().agg(...)` for `all(avg_distance)`): `groupBy().agg(avg("avg_distance").alias("__sl_totals__avg_distance"))` — broadcasted, cross-joined to layer 1's output.
4. **Layer 2** (one `select`): applies the `pct_of_avg_distance` formula against the cross-joined data.

Two `select`s for two layers. No `withColumn` chains.

### Worked example — `t.all(...)` mechanics

For `t.all("name")`:
1. Build a totals `DataFrame = df.groupBy().agg(...)` — one row.
2. `broadcast()` it, `crossJoin` it with the per-group result.
3. The `t.all("name")` lambda returns `col("__sl_totals__name")`,
   which now resolves correctly in the cross-joined DataFrame.
4. Recompute the formula at the totals grain so non-sum totals
   (`all("avg...")` = 225, not 675) are correct.

The cross-join is the cost; the cleanness of "everything resolves by
name" is the win.

---

## Joins without fan-out

`join_many` is where SemanticDF earns its keep on multi-fact stars.

### Worked example — the cartesian-product hazard

Imagine a small dataset:

```
flights:            customers:
carrier | count     carrier | customer_id
AA     | 1000       AA       | c-001
UA     | 2000       UA       | c-002
DL     | 3000       DL       | c-003
                      AA       | c-004   ← two AA customers
```

`flights.join_many(customers, "carrier")` — joining on `carrier` —
should give 6 rows. If we'd written the join as plain SQL:

```sql
SELECT * FROM flights JOIN customers ON flights.carrier = customers.carrier
```

Spark (without our pre-aggregation step) produces a cross-product
per carrier: `AA → 2 rows × 2 customer rows = 4 rows`. So we get
10 rows instead of 6, and each carrier's count gets duplicated by the
number of customers — the numbers are nonsense.

`SemanticTable` does **not** trust the SQL planner for this; it
intercepts at join time.

### How the pre-aggregation flow works

The framework, at `SemanticJoinOp.compile` time:
1. Captures the join keys at compile time.
2. Probes each side's measures against that side's DataFrame to find
   what survives the pre-aggregation.
3. **Pre-aggregates** each side to the join-grain: `groupBy(allResolvableDims).agg(allResolvableMeasures)`.
4. Joins the pre-aggregated sides with a left-outer equi-join.

```scala
// After pre-aggregation, each side has exactly one row per carrier:

flights_agg:           customers_agg:
carrier | count        carrier | total_spend
AA      | 1000         AA       | 1250.0
UA      | 2000         UA       | 3400.0
DL      | 3000         DL       | 2100.0

// The join is now correctness-safe:
// AA     | 1000       | 1250.0
// UA     | 2000       | 3400.0
// DL     | 3000       | 2100.0
```

If you then `groupBy("carrier").aggregate("count * total_spend / 1")`,
the answer is well-defined.

If two non-key dimensions share a name (e.g. both sides have a column
called `category`), the framework raises a clear error pointing at the
collision — see `docs/known-limitations.md`.

### When to use which cardinality

- **`join_one`** — the joined table is at *or above* the join key's
  grain (e.g. a customer dimension). No pre-aggregation needed.
- **`join_many`** — the joined table is *below* the join key's grain
  (e.g. a fact table). Pre-aggregates the "many" side to the join
  grain to prevent fan-out.
- **`join_cross`** — Cartesian product. Use sparingly; you've opted
  out of fan-out prevention.

More in `DESIGN.md` §6.3.

---

## The terminal — batch now, streaming-shaped for later

The same `SemanticTable` compiles against different terminals:

- **Batch** (current) — `.toDataFrame(spark)` / `.execute(spark)` /
  `.collectAs[T](spark)`. Returns a Spark `DataFrame` (batch
  flavor) or `Seq[T]` (typed flavor).
- **Streaming** (ADR 0002) — `.toStreamingQuery(spark, opts)`. Same
  op tree, different terminal. Not yet built; the interface is
  shaped so adding it is a terminal, not a new API.

The construction API (`toSemanticTable(df).withDimensions(...).withMeasures(...)`)
is **identical** for batch and streaming; the user only diverges at
the terminal. The model file you build for a batch source today is
the same model you'd point at a streaming source tomorrow.

---

## Filters — pre-join hygiene, public `where` for query-time

Two kinds of filter, with different lifetimes:

| Where defined | When applied | Lifetime |
|---|---|---|
| YAML `filters:` block | Source DataFrame (incl. transform outputs), before any join | Per model — a permanent hygiene predicate |
| `st.where(...)` / `where=` field of `query()` | Group-of-dims → WHERE; group-of-measures → HAVING | Per query — a query-time filter |

### YAML `filters:` — pre-join row-level hygiene

The YAML `filters:` block applies **pre-join column visibility** —
joined columns aren't visible yet at filter time, since joins haven't
run. `SparkFilterValidator` enforces this at model-load time, and
`ExpressionValidator` catches typos against the visible column set.

```yaml
flights:
  table: flights_csv
  filters:
    require_origin:
      expr: "origin IS NOT NULL"
    require_recent_season:
      expr: "flight_date >= '2024-01-01'"
  dimensions:
    origin: origin
    carrier: carrier
  measures:
    flight_count: "count(1)"
```

These two filters are applied to `flights_csv` *before* any join, any
aggregation, any window. They're a permanent hygiene predicate — the
assumption that "ship-time data with a NULL origin doesn't reach the
warehouse" is expressed once, not in every dashboard.

If a filter references a transform output (e.g. `ship_days < 30`),
that works — transforms run before filters. If a filter references a
joined-side column (e.g. `carriers.region = 'EU'`), that fails at
load time with a clear error — joins haven't run yet.

### Query-time `where` — WHERE/HAVING auto-routing

A query-time filter routes **dimension refs to WHERE (pre-agg)**
and **measure refs to HAVING (post-agg)**. AND compounds split
condition-by-condition; OR compounds touching any measure stay whole
(post-agg).

```scala
// Pre-agg filter on a dimension — compiles to WHERE
flights.where("carrier" === "AA").aggregate("flight_count")

// Post-agg filter on a measure — compiles to HAVING
flights.groupBy("carrier").aggregate("flight_count")
       .where("flight_count" > 100)

// Mixed: AND across dimensions and measures — splits condition-by-condition.
// Each dimension predicate goes to WHERE; the measure predicate to HAVING.
flights.where(("carrier" === "AA") and ("flight_count" > 100))
       .groupBy("carrier").aggregate("flight_count")
```

Three filter weirdnesses worth knowing up front:

- **Lambda purity required.** Measure / dimension lambdas run twice
  (once at classification, once at compile). Side effects run twice.
- **Spark 3 vs Spark 4 divide-by-zero.** `/` returns null on Spark 3,
  throws on Spark 4 with ANSI mode. Use `CalcHelpers.safeDivide` if you
  need stable behavior across versions.
- **`SparkFilterValidator` enforces pre-join visibility.** A filter
  that references a joined column is rejected at model-load. The error
  names the offending column and explains the visibility rule.

Full limitations list in `docs/known-limitations.md`.

---

## The typed layer (compile-time safety)

The optional typeclass layer wraps every dimension and measure in a
phantom type so dimension-vs-measure confusion is a compile error,
not a runtime surprise.

### Worked example — common compile errors

```scala
object Flights {
  sealed trait Carrier
  sealed trait TotalPassengers
  implicit val carrier: SemanticDimension[Carrier] =
    SemanticDimension.of[Carrier]("carrier")
  implicit val pax: SemanticMeasure[TotalPassengers] =
    SemanticMeasure.of[TotalPassengers]("total_passengers")
}

import Flights._

st.groupByDimensions(carrier)        // OK
st.aggregateMeasures(pax)            // OK
st.groupByDimensions(pax)            // COMPILE ERROR — pax is a Measure, not a Dimension
st.aggregateMeasures(carrier)        // COMPILE ERROR — carrier is a Dimension, not a Measure
```

Other compile-time catches:
- `Compare.Greater(pax, 600)` — typo; only `Eq`/`Ne`/`Lt`/`Le`/`Gt`/`Ge` exist.
- Sealed `Predicate.Compare` ADT — `Compare("gt", ...)` is no longer accepted as a string; you must use one of the typed case classes.
- Arity 5+ — single runtime check (string-based API still works for any arity, but loses compile-time safety).

### Practical notes

- **Pure additions to the library** — the string-based API still
  works. Phantom types are opt-in per consumer.
- **Arities 1–4 are fully type-checked at compile time.**
- **`FieldRef[T]` is a value class** — no allocation on the hot path.
- **Same approach on the result side** — `ResultDecoder.derive[T]`
  (PR `#64`) does the compile-time generation for `case class`
  decoders.

For details on why the typeclass pattern was chosen over a richer DSL,
see `DESIGN.md` §E and `docs/phase-E-plan.md`.

---

## Notebook escape hatch — raw SQL via a temp view

For notebook users (Jupyter, Databricks, Zeppelin) and any SQL-first
consumer (BI tools via the Spark Thrift Server, dbt, etc.), a compiled
`SemanticTable` can be registered as a Spark temp view and queried
with plain SQL:

```scala
val flights = YamlLoader.load("flights.yml", dataConfig)("flights")
flights.createOrReplaceTempView("flights_view")  // session-scoped

// Now any cell, any language, any tool can query the model:
spark.sql("SELECT carrier, total_passengers FROM flights_view GROUP BY carrier")
```

The view is the **compiled output** of the model — joins, pre-join
filters, and pre-aggregation all happen *before* the SQL queries the
view. The SQL is free to do whatever a `SELECT` against the compiled
schema can do: aggregations, joins with other views, window
functions, etc. What it cannot do is reach back into SemanticDF — the
view is a one-way edge.

### Lifecycle and scoping

A temp view is **session-scoped** in Spark. `createOrReplaceTempView`
replaces a view of the same name; `createOrReplaceGlobalTempView`
makes it accessible across all sessions on the cluster. Both
delegate to the standard Spark temp-view mechanism — SemanticDF adds
nothing here beyond the convenience of calling it on a model.

If the underlying source DataFrame changes, the view automatically
reflects the change on the next `spark.sql(...)` call (views are
*re-resolved* at query time, not materialized at view creation).

### Worked example — notebook workflow

```scala
// Cell 1 — load the model
val orders = YamlLoader.load("orders.yml", dataConfig)("orders")
orders.createOrReplaceTempView("orders_view")
println("View registered: orders_view")

// Cell 2 — your SQL-first colleague works directly:
//   spark.sql("SELECT region, SUM(amount) FROM orders_view GROUP BY region")

// Cell 3 — you come back, you want a typed Seq:
case class ByRegion(region: String, total: Double)
implicit val dec: ResultDecoder[ByRegion] = ResultDecoder.derive[ByRegion]
orders.collectAs[ByRegion](spark)
```

The same model, two terminals: `spark.sql(...)` and
`collectAs[ByRegion](...)` over a shared underlying compile.

For tools that consume the model *as data* rather than as a view (an
LLM reading about a model rather than querying it), `okfgen` produces
a markdown knowledge bundle.

---

## Where to go from here

You've read the narrative. Now choose:

- **You want to use SemanticDF:** open [`examples/starter/`](../examples/starter/)
  and run it; the README's `## Quick start` is also a fair starting point.
- **You want to write models in YAML:** read
  [`examples/hospital/models/patients.yml`](../examples/hospital/models/) and
  [`examples/starter/models/flights.yml`](../examples/starter/models/) for real-world shapes.
- **You want to add a measure / dimension:** read `DESIGN.md` §6 (the hard
  problems) — especially §6.1 (calc measures) and §6.2 (percent-of-total).
- **You want to add an MCP tool:** read
  [`docs/agents/mcp-contract.md`](agents/mcp-contract.md) and
  [`semanticdf-mcp/README.md`](../semanticdf-mcp/README.md).
- **You want to understand why a design choice was made:** read
  [`DESIGN.md`](../DESIGN.md) and the three [`docs/adr/`](adr/) files.
