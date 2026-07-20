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
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// 1. SparkSession (omit in your own code if you already have one)
val spark = SparkSession.builder().master("local[2]").getOrCreate()
import spark.implicits._

// 2. A sample source DataFrame
val flights = Seq(
  ("AA", 100, 5), ("AA", 120, 4),
  ("UA",  80, 3), ("DL", 150, 6),
).toDF("carrier", "distance", "passengers")

// 3. A semantic model (declarative — doesn't run anything)
val flightsModel = toSemanticTable(flights, name = Some("flights"))
  .withDimensions(
    Dimension("carrier", t => t("carrier")),
  )
  .withMeasures(
    Measure("flight_count",  t => count(lit(1))),
    Measure("total_distance",t => sum(t("distance"))),
    // Calc measure — references other measures by name. Framework pulls
    // `total_distance` and `flight_count` automatically when you ask for
    // `avg_distance_per_flight`.
    Measure("avg_distance_per_flight",
            t => t("total_distance") / t("flight_count")),
  )

// 4. ONE query, THREE terminals. Note the `byCarrier` val is shared:
val byCarrier = flightsModel
  .groupBy("carrier")
  .aggregate("flight_count", "avg_distance_per_flight")

// Terminal 1: returns a Spark DataFrame (batch flavor, for SQL/BI tools)
byCarrier.execute(spark)
// → DataFrame[(carrier: String, flight_count: Long, avg_distance_per_flight: Double)]

// Terminal 2: returns a typed Seq (compile-time field safety via ResultDecoder)
case class Row(carrier: String, flight_count: Long, avg_distance_per_flight: Double)
implicit val dec: ResultDecoder[Row] = ResultDecoder.derive[Row]
byCarrier.collectAs[Row](spark)
// → Seq(Row("AA", 2L, 110.0), Row("UA", 1L, 80.0), Row("DL", 1L, 150.0))

// Terminal 3: register as a Spark temp view, then query via plain SQL
byCarrier.execute(spark).createOrReplaceTempView("flights_view")
spark.sql("SELECT * FROM flights_view WHERE flight_count >= 2").show()
```

The same `byCarrier` query, executed three ways — that's the contract.
The compilation step happens once per terminal call; the model itself
doesn't change.

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

A small orders + line-items dataset:

```
orders:                          lineItems:
order_id | customer_id | amount  item_id | order_id | qty
1        | 101         | 5000   1        | 1        | 2
2        | 102         | 3000   2        | 1        | 1
3        | 101         | 7500   3        | 2        | 3
4        | 103         | 2000   4        | 3        | 1
5        | 102         | 1000   5        | 4        | 2
                                  6        | 5        | 1
```

`orders.join_many(lineItems, "order_id")` should give exactly **5
rows** (one per order). If we'd written the join as plain SQL:

```sql
SELECT * FROM orders JOIN lineItems ON orders.order_id = lineItems.order_id
```

That's still right here (orders has 5 rows, lineItems has 6 rows, the
join is many-to-one along order_id). But the danger is real: the
moment you have a multi-fact join where both sides have the same
join-key at a HIGHER grain than the fact detail — say you instead
join `orders` (50 rows: 50 orders by 3 customers = some customers
place >1 order) against `lineItems` (one per order line) — Spark
without our pre-aggregation step produces a cross-product per
order. Customer 101 places 2 orders of 2 items each = order row
expanded 2 times then per-item, fanning the per-order aggregation out.

`SemanticTable` does **not** trust the SQL planner for this; it
intercepts at join time.

### How the pre-aggregation flow works

The framework, at `SemanticJoinOp.compile` time:
1. Captures the join keys at compile time.
2. Probes each side's measures against that side's DataFrame to find
   what survives pre-aggregation.
3. **Pre-aggregates** each side to the join-grain: `groupBy(allResolvableDims).agg(allResolvableMeasures)`.
4. Joins the pre-aggregated sides with a left-outer equi-join.

A runnable example:

```scala
import io.semanticdf._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val spark = SparkSession.builder().master("local[2]").getOrCreate()
import spark.implicits._

val orders = Seq(
  (1, 101, 5000, "shipped"),
  (2, 102, 3000, "shipped"),
  (3, 101, 7500, "delivered"),
  (4, 103, 2000, "processing"),
  (5, 102, 1000, "shipped"),
).toDF("order_id", "customer_id", "amount", "status")

val lineItems = Seq(
  (1, 1, 2, 2500), (2, 1, 1, 3000),
  (3, 2, 3, 1000), (4, 3, 1, 7500),
  (5, 4, 2, 1000), (6, 5, 1, 1000),
).toDF("item_id", "order_id", "qty", "price_cents")

val ordersModel = toSemanticTable(orders, name = Some("orders"))
  .withDimensions(
    Dimension("customer_id", t => t("customer_id")),
    Dimension("status",      t => t("status")),
  )
  .withMeasures(Measure("order_amount", t => sum(t("amount"))))

val itemsModel = toSemanticTable(lineItems, name = Some("items"))
  .withDimensions(Dimension("order_id", t => t("order_id")))
  .withMeasures(Measure("item_count", t => count(lit(1))))

// join_many: each side pre-aggregated to order_id grain, then equi-joined.
// After join, the result has one row per order_id with these columns:
//   order_id, customer_id, order_amount   (from orders side)
//   order_id, item_count                  (from items side)
val joined = ordersModel.join_many(itemsModel,
  (l, r) => l("order_id") === r("order_id"))

joined.execute(spark).show(false)
// Note: row count === orders.count() == 5. The fact table did NOT
// multiply — that's the fan-out prevention working.
```

What `join_many` does here:
- **Pre-aggregates each side to the join key.** Each side becomes one
  row per `order_id`: orders → `(order_id, customer_id, order_amount)`;
  items → `(order_id, item_count)`.
- **Equi-joins** the two on `order_id`. With both sides pre-aggregated,
  the join is **correctness-safe** regardless of how many line items each
  order has — the per-order `total_amount` doesn't get multiplied by
  the line count.

After the join, the result is a regular `DataFrame` containing both
sides' measures as columns. You can aggregate further with **plain
Spark** — for example, grand totals across all orders:

```scala
joined.execute(spark).agg(
  sum(col("order_amount")).as("grand_orders"),
  sum(col("item_count")).as("grand_items"),
).show()
```

**Name collisions.** If both sides declare a non-key column with the
same name (e.g. both have a column called `category`), the join raises
a clear error pointing at the collision — see
`docs/known-limitations.md`. To disambiguate, rename one side's column
with the merged model's `.withDimensions(...)` / `.withMeasures(...)`.



### When to use which cardinality

- **`join_one`** — the joined table is at *or above* the join key's
  grain (e.g. a customer dimension). No pre-aggregation needed.
- **`join_many`** — the joined table is *below* the join key's grain
  (e.g. a fact table). Pre-aggregates **both sides** to the join grain
  to prevent fan-out.
- **`join_cross`** — Cartesian product. Use sparingly; you've opted
  out of fan-out prevention.

More in `DESIGN.md` §6.3.

---

## The terminal — batch now, streaming-shaped for later

The same `SemanticTable` compiles against different terminals:

- **Batch** (current) — `.toDataFrame(spark)` / `.execute(spark)` /
  `.collectAs[T](spark)`. Returns a Spark `DataFrame` (batch
  flavor) or `Seq[T]` (typed flavor). `.queryAs[T](...)` bundles
  `query(...)` parameters and returns `Dataset[T]` (Spark's typed
  collection) — the one-shot pick when you want a typed result
  in one line.
- **Streaming** (ADR 0002) — `.toStreamingQuery(spark, opts)`. Same
  op tree, different terminal. Not yet built; the interface is
  shaped so adding it is a terminal, not a new API.

### `queryAs[T]` — typed one-shot bundle (Phase E1)

`queryAs[T]` is the typed-flavor sibling of `query(...)`: same
parameter shape (measures, dimensions, where, having, orderBy,
limit, timeGrain), but the result is a `Dataset[T]` decoded from
the rows via the implicit `ResultDecoder[T]` + Spark `Encoder[T]`.

```scala
import io.semanticdf.ResultDecoder
import org.apache.spark.sql.SparkSession

implicit val spark: SparkSession =
  SparkSession.builder().master("local[*]").getOrCreate()
import spark.implicits._  // required for Encoder[T] on case classes

// 1. Define a case class for the result.
case class CarrierRevenue(carrier: String, total: Long)

// 2. Build a SemanticTable as usual.
val model = toSemanticTable(flights, name = Some("flights"))
  .withDimensions(Dimension("carrier", t => t("carrier")))
  .withMeasures(Measure("flight_count", t => count(lit(1))))

// 3. queryAs[T] bundles query(...) and decodes into a Dataset[T].
val ds: Dataset[CarrierRevenue] = model.queryAs[CarrierRevenue](
  measures = Seq("flight_count"),
  dimensions = Seq("carrier"),
)
// ds.collect() -> Array(CarrierRevenue("AA", 10), CarrierRevenue("UA", 10), ...)

// Compile-time safety: a typo in the case class field name fails to
// derive ResultDecoder[CarrierRevenue] at the call site:
//   case class CarrierRevenew(carrier: String, total: Long)  // typo
//   model.queryAs[CarrierRevenew](...)                       // compile error
```

Notes:
- Field names in the case class must match the column names the
  query produces (use `.alias("...")` in the measure lambda, or
  rely on the default measure name).
- For the macro: bring `ResultDecoder.derive[T]` into scope via
  `import scala.language.experimental.macros` at the call site, or
  import `io.semanticdf.ResultDecoder._` once and let the implicit
  resolution find it.
- The case class must be top-level (not nested inside a method or
  class) for Spark's `newProductEncoder` to find a no-arg constructor.
- See [`docs/phase-E-plan.md`](phase-E-plan.md) §E1 for the design
  rationale and the macro mechanics.

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

## Advanced — extending the framework

### Custom `SemanticScope` (analyzer adapter)

`SemanticScope` is the trait that every measure/dimension lambda
receives. The framework ships four built-in implementations
(`BaseScope`, `MeasureScope`, `ClassificationScope`, `MeasureProbeScope`),
but the trait is public and open — you can implement your own to
inspect measure expressions without running a query.

The intended use is **tooling**: catalog/dependency analyzers, line-of-business
glossary generators, or static checks that flag column references without
executing any Spark query. The expression may contain arbitrary Spark
logic (UDFs, window functions, subqueries), so any analyzer is
best-effort — the contract is "no false negatives on simple refs", not
"see everything the compiler sees."

```scala
// Simple analyzer that records which dimension/measure names a lambda touches.
final class RecordingScope extends SemanticScope {
  val fields = scala.collection.mutable.Set.empty[String]
  val totals = scala.collection.mutable.Set.empty[String]
  override def apply(name: String): Column = { fields += name; lit(0.0) }
  override def all(name: String): Column = { totals += name; lit(0.0) }
}

val scope = new RecordingScope
model.measures("total_amount").expr(scope)   // pure metadata pass; no SparkSession needed
println(scope.fields)                       // Set(total_amount)
```

This is **not** a way to inject an alternative execution backend —
`execute` and `toDataFrame` always run the compiled op tree against the
SparkSession you pass. The use case is "what would this measure
reference, without running the query?"

This pattern is already used internally by the framework's own catalog
introspection (see `ClassificationScope` and `MeasureProbeScope` in
`Scope.scala` and `SemanticTable.scala`). It is the documented public
extension point for any third-party tool that needs to inspect measure
expressions.

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
