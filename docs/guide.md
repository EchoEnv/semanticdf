# SemanticDF — A reader's guide

A narrative walkthrough for new readers. Pairs with `DESIGN.md` (the
architecture of record) the way a guidebook pairs with a map: this
explains *how it works* and *why you'd reach for it*, while DESIGN.md
records *the decisions that produced it*.

If you want to *use* SemanticDF, read this. If you want to *contribute*
to it, read this, then read `DESIGN.md`.

---

## The 5-minute mental model

A `SemanticTable` is an **immutable description of a slice of your
business**: dimensions, measures, joins, filters, time grains. It is
not a DataFrame; it's *what* you want, separate from *how* it's computed.
The framework compiles the description into a Spark `DataFrame` on
demand, at the terminal you call:

```scala
flights.groupBy("carrier").aggregate("flight_count").execute(spark)
// → DataFrame
flights.collectAs[CarrierCount](spark)   // → Seq[CarrierCount]
flights.toDataFrame(spark)              // → DataFrame (sync, same result)
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
roughly these steps:

1. **Classify** the requested measures. For each lambda, probe the
   base DataFrame to find what columns it touches. Lambdas that touch
   only base columns are *base* measures; lambdas that reference other
   measure names are *calc* measures.
2. **Pull** transitive dependencies. If you ask for `avg_distance` and
   it depends on `total_distance` and `flight_count`, both are pulled.
3. **Compile** the base measures into an aggregated `DataFrame`
   (`df.groupBy(keys).agg(...)`), each aliased to its measure name.
4. **Layer** the calc measures on top, one `select` per topologically
   ordered layer. A 50-measure-wide model with depth-2 calcs is two
   `select`s, not 50 chained `withColumn`s.
5. **Apply** post-agg routing. WHERE filters on dimensions compile
   before the aggregate; HAVING filters on measures compile after.

The `ResultDecoder.derive[T]` macro (PR `#64`) does the same kind of
"compile from structure" at the *result* side: a `case class
CarrierCount(carrier: String, count: Long)` becomes a typed decoder
that reads `row.getString(0)` and `row.getLong(1)`.

---

## Dimensions and measures

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

For `t.all(...)`:
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

```scala
flights.join_many(bookings, "carrier")        // pre-agg fan-out prevention
```

Without intervention, joining a 1000-row flights fact to a customer
dimension that's keyed by carrier would produce `customer_x_orders`
rows — incorrect double-counting. The "obvious" approach is to trust
the SQL planner to handle it; SemanticDF **doesn't trust it** because
correctness matters more than letting the planner figure it out.

The framework:
1. Captures the join keys at `SemanticJoinOp.compile` time
2. Probes each side's measures against that side's DataFrame to find
   what survives the pre-aggregation
3. **Pre-aggregates** each side to the join-grain: `groupBy(allResolvableDims).agg(allResolvableMeasures)`
4. Joins the pre-aggregated sides with a left-outer equi-join

If two non-key dimensions share a name (e.g. both sides have a column
called `category`), the framework raises a clear error pointing at the
collision — see `docs/known-limitations.md`.

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
| YAML `filters:` block | Source DataFrame, before any join | Per model — a permanent hygiene predicate |
| `st.where(...)` / `where=` field of `query()` | Group-of-dims → WHERE; group-of-measures → HAVING | Per query — a query-time filter |

The YAML `filters:` block uses **pre-join column visibility** (joined
columns aren't visible yet, since joins haven't run). `ExpressionValidator`
catches typos at model-load time. The query-time `where` uses
**WHERE/HAVING auto-routing**: dimension refs go pre-agg, measure refs
go post-agg, AND compounds split condition-by-condition, OR compounds
stay whole.

Three filter weirdnesses worth knowing (full list in
`docs/known-limitations.md`):
- **Lambda purity required** — measure / dimension lambdas run twice
  (once at classification, once at compile). Side effects run twice.
- **Spark 3 vs Spark 4 divide-by-zero** — `/` returns null on Spark 3,
  throws on Spark 4 with ANSI mode. Use `CalcHelpers.safeDivide` if you
  need stable behavior across versions.
- **`SparkFilterValidator` enforces pre-join visibility** — a filter
  that references a joined column is rejected at model-load.

---

## The typed layer (compile-time safety)

The optional typeclass layer wraps every dimension and measure in a
phantom type so dimension-vs-measure confusion is a compile error,
not a runtime surprise:

```scala
object Flights {
  sealed trait Carrier
  sealed trait TotalPassengers
  implicit val carrier: SemanticDimension[Carrier]   = SemanticDimension.of[Carrier]("carrier")
  implicit val pax:     SemanticMeasure[TotalPassengers] = SemanticMeasure.of[TotalPassengers]("total_passengers")
}

import Flights._

st.groupByDimensions(carrier)        // OK
st.aggregateMeasures(pax)            // OK
st.groupByDimensions(pax)            // COMPILE ERROR — pax is a Measure, not a Dimension
st.aggregateMeasures(carrier)        // COMPILE ERROR — carrier is a Dimension, not a Measure
```

Pure additions to the library — the string-based API still works.
Arities 1–4 are fully type-checked at compile time; arity 5+ uses a
single runtime check. `FieldRef[T]` is a value class so there's no
allocation on the hot path. The same approach on the result side
gave us `ResultDecoder.derive[T]` (PR `#64`).

For details on why the typeclass pattern was chosen over a richer DSL,
see `DESIGN.md` §E.

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
filters, and pre-aggregation all happen before the SQL queries the
view. This is the standard Spark temp-view escape hatch, not a
SemanticDF API; it's documented here for notebook users who want to
hand the view off to a colleague working in SQL.

For tools that consume the model *as data*, `okfgen` produces a
markdown knowledge bundle that an LLM can read instead.

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
