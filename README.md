# SemanticDF

A **semantic layer for Apache Spark** (JVM/Scala), adapted from the
[Boring Semantic Layer](https://github.com/boringdata/boring-semantic-layer) (Python/Ibis).

A `SemanticTable` is a deferred, source-agnostic definition that compiles to a Spark
`DataFrame` at a batch terminal (`.toDataFrame(spark)` / `.execute(spark)`). It is *not*
a `DataFrame` itself — it captures *what* you want (dimensions, measures, joins, filters,
grains) so the engine can decide *how* to compute it. A future streaming terminal would
reuse the same definition against a different sink (ADR 0002).

**Status:** v0.1.3 + post-tag fixes (`#54`–`#59` shipped in 0.1.3; `#61` and `#62` shipped after the tag, included on `main`). **401/401 tests green** (329 library + 72 MCP) under Spark 3.5.8 (default) and Spark 4.1.1. See [`DESIGN.md`](DESIGN.md) for the architecture of record and [`docs/adr/`](docs/adr/) for recorded decisions.

## Build

Requires **JDK 17** and **Maven 3.9+**. Spark is on the classpath as `provided` (it comes
from your cluster/runtime).

```bash
mvn test                      # Spark 3.5.8 (default)
mvn -Pspark4 test             # Spark 4.1.1 (latest stable)
```

## Quick start

```scala
import io.semanticdf._
import io.semanticdf.Predicate._    // "field" === value, "field" > value, SortKey.desc, ...
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

### Window functions

A Measure is just `SemanticScope => Column`, so any Spark window function is legal
inside the lambda. The window evaluates against the post-aggregation DataFrame (Pass 2
of the calc layer), so it can reference group-by keys and base measures by name.

```scala
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{row_number, rank, sum}

val st = toSemanticTable(flightsDf, name = Some("flights"))
  .withDimensions(
    Dimension("carrier", t => t("carrier")),
    Dimension("origin",  t => t("origin")),
  )
  .withMeasures(
    Measure("flight_count", t => count(lit(1))),
    // rank within each carrier by origin:
    Measure("rank_per_carrier_origin",
      t => row_number().over(Window.partitionBy(t("carrier")).orderBy(t("origin")))),
    // running total of total_passengers across origins per carrier:
    Measure("running_total",
      t => sum(t("total_passengers")).over(
        Window.partitionBy(t("carrier")).orderBy(t("origin")))),
  )
  .groupBy("carrier", "origin")
  .aggregate("flight_count", "rank_per_carrier_origin", "running_total")
```

Window functions work in the Scala DSL. The YAML loader also accepts raw SQL
window expressions in `measures:` (e.g. `row_number() over (partition by carrier
order by origin)`); the parser blocklist covers `row_number`, `rank`, `dense_rank`,
`lag`, `lead`, `ntile`, `first_value`, `last_value`, plus window-frame SQL
keywords (`order`, `rows`, `range`, `between`, `unbounded`, `preceding`,
`following`, `and`, `current`, `asc`, `desc`, `nulls`).

> **Known limitation:** a window function that references group-by keys (e.g.
> `Window.partitionBy(t("carrier"))`) cannot be combined with `t.all(...)` for
> percent-of-total — the zero-grain totals table has no group-by keys, so the
> window evaluation fails. Workaround: use a window that doesn't reference
> group-by keys (e.g. `Window.orderBy(...)` only), or compute percent-of-total
> as a separate measure.

### Transforms (per-row computations, applied at model-load)

Per-row logic — `datediff(...)`, `case when ...`, window functions —
doesn't fit the measure's aggregate context. Use the new `transforms:`
block (YAML) or `withTransforms(...)` (Scala DSL) to apply such logic
to the source data at model-load time. Transforms correspond to
[dbt staging models](https://docs.getdbt.com/docs/build/staging-models)
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

**Single source of truth:** YAML `transforms:` and Scala `withTransforms(...)`
produce equivalent results — the same per-row logic, expressed in either.
Transformed columns become part of the source DataFrame; downstream
measures see them as regular columns and aggregate them freely.

**Order matters:** transforms apply in declaration order. If transform B
references a column added by transform A, declare A first. There is
no automatic topological sort (the user is responsible for ordering
the dependencies correctly).

### Filters (pre-join row-level hygiene, applied at model-load)

Row-level **hygiene** — drop rows missing a required field, drop cancelled
orders, dedup before aggregation — doesn't fit a query, because it
governs which rows the model *contains*, not which rows a particular
query returns. Declare it on the model via `filters:` (YAML) or
`withRowFilter(...)` (Scala DSL). Filters run **pre-agg, pre-join**,
against this model's source table only.

```yaml
# YAML
flights:
  table: flights_csv
  # Pre-join row-level hygiene. Column references must resolve against
  # flights_* — joined-side columns are rejected at load time.
  filters:
    require_origin_and_carrier:
      expr: "origin IS NOT NULL AND carrier IS NOT NULL"
      description: "Drop rows with null origin or carrier — flagged in upstream QA rule."
      metadata:
        owner: data-platform-team
        tags: [data-quality]
  dimensions:
    carrier:
      expr: carrier
```

```scala
// Scala DSL — equivalent
val flights = toSemanticTable(flightsDf, name = Some("flights"))
  .withRowFilter(
    name        = "require_origin_and_carrier",
    expr        = "origin IS NOT NULL AND carrier IS NOT NULL",
    description = Some("Drop rows with null origin or carrier — flagged in upstream QA rule."),
    metadata    = Map("owner" -> "data-platform-team", "tags" -> "[data-quality]"),
  )
```

**Source-only / pre-join — enforced.** `SparkFilterValidator` runs at YAML
load time and rejects any filter whose `expr` references a column that
isn't on this model's source table or produced by a `transforms:` entry
(transforms run before filters, so their outputs are visible at filter
time). Joined-side columns are NOT visible — filters apply pre-join by
design. The guarantee: a filter runs exactly once per row of source
data, regardless of how the model is later joined, aggregated, or
sliced.

**Distinct from `.where(...)`.** Query-time `.where(predicate)` /
`.query(where = ...)` compile to a `SemanticFilterOp` inside the op
tree. Those are WHERE/HAVING clauses routed against the model's current
grain — not pre-join model-load hygiene (see *Filters — WHERE/HAVING
auto-routing* below for the query-time kind).

**YAML expression validation (v0.1.1+).** `ExpressionValidator` parses
every `dimensions:`, `transforms:`, and `measures:` `expr` at load time
(via Spark's `CatalystSqlParser`) and checks that every column reference
resolves against the visible columns at that point — source columns plus
any previously-declared transforms (for `transforms:` and `measures:`)
and previously-declared measures (for `measures:` referencing other
measures, common in window-function ORDER BYs). A typo fails fast at
load time with a clear error, not later at first query time as a
cryptic Spark `UNRESOLVED_COLUMN.WITH_SUGGESTION`.

`calculated_measures:` are validated separately by `CalcExpr.validateReferences`
(the CalcExpr DSL is arithmetic over already-aggregated measures, with
`all(name)` for percent-of-total — a different parser). Same fail-fast
guarantee: a typo in a referenced measure name or `all()` arg throws
`IllegalArgumentException` at `YamlLoader.load(...)` instead of surfacing
as a cryptic `UnknownFieldError` at query time. Calc measures can
reference base measures and earlier-declared calc measures (in
declaration order).

See [`ExpressionValidator`](src/main/scala/io/semanticdf/ExpressionValidator.scala)
and [`CalcExpr`](src/main/scala/io/semanticdf/CalcExpr.scala) for the
full visibility rules.

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

### Querying from a notebook via `spark.sql(...)`

For notebook users (Jupyter, Databricks, Zeppelin) and any SQL-first consumer
(BI tools via Spark Thrift Server, dbt, etc.), a compiled semantic model can
be registered as a Spark temp view and queried with plain SQL.

```scala
// One-time setup: register the model as a Spark temp view
implicit val spark: SparkSession = ...
val flights = YamlLoader.load("flights.yml", dataConfig)("flights")
flights.createOrReplaceTempView("flights")   // session-scoped

// Now any cell, any language, any tool can query the model:
spark.sql("SELECT carrier, total_passengers FROM flights GROUP BY carrier")
```

The view is the **compiled output** of the model — joins, pre-join filters,
dimensions, base measures, and calc measures are all baked in. SQL consumers
see a clean relational view, not the raw source table.

**"What can I query?" — discovery.** Before writing SQL, a consumer needs to
know what dimensions and measures the model exposes. Use `schema(spark)`:

```scala
flights.schema(spark).show(false)
// +-----------+-------------------+-----------+---------+-----------------------------+--------------------+...
// |model_name |model_description  |field_name |field_type|description                |metadata_keys       |...
// +-----------+-------------------+-----------+---------+-----------------------------+--------------------+...
// |flights    |Flight facts:...   |carrier    |dimension |Airline carrier code       |owner,tags          |...
// |flights    |Flight facts:...   |origin     |dimension |Origin airport code        |owner,tags          |...
// |flights    |Flight facts:...   |flight_date|dimension |Scheduled flight date      |null                |...
// |flights    |Flight facts:...   |total_passengers|measure|Total passengers across...|owner,unit,aggregation|...
// |flights    |Flight facts:...   |flight_count   |measure|Number of flights (rows)...|owner,unit,aggregation|...
// +-----------+-------------------+-----------+---------+-----------------------------+--------------------+...
```

The same metadata is also served by the MCP server's `describe_model` tool
and rendered as Markdown via `okfgen` (see [`docs/agents/okf-mapping.md`](docs/agents/okf-mapping.md)).
Pick the surface that fits your tool.

**Temp view scope.** Three variants map to standard Spark conventions:

| Method | Scope | Conflict policy |
|---|---|---|
| `createOrReplaceTempView(name)` | session | replaces if exists |
| `createTempView(name)` | session | throws if exists |
| `createOrReplaceGlobalTempView(name)` | cluster (across sessions) | replaces if exists |

All three take `(implicit spark: SparkSession)` — call from inside a
`SparkSession.builder()` block.

**Limitations.** A registered view has a single fixed shape — the grain at
which the model was compiled. SQL consumers can't switch grains in-band
(`SELECT ... WHERE grain = 'monthly'`); use the typed `atTimeGrain(...)` API
or build separate views for each grain you need.

#### BI tools via Spark Thrift Server (Power BI, Tableau, Looker, Metabase, ...)

The temp view pattern above is per-session. To expose semantic models to
**external BI tools** that speak JDBC/ODBC, the standard path is
**Spark Thrift Server** (a.k.a. Spark HiveServer2). Once running, a
Power BI desktop / Tableau workbook / dbt project can connect with the
standard Hive JDBC driver and query the models as if they were regular
SQL tables.

**Architecture:**

```
+--------------------+        +-----------------------+        +------------------+
| Power BI / Tableau | JDBC   |  Spark Thrift Server  |  talks |  semanticdf      |
| (your laptop)      | -----> |  (HiveServer2 on a   | -----> |  YAML model     |
|                    |        |   long-lived box)     |        |  + temp view     |
+--------------------+        +-----------------------+        +------------------+
```

**One-time setup** (a small launcher script that runs in a long-lived
Spark process — not the same JVM as your notebook):

```scala
// Server.scala — run as a long-lived service
import org.apache.spark.sql.SparkSession
import io.semanticdf._

object SemanticServer extends App {
  // A long-lived Spark session
  implicit val spark: SparkSession = SparkSession.builder()
    .appName("semanticdf-bridge")
    .enableHiveSupport()                  // required for Thrift Server
    .getOrCreate()

  // Load all your models and register them as global temp views
  val models = YamlLoader.loadDir("models/", dataConfig)
  models.values.foreach { st =>
    // Global temp views are visible across all sessions connected to
    // this Thrift Server, including the BI tool's session
    st.createOrReplaceGlobalTempView(s"semantic_${st.name}")
    // Optional: also expose schema metadata as a sidecar view for
    // the BI tool's data catalog
    st.schema(spark).createOrReplaceGlobalTempView(s"semantic_${st.name}__schema")
  }

  // Block forever — the Thrift Server is now serving SQL over your
  // semantic models on the standard port (10000)
  Thread.currentThread().join()
}
```

**Start the Thrift Server** with your JAR on the classpath:

```bash
# Standard Spark distribution layout
$SPARK_HOME/sbin/start-thriftserver.sh \
  --master yarn \
  --jars /path/to/semanticdf_2.13-0.1.3.jar,/path/to/your-models-loader.jar \
  --conf spark.sql.hive.thriftServer.singleSession=false
```

The server exposes the standard HiveServer2 port (default `10000`).

**Connect from Power BI / Tableau / etc.** — use the standard **Hive
JDBC driver** with a JDBC URL like:

```
jdbc:hive2://thrift-server-host:10000/default;transportMode=http;httpPath=cliservice
```

In the BI tool:
- The temp views appear as tables in the `default` database (or
  `global_temp` if you registered them there). For Power BI: rename
  `global_temp.semantic_flights` to `semantic_flights` for clarity.
- Each view is the compiled model — joins, pre-join filters, calc
  measures, all baked in.
- Discovery: query `SELECT * FROM global_temp.semantic_flights__schema`
  to see the dimension/measure list, or open the OKF Markdown at
  `docs/agents/reference/<project>/<model>.md`.

**Limitations vs the typed API:**

- Single-grain views (same as notebook case above)
- No typed compile-time checks — typos surface at query time as
  `AnalysisException: cannot resolve 'foo'`
- No access to `.explainSemantic(...)` — use the local library for
  plan-level debugging

For the full REST API path (JSON over HTTP, no JDBC driver needed,
works with any HTTP-capable tool), see the v0.2 roadmap in
[`docs/feature-roadmap.md`](docs/feature-roadmap.md) §3.1. Thrift Server
is the pragmatic v0.1.1 path.

### Typed queries (compile-time safety)

The string-based API above is convenient but typo-prone — a wrong field name is a runtime
error. An **additive** typed API catches those mistakes at compile time.

```scala
// Declare phantom types + implicit typeclass witnesses (one-time, per field):
object Flights {
  sealed trait Carrier
  sealed trait Origin
  sealed trait TotalPassengers
  sealed trait FlightCount

  implicit val carrier: SemanticDimension[Carrier]           = SemanticDimension.of[Carrier]("carrier")
  implicit val origin:  SemanticDimension[Origin]            = SemanticDimension.of[Origin]("origin")
  implicit val pax:     SemanticMeasure[TotalPassengers]     = SemanticMeasure.of[TotalPassengers]("total_passengers")
  implicit val count:   SemanticMeasure[FlightCount]         = SemanticMeasure.of[FlightCount]("flight_count")
}
import Flights._

// Typed query — wrong ref types are caught at compile time:
val st = toSemanticTable(flightsDf, name = Some("flights"))
val rows = st.groupByDimensions(carrier)
              .aggregateMeasures(pax, count)
              .orderBy(SortKey.desc(pax))
              .limit(10)
              .execute(spark)

// Typed predicate (operator kind is in the method name, not a runtime string):
val highPax = st.where(Predicate.Gt(pax, 600)).execute(spark)

// Typed measure declaration (v0.1.1) — name read from the SemanticMeasure witness:
import org.apache.spark.sql.functions.row_number
import org.apache.spark.sql.expressions.Window
val enriched = st.withMeasures(pax, t => row_number().over(Window.partitionBy(t("carrier")).orderBy(t("total_passengers").desc)))

// Compile-time guarantees:
//   groupByDimensions(pax)          // COMPILE ERROR — pax is a Measure, not a Dimension
//   aggregateMeasures(carrier)      // COMPILE ERROR — carrier is a Dimension, not a Measure
//   Compare.Greater(pax, 600)       // COMPILE ERROR — typo; only Eq/Ne/Lt/Le/Gt/Ge compile
//   Predicate.Gt(pax, "six hundred")  // compiles — predicate.value is Any (fails at runtime, not compile time)
```

- Pure additions to the library: the string API is unchanged. Zero runtime overhead —
  `groupByDimensions`/`aggregateMeasures` and `Compare.Gt` compile to the same Spark
  `Column` expressions as the string forms.
- Arities 1–4 are fully type-checked at compile time; the `…All(refs)` overloads do a
  single runtime check for arity 5+ (rare in practice).
- The `FieldRef[T]` carrier is a value class — no allocation on the hot path.
- See [Phase E — type safety via typeclasses](docs/phase-E-plan.md) for the design rationale
  and what's still deferred (notably `ResultDecoder[T]` / typed query results).

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

### EXPLAIN — op tree, Spark plan, and semantic intent

Three flavours of plan inspection, each for a different debugging need:

```scala
model.explain()           // op tree shape (no Spark compilation)
model.explain(spark)      // Catalyst physical plan
model.explainSemantic(spark)  // WHY: where each filter routed, transitively-pulled
                              //      measures, join strategies, warnings, Spark plan
```

`explainSemantic` is the one a developer usually wants. See
[`docs/feature-roadmap.md`](docs/feature-roadmap.md) §1.5 for an example output.

## API reference

| Method | Description |
|---|---|
| `toSemanticTable(df, name?)` | Construct a semantic model from a base `DataFrame`. |
| `.withDimensions(...)` / `.withMeasures(...)` | Immutable model extension. Typed `withMeasures(measure, expr)` overload accepts a `SemanticMeasure` witness directly via subtyping (v0.1.1). |
| `.withTransforms(transforms*)` | Per-row logic (e.g. `datediff`, `case when`) applied to source data at model-load. Mirrors the YAML `transforms:` block. |
| `.withRowFilter(name, expr, description: Option[String], metadata: Map[String, String])` | Attach a pre-join row filter (Spark SQL string) declared in the model. Mirrors the YAML `filters:` block. `SparkFilterValidator` enforces pre-join column visibility (source + transforms; joined-side columns not visible) at load time. |
| `.version(v: Int)` | Set the model's version (forward-compat hint for consumers). `table.version` reads the current value (0 = unversioned). |
| `.join_one(other, on)` / `.join_many(other, on)` / `.join_cross(other)` | Joins. |
| `.where(pred)` / `.having(pred)` | Filters (auto-routed WHERE/HAVING). |
| `.groupBy(keys...).aggregate(measures...)` | Group-by + aggregate. |
| `.groupByDimensions[D1..D4](refs...)` / `.groupByDimensionsAll(refs)` | Typed group-by — ref kind (dimension) is checked at compile time (arity 5+ at runtime). |
| `.aggregateMeasures[M1..M4](refs...)` / `.aggregateMeasuresAll(refs)` | Typed aggregate — ref kind (measure) is checked at compile time (arity 5+ at runtime). |
| `Predicate.Eq/Ne/Gt/Ge/Lt/Le/in/notIn/isNull/isNotNull[F](ref, v)` | Typed predicate factories — `ref: FieldRef[F]` with `SemanticField[F]` witness. |
| `Compare.Gt(field, value)` / `Compare.Eq(field, value)` / etc. | Sealed comparison ADT — operator kind (Eq/Ne/Lt/Le/Gt/Ge) is in the type, not a string. `Compare.apply("gt", ...)` legacy factory is preserved. |
| `.atTimeGrain(dim, grain)` | Truncate a time dimension for grouping. |
| `.orderBy(keys...)` / `.limit(n)` | Terminal ordering / top-N. `SortKey.asc(ref)` / `SortKey.desc(ref)` accept typed `SemanticField` witnesses (v0.1.1). |
| `.query(measures, dimensions?, where?, having?, orderBy?, limit?, timeGrain?, timeGrains?, timeRange?)` | One-shot bundle. |
| `.toDataFrame(spark)` / `.execute(spark)` | Batch terminal (compile to `DataFrame`). |
| `.previewSchema(spark)` | Output schema (compile to `StructType`, no rows). |
| `.withHint(strategy, params*)` | Apply a Spark planner hint (e.g. `"broadcast"`, `"repartition", n`). |
| `.validate()` | Compile-free structural check; returns `ValidationResult(errors, warnings, isValid)` for CI pre-flight. |
| `.joins: Seq[JoinInfo]` | All join edges in the model (left/right keys, cardinality: one/one_to_many/many_to_many/cross). Captures join keys at construction time (no compile required). |
| `.measureKind(name): MeasureKind` | Classify a measure as `Base` / `Calc` / `Window` — useful for tooling that needs to know which measures have a known-name calc dependency chain. |
| `.sourceTable: Option[String]` | Back-reference to the originating table name (when loaded from a registered/catalog source via `YamlLoader`). |
| `.filters: Seq[SemanticFilter]` | The model's pre-join row filters in declaration order (`name`, `expr`, `description`, `metadata`). |
| `.dimensions: Map[String, Dimension]` / `.measures: Map[String, Measure]` / `.findDimension(name)` / `.findMeasure(name)` | Catalog accessors. |
| `.createOrReplaceTempView(name)` / `.createTempView(name)` / `.createOrReplaceGlobalTempView(name)` | Compile to `DataFrame` and register as a Spark temp view (session or global). All three take `(implicit spark: SparkSession)` — call from inside a `SparkSession.builder()` block. |
| `.explain()` | Print the SemanticDF op-tree summary (no Spark compile). |
| `.explain(spark)` | Run the full query and print Spark's **simple** physical plan. |
| `.explainExtended(spark)` | Run the full query and print Spark's **extended/cost** plan (incl. logical-plan sections). |
| `.explainSemantic(spark?)` / `.explainSemantic(spark?, Scope)` | Multi-section human-readable plan: filter routing, transitive deps, join strategies, warnings. | |

`Dimension.time(...)` / `Dimension.entity(...)` are ergonomic factories. `Predicate._`
brings the filter DSL into scope. `SortKey.desc(...)` / bare `String` (ascending) drive
`orderBy`. `CalcHelpers.safeDivide(num, denom, defaultValue)` guards zero/missing denominators
with an explicit default (Spark `/` returns null on div-by-zero — correct SQL semantics;
use `safeDivide` only when null is undesirable).

## Runnable examples

In-library examples are in `src/main/scala/io/semanticdf/examples/`.
Compile once with `mvn compile`, then run any example:

```bash
mvn compile -q

mvn scala:run -DmainClass=io.semanticdf.examples.FlightsBasic
mvn scala:run -DmainClass=io.semanticdf.examples.FlightsPctTotals
mvn scala:run -DmainClass=io.semanticdf.examples.OrdersJoinMany
mvn scala:run -DmainClass=io.semanticdf.examples.FiltersRouting
mvn scala:run -DmainClass=io.semanticdf.examples.TimeSeries
mvn scala:run -DmainClass=io.semanticdf.examples.Benchmark
```

Or submit as a Spark app:
```bash
mvn package -q
spark-submit --class io.semanticdf.examples.FlightsBasic target/semanticdf_2.13-*.jar
```

## Consumer-facing templates

The `examples/` directory holds **consumer templates** — standalone Maven sub-projects
that show how to *use* SemanticDF in your own codebase. Each is a runnable, copy-pasteable
project. They depend on SemanticDF from your local `~/.m2` (run `mvn install` on the
parent first).

| Template | What it teaches |
|---|---|
| [`examples/starter`](examples/starter/README.md) | 7 queries (group-by, pct-of-total, joins, time-grain, filter, top-N window, MoM window) — the canonical "hello world" |
| [`examples/pipeline`](examples/pipeline/README.md) | Full BI lifecycle — raw CSV → ETL → cleaned parquet → declarative YAML queries |
| [`examples/window-analytics`](examples/window-analytics/README.md) | Window functions: top-N per group, period-over-period, running totals |
| [`examples/customer-analytics`](examples/customer-analytics/README.md) | RFM segmentation + cohort activity (calc-of-calc composition) |
| [`examples/operations-analytics`](examples/operations-analytics/README.md) | Order fulfillment time, on-time rate, anomaly detection (z-score) |
| [`examples/telco-analytics`](examples/telco-analytics/README.md) | Telco: monthly ARPU per plan, promotion effectiveness, roaming revenue |
| [`examples/hospital`](examples/hospital/README.md) | Hospital: data cleansing workflow (dedup, normalize, fill), ALOS, 30-day readmission rate |

Run any of them:

```bash
cd examples/window-analytics   # or any other template
mvn scala:run -DmainClass=com.example.windowanalytics.Main
```

## CLI Tools

Two tools live in `src/main/scala/io/semanticdf/tools/`, both runnable via `mvn exec:java`:

### docsgen — YAML model → browsable HTML

```bash
mvn exec:java \
  -Dexec.mainClass=io.semanticdf.tools.Main \
  -Dexec.args="docsgen --path examples/starter/models/ --out docs/index.html"
# Open docs/index.html in a browser
```

Reads one YAML file or a directory of `.yml` files and emits a self-contained HTML page (sidebar nav, per-model cards, dimension/measure/join tables, time/entity/pii badges). No Spark needed; no external dependencies.

### introspect — DataFrame → YAML model starter

```bash
mvn exec:java \
  -Dexec.mainClass=io.semanticdf.tools.Main \
  -Dexec.args="introspect --path examples/starter/data/flights.csv --format csv --model flights"
# Writes a starter YAML to stdout (or --out models/flights.yml to write to a file).
```

Reads a data file via Spark, infers dimensions (StringType → dim, NumericType → sum/avg, TimestampType → time dimension with `is_time_dimension: true`), and emits a starter YAML model. Edit the output to refine types, add descriptions, and customise expressions.

**Note — JDK 17 + Spark needs `--add-opens` flags** for any command that touches Spark (which includes `introspect`). Without them, the JVM crashes with `sun.nio.ch.DirectBuffer` access errors. Either set `MAVEN_OPTS` to the full flag set (see [`docs/runtime-quickstart.md`](docs/runtime-quickstart.md#traps) trap #1) or, for project-local reproducibility, drop a `.mvn/jvm.config` with one flag per line. `docsgen` does not need Spark, so it works without the flags.

### okfgen — YAML models → agent knowledge catalog (sidecar markdown)

```bash
mvn exec:java \
  -Dexec.mainClass=io.semanticdf.tools.Main \
  -Dexec.args="okfgen --path examples/starter/models/ --out docs/agents/reference/starter/"
# Writes one Markdown concept doc per model under the --out directory.
```

Generates per-model **sidecar Markdown** (OKF — the agent knowledge format) for an
external agent catalog. Each `models/foo.yml` becomes `agents/reference/<project>/foo.md`,
with one-line dimensions/measures/joins/filters plus a row-by-row examples reference.
The YAML stays the engine source of truth — OKF is a publishing layer, not a schema
replacement. See [`docs/agents/okf-mapping.md`](docs/agents/okf-mapping.md) for the
mapping rules and output format. `make okfgen-check` is the CI drift check — it re-runs
okfgen to a tempdir and `diff -ru`'s the result against the committed bundle.

## MCP server (`semanticdf-mcp`)

The `semanticdf-mcp/` sibling module is a Model Context Protocol server that
exposes semanticdf to any MCP-compatible client (Claude Desktop, Cursor, Continue)
over **stdio**. All five tools from [`docs/agents/mcp-contract.md`](docs/agents/mcp-contract.md)
ship in v0.1:

| Tool | Purpose |
|---|---|
| `list_models`    | Reports loaded models (name + description) |
| `describe_model` | Full schema (dimensions, measures, joins, filters, version) + optional OKF sidecar |
| `query`          | Runs a query, returns rows + columns |
| `explain`        | Same request shape, no execution — emits the semantic plan |
| `introspect`     | Auto-generate starter YAML from a DataFrame |

### Run the server

```bash
mvn install -DskipTests                                # install parent library to local ~/.m2
cd semanticdf-mcp && mvn package
mvn exec:java -Dexec.mainClass=io.semanticdf.mcp.Main \
  -Dexec.args="--models ../examples/starter/models/ \
               --data ../examples/starter/data-config.yaml \
               --okf-bundle /tmp/okf/"
```

All three flags are required:

| Flag | What |
|---|---|
| `--models <dir>`     | directory of `*.yml` model files |
| `--data <file>`      | data-config YAML (`data:` block per the contract) |
| `--okf-bundle <dir>` | where OkfGen writes the OKF markdown; server reads it into memory at startup |

### Wire up a client (Claude Desktop example)

```json
{
  "mcpServers": {
    "semanticdf": {
      "command": "java",
      "args": [
        "-jar",
        "/path/to/semanticdf-mcp/target/semanticdf-mcp_2.13-0.1.1.jar",
        "--models",
        "/path/to/your/models",
        "--data",
        "/path/to/your/data-config.yaml",
        "--okf-bundle",
        "/tmp/okf/"
      ]
    }
  }
}
```

The server source lives in [`semanticdf-mcp/`](semanticdf-mcp/README.md). See
[`docs/agents/mcp-contract.md`](docs/agents/mcp-contract.md) v2 for the
request/response schema of every tool.

## Cross-version compatibility

Verified green on all three lines (329 library tests + 72 MCP tests on each — `main` head as of PR `#62`):

| Spark | Scala | Status |
|---|---|---|
| 3.5.8 (default) | 2.13.14 | ✅ |
| 4.0.0 (`-Pspark4`) | 2.13.14 | ✅ |
| 4.1.1 (latest, profile default) | 2.13.14 | ✅ |

No code shims are needed — the codebase uses only Spark APIs stable across 3.5→4.x.

## Design & decisions

- **[`DESIGN.md`](DESIGN.md)** — architecture of record: op tree, scopes, calc compilation,
  joins, percent-of-total, filters, time semantics, invariants.
- **[`docs/runtime-quickstart.md`](docs/runtime-quickstart.md)** — JDK/Scala/Spark/Maven
  matrix, build & test commands, CLI tools, the four runtime traps (Java-17
  module flags, `scala:run` arg leak, deprecated import, version files).
- **[`docs/known-limitations.md`](docs/known-limitations.md)** — what doesn't work in v0.1 (streaming, metastore registration, multi-hop joins, etc.). Read before first consumer.
- **[`docs/calc-author-guide.md`](docs/calc-author-guide.md)** — how to write correct calc measures: ratio, pct-of-total, calc-of-calc, `safeDivide`.
- **[`docs/first-consumer-plan.md`](docs/first-consumer-plan.md)** — 3-week structured soak test plan with criteria for go/no-go.
- **[`docs/feature-roadmap.md`](docs/feature-roadmap.md)** — T1-T4 prioritized list of features and performance improvements. T1 ships next; T2-T4 gated on real consumer demand.
- **[`docs/adr/`](docs/adr/)** — recorded decisions:
  - [0001](docs/adr/0001-adopt-karpathy-guidelines-not-app-design.md) — karpathy guidelines adopted (think-before-coding, simplicity-first, surgical changes, goal-driven execution); app-design plugin/portal rejected.
  - [0002](docs/adr/0002-streaming-batch-first-streaming-shaped.md) — batch-first, streaming-shaped (DSL/source-agnostic op tree; batch terminal now, streaming terminal deferred).
  - [0003](docs/adr/0003-re-sequence-calc-proof-first.md) — re-sequence to prove name-based calc compilation early.
