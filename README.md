<p align="center"><img width="256" height="256" alt="ChatGPT Image Jul 21, 2026, 02_57_52 AM" src="https://github.com/user-attachments/assets/77955306-216d-4357-90b2-77e315fa9f1a" /></p>

# SemanticDF

A **semantic layer for Apache Spark** (JVM/Scala), adapted from the
[Boring Semantic Layer](https://github.com/boringdata/boring-semantic-layer) (Python/Ibis).

A `SemanticTable` is a deferred, source-agnostic definition that compiles to a Spark
`DataFrame` at a batch terminal (`.toDataFrame(spark)` / `.execute(spark)`) or a
`StreamingQuery` at the streaming terminal (`.toStreamingQuery(spark, opts)`). It is *not*
a `DataFrame` itself — it captures *what* you want (dimensions, measures, joins, filters,
grains) so the engine can decide *how* to compute it. The same definition serves both
batch and streaming sources; only the terminal differs.

## What problems SemanticDF solves

Modern data teams have a **plumbing problem**: every dashboard, notebook, or AI agent
needs the same business metrics defined somewhere — usually duplicated across queries,
spreadsheets, and tribal lore. When the metric changes (`"total_passengers"` now means
deplanements not boarded-then-deplaned), every copy drifts. SemanticDF puts the metric
definitions in **one checked-in source** — a small Scala DSL or a YAML model — and
gives every consumer (your code, your tests, your LLM agent) the same compile-time
guarantee that they're asking for the right thing.

## What you can do with it

- **Define a metric once, query it everywhere.** A `SemanticTable` is an immutable
  description. Use it from `flights.query(...)` in code, from a YAML model in
  `models/flights.yml`, or from an MCP agent that calls `query` / `describe_model`
  over JSON.
- **Calc + percent-of-total measures with no expression-tree surgery.** A measure that
  references other measures (`t.total / t.flight_count`) resolves by name against the
  aggregated DataFrame; a percent-of-total (`t.total / t.all(t.total)`) cross-joins a
  broadcasted totals row.
- **Compile-time typo safety on the query side.** The optional typeclass layer
  (`SemanticField[T]` phantom types) catches dimension-vs-measure confusion at the
  call site rather than at first execution. `ResultDecoder.derive[T]` does the same
  for the *result* side of a query.
- **One model across batch and streaming.** The op tree is source-agnostic;
  only the execution terminal differs (`.toDataFrame(...)` for batch,
  `.toStreamingQuery(...)` for Structured Streaming).
- **A models → agents bridge.** `okfgen` produces OKF markdown an LLM can read;
  the MCP server exposes the tools (`list_models`, `describe_model`, `query`,
  `introspect`) over stdio or REST.

## When (and when not) to use it

- **Good fit:** small-to-mid data teams with a stable set of business metrics,
  already on Spark 3.5+ (or 4.x), who want one definition everyone shares — including
  LLM agents.
- **Not yet:** stream-stream joins (only static-stream joins are supported today —
  `join_one(batchTable, streamingModel, ...)`); heavy-numeric ML workloads without
  rollup needs; sub-second interactive dashboards where another tool's tighter
  latency matters more than metric consistency.

## Where to read next

- **[`docs/getting-started.md`](docs/getting-started.md) — 5-minute paste-and-run setup (Maven + SparkSession + first query)**
- [`docs/guide.md`](docs/guide.md) — narrative walkthrough: how SemanticDF works, in plain English
- [`DESIGN.md`](DESIGN.md) — architecture of record (decisions, the hard problems)
- [`docs/DOCS_MAP.md`](docs/DOCS_MAP.md) — wayfinding guide: which doc to read for which question
- [`docs/GLOSSARY.md`](docs/GLOSSARY.md) — terms-of-art (op tree, BaseScope, MeasureScope, expression-tree surgery, …)
- [`docs/adr/`](docs/adr/) — recorded decisions
- [`RELEASE.md`](RELEASE.md) — version-by-version changelog
- [`docs/known-limitations.md`](docs/known-limitations.md) — current scope & guardrails (what's in, what's deferred, with workarounds)
- [`examples/`](examples/) — runnable end-to-end examples

## Build

Requires **JDK 17** and **Maven 3.9+**. Spark is on the classpath as `provided` (it comes
from your cluster/runtime).

```bash
mvn test                      # Spark 3.5.8 (default)
mvn -Pspark4 test             # Spark 4.1.1 (latest stable)
```

## Quick start

Add the Maven dep and paste this into your project:

```scala
import io.semanticdf._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{count, lit, sum}

implicit val spark = SparkSession.builder().master("local[2]").getOrCreate()
import spark.implicits._

val flights = Seq(
  ("AA", 100, 5), ("UA",  80, 3), ("DL", 150, 6),
).toDF("carrier", "distance", "passengers")

val flightsModel = toSemanticTable(flights, name = Some("flights"))
  .withDimensions(Dimension("carrier", t => t("carrier")))
  .withMeasures(
    Measure("flight_count",     t => count(lit(1))),
    Measure("total_passengers",  t => sum(t("passengers"))),
    Measure("avg_passengers",    t => t("total_passengers") / t("flight_count")),
  )

flightsModel.groupBy("carrier").aggregate("flight_count", "avg_passengers").execute.show
```

> For a full walkthrough (prerequisites, Maven coordinates, troubleshooting) see **[`docs/getting-started.md`](docs/getting-started.md)**.

Once this runs, continue with [`docs/guide.md`](docs/guide.md) for the narrative walkthrough that explains how the compilation works under the hood.

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
        "/path/to/semanticdf-mcp/target/semanticdf-mcp_2.13-0.1.6.jar",
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

Per-row logic — `datediff(...)`, `case when ...`, window functions — is
applied to the source DataFrame at model-load time via the YAML `transforms:`
block or Scala `withTransforms(...)`. Transformed columns become part of
the source DataFrame and are visible to subsequent filters and measures.
Order matters (no automatic topological sort).

For a worked example with the YAML + Scala equivalents, the model-load
lifecycle, and what fields downstream measures see, see
[`docs/guide.md` → Transforms](docs/guide.md#dimensions-measures-and-transforms).

### Filters (pre-join row-level hygiene, applied at model-load)

Row-level **hygiene** — drop rows missing a required field, drop cancelled
orders, dedup before aggregation — doesn't fit a query, because it
governs which rows the model *contains*, not which rows a particular
query returns. Declare it on the model via `filters:` (YAML) or
`withRowFilter(...)` (Scala DSL). Filters run **pre-agg, pre-join**,
against this model's source table only.

```yaml
flights:
  table: flights_csv
  filters:
    require_origin_and_carrier:
      expr: "origin IS NOT NULL AND carrier IS NOT NULL"
      description: "Drop rows with null origin or carrier."
  dimensions:
    carrier: carrier
```

`SparkFilterValidator` enforces pre-join visibility at load time — a
filter referencing a joined-side column is rejected. For the Scala DSL
form, the visibility rules, and a worked example, see
[`docs/guide.md` → Filters](docs/guide.md#filters-pre-join-hygiene-public-where-for-query-time).

Also see [`docs/calc-author-guide.md`](docs/calc-author-guide.md) for the
detailed validator rules.

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

For notebook / SQL-first consumers, a compiled `SemanticTable` can be registered
as a Spark temp view and queried with plain SQL via
`.createOrReplaceTempView("flights_view")` followed by `spark.sql(...)`. The view is the
*compiled output* of the model — joins, pre-join filters, and pre-aggregation
all happen *before* the SQL queries the view.

See [`docs/guide.md` → Notebook escape hatch](docs/guide.md#notebook-escape-hatch--raw-sql-via-a-temp-view)
for the worked example, scoping rules, and a multi-cell notebook workflow.

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
- See [the typeclass-design rationale](docs/phase-E-plan.md) for the design rationale
  and what's still deferred (the typed-arithmetic DSL (planned) — see
  [`docs/phase-E-plan.md`](docs/phase-E-plan.md) §E3). The `ResultDecoder[T]`
  typeclass (including macro derivation for case classes via `ResultDecoder.derive[T]`)
  and the `queryAs[T]: Dataset[T]` terminal are shipped.

### Typed query results — `ResultDecoder[T]`

The same compile-time guarantee applies to the **output** side. `SemanticTable.collectAs[T]`
returns a `Seq[T]` rather than untyped `Seq[Row]`, plumbed through a small
typeclass:

```scala
// Built-in primitive decoders read column 0 of each row:
val names: Seq[String] = table.collectAs[String](spark)
val counts: Seq[Long]  = table.collectAs[Long](spark)

// Case-class decoders — derive[T] generates the instance at compile time:
case class CarrierCount(carrier: String, count: Long)
implicit val decoder: ResultDecoder[CarrierCount] = ResultDecoder.derive[CarrierCount]
val typed: Seq[CarrierCount] = table.collectAs[CarrierCount](spark)
```

The macro (`ResultDecoder.derive[T]`) is a Scala 2 blackbox macro that inspects the
case class's primary constructor and emits one `row.getX(i)` call per field.
Supported field types: `String`, `Int`, `Long`, `Double`, `Float`, `Boolean`,
`Short`, `Byte`, `java.math.BigDecimal`. Unsupported field types
(`java.time.Instant`, sealed traits, `Option[T]`, nested case classes, ...)
produce a **compile-time error** pointing at the offending constructor parameter,
so the user can either rename, restructure, or supply a manual
`ResultDecoder[T]` instance via `implicit val`.

For richer shapes that the macro doesn't support, the manual form is just as
concise as a one-line `val`:
```scala
implicit val decoder: ResultDecoder[Foo] = new ResultDecoder[Foo] {
  def decode(row: Row): Foo = Foo(row.getString(0), foo.bar(row))
}
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

### EXPLAIN — op tree, Spark plan, and semantic intent

Three flavours of plan inspection, each for a different debugging need:

```scala
model.explain()                // op tree shape (no Spark compilation)
model.explain(spark)           // Catalyst physical plan
model.explainSemantic(spark)   // WHY: filter routing, pulled measures, etc.
```

`explainSemantic` is the one a developer usually wants. See
[`docs/guide.md` → How a query compiles](docs/guide.md#how-a-query-compiles)
for the worked example with sample output.

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
| `.queryAs[T](measures, dimensions?, where?, having?, orderBy?, limit?, timeGrain?, timeGrains?, timeRange?)(implicit spark, decoder: ResultDecoder[T], encoder: Encoder[T]): Dataset[T]` | Typed one-shot bundle. Same shape as `.query` but returns a `Dataset[T]`, decoding rows into a case class via the implicit `ResultDecoder[T]` (use `ResultDecoder.derive[T]` for the case-class witness) and `Encoder[T]` (use `import spark.implicits._`). Compile-time type-safety on result field names and types. |
| `Measure.typed[T](name: String, expr: TypedSemanticScope => TypedColumn[T]): Measure` | Typed measure factory. Same shape as the `Measure` case class but the lambda's return type is type-checked at compile time via the phantom `T`. Compose with `TypedArithmetic.{divide, plus, minus, multiply}` for type-checked arithmetic. The typed form lowers to a plain `Measure` at runtime — works with `withMeasures(...)` as usual. Zero runtime overhead, no memory leak. |
| `TypedArithmetic.{divide, plus, minus, multiply}[T, U, R](a: Column, b: Column)(implicit nt: Numeric[T], nu: Numeric[U], nr: Numeric[R]): TypedColumn[R]` | Typed arithmetic ops for measure lambdas. The compiler requires `Numeric[T]`, `Numeric[U]`, `Numeric[R]` to be in implicit scope — `String` and other non-numeric types fail at compile time. Returns a `TypedColumn[R]` (value class wrapping `Column`); implicit conversion to `Column` makes the typed form drop-in compatible with the untyped `SemanticScope => Column` lambda. The function body is just the corresponding Spark `Column` op — type parameters are erased. |
| `.toDataFrame(spark)` / `.execute(spark)` | Batch terminal (compile to `DataFrame`). With `implicit val spark: SparkSession` in scope, both can be called without the argument (`.toDataFrame` / `.execute`). |
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

## Cross-version compatibility

Verified green on both Spark lines (442 library + 90 MCP + 18 CLI = 550 total,
on each):

| Spark | Scala | Status |
|---|---|---|
| 3.5.8 (default) | 2.13.18 | ✅ |
| 4.1.1 (`-Pspark4`) | 2.13.18 | ✅ |

No code shims are needed — the codebase uses only Spark APIs stable across 3.5→4.x.

## Design & decisions

- **[`DESIGN.md`](DESIGN.md)** — architecture of record: op tree, scopes, calc compilation,
  joins, percent-of-total, filters, time semantics, invariants.
- **[`docs/runtime-quickstart.md`](docs/runtime-quickstart.md)** — JDK/Scala/Spark/Maven
  matrix, build & test commands, CLI tools, the four runtime traps (Java-17
  module flags, `scala:run` arg leak, deprecated import, version files).
- **[`docs/known-limitations.md`](docs/known-limitations.md)** — current scope & guardrails (batch-only, per-session security, symmetric join keys, etc.) with workarounds and roadmap hints. Read before first consumer.
- **[`docs/calc-author-guide.md`](docs/calc-author-guide.md)** — how to write correct calc measures: ratio, pct-of-total, calc-of-calc, `safeDivide`.
- **[`docs/first-consumer-plan.md`](docs/first-consumer-plan.md)** — 3-week structured soak test plan with criteria for go/no-go.
- **[`docs/feature-roadmap.md`](docs/feature-roadmap.md)** — T1-T4 prioritized list of features and performance improvements. T1 ships next; T2-T4 gated on real consumer demand.
- **[`docs/adr/`](docs/adr/)** — recorded decisions:
  - [0001](docs/adr/0001-adopt-karpathy-guidelines-not-app-design.md) — karpathy guidelines adopted (think-before-coding, simplicity-first, surgical changes, goal-driven execution); app-design plugin/portal rejected.
  - [0002](docs/adr/0002-streaming-batch-first-streaming-shaped.md) — batch-first, streaming-shaped (DSL/source-agnostic op tree; batch and streaming terminals share the model definition).
  - [0003](docs/adr/0003-re-sequence-calc-proof-first.md) — re-sequence to prove name-based calc compilation early.
