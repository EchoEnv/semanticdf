# SemanticDF — Design & Porting Plan

**Goal:** Adapt the [Boring Semantic Layer (BSL)](https://github.com/boringdata/boring-semantic-layer) — a Python/Ibis semantic layer — into **semanticdf**, a semantic layer framework for **Apache Spark (JVM)** written in **Scala**.

This document records: what BSL is, the architectural mapping to Spark/Scala, the hard problems and how we solve them on the JVM, the proposed DSL, module layout, build strategy, and a phased delivery plan.

> **Execution scope (batch now, streaming-shaped).** v0.1 targets **batch** `DataFrame`s. The DSL and op tree are designed **source-agnostic** so that **Structured Streaming shares the same model definition** — batch and streaming diverge only at the *execution terminal* (§4.5), mirroring Spark's own `df.write` vs `df.writeStream`. Streaming *execution* is deferred (ADR 0002); the *interface unification* is built in from the start, so enabling streaming later adds a terminal, not a second API.

---

## 1. What BSL is (source architecture)

BSL is a **lazy semantic algebra layered on top of Ibis**. Ibis is a Python dataframe library with a backend-agnostic logical plan (an *op tree* of `Relation` nodes) that compiles to SQL for 20+ backends.

The load-bearing concepts in BSL:

| Concept | Where | Role |
|---|---|---|
| `SemanticModel` / `SemanticTable` | `expr.py` | Immutable facade wrapping a base table + **dimensions** + **measures** + **calc_measures**. It is *itself* an Ibis `Table` (subclass of `ir.Table`). |
| **Op tree** | `ops.py` (`SemanticTableOp`, `SemanticJoinOp`, `SemanticFilterOp`, `SemanticGroupByOp`, `SemanticAggregateOp`, `SemanticOrderByOp`, `SemanticLimitOp`, `SemanticUnnestOp`, `SemanticIndexOp`) | Each operation builds an immutable node; nothing runs until `.execute()`. |
| `to_untagged()` | each op | Compiles the semantic op tree down to a plain Ibis `Table` (→ SQL). |
| **Dimension / Measure** | `ops.py` | Frozen records holding `expr: Table => Column` plus metadata. |
| **Calc measures** | `calc_analyzer.py`, `calc_compiler.py` | Measures that reference *other measures* and `t.all(...)` (percent-of-total). Classified by *analyzing the expression tree*, not by a curated AST (ADR 0001). |
| `MeasureScope` / `ColumnScope` | `measure_scope.py` | The proxy passed to measure lambdas; resolves `t.measureName` to a **virtual aggregated column**. |
| `query()` + `Filter` | `query.py`, `predicate.py` | Parameter-based query API (dimensions/measures/filters/order_by/limit/time_grain/time_range/having) + a JSON filter AST that compiles to Ibis. Auto-splits filters into pre-agg (WHERE) vs post-agg (HAVING). |
| **Join cardinality** | `SemanticJoinOp` | `join_one` vs `join_many`. `join_many` **pre-aggregates the "many" side to the join-key grain** before joining — this is what prevents fan-out / double counting in multi-fact stars. |
| YAML config | `yaml.py` | Declarative model definitions (`from_yaml`/`from_config`). |
| Dependency graph | `graph_utils.py` | Introspection: which fields depend on which. |
| Serialization (tagged) | `serialization/` | xorq integration — cross-backend query caching. |
| Agents / MCP / charts | `agents/`, `chart/` | LLM tool server + visualization backends. |

### The two intellectual cores

1. **Calc-measure compilation (the hard one).** A calc measure lambda like `t => t.total / t.all(t.total) * 100` is evaluated against a `MeasureScope` that resolves measure names to columns of a *synthetic "virtual aggregated table"*. The compiler then:
   - classifies the resulting expression (`pushable`? `references_AllOf`? `has_window`? `post_agg_only`?),
   - lifts inline reductions out of the calc into the base aggregation,
   - substitutes the virtual table with the real aggregated table,
   - handles `t.all(...)` by cross-joining a **no-group-by totals subquery** (so non-sum measures — mean/median — get correct overall values, not a windowed sum of per-group values),
   - topologically orders calc-of-calc chains.

2. **Correct multi-fact joins.** When you `join_many` two fact tables at different grains, BSL pre-aggregates each side to the join key before joining, guaranteeing no double counting. This is *explicit* (not left to the SQL engine) precisely because correctness matters more than letting the planner figure it out.

Everything else is composition, ergonomics, and integration surface.

---

## 2. Architectural mapping: Python/Ibis → Scala/Spark

| BSL (Python/Ibis) | SemanticDF (Scala/Spark) | Notes |
|---|---|---|
| `ibis.Table` | `org.apache.spark.sql.DataFrame` | The runtime relation. |
| `ibis.Column` / `Deferred` (`_.col`) | `org.apache.spark.sql.Column` | Spark Columns are **already lazy and table-free**, so we *don't need* a `Deferred` concept — a measure is just `Scope => Column`. This **simplifies** BSL. |
| `ibis.expr.operations.Relation` (op-tree base) | Our own `sealed trait SemanticOp` (case classes) | We do **not** subclass Catalyst `LogicalPlan`. See §4.1. |
| `SemanticTable` *is-an* `ir.Table` | `final class SemanticTable(val op: SemanticOp)` | Scala `DataFrame` is final; we compose, not inherit. Faithful in *behavior*, not in type. |
| `attrs @frozen` dataclasses | Scala `case class`es | Immutable, structural equality, pattern-matchable — ideal fit. |
| `with_dimensions(**kw)` | `withDimensions(d1, d2, ...)` varargs of `Dimension` case classes, or `"name" -> expr` pairs | No Python kwargs in Scala; see DSL §5. |
| `lambda t: t.distance.sum()` | `t => sum(t("distance"))` | `t` is a `SemanticScope`. |
| `t.all(measure)` | `scope.all("measure")` | See §6.2. |
| `returns.result` (`Success`/`safe`) | `scala.util.Try` / `Either` / `Option` | Idiomatic JVM error handling. |
| `toolz.curry` / `functools` | plain Scala methods / curried `def f(a)(b)` | — |
| op `to_untagged()` | `op.compile(spark): DataFrame` | Each node compiles to a DataFrame. |
| `group_by().aggregate()` | `.groupBy(cols: _*).agg(exprs: _*)` | Direct Spark mapping. |
| Catalyst `ColumnPruning` | **free** | We get projection pushdown from Spark's optimizer — no need to reimplement BSL's. |
| xorq tagged serialization | **dropped** | Spark has its own plan caching/AQE; no analog. |
| MCP / agents / charts | **deferred** (separate modules, later phases) | — |

---

## 3. What we port, what we drop, what we defer

### Port (core) — v0.1 contract
- `SemanticModel` + `Dimension`/`Measure`/`CalcMeasure` records
- Immutable op tree + `compile()`
- `with_dimensions` / `with_measures`
- `group_by` / `aggregate` / `filter` / `order_by` / `limit`
- **Calc measures + percent-of-total** (`t.all("name")`) — the flagship feature
- **`join_one` / `join_many` / `join_cross`** with grain-correct pre-aggregation
- `query()` parameter API + `Filter` AST + pre/post-agg split (WHERE vs HAVING) + `having`
- Time/entity dimensions, derived time parts, time-grain validation

### Drop (no Spark analog / not worth it)
- xorq vendored-ibis bridging (`_xorq.py`, `_ensure_xorq_table`, `_rebind_to_*`) — Spark is the single engine.
- Tagged serialization (`serialization/*`) — Spark plan + caching replaces it.
- The `_RenamedResolver` / ibis-specific ambiguity workarounds (Spark handles join column disambiguation differently).
- Malloy interop tests.
- `profile.py` connection profiles — replaced by Spark `SparkSession` + catalog/table resolution (not deferred; *replaced*).

### Defer — behind explicit revive-triggers (per ADR 0001)
Nothing here is scheduled. Each item lists the concrete signal that pulls it in.

| Deferred | Revive trigger |
|---|---|
| `compare_periods` | A model needs current-vs-previous-period deltas |
| `index()` (dimensional indexing) | A consumer wants a searchable value catalog |
| `unnest()` | A model has array-typed dimensions to flatten |
| Dependency-graph introspection (`get_graph`) | A consumer wants field-level lineage |
| `t.all(expr: => Column)` inline-reduction totals | A real measure needs totals of an inline reduction |
| Sum-special-case windowed totals fast path | A benchmark shows cross-join totals is too slow |
| YAML config loader + expr mini-DSL (§6.4) | A user wants declarative config over the Scala API |
| Multi-package layout (`expr/ ops/ …`) | A flat `io.semanticdf` package grows a 3+ file cluster |
| Second DSL style (tuple-builder) / macro DSL | Record-varargs proves insufficient in practice |
| Cross-build Scala 2.12 | A consumer is pinned to Spark < 4 (Spark 4 is 2.13-only) |
| MiMa binary compatibility | There is a downstream user to not break |
| Maven Central publishing | There is a consumer to publish to |
| MCP / HTTP tool server, chart JSON emitter | A consumer needs LLM-tool or viz integration |
| LangGraph/LLM agent backend | JVM agent demand; design a clean tool interface (no LangGraph on JVM) |

---

## 4. Architecture of semanticdf

### 4.1 Key decision: own op tree, sized to its requirement

Three options were considered (per ADR 0001 — surface the simpler alternative before committing, then size to the requirement):

- **(A) Minimal op tree (chosen).** A `sealed trait SemanticOp` with case-class nodes; each implements `compile(spark): DataFrame`. `SemanticTable` is a thin immutable facade over the op-tree root.
- **(B) No op tree — direct builders.** Methods build and return `DataFrame`s eagerly. Simplest, but loses the three things a semantic layer actually needs: (i) **deferred compilation** — a `SemanticTable` must serve many `groupBy/aggregate` queries off one definition; (ii) **introspection / `asTable()`** that re-derives field lineage without re-running the user's lambdas; and (iii) **join re-planning** — `join_many` must intercept an aggregate to pre-aggregate the "many" side to the join grain *before* joining. That interception needs a tree, not a stream of DataFrames.
- **(C) Subclass Catalyst `LogicalPlan`/`UnaryNode`.** Rejected — Catalyst internals are unstable across Spark versions (directly relevant once the `-Pspark4` leg is real), access-restricted, and would couple us to Spark's optimizer invariants.

**Why A:** B's three requirements are real (each maps to a v0.1 feature: query reuse, introspection, grain-correct joins), so an op tree earns its keep. We keep it *minimal* — only the node types those features require (`table`, `join`, `filter`, `groupBy`, `aggregate`, `orderBy`, `limit`); no `mutate`/`unnest`/`index` nodes until their features leave the deferred table above. It mirrors BSL's proven shape at lower risk, is Spark-version-resilient (important for the Spark 4 path), and Scala pattern matching makes the tree elegant. The cost — we don't inherit arbitrary DataFrame methods on `SemanticTable` — is acceptable: BSL itself *blocks* most raw-ibis methods deliberately, and we expose a `toDataFrame()` escape hatch.

**Source-agnostic by design.** `compile(spark)` returns a `DataFrame` whose flavor (batch or streaming) comes from the **leaf source** the user supplied — nodes never branch on mode. This is what lets the same DSL serve Structured Streaming: construction is identical; only the *terminal* (§4.5) differs.

### 4.2 The expression model

```
SemanticScope  (the "t" passed to measure/dimension lambdas)
  ├── apply(name: String): Column        // t("distance"), t("measure_name")
  └── all(name: String): Column          // t.all("measure")  → percent-of-total

(The `all(expr: => Column)` inline-reduction totals overload is deferred — see the trigger table in §3.)
```

Two concrete scopes:
- **`BaseScope(df)`** — `apply(name)` returns `df(name)` (a real base column) or a registered measure's column. Used for plain base measures.
- **`MeasureScope(df, knownMeasures)`** — used by **calc measures**. `apply(name)` resolves: base column wins on collision, else known measure (with unique-suffix matching for joined/prefixed names, e.g. `t("flight_count")` → `flights.flight_count`), else fail with a "did you mean?" suggestion. `all(...)` returns a reference to a **totals column** (`__sl_totals__<name>`) — see §6.2.

### 4.3 Package layout — start flat

Per ADR 0001: begin in a single `io.semanticdf` package. Do **not** pre-partition into `expr/ ops/ model/ scope/ calc/ query/ config/ graph/ util/` — that was speculative for zero code. Split into a sub-package only when a cluster of 3+ cohesive files justifies one.

Expected first files (flat, v0.1):

```
io.semanticdf/
├── SemanticTable.scala        // immutable facade over the op-tree root + delegated methods
├── SemanticOp.scala           // sealed trait + case-class nodes (table, join, filter, groupBy, aggregate, orderBy, limit)
├── Model.scala                // Dimension / Measure / CalcMeasure records
├── Scope.scala                // SemanticScope trait + BaseScope + MeasureScope (incl. all())
├── Calc.scala                 // analyzer (CalcExprAnalysis) + compiler (classify, applyCalcMeasures, totals)
├── Query.scala                // query() + Filter + Predicate AST + WHERE/HAVING split + time grains
└── SemanticDF.scala            // entry: toSemanticTable, entityDimension, timeDimension
```

### 4.4 Non-functional invariants (memory & perf)

State these as hard rules now — they are nearly free at design time and expensive
to retrofit (the failure modes are slow/hung jobs and driver/executor leaks,
not crashes).

1. **Internal intermediates are never `cache()`/`persist()`-ed.** Only the user may
   cache the final returned `DataFrame`. A cached pre-agg or totals intermediate pins
n   Catalyst structures + shuffle output across the model's lifetime — a genuine leak
   in a long-lived/serving session. This invariant is *mandatory* for any future
   streaming work (caching across micro-batches is catastrophic).
2. **Introspection is analysis-only.** `schema`/field-type discovery uses
   `df.schema` / `df.queryExecution.analyzed` (driver-side). **No actions**
   (`.count()`/`.first()`/`.limit().collect()`) during construction — those kick a
   distributed job at model-build time.
3. **The op tree holds no `SparkSession`, and the compiled `DataFrame` is never stored in a node.**
   Nodes are pure case classes; every `toDataFrame(spark)` recompiles against the
   passed session. This makes a model reusable across sessions (build in a notebook,
   run in a job) and prevents session retention.

### 4.5 Execution terminals — the unification portal (batch / streaming)

**One model, two terminals.** This is how semanticdf keeps a single DSL for batch and
streaming without paying streaming machinery into the v0.1 batch model:

- `.toDataFrame(spark): DataFrame` — **batch terminal** (v0.1). Requires a
  non-streaming source; returns a batch `DataFrame`.
- `.toStreamingQuery(spark, options): StreamingQuery` — **streaming terminal**
  (deferred, ADR 0002). Same op tree; validates streaming constraints at the terminal,
  then returns a `StreamingQuery`.

This mirrors Spark's own design — one `Dataset`/`DataFrame` DSL, two sinks
(`df.write` vs `df.writeStream`). The user writes the **same** `toSemanticTable(…)
  .withDimensions(…).withMeasures(…).groupBy(…).aggregate(…)` against either a
batch table or a streaming `readStream`; nothing in the construction branches on mode.

**Streaming constraints are validated late, at the streaming terminal** (not at
construction) — so they never burden the batch path:

- window-less aggregation (`groupBy(dim).aggregate(...)` with no time window) →
  rejected (needs a window + watermark; unbounded state growth is the real memory leak);
- `t.all(...)` cross-join totals → rejected or limited to a windowed totals scope
  (a stream has no whole-stream total);
- `limit`, and stream×stream `join_many` pre-aggregation → rejected unless time-bounded.

Full analysis of what ports, what's forbidden, and the trigger to implement the
streaming terminal is in **ADR 0002**. The point recorded here: the architecture is
**batch-first but streaming-shaped** — the unification is structural, not an
afterthought.

---

## 5. DSL design (Scala idioms)

Python relies on `**kwargs` for `with_dimensions(origin=…)`. Scala has no kwargs, so the API uses **named `Dimension`/`Measure` records + varargs**. (A tuple `"name" -> measure(...)` builder style and a macro DSL are both deferred — see §3 triggers.)

### 5.1 Building a model

```scala
import io.semanticdf._
import org.apache.spark.sql.functions._

val flights = toSemanticTable(flightsDf, name = "flights")
  .withDimensions(
    Dimension("origin",      t => t("origin")),
    Dimension("destination", t => t("dest")),
    Dimension("carrier",     t => t("carrier"), description = "Airline code"),
    Dimension("arr_time",    t => t("arr_time"),
              isEventTimestamp = true, isTimeDimension = true, smallestTimeGrain = "DAY"),
  )
  .withMeasures(
    Measure("flight_count",   t => count(lit(1))),
    Measure("total_distance", t => sum(t("distance"))),
    Measure("avg_distance",   t => avg(t("distance"))),
    // calc measure: references other measures by name
    Measure("avg_per_flight", t => t("total_distance") / t("flight_count")),
    // percent-of-total
    Measure("market_share",   t => t("flight_count") / t.all("flight_count") * 100.0),
  )
```

One DSL style only for v0.1 (records + varargs). `Dimension`/`Measure` live directly under `io.semanticdf` (flat layout, §4.3), so there is no separate `io.semanticdf.model._` import.

### 5.2 Querying

**Fluent (chain) API:**
```scala
flights.groupBy("origin")
  .aggregate("flight_count")
  .orderBy(desc("flight_count"))
  .limit(10)
  .execute()                                   // => DataFrame
```

**Parameter API (`query()`):**
```scala
flights.query(
  dimensions = Seq("carrier"),
  measures   = Seq("flight_count", "market_share"),
  filters    = Seq(Filter.field("distance") > 1000),   // DSL builder
  order_by   = Seq("flight_count" -> "desc"),
  limit      = Some(10),
).execute()
```

**JSON-style filters** (same AST as BSL):
```scala
Filter(Map("operator" -> "AND",
           "conditions" -> Seq(
             Map("field" -> "country", "operator" -> "=", "value" -> "US"),
             Map("field" -> "tier", "operator" -> "in", "values" -> Seq("gold","platinum")))))
```

### 5.3 Joins

```scala
val orders   = toSemanticTable(ordersDf, "orders").withMeasures(...)
val customers = toSemanticTable(custDf, "customers").withDimensions(...)

orders.join_one(customers, on = "customer_id")           // 1:1 lookup
customers.join_many(orders, on = "customer_id")          // 1:many → pre-agg to grain
flightsA.join_cross(flightsB)                            // cartesian
```

`on` accepts: a column-name `String`, a `Seq[String]` (compound equi-join), or `(l, r) => Column` (arbitrary predicate).

---

## 6. The hard problems — solved on the JVM

### 6.1 Calc measures (measure-of-measure)

**BSL approach:** evaluate the calc lambda against a `MeasureScope` whose measure lookups return columns of a *synthetic virtual table*; then substitute the virtual table with the real aggregated table via op-graph rewriting.

**SemanticDF approach — name-based (simpler & robust):**
1. **Auto-pull** the transitive closure of requested measures over the calc dependency graph (probe each lambda against a classification scope; a referenced measure name is added and re-probed, recursively). A user can request a leaf calc alone; its base/calc deps are pulled automatically.
2. **Classify** each measure: probe its lambda against the base DataFrame with all model measures as `known`. Any lambda referencing a known-measure name (rather than only base columns) is a **calc**; the rest are **base** measures. Cycles raise a clear error before any Spark work.
3. Compile the **base measures** into an aggregated DataFrame: `df.groupBy(keys).agg(baseExprs: _*)`, each aliased to its measure name.
4. Apply **calcs one `select` per topological layer** of the calc dependency DAG (invariant A1, revised below). Within a layer, every calc depends only on base measures or on calcs already applied in earlier layers, so one `select` per layer suffices. Depth-bounded, not width-bounded — a 50-wide model with depth-2 calcs is 2 selects, not 50.
5. Base-column-wins-on-collision and unique-suffix matching for joined/prefixed names are reproduced verbatim from BSL.

We avoid op-graph substitution entirely: Spark `Column` expressions are **re-runnable against any DataFrame having the named columns**, so name identity is sufficient. This is the single biggest simplification of the port.

> **Invariant A1 (revised).** Apply calc measures in **one `select` per topological layer**, never a chained `.withColumn` per calc. The original (Phase 1b) form — a single flat `select` for *all* calcs — only held for one layer of calc depth; calc-of-calc needs the second calc to see the first as a real column, which requires layered application. The depth-bounded layered form keeps the width-bounded benefit (no per-column `Project` within a layer) while making calc-of-calc correct.

### 6.2 Percent-of-total `t.all(...)`

`t.all("measure")` must resolve to the measure computed over the **whole filtered base** (no group-by), so non-sum measures (mean/median) are correct.

**SemanticDF approach** — cross-join totals (the only v0.1 path; a windowed-sum fast-path is deferred behind a benchmark trigger — see §3):

- The analyzer flags `references_AllOf`.
- The compiler builds a **totals DataFrame** = same measures aggregated with `groupBy()` (no keys) → 1 row.
- `scope.all("m")` returns `col("__sl_totals__m")`. We `crossJoin` the totals row into the per-group result with an **explicit `broadcast(...)` hint** (never rely on the auto-broadcast threshold) and guard that the totals side is exactly 1 row — a stray >1-row totals table would degrade `crossJoin` into an O(N·M) `CartesianProduct` and hang the job. The totals table is **pruned to only the measures actually referenced by some `t.all(...)`**, not every measure on the model.
- Calc-of-calc that references totals of another calc is handled by evaluating the referenced calc's lambda against a `_TotalsResolvingScope` (topologically ordered) — same as BSL's `attach_calc_totals`.

Because our scope is name-based, "rewriting" the calc expression is trivial — `all(name)` just returns the prefixed totals column. No Catalyst expression surgery.

### 6.3 Join fan-out / multi-fact correctness

`join_many` (and `join_one` auto-upgraded on grain mismatch) triggers, at `SemanticJoinOp.compile` time:

1. Evaluate the join predicate `(l, r) => Column` against `JoinSide` proxies that capture the column names accessed on each side. These captured names are the **join keys** (grain).
2. For each side, **probe** every dimension and measure from the merged model against that side's DataFrame. Only the ones that resolve (column exists in the DF) are included — this handles the case where a dimension (e.g. `carrier`) belongs to the left side only.
3. **Pre-aggregate** each side at the join-key grain: `groupBy(allResolvableDims).agg(allResolvableMeasures)`. ALL of a side's dimensions are included in the groupBy (not just the join keys), preserving non-key dimensions through the pre-agg. This is safe when non-key dimensions are functionally dependent on the grain (the common star-schema case: one `order_id` → one `carrier`, one `customer_id`).
4. Join the pre-aggregated sides with a left-outer equi-join. When both sides use the same key name (e.g. `customer_id`), Spark keeps both columns — we dedup by selecting with **qualified column references** (`leftDf("customer_id")` vs `rightDf("name")`), which carry source-DF scoping and resolve unambiguously.
5. In `SemanticAggregateOp.compile`, base measures that are already pre-aggregated columns are **re-aggregated with `sum(col(name))`** rather than re-evaluating their original source-column expressions (which would fail because source columns like `qty` are gone post-join).
6. `ClassificationScope` checks **known measures first** (before columns), so a calc that references a measure which is also a pre-aggregated column is correctly classified as a calc, not a base measure.

This is the explicit correctness guarantee BSL makes; we keep it rather than trusting Catalyst, because correctness in multi-fact stars is the whole point of a semantic layer.

### 6.4 YAML expression strings *(deferred — YAML is out of v0.1; see §3 trigger table)*

> Retained as design reference for when YAML config is revived. Until then, measures are defined in Scala only.

BSL uses `eval` on Python strings (`_.distance.sum()`). The JVM has no safe equivalent. Two-pronged strategy:

- **Scala API:** full power — measures are real `Scope => Column` functions, so any Spark expression works (windows, `when/otherwise`, `struct`, `array`, UDFs).
- **YAML:** a **restricted, parsed expression mini-DSL** for the common cases: column refs (`distance`), dotted (`flights.distance`), and aggregate calls (`sum(distance)`, `count()`, `mean(x)`, `x / y`). Implemented as a small parser combinator → `Scope => Column`. Anything beyond that must be defined in Scala and *referenced* by name from YAML. This bounds the YAML grammar to a safe, reviewable surface.

### 6.5 Filter pre/post-agg split (WHERE vs HAVING)

Reproduced exactly: a `Filter` over a **measure** field is routed post-aggregation (HAVING); over a dimension, pre-aggregation (WHERE). AND-compounds are split condition-by-condition; OR-compounds touching any measure stay whole (post-agg). The `Predicate` AST (`Compare | In | IsNull | And | Or | Not | Custom`) compiles to a Spark `Column` for either phase, with bracket-access field resolution post-agg (to preserve dotted names like `orders.total_amount`).

---

## 7. Build & dependency strategy

- **Build tool:** **Maven** (`pom.xml`). Spark ships to Maven Central, so Maven is the natural fit for the JVM/Spark ecosystem and for orgs whose CI/artifact pipelines are already Maven-based. Scala is added via the `scala-maven-plugin` (net.alchim31); tests run via `scalatest-maven-plugin`. (sbt was considered and rejected as the primary tooling to match the surrounding JVM/Spark Maven conventions; sbt remains usable by consumers who prefer it since the artifact is a plain Maven-coordinate jar.)
- **Scala:** **2.13** only. No 2.12 cross-build (ADR 0001) — Spark 4 is 2.13-only, so 2.13 covers both target Spark lines; reintroduce 2.12 only if a consumer is pinned to Spark < 4.
- **Spark:** **3.5.8** is the default/primary target. **Room for Spark 4.x** via a Maven profile (`-Pspark4`) that overrides `spark.version` (and the Scala patch version if Spark 4 pins a newer 2.13.x). One POM; CI builds both legs.
- **Java:** 17 (the intersection that satisfies both Spark 3.5.x — which supports 8/11/17 — and Spark 4, which requires 17+).
- **Spark scope:** `provided` — semanticdf is a library dropped into the user's Spark job; the runtime Spark comes from the user's cluster/distro.
- **No Python, no xorq, no Ibis, no YAML libs.** Pure JVM, Scala-only config for v0.1.
- **Testing:** `scalatest` 3.2.x; an in-memory local `SparkSession` fixture; a `flights` test fixture mirroring BSL's so behavior parity is checkable.
- **Publishing / binary compat:** deferred (ADR 0001) — no Maven Central deploy and no `mima-maven-plugin` until there is a downstream consumer to keep stable.

**pom.xml (planned, abridged):**

```xml
<project>
  <modelVersion>4.0.0</modelVersion>
  <groupId>io.semanticdf</groupId>
  <artifactId>semanticdf_2.13</artifactId>
  <version>0.1.0-SNAPSHOT</version>
  <packaging>jar</packaging>

  <properties>
    <scala.version>2.13.14</scala.version>
    <scala.binary.version>2.13</scala.binary.version>
    <spark.version>3.5.8</spark.version>          <!-- default profile -->
    <scalatest.version>3.2.19</scalatest.version>
    <maven.compiler.release>17</maven.compiler.release>
  </properties>

  <dependencies>
    <dependency>
      <groupId>org.apache.spark</groupId>
      <artifactId>spark-sql_${scala.binary.version}</artifactId>
      <version>${spark.version}</version>
      <scope>provided</scope>
    </dependency>
    <dependency>
      <groupId>org.scalatest</groupId>
      <artifactId>scalatest_${scala.binary.version}</artifactId>
      <version>${scalatest.version}</version>
      <scope>test</scope>
    </dependency>
  </dependencies>

  <build>
    <plugins>
      <plugin>
        <groupId>net.alchim31.maven</groupId>
        <artifactId>scala-maven-plugin</artifactId>
        <version>4.9.2</version>
        <executions><execution><id>default</id><goals><goal>compile</goal><goal>testCompile</goal></goals></execution></executions>
      </plugin>
      <plugin>
        <groupId>org.scalatest</groupId>
        <artifactId>scalatest-maven-plugin</artifactId>
        <version>2.2.0</version>
      </plugin>
    </plugins>
  </build>

  <profiles>
    <profile>
      <id>spark4</id>                               <!-- activate: mvn -Pspark4 -->
      <properties>
        <spark.version>4.0.0</spark.version>
        <!-- bump scala.version here if Spark 4 pins a newer 2.13.x -->
      </properties>
    </profile>
  </profiles>
</project>
```

Exact plugin/dependency pins are finalized at Phase 0; the structure (one POM, Scala 2.13, Spark `provided`, a `-Pspark4` override profile) is the commitment.

---

## 8. Phased delivery plan

Each phase ends green (`mvn test`) and cites the concrete BSL test file whose output it must reproduce (per ADR 0001 — verifiable, not "works"). Deferred features (YAML, `index`, `unnest`, `compare_periods`, graph introspection, cross-build, MiMa) are removed from this plan; they return only on their ADR trigger.

| Phase | Scope | Exit criteria (verifiable) |
|---|---|---|
| **0 — Skeleton** ✅ | Maven project, `io.semanticdf` package (flat), in-memory `SparkSession` fixture, `flights` fixture | `mvn test` green; `toSemanticTable(df).toDataFrame()` returns the df. **DONE** |
| **1a — Core model + golden group-by** ✅ | `Dimension`, `Measure`, `SemanticScope`/`BaseScope`, op nodes (`table` + `aggregate`), `withDimensions`/`withMeasures`/`groupBy`/`aggregate`/`execute` | **Golden test:** with BSL `test_query.py`'s exact fixture data, `groupBy("carrier").aggregate("total_passengers")` → `{AA→550, UA→775, DL→1050}` (3 rows, 2 cols). **DONE** |
| **1b — Calc-measure proof slice** ✅ | `MeasureScope` (by-name resolution), trivial base/calc classification, single-`select` calc compilation (DESIGN §6.1) | one calc measure `avg_distance_per_flight = total_distance / flight_count` resolves **by name** against the aggregated df → `{AA→125, UA→225, DL→325}`. Proves the name-based-compilation bet. **DONE** |
| **2a — Calc-of-calc + dependency auto-pull** ✅ | transitive dep auto-pull, topological-layered calc application (invariant A1 revised), cycle detection | 2-level calc chain resolves; leaf-only request auto-pulls deps; cycle raises 'cycle'. **DONE** |
| **2b — Full calc analyzer** | `analyze_calc_expr` (pushable / has_window / post_agg_only / depends_on), calc-of-calc topo sort, base-column-wins, "did you mean?" | reproduces `test_calc_compiler.py` calc-of-calc chains. (Base/calc split + name-based compilation + dep auto-pull already proven in 1b/2a; this phase adds structural classification + the deferred fast-path decisions) |
| **3 — Percent-of-total** ✅ | `scope.all("name")`, grand-total table at zero grain over the **same filtered base**, cross-join (broadcast), calc-formula re-application (non-sum totals correct: `all("avg...")` = 225, not 675). WHERE filters propagate to totals for free. 3 tests green. **DONE** (nested percent-of-percent deferred) |
| **4 — Joins** ✅ | `join_one/join_many/join_cross`, **pre-agg fan-out prevention**, merged model with left-precedence collision resolution, per-side dim/measure probing, qualified-column-ref dedup | `join_one` (fact→dim), `join_many` (fan-out-safe pre-agg), `join_cross`, calc-of-calc on joined models. 5 join tests green. **DONE** |
| **5 — Query API + filters** ✅ | `Predicate` AST (`Compare/In/IsNull/And/Or/Not`), fluent DSL, **WHERE/HAVING auto-routing** (dim→pre-agg, measure→post-agg, AND-split, OR-whole), `SemanticFilterOp` op node; **`orderBy`/`limit`** deferred op nodes + `SortKey` DSL (bare string=asc, `SortKey.desc`); **`query()`** one-shot bundler (where→groupBy→aggregate[having]→orderBy→limit) | `where()` auto-routes; `having()`; AND-split; OR-whole; top-N via orderBy+limit; `query()`. 10 query/filter tests green. **DONE** (offset + time params in `query()` deferred to Phase 6) |
| **6 — Time semantics** ✅ | `Dimension.time(...)` factory, `TimeGrain` (normalize/order/validate), `atTimeGrain(dim,grain)` via dim-expr override + `date_trunc`, **group keys now resolve via dimension expr** (makes dim exprs load-bearing), `query(timeGrain=, timeGrains=, timeRange=)`, `withDimensions`/`withMeasures` traverse passthrough ops (filter/orderBy/limit) | truncated month grouping (3 buckets); `TIME_GRAIN_MONTH` alias; grain-too-fine raises; non-time-dim raises; query() timeGrain + timeRange (raw-col filter pre-truncation). 6 tests green. **DONE** (derived parts year/month/day, point-in-time joins, compare_periods deferred) |
| **7 — Spark 4 leg** ✅ | `-Pspark4` profile green on the Phase 0–6 suite | full suite (34 tests) passes under Spark **3.5.8** (default), **4.0.0**, and **4.1.1** (latest). No code shims — uses only APIs stable across 3.5→4.x. **DONE** |
| **8 — Polish** ✅ | README rewrite (v0.1, all capabilities with runnable examples, API reference, cross-version table), ADR cross-links | publishable v0.1 (publishing itself still deferred pending a consumer). **DONE** |
| **D — First-consumer hardening** ✅ | Metastore integration, catalog accessors, typed result schema, error message audit, benchmark harness, onboarding docs, **integration tests (real CSV I/O + 10K-row perf baseline)** | `createOrReplaceTempView` / `createTempView` / `createOrReplaceGlobalTempView`; `dimensions` / `measures` / `findDimension` / `findMeasure`; `previewSchema(spark)`; improved error messages with actionable hints; `Benchmark.scala`; `IntegrationSpec` (6 tests, real file reads + metastore + 10K perf baseline 282ms); `docs/known-limitations.md`, `docs/calc-author-guide.md`, `docs/first-consumer-plan.md`. 59 tests green (6 new integration). **DONE** (data catalog metadata, named queries deferred) |
| **E — Type safety via typeclasses** | `ResultDecoder[T]` typeclass + `query[T]`; `Dimension[T]` / `Measure[T]` phantom types; typed calc arithmetic (`divide[N, D, R]`). Additive — no breaking API changes. Deferred until first consumer validates runtime error pain is real. | See `docs/phase-E-plan.md` for full scope, exit criteria, and risk log. **NOT STARTED** |
| **YAML model loader** ✅ | Declarative model definitions in `.yml` files, for consumers who know YAML not Scala. Coexists with the Scala DSL — builds identical `Dimension`/`Measure`/`SemanticTable` objects. | `YamlLoader.load(path, tables)` and `YamlLoader.load(path, spark)`; `CalcExpr` recursive-descent parser (arithmetic + `all()` for percent-of-total); 16 tests verifying parity with Scala DSL. `flights_model.yml` example + `YamlFlightsExample` runner. **DONE** (asymmetric join keys, function-call calcs deferred — see `docs/known-limitations.md`). |

**Re-sequencing note (ADR 0003):** Phase 1 is split into 1a (minimal core) and 1b (calc-measure proof slice) to test the port's riskiest assumption — name-based calc compilation — *before* building Phase 1's original breadth (schema introspection etc., now folded into the phases that need it). The earlier "end-to-end percent-of-total smoke test in Phase 1" was removed: percent-of-total needs calc measures (Phase 2) + totals (Phase 3), so it was impossible in Phase 1; it lives in Phase 3's exit criterion.

---

## 9. Risks & open questions

1. **`t.all(...)` over nested-array measures.** BSL raises `TotalsNotAvailableError` because multi-grain totals aren't well-defined. We inherit the same limitation and the same loud error — acceptable for v0.1.  *Note (Phase A):* Spark's `/` returns `null` on divide-by-zero (correct SQL semantics, not a bug). Use [[CalcHelpers$.safeDivide]] for calc authors who want an explicit default on zero/missing denominators.
2. ~~**Schema/dtype inference.**~~ ~~Edge cases (nullability, decimal precision)~~ — **RESOLVED (Phase A).** Spark SQL semantics govern throughout: `sum(null) = null`, null group keys coalesce, null join keys don't match (`null != null`), decimal precision/type-promotion is handled natively by Spark's Catalyst. Confirmed by exploratory test suite (7 hardening tests, 41/41 green).
3. **Calc lambda double-execution + purity contract.** Like BSL, a calc lambda runs once at classification (construction) and once at compile. (a) Construction should be **once per session, not per query** — BSL deliberately does not cache classification across queries (it depends on the known-measure set); reuse the built `SemanticModel`. (b) Measure/dimension lambdas must be **pure and capture no large external state** — a Scala lambda retains its enclosing scope for the model's lifetime (silent retention of lookup maps / broadcast vars / open iterators). Both go in the documented contract. *(Note: Phase A found and fixed a latent classifier bug where `Measure("x", t => sum(t("x")))` — same-name base-column aggregation — was misclassified as a self-dependency → false cycle. Fixed by excluding the measure's own name from its dependency probe.)*
4. **Naming collisions across joined models.** Reproduce BSL's prefix scheme (`model.field`) and unique-suffix resolution; verify Spark's column-qualifying (`df("model.field")` needs backtick-quoted identifiers) — may need an aliasing strategy.
5. **Spark 3.5 vs 4 API drift.** A few functions we lean on differ between lines (e.g. `arbitrary()` availability, AQE/window defaults, Spark 4's variant type). The `-Pspark4` leg (Phase 7) is where these surface; isolate version-specific calls behind a thin shim rather than scattering `if` checks.
6. **Maven + Scala incremental compilation.** `scala-maven-plugin`'s incremental compiler is less polished than sbt's; cold builds are fine, but watch for stale-output surprises — IntelliJ (with the Scala plugin) is the expected IDE and must keep its Maven import in sync.
7. **Per-batch analysis cost (streaming only, future).** Each micro-batch re-analyzes/re-optimizes the Catalyst plan, so deep op trees and the calc `select` depth are paid *per batch*, not once. The §4.4/§6.1 invariants (single `select`, no cached internals) keep this bounded; the streaming terminal (ADR 0002) is where high-throughput plan cost would first show up and get benchmarked.

---

## 10. Faithfulness summary

SemanticDF will be a **behavioral port** of BSL: same conceptual model (semantic table, dimensions/measures/calc-measures, op tree, join cardinality, percent-of-total), same correctness guarantees (grain-correct joins, non-sum totals, WHERE/HAVING split), same user mental model (`to_semantic_table → with_dimensions/with_measures → group_by/aggregate/query`). It is **not** a line-by-line translation: we exploit Scala's strengths (case classes, pattern matching, first-class functions, `Try`/`Option`) and Spark's strengths (Catalyst column pruning, native window functions, broadcast joins) to make the code simpler where BSL fights Python/ibis limitations (Deferred, op-graph substitution, xorq bridging, ambiguous-column renaming dances).

The result: a semantic layer that feels native to Spark developers while preserving the design discipline that makes BSL correct.
