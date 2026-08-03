# Design: SemanticDF multi-engine portability (v0.3.0)

> Status: **DRAFT for review** — addresses: engine-portability (Spark, Trino,
> Unity Catalog, Hive metastore, Dremio, Databricks, custom in-house platforms
> with bundled catalog + REST/JAR query API).
> Author: assistant, awaiting stacked-lens review before PR.
> Follows skills: `karpathy-guidelines`, `scala-data-driven-refactor` (typed +
> data-driven + serializable), `debug-mantra`.
> Target artifact split: **non-breaking for v0.2.4 users**.

---

## 0. TL;DR

We split `semanticdf` into two artifacts:

  - `semanticdf-core` — pure data, pure typeclasses, **engine-neutral**.
  - `semanticdf-spark` — current library renamed; **same public API**, depends
    on `-core`, implements `Engine[DataFrame]` + `CatalogAdapter` for filesystem
    YAML discovery.

New artifacts are additive and depend on `-core`:

  - `semanticdf-trino` — `Engine[TrinoResult]`
  - `semanticdf-databricks` — `Engine[DataFrame]` + Unity-Catalog adapter
  - `semanticdf-snowflake` — JDBC `Engine[SnowflakeResult]` + Snowflake-catalog adapter
  - `semanticdf-platform-<vendor>` — third-party / in-house adapters (one per vendor)

**No existing user contract changes** (CLI, MCP, code-API, fluent chain, YAML,
manifest) — `semanticdf-spark` is a strict renaming + minor rewiring, not a
semantic break. Every contract entry point keeps its current signature.

Per karpathy-guidelines §1 ("think before coding"):

  > State your assumptions explicitly. If uncertain, ask.
  > If multiple interpretations exist, present them — don't pick silently.

This document presents 4 architectural options with their tradeoffs before
recommending the split.

---

## 1. Goals & non-goals

### Goals (must-have)

1. **Keep every current user contract.** CLI (`sql-cli`, `lineage-cli`,
   `docs-gen`, `okf-gen`, `introspect`), MCP (`list_models`, `describe_model`,
   `query`, `explain`, `introspect`, `audit_log`, `get_field_lineage`,
   `get_dependencies`), code-API (fluent chain, `toSemanticTable`,
   `SemanticTable.query`, `RollupQuery.execute(spark)`), and YAML/manifest
   format. Same signatures. Same wire shapes.

2. **Add a portable execution layer.** `Query.execute(engine: Engine[A])`
   where `A` is the engine's row type. Existing `execute(spark: SparkSession)`
   stays as a back-compat alias for `execute(SparkEngine(spark))`.

3. **Add a portable catalog layer.** `CatalogAdapter` interface; the existing
   YAML-file-based model loading becomes one implementation; Unity Catalog,
   Hive metastore, in-house REST catalogs become additional implementations.

4. **Type-safe at compile time, not "first user loses".** Per the
   `scala-data-driven-refactor` mantra: parse-don't-validate at the boundary,
   sealed ADT for all engine-switching vocabulary, exhaustiveness checked by
   the compiler.

5. **Runtime-serializable without surprises.** Per the distributed-serialization
   reference: every case class that flows across the cluster is *transitively*
   `Serializable` (no `Logger` / DB / HTTP / `() => DataFrame` captures on a
   record). The v0.2.4 `RollupSerializationSpec` is the template — extend
   to `Model`, `Query`, `EngineRef`, `EngineContext`.

### Non-goals (deferred / out of scope)

1. **Portable streaming.** Spark `Structured Streaming` is unique; each
   engine has its own streaming semantics. Skip in v0.3.0.
2. **Spark-lambda escape hatch portability.** `Dimension("x", t => t("x"))`
   stays `semanticdf-spark`-only. Portable dimensions use
   `Dimension("x")` + a portable `Expr` IR.
3. **AWT-killing feature parity.** Spark rollups auto-routing, AQE skew
   handling, `withMaterialize` — these are Spark features; engines that lack
   them declare so via `CapabilitySet` and surface the gap. We don't fake
   parity.
4. **Source-resolution magic.** `SourceRef.byName("orders")` resolves
   differently per engine — that's the adapter's job, not the core's.
5. **Migration tooling.** v1+: SQL-to-portable-IR reverser for legacy DDL.
   v0.3.0 ships the new surface; legacy can coexist.

---

## 2. User contract preservation matrix

Every existing public entry point listed below is preserved **without
signature change**. Where the new design introduces an additional entry point
(e.g., `Query.execute(engine)`), the old entry point becomes a thin alias.

| Surface | Today | After | Change |
|---|---|---|---|
| `SemanticTable.toDataFrame(spark)` | returns `DataFrame` | unchanged (now delegates to `SparkEngine`) | NONE (alias) |
| `SemanticTable.toStreamingQuery(spark, opts)` | returns `StreamingQuery` | unchanged | NONE |
| `SemanticTable.query(...).execute(spark)` | returns `DataFrame` | unchanged (alias) | NONE |
| `SemanticTable.query(...).execute()` (implicit) | returns `DataFrame` | unchanged (implicit `SparkEngine`) | NONE |
| `RollupQuery.execute(spark)` | returns `DataFrame` | unchanged (alias) | NONE |
| `toSemanticTable(df, name = ...)` | constructs `SemanticTable` | unchanged | NONE |
| `Dimension("x", t => t("x"))` | Spark `Column` factory | unchanged in `semanticdf-spark` | NONE (still Spark) |
| `Dimension("x")` — NEW | N/A | portable form | additive |
| CLI `semanticdf sql` | runs SQL against loaded models | unchanged | NONE |
| CLI `semanticdf introspect` | reports model schema | unchanged | NONE |
| CLI `semanticdf lineage` | reports lineage | unchanged | NONE |
| MCP `list_models` | reports loaded models | unchanged | NONE |
| MCP `describe_model` | reports model fields + joins + manifest | unchanged | NONE (added `engines` field optional) |
| MCP `query` | submits query, returns rows | unchanged | NONE (added `engine` parameter, default `"spark"`) |
| MCP `audit_log` | retrieves audit events | unchanged | NONE |
| `SemanticManifest.toJson` / `fromJson` | wire format | unchanged | NONE (added `engineCapabilities: Map[String, ...]` field with default absent) |
| `YamlLoader.load(path)` | reads YAML, returns `ModelRegistry` | unchanged | NONE |

Per karpathy-guidelines §2 ("simplicity first"):

> "No error handling for impossible scenarios."

We don't add back-compat shims beyond the listed aliases. Anyone bypassing the
public API is on their own.

---

## 3. Architectural options

### Option A — Minimal `Engine` interface on top of existing code

```scala
trait Engine[+R] {
  def compile(op: SemanticOp, ctx: EngineContext): ExecutionPlan[R]
  def capabilities: CapabilitySet
  def execute(plan: ExecutionPlan[R], ctx: EngineContext): ResultSet[R]
}
```

with `SparkEngine` as a passthrough to existing `SemanticOp.compile`.

**Verdict (per debug-mantra — falsify first):**
Falsification: existing `SemanticOp.compile(SparkSession): DataFrame` carries
Spark objects in its tree (DataFrames at leaves, Column factories in
`Dimension`/`Measure`). A `TrinoEngine.compile(op, ctx)` receives the same
op tree but cannot convert the embedded `DataFrame` and `Column` to Trino
SQL. The Engine seam gives a *dispatch* point but no engine-portable IR.

→ **Useful as a dispatch seam but not sufficient alone.** Rejected as the
final shape; evolved into Option C below where the IR is the focus.

### Option B — Add `semanticdf-trino` adapter only

Keep `semanticdf` Spark-coupled. Add an adapter module that re-implements
the model compilation path for Trino from scratch.

**Verdict:** large duplicated code (~3000 LoC); semantic drift between
engines is permanent; BSL parity not preserved per-engine. Same Engine seam
needed eventually.

→ Rejected as primary strategy. Useful for tactical integrations.

### Option C — Architectural split with portable IR

Move engine-portable logic into `semanticdf-core`. `semanticdf-spark`
becomes an engine implementation. New adapters are small.

**Verdict (recommended):** reuses existing work, gates per-engine cost on
real demand, preserves all contracts. Detailed below.

### Option D — Redesign from scratch

Throw out v0.2.4, build a backend-neutral IR from first principles.

**Verdict:** 12,000–25,000 LoC over 6+ months. Per karpathy-guidelines §1
("no features beyond what was asked"), this is speculative — the user only
asked for `Engine`+adapter support, not a full IR rewrite.

→ Rejected.

**Chosen: Option C.**

---

## 4. The portable IR (the actual change)

Per scala-data-driven-refactor step 1 ("data is data"), the
`SemanticOp` ADT today conflates shape with compilation. We split:

```
SemanticOp (today)        = shape + (spark-specific compile path)
SemanticOp (after)         = shape only
ScalarExpr / RelExpr       = NEW: portable expression IR
Engine.compile(op, ctx)   = NEW: lowers portable IR to engine form
```

### 4.1 Portable `Expr` ADT (NEW)

```scala
package io.semanticdf.core.expr

/** Portable scalar / relational expression tree.
  *
  * Pure data. Every implementation is a `case class`/`case object` whose
  * fields are strings, primitives, sealed-ADT constructors, or collections
  * of the same — no `Column`, no `DataFrame`, no closures, no `Logger`,
  * no DB/HTTP clients (per distributed-data-serialization rules).
  *
  * Spans: scalar arithmetic, comparisons, aggregates, lambdas (deferred
  * to engine capability), and reference to a named field.  Engines that
  * lack a constructor (e.g., Trino lacks `array_contains` on map keys)
  * report `CapabilityUnsupported` rather than silently miscompiling.
  */
sealed trait Expr extends Product with Serializable

object Expr {
  // References
  final case class ColumnRef(name: String) extends Expr
  final case class Literal(value: LiteralValue, dataType: DataType) extends Expr

  // Boolean
  final case class Compare(left: Expr, op: CompareOp, right: Expr) extends Expr
  final case class Not(inner: Expr) extends Expr
  final case class And(parts: List[Expr]) extends Expr
  final case class Or(parts: List[Expr]) extends Expr

  // Null
  final case class IsNull(inner: Expr, negated: Boolean) extends Expr
  final case class In(field: Expr, values: List[Literal], negated: Boolean) extends Expr

  // String
  final case class Contains(field: Expr, needle: Expr) extends Expr
  final case class StartsWith(field: Expr, prefix: Expr) extends Expr
  final case class EndsWith(field: Expr, suffix: Expr) extends Expr

  // Array
  final case class ArrayContains(field: Expr, value: Expr) extends Expr

  // Arithmetic (deferred to engine when complex)
  final case class Add(parts: List[Expr]) extends Expr
  final case class Mul(parts: List[Expr]) extends Expr
  final case class Sub(left: Expr, right: Expr) extends Expr
  final case class Div(left: Expr, right: Expr) extends Expr

  // Cast
  final case class Cast(inner: Expr, targetType: DataType) extends Expr
}

/** Operator kind — exhaustive enumeration; sealed per ADT-mantra step 3
  * (do NOT substitute a `Map[Op, ...]` — typo'd keys silently default). */
sealed trait CompareOp
object CompareOp {
  case object Eq  extends CompareOp
  case object Ne  extends CompareOp
  case object Lt  extends CompareOp
  case object Le  extends CompareOp
  case object Gt  extends CompareOp
  case object Ge  extends CompareOp
}

/** Type-safe literal values — engine-portable, no Scala `Any`. */
sealed trait LiteralValue extends Serializable
object LiteralValue {
  final case class IntValue(v: Long)        extends LiteralValue
  final case class FloatValue(v: Double)    extends LiteralValue
  final case class StringValue(v: String)   extends LiteralValue
  final case class BoolValue(v: Boolean)    extends LiteralValue
  final case object NullValue                extends LiteralValue
}
```

Per scala-data-driven-refactor step 4 ("every lookup is a bet you'll never
mistype a key"), `CompareOp` is a sealed trait — `match` exhaustiveness is
the guard, not a `Map[String, CompareOp]`.

### 4.2 Portable `RelOp` ADT (NEW — the relational tree)

```scala
package io.semanticdf.core.rel

/** Relational operators — engine-neutral.  Equivalent to a query's
  * logical plan / Catalyst plan / Calcite plan / Trino plan; engine
  * implementations translate this to their native form.
  *
  * Pure data.  Each node is a `case class` of `Expr` + `RelOp`
  * children; no engine types.
  */
sealed trait RelOp extends Product with Serializable

object RelOp {
  final case class Scan(source: SourceRef, projection: List[Expr]) extends RelOp
  final case class Filter(input: RelOp, predicate: Expr) extends RelOp
  final case class Project(input: RelOp, exprs: List[(Expr, String)]) extends RelOp
  final case class Aggregate(
    input: RelOp,
    groupBy: List[Expr],
    aggregates: List[AggregateCall],
  ) extends RelOp
  final case class Join(
    left: RelOp, right: RelOp, kind: JoinKind, condition: Expr
  ) extends RelOp
  final case class Sort(input: RelOp, keys: List[SortKey]) extends RelOp
  final case class Limit(input: RelOp, n: Long) extends RelOp
}

sealed trait JoinKind
object JoinKind {
  case object Inner          extends JoinKind
  case object Left           extends JoinKind
  case object Right          extends JoinKind
  case object Full           extends JoinKind
  case object Cross          extends JoinKind
}

final case class AggregateCall(
  fn: AggregateFn,
  input: Expr,
  alias: String,
  distinct: Boolean = false,
)

sealed trait AggregateFn
object AggregateFn {
  case object Sum    extends AggregateFn
  case object Count  extends AggregateFn
  case object Avg    extends AggregateFn
  case object Min    extends AggregateFn
  case object Max    extends AggregateFn
  case object Stddev extends AggregateFn
}

final case class SortKey(expr: Expr, direction: SortDirection, nullsFirst: Boolean)
sealed trait SortDirection
object SortDirection {
  case object Ascending  extends SortDirection
  case object Descending extends SortDirection
}
```

### 4.3 Source reference (NEW)

```scala
package io.semanticdf.core.source

/** Engine-portable reference to a source dataset.
  *
  * The `kind` field discriminates which `adapter` knows how to resolve
  * it.  Engine-specific knobs (Spark paths, Trino catalog/schema,
  * Snowflake database/schema, in-house REST URL) live in the `kind`'s
  * payload.
  */
sealed trait SourceRef extends Product with Serializable

object SourceRef {
  /** "by name" — resolve via the engine's default catalog. */
  final case class ByName(catalog: Option[String], schema: Option[String], table: String)
    extends SourceRef
  /** explicit path — resolved by the adapter. */
  final case class ByPath(format: String, path: String, options: Map[String, String])
    extends SourceRef
  /** programmatic — adapter calls back to the user. */
  final case class ByProvider(thunkRef: ProviderRef) extends SourceRef
  /** captured but not serializable — `() => DataFrame`-like closures
    * stay `semanticdf-spark`-only. */
  case object Unresolved extends SourceRef
}

/** Identifier for a provider — the actual closure lives in the engine
  * adapter's registry (e.g., a `Map[String, Provider]`).  We never put
  * a `() => DataFrame` in the model itself, because the model must
  * round-trip through every persist/restore boundary (manifest JSON,
  * catalog sidecar, Restate journal — see the
  * distributed-data-serialization rules). */
final case class ProviderRef(name: String, kind: String) extends Serializable
```

Per scala-data-driven-refactor step 1 + distributed-data-serialization,
`SourceRef` deliberately does NOT contain `() => DataFrame`. The closure
lives in the engine adapter's registry, never on the model.

### 4.4 `Model` becomes pure data

```scala
package io.semanticdf.core

/** A semantic model — dimensions, measures, sources, joins, freshness.
  * Pure data. Serializable.  Engine-neutral.
  *
  * Smart constructor in companion enforces validity once at the
  * boundary (per scala-data-driven-refactor step 2).
  */
final class Model private[core] (
  val name:            String,
  val description:     Option[String],
  val source:          SourceRef,
  val dimensions:      List[Dimension],
  val measures:        List[Measure],
  val joins:           List[Join],
  val calcMeasures:    List[CalcMeasure],
  val freshness:       Option[Freshness],
  val sourceTable:     Option[String],       // alias for the underlying table
  val version:         Int,                  // 0 = unversioned
  val status:          ModelStatus,
  val rollups:         List[Rollup],         // metadata only, no provider
  val extensions:      Map[String, ExtensionValue],  // vendor-specific payloads
) extends Serializable { ... }

object Model {
  /** Smart constructor: parse don't validate (per data-design step 2). */
  def of(
    name: String,
    source: SourceRef,
    dimensions: List[Dimension] = Nil,
    measures: List[Measure] = Nil,
    joins: List[Join] = Nil,
    calcMeasures: List[CalcMeasure] = Nil,
    freshness: Option[Freshness] = None,
    sourceTable: Option[String] = None,
    version: Int = 0,
    status: ModelStatus = ModelStatus.Published,
    description: Option[String] = None,
    rollups: List[Rollup] = Nil,
    extensions: Map[String, ExtensionValue] = Map.empty,
  ): Model = {
    require(name.nonEmpty, "Model.name must not be empty")
    val dimSet = dimensions.map(_.name).toSet
    val measSet = measures.map(_.name).toSet
    require(measSet.intersect(dimSet).isEmpty,
      s"Model '$name': name collision between dimensions and measures: ${measSet.intersect(dimSet)}")
    // ... rest of validation runs once at construction
    new Model(...)
  }
}
```

Per the distributed-data-serialization rules: every field on `Model` is
either a primitive, a `String`, a sealed ADT, or a `List[Serializable]`.
No closures, no `Logger`, no `() => DataFrame`, no captured outer `this`.
Round-trips cleanly through Java serialization, Restate journal, manifest
JSON, catalog sidecar, MCP wire.

### 4.5 Engine-portable `Engine` trait

```scala
package io.semanticdf.core.engine

trait Engine[+R] {
  /** Build an engine-specific plan from a portable `RelOp` tree. */
  def compile(plan: RelOp, ctx: EngineContext): ExecutionPlan[R]

  /** What this engine can / cannot do — declarative, queryable.
    * Per scala-data-driven-refactor step 4, capability checks are
    * sealed-ADT `match`, not string lookup. */
  def capabilities: CapabilitySet

  /** Execute the plan and return a row container. */
  def execute(plan: ExecutionPlan[R], ctx: EngineContext): ResultSet[R]
}

/** What an engine can do.  Used by the query builder to either
  * emit a clean error or auto-fallback. */
final class CapabilitySet(val set: Set[Capability]) extends Serializable {
  def supports(c: Capability): Boolean = set.contains(c)
  def missing(required: Set[Capability]): Set[Capability] = required -- set
}

sealed trait Capability
object Capability {
  case object Streaming           extends Capability
  case object CalcMeasures        extends Capability
  case object Rollups             extends Capability
  case object TimeGrain           extends Capability
  case object NativeLambdas       extends Capability
  case object NativeUDFs          extends Capability
  case object PlannerHints        extends Capability
  case object MaterializePersist   extends Capability
  case object AqeSkewHandling     extends Capability
}

case class EngineContext(
  timeout: Duration,
  parameters: Map[String, String] = Map.empty,
) extends Serializable
```

---

## 5. Backend implementations

### 5.1 `semanticdf-spark` (the renamed existing library)

```scala
class SparkEngine(spark: SparkSession) extends Engine[DataFrame] {
  def compile(plan: RelOp, ctx: EngineContext): ExecutionPlan[DataFrame] =
    SparkPlanLowering.lower(plan)        // RelOp/Expr → Catalyst planner call

  def capabilities: CapabilitySet = CapabilitySet(Set(
    Capability.Streaming,
    Capability.CalcMeasures,
    Capability.Rollups,
    Capability.TimeGrain,
    Capability.NativeLambdas,
    Capability.NativeUDFs,
    Capability.PlannerHints,
    Capability.MaterializePersist,
    Capability.AqeSkewHandling,
  ))

  def execute(plan, ctx) = plan.run
}
```

The existing `SemanticOp.compile(SparkSession)` becomes the lowering
target's input — its current behaviour is preserved (per karpathy §3
"surgical changes"). The lowering adapter is a thin shim wrapping today's
`compile` calls. Every existing test continues to assert the same result
rows.

### 5.2 `semanticdf-trino` (new)

```scala
class TrinoEngine(client: TrinoStatementClient) extends Engine[TrinoResult] {
  def compile(plan: RelOp, ctx: EngineContext): TrinoSqlPlan =
    TrinoSqlLowering.lower(plan, ctx)    // RelOp → ANSI SQL string + params

  def capabilities: CapabilitySet = CapabilitySet(Set(
    Capability.CalcMeasures,             // SUM/COUNT/AVG/MIN/MAX yes
    Capability.TimeGrain,
    // no Streaming/Rollups/NativeLambdas — Trino lacks them
  ))

  def execute(plan: TrinoSqlPlan, ctx) =
    client.execute(plan.sql, plan.parameters, ctx.timeout)
}
```

Lowering is mechanical for the 80/20 SQL subset (projection, filter, join,
group-by, order, limit, calc measures). The unsupported capabilities surface
clean errors at query-build time, not silent wrong data at execution time.

### 5.3 `semanticdf-databricks` (new, low-effort)

Databricks Runtime IS Spark. `semanticdf-databricks` reuses
`semanticdf-spark`'s engine and adds a `CatalogAdapter` for Unity Catalog
plus `DatabricksConnect` aware session management. ~1500 LoC.

### 5.4 Catalog adapters (shared shape)

```scala
/** Engine-portable catalog adapter contract. */
trait CatalogAdapter extends Serializable {
  /** Register a model as a catalog-managed dataset/table/view. */
  def publish(model: Model, manifest: SemanticManifest, as: CatalogEntity): CatalogRef

  /** Discover a registered entity as a Model.
    *
    * Reads native catalog metadata + the SemanticManifest sidecar that
    * carries measure/calc/rollup metadata the native schema cannot
    * represent losslessly.
    */
  def discover(ref: CatalogRef): Option[Model]

  /** List entities carrying SemanticManifest sidecars. */
  def list(filter: CatalogFilter = CatalogFilter.all): Seq[CatalogEntry]
}

/** Engines that have no catalog (e.g., a vanilla Spark cluster reading
  * parquet files) use `FilesystemCatalogAdapter`, which is what
  * `YamlLoader.load` becomes under the new name. */
class FilesystemCatalogAdapter(roots: List[Path]) extends CatalogAdapter { ... }
```

---

## 6. User-facing interface (preserved)

### 6.1 Existing code keeps working unchanged

```scala
import io.semanticdf.{SemanticTable, toSemanticTable, Dimension, Measure}
import org.apache.spark.sql.functions._

val orders = toSemanticTable(ordersDf, name = Some("orders"))
  .withDimensions(
    Dimension("customer_id", t => t("customer_id")),
    Dimension("region",      t => t("region")),
  )
  .withMeasures(
    Measure("amount",      t => sum(t("amount"))),
    Measure("order_count", t => count(lit(1))),
  )

implicit val spark: SparkSession = SparkSession.builder()...getOrCreate()
implicit val engine: Engine[DataFrame] = SparkEngine(spark)

// All three forms compile and yield equivalent results:
// (1) implicit engine
val df1: DataFrame = orders.query(measures = Seq("amount"), dimensions = Seq("region")).execute()
// (2) explicit engine
val df2: DataFrame = orders.query(measures = Seq("amount"), dimensions = Seq("region")).execute(engine)
// (3) legacy alias — passes `spark` straight through
val df3: DataFrame = orders.query(...).execute(spark)
```

### 6.2 New portable surface

```scala
import io.semanticdf.core.{Model, ModelBuilder, Dimension, Measure}
import io.semanticdf.core.expr.{ColumnRef, Literal, LiteralValue, CompareOp}
import io.semanticdf.core.engine.{Engine, SparkEngine, TrinoEngine}

// Portable model — engine-neutral.
val ordersPortable: Model = Model.of(
  name = "orders",
  source = SourceRef.ByName(catalog = None, schema = None, table = "orders_parquet"),
  dimensions = List(Dimension("customer_id"), Dimension("region")),
  measures = List(
    Measure.sum("amount", "amount"),
    Measure.count("order_count"),
  ),
)
```

### 6.3 Per-engine query

```scala
// Spark
implicit val sparkEngine: Engine[DataFrame] = SparkEngine(spark)
val sparkRows: DataFrame = ordersPortable.query(...).execute

// Trino (different adapter module, same client code)
implicit val trinoEngine: Engine[TrinoResult] = TrinoEngine.fromJdbc(
  "jdbc:trino://coord:8080", catalog = "hive", schema = "silver"
)
val trinoRows: TrinoResult = ordersPortable.query(...).execute

// Materialize to DataFrame for downstream Spark consumers
val df: DataFrame = trinoRows.toDataFrame(spark)
```

### 6.4 CLI

The CLI doesn't change at all. Same `semanticdf sql`,
`semanticdf introspect`, `semanticdf lineage` — they all use the
implicit `SparkEngine(spark)` under the hood. Adding `--engine trino`
to a subcommand is an additive flag gated on the engine being on the
classpath.

### 6.5 MCP server

The MCP server is a Spark-only deployment today (line 10 of
`query.scala`: `import org.apache.spark.sql.{DataFrame, SparkSession}`).
`mcp-contract.md` §"Decisions baked in" keeps this — Spark is the v0.3.0
server's deployment target. Trino / others deploy as separate MCP
configurations, each pinning their own engine, each using the same 8
tool names so MCP clients don't change.

A new optional `engine` field is added to the MCP `query` request body,
defaulting to `"spark"`:

```json
{ "model": "orders_by_region", "measures": ["amount"],
  "dimensions": ["region"], "engine": "trino" }
```

`engine` is opaque to the rest of the contract — the server's
configuration determines what engines are available; the agent picks.
This is additive per karpathy-guidelines §3 ("surgical changes") —
no existing request body breaks.

---

## 7. Implementation phases

Each phase is a self-contained, verifiable increment.

### Phase 1 — `semanticdf-core` extraction (2-3 releases)

**Goal:** split engine-portable bits out, no observable behavior change.

**Move** (no code change, just artifact):
- `audit.AuditEvent`, `QueryRequest`, `PredicateHasher`, `Clock`,
  `InMemoryAuditSink`, `JsonlStdoutSink` interface
- `cache.CacheKey`, `LengthPrefixed` (portable framing)
- `Predicate` data (only the ADT; the `compile(scope): Column` method
  stays in `semanticdf-spark`)
- `SortKey` data
- `TimeGrain` data
- `Model`, `Dimension`, `Measure`, `Transform`, `Join` data
- `SemanticManifest` data (the `toJson`/`fromJson` lives here)
- `SourceRef`, `Engine`, `CapabilitySet`, `EngineContext`

**Add:**
- `Expr` and `RelOp` ADTs (pure data)
- Portable engine interface `Engine[+R]`
- `semanticdf-spark` artifact with `SparkEngine` impl
- `FilesystemCatalogAdapter`

**Verification (debug-mantra §5 — verify the fix):**
- 992 existing tests still pass byte-for-byte
- `RollupSerializationSpec` extended to cover `Model`, `Query`,
  `EngineRef`, `EngineContext` (8 new tests, mirroring the v0.2.4
  pattern from PR #334)
- New `EngineConformanceSpec` enumerates the 8 capabilities × 9 expr
  types matrix; `SparkEngine` reports the full set; `TrinoEngine`
  reports a subset. 72 assertions, contract-locked.

### Phase 2 — Trino engine proof (1-2 releases)

- `semanticdf-trino` artifact
- `TrinoSqlLowering`: RelOp → ANSI SQL string with parameter binding
- `TrinoEngine` impl
- `TrinoResult` impl with `toDataFrame(spark)` bridge
- `TrinoCatalogAdapter` (HTTP, no JAR; pure sttp / okhttp)
- Golden SQL snapshots: ~20 hand-written tests + 100 generated
  property tests (per the `PredicateOpsSpec` template)
- Documentation: `docs/design/trino-adapter.md`

### Phase 3 — Databricks + Unity Catalog (1 release)

- `semanticdf-databricks` artifact
- `UnityCatalogAdapter` (reuses Databricks REST API)
- Wire MCP server's `engine: "databricks"` config
- Streaming via Unity Catalog's streaming tables (Phase 3.1, optional)

### Phase 4 — Snowflake / Dremio adapters (1 release each)

- Pattern identical to Trino; ~2,000 LoC per engine

### Phase 5 — Custom-platform SDK (1 release)

- `semanticdf-platform-sdk` artifact: helper base classes for
  in-house catalog adapters (REST or JAR-based)
- ~500 LoC reference impls

---

## 8. Risk + how debug-mantra mitigates

Per `debug-mantra` §1 ("reproduce") the migration starts with the
existing 992 tests — they are the reproducer for "no observable change."
§2 ("trace the fail path") every `Engine.compile` call is followed by a
property test asserting `ResultSet.toJson == lower(plan).toJson`. §3
("falsify the hypothesis") each phase's NEW tests target the case the
existing tests don't cover (especially `Trino` vs `Spark` result-set
equivalence). §4 ("cross-reference") manifest JSON + sidecar manifest +
catalog metadata + MCP wire shape are all bitwise-equal across the
migration. §5 ("verify the fix") the property test matrix.

### Specific risks

| Risk | Probability | Mitigation |
|---|---|---|
| Existing Spark users see a behavior change | Low | 992 regression tests as the reproducer; `SparkEngine` is a thin shim, not a rewrite |
| Engine capability gap surfaces silently as wrong data | Medium | `CapabilitySet` checked at `Query.build` time, `IllegalArgumentException` raised; not at runtime |
| Existing YAML models stop loading | Low | `FilesystemCatalogAdapter` IS the new home of `YamlLoader` — wire-compatible |
| MCP wire format breaks for existing clients | Low | `engine` field is optional + ignored when absent; deferred engines (Trino) require opt-in config; default is `"spark"` |
| `SemanticManifest.toJson` shape changes | High without care | `extensions: Map[String, ExtensionValue]` is the *only* new field; deserializer is tolerant of unknown fields; reverse compat verified via `SerializationSpec` round-trip |
| Closure captures in `Model` cause `NotSerializableException` at runtime | Low (by construction) | `SourceRef` does not contain `() => DataFrame`; checked in `Model.of(...)` smart constructor |

### Falsifiable claims (per debug-mantra §3)

For each phase, ONE claim that the new tests must be able to **break** if
the design is wrong:

1. **Phase 1:** A `Model` constructed with a non-serializable `ExtensionValue`
   fails `SerializationSpec` round-trip.
2. **Phase 2:** A query that uses `Capability.Streaming` against `TrinoEngine`
   throws `IllegalArgumentException` at `Query.execute` time, NOT at
   Trino server time.
3. **Phase 3:** A `UnityCatalogAdapter.publish` followed by `.discover`
   returns the original `Model` instance bitwise-equal under
   `==` after manifest round-trip.
4. **All phases:** Every MCP wire-format schema version adds a new
   `schema_version` int; old clients send `null`, server returns
   `schema_version: 1` in its envelope; clients ignore future versions.

---

## 9. Decisions needing explicit user input

Per karpathy-guidelines §1 ("don't assume; surface tradeoffs"):

| # | Decision | Options | Default if no answer |
|---|---|---|---|
| D1 | Should the existing v0.2.4 fluent API (`Dimension("x", t => t("x"))`) coexist with the portable form, OR be deprecated in a future release? | (a) coexist indefinitely (b) deprecate in v0.4.0 | (a) coexist |
| D2 | MCP `engine` parameter — visible to agents or server-config-only? | (a) visible (agent picks) (b) server-config-only (agent doesn't see it) | (b) — server-config-only keeps the existing contract untouched; v0.4.0 can add (a) if needed |
| D3 | Streaming scope — defer all engines, or ship Spark-only and accept the loss? | (a) defer entirely (b) ship Spark-only | (a) defer |
| D4 | First new engine after Spark — Trino or DuckDB? | (a) Trino (SQL-first, broad data federation) (b) DuckDB (embedded, smaller surface area, easier first proof) | (a) Trino (BSL alignment; existing BSL test data already covers Trino-style SQL) |
| D5 | Backwards compat shim lifetime — keep `SemanticOp.compile(spark)` forever or remove in v0.5.0? | (a) keep forever (b) remove in v0.5.0 | (a) — until a major version bump |

---

## 10. Out of scope (explicitly)

Per karpathy-guidelines §2 ("no features beyond what was asked"):

- Performance optimisation across engines (v0.3.0 is correctness-only)
- Cost-based query optimizers (CBO across engines)
- Cross-engine query federation (one SemanticDF model = one engine)
- Stream-processing unification (Spark Structured Streaming is unique)
- Vendor-specific feature exposure beyond `extensions: Map` (e.g., direct
  Snowflake `ROW_NUMBER() OVER ...` clauses)
- Auto-routing: pick the cheapest engine at query time (Phase 6+, after
  capability sets stabilize)

---

## 11. Open questions

1. Does `extensions: Map[String, ExtensionValue]` need a typed-extension
   surface, or stay as opaque JSON-serializable payload? — Default: opaque.
2. Should `Engine` carry `ExecutionContext` for async engines? — Defer.
3. Should audit events report the engine used? — Add `engine: String`
   field to `AuditEvent`, default `"spark"` for backward compat.
4. Manifest extension schema versioning — major / minor / patch, or
   freeform `schema_version: Int`? — Freeform int matches current
   `SemanticManifest` style.

---

## 12. Glossary

| Term | Definition |
|---|---|
| `Engine[+R]` | Engine-portable trait that compiles a portable plan and executes it |
| `RelOp` / `Expr` | Engine-portable relational / scalar expression ADTs |
| `Capability` | Sealed trait enumerating engine features for negotiation |
| `CatalogAdapter` | Engine-portable trait for publishing/discovering semantic models |
| `SourceRef` | Engine-portable reference to a source dataset |
| `EngineContext` | Per-query engine parameters (timeout, parameters) |
| `CapabilitySet` | Per-engine capability inventory |
| `ExecutionPlan[R]` | Engine-specific plan (sealed by engine, lowered from RelOp) |
| `ResultSet[R]` | Engine-specific row container |
| `FilesystemCatalogAdapter` | YAML-loader as a `CatalogAdapter` |
| `SparkEngine` / `TrinoEngine` | Concrete `Engine[DataFrame]` / `Engine[TrinoResult]` impls |
| `ProviderRef` | Identifier for a data provider closure; closure lives in adapter registry, never on the model |

---

## 13. References

- `docs/agents/mcp-contract.md` — MCP server tool contract (preserved verbatim)
- `docs/design/rollups.md` — pattern: sealed-trait ADT for engine semantics
- `src/test/scala/io/semanticdf/RollupSerializationSpec.scala` — pattern for
  serializability coverage
- `src/test/scala/io/semanticdf/Spec/Post329RedesignSpec.scala` — pattern
  for "type-system enforces contract" tests
- `.pi/skills/scala-data-driven-refactor/SKILL.md`
- `.pi/skills/scala-data-driven-refactor/references/distributed-data-serialization.md`
- `.pi/skills/karpathy-guidelines/SKILL.md`
- `.pi/skills/debug-mantra/SKILL.md`

---

## 14. Changelog

- **v0.3.0-draft**: this doc.
