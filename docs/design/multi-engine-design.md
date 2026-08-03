# Design: SemanticDF multi-engine portability (v0.3.0)
> Status: **REVISED DRAFT (revision 3) for final stacked-lens re-review**.
> Approved direction: **Path A — portable core plus engine adapters**.
> Target engines: Spark, Trino, Databricks/Unity Catalog, Snowflake, Dremio,
> and custom in-house query/catalog platforms.
> Compatibility target: source and binary compatibility for the v0.2.4 Spark API,
> except for the explicitly versioned manifest-v2 wire migration described in §4.4.
> Scope of this revision: design only; no implementation is implied by this document.
## 0. TL;DR
SemanticDF will become a small portable semantic/query core surrounded by engine and
catalog adapters. The compatibility artifact `semanticdf_2.13` remains available and
forwards to `semanticdf-spark_2.13`; users do not have to rename a dependency in the
first migration release.
Primary artifacts:
- `semanticdf-core_2.13`: pure model, expression/relational IR, typed policies,
  capability requirements, result schema, and catalog contracts; no Spark classes.
- `semanticdf-spark_2.13`: the current implementation adapted to the portable IR;
  preserves the existing DataFrame and lambda APIs.
- `semanticdf-trino_2.13`: SQL lowering, JDBC/HTTP execution, cancellation, result
  normalization, and Trino source/catalog resolution.
- `semanticdf-databricks_2.13`: Spark engine reuse plus Databricks session and Unity
  Catalog integration.
- `semanticdf-snowflake_2.13` and later adapters: SQL-engine implementations.
- `semanticdf-mcp`: an engine-neutral handler with a concrete server-side engine
  registry; an optional request `engine` selects a registered provider.
The design makes five material corrections to the previous draft:
1. `EngineContext` is not a timeout plus string map. It carries typed materialize,
   cache, audit, join, timeout, and cancellation policies.
2. Capabilities describe what an engine supports; policies describe what this query
   asks the engine to do. Structured calc-depth, aggregate-function, expression-shape,
   late-binding, and set-op support replace coarse booleans.
3. Sources are resolved before lowering. Every scan carries a resolved portable schema.
4. Manifest v2 is an acknowledged wire change with dual readers, a JSON Schema, and
   explicit v1-to-v2 migration behavior.
5. Trino and Databricks are budgeted as real integrations, not thin adapters. Phase 1
   ends at a decision gate that requires a working Trino vertical proof.
### 0.1 Compatibility summary
- Spark fluent calls, explicit `execute(spark)`, implicit `.execute()`, streaming
  terminals, CLI tool names, and MCP response envelopes remain available.
- The optional MCP `engine` request member is additive, but implementing it requires a
  handler and deployment change; it is not “wire-only.”
- Manifest v1 remains readable. Manifest v2 becomes the default writer and moves
  op-derived arrays beneath `model`; byte-for-byte manifest shape is not preserved.
- Engine identity is added to cache and audit hashes. Old cache entries intentionally
  become unreachable when this rolls out.
- Portable window expressions, portable percent-of-total, and set operations are
  explicitly deferred to v0.4.0. Existing Spark-lambda behavior is not removed.
### 0.2 Finding-disposition ledger and ground truth
The eight cross-cutting HIGH findings are listed first and in review order. Each row
points to the current implementation or, where the finding was an estimate in the
previous draft, to the pre-revision document range.

| # | Revision disposition | Ground-truth citation |
|---|---|---|
| 1 | Replace the underspecified context with typed request policies and cancellation (§4.5.1). | `src/main/scala/io/semanticdf/SemanticTableCore.scala:80-220` shows timeout-adjacent execution behavior split across audit, cache, materialize, and Spark configuration paths. |
| 2 | Separate per-query policies from engine feature capabilities; define non-Spark disposition rules (§4.5.1-§4.5.3). | `src/main/scala/io/semanticdf/SemanticTableCore.scala:108-159,218-276` and `src/main/scala/io/semanticdf/SemanticTable.scala:109-298` show the runtime knobs are behavioral, not yes/no features. |
| 3 | Add `MCPEngineRegistry`, optional request selection, default selection, and `ENGINE_UNAVAILABLE` (§6.4). | `semanticdf-mcp/src/main/scala/io/semanticdf/mcp/handlers/Query.scala:29-40,75-95,601-621` hard-wires a `SparkSession` and has no engine member today. |
| 4 | Acknowledge manifest wire change; specify `ManifestDocument` v2, before/after JSON, schema, and compatibility (§4.4). | `src/main/scala/io/semanticdf/adapters/SemanticManifest.scala:1-1590`, especially `893-1118`, reconstructs Spark/op-tree state; `src/test/scala/io/semanticdf/RollupSerializationSpec.scala:1-217` demonstrates the separate metadata/provider lifecycle. |
| 5 | Resolve sources to schema-bearing `RelOp.ResolvedScan` before compile (§4.2, §4.3). | `src/main/scala/io/semanticdf/SemanticOp.scala:28-49,343-350` embeds a Spark `DataFrame` at the leaf and therefore obtains schema from the engine object today. |
| 6 | Re-budget adapters with a WBS and require a Phase-1 proof gate (§7). | Pre-revision `docs/design/multi-engine-design.md:671-735` estimated Phase 2/3 without source resolution, type/null semantics, cancellation, result encoding, catalog, deployment, and full tests. |
| 7 | Replace coarse calc/expr support with structured capability values (§4.5.2). | `src/main/scala/io/semanticdf/SemanticOp.scala:1139-1455` has layered calc dependency behavior; `src/main/scala/io/semanticdf/SemanticTableMutation.scala:241-250` shows expression support is finer-grained than one boolean. |
| 8 | Type provider references and define registry ownership, lifecycle, cluster behavior, failure results, and rollup precompute (§4.3). | `src/main/scala/io/semanticdf/rollup/Rollup.scala:13-20,55-100,320-353` currently separates provider lookup but still precomputes through a thunk during metadata construction. |
Architect-specific HIGH findings:

| # | Revision disposition | Ground-truth citation |
|---|---|---|
| 9 | Add a Maven module/dependency graph and a Spark-free core build proof (§5.1). | `pom.xml:1-98` is one JAR with Spark `provided` and Spark-coupled main sources. |
| 10 | Make execution plans sealed and inspectable (§4.5.4). | Pre-revision `docs/design/multi-engine-design.md:127-145,420-486` named `ExecutionPlan` without defining its observable contract. |
| 11 | Add `EngineIdentity` to cache and audit identity; invalidate legacy cache entries (§4.5.5). | `src/main/scala/io/semanticdf/cache/CacheKey.scala:39-115` and `src/main/scala/io/semanticdf/audit/AuditEvent.scala:65-143` currently omit engine identity. |
| 12 | Close `ExtensionValue` and define canonical encoding (§4.4.1). | Pre-revision `docs/design/multi-engine-design.md:394-396,807-817` used `ExtensionValue` without a type definition. |
| 13 | Version catalog identity and define create-only/upsert/CAS results (§5.3). | Pre-revision `docs/design/multi-engine-design.md:532-571` returned an undefined, unversioned `CatalogRef`. |
| 14 | Bound inline extension sidecars and externalize larger payloads (§4.4.1). | `src/main/scala/io/semanticdf/adapters/SemanticManifest.scala:893-1021` emits unbounded model arrays/runtime data today; the old design added an unbounded extension map. |
| 15 | Define `SourceResolver` independently from `Engine` and return typed failures (§4.3.2). | `src/main/scala/io/semanticdf/SemanticOp.scala:343-350`; pre-revision `docs/design/multi-engine-design.md:312-344` delegated resolution without a contract. |
| 16 | Define `ResultSchema`, `ResultRow`, and cross-engine normalization (§4.5.4). | `semanticdf-mcp/src/main/scala/io/semanticdf/mcp/handlers/Query.scala:94-100,303-335` converts Spark rows/types directly and has no engine-neutral result contract. |
| 17 | Make timeout cancellation a negotiated engine capability and verify remote cancellation (§4.5.1, §4.5.3, §7.6). | `semanticdf-mcp/src/main/scala/io/semanticdf/mcp/handlers/Query.scala:75-95,226-299` implements Spark job-tag cancellation directly in the handler. |
DE-specific HIGH findings:

| # | Revision disposition | Ground-truth citation |
|---|---|---|
| 18 | Defer portable analytic-frame expressions to v0.4.0; reject them in v0.3.0 (§4.1.3). | `src/main/scala/io/semanticdf/SemanticTableMutation.scala:241-250`; pre-revision portable `Expr` omitted the shape. |
| 19 | Defer portable percent-of-total expressions to v0.4.0 (§4.1.3). | `src/main/scala/io/semanticdf/Scope.scala:12-26,68-94` and `src/main/scala/io/semanticdf/SemanticOp.scala:1281-1318` establish existing Spark `t.all(name)`. |
| 20 | Add decimal, timestamp, date, array, map, and struct literals (§4.1.1). | `semanticdf-mcp/src/main/scala/io/semanticdf/mcp/handlers/Query.scala:323-335`; pre-revision literal ADT at `docs/design/multi-engine-design.md:251-259` omitted them. |
| 21 | Define the full portable `SealedDataType` ADT and type `Cast` with it (§4.1.1). | `src/main/scala/io/semanticdf/adapters/SemanticManifest.scala:1025-1118` and `semanticdf-mcp/src/main/scala/io/semanticdf/mcp/handlers/Query.scala:323-335` depend on Spark types today. |
| 22 | Explicitly defer Union/Except/Intersect to v0.4.0; this carries forward current scope (§4.2). | `src/main/scala/io/semanticdf/SemanticOp.scala:343-1139` has no set-operation node. |
| 23 | Expand `AggregateFn` and publish per-engine function support (§4.2, §5.4). | `src/main/scala/io/semanticdf/rollup/Rollup.scala:219-262` closes rollup aggregation to Sum/Count; base measures remain Spark lambdas. |
| 24 | Remove `NativeLambdas`; keep only `SparkLambdaEval` as a Spark-specific compatibility feature (§4.5.2). | `src/main/scala/io/semanticdf/Model.scala:278-281` stores a `SemanticScope => Column`, which cannot be a portable feature. |
| 25 | Specify the exact execution overloads and add `ExecuteAmbiguitySpec` (§6.2). | `src/main/scala/io/semanticdf/SemanticTableCore.scala:308-311`; `src/main/scala/io/semanticdf/rollup/RollupQuery.scala:92-96` has the explicit Spark terminal. |
| 26 | Correct the regression baselines to 963 Scala-only / 1124 cross-project (§7.6). | Pre-revision `docs/design/multi-engine-design.md:700-708,740-748` used stale 992; Maven wiring is `pom.xml:100-181`. |
  18-26 | Keep the reviewed semantic direction, but close the remaining definitional and wiring gaps below. | This revision is the approved final pass before round-3 sign-off.

**Round-2 closure section (revision 3).** The following changes close the final round-2
findings and are intentionally narrow. H1 removes speculative portable ADTs rather than
inventing behavior: the six set-operation cases, the analytic-window/frame/total
expression cases, streaming sink policy/output modes, and the provider sink callback are
reserved in §11 only. This explicitly honors karpathy-guidelines §2 and closes the
round-2 phantom-ADT finding. H2 defines `TransformSpec`, `PortableQueryResult`,
`ExplainResult`, `EngineError`, `CatalogError`, `TrinoRuntimeRegistry`, `SparkPlanHandle`,
and `CalculatedMeasure` inline. H3 corrects the Maven/Spark-free verification. H4 wires
`AuditSinkRef` through `AuditSinkRegistry`; H5 wires concrete MCP providers through
`MCPEngineProvider` and its registry. M1-M10 are closed in §§4-7 and each affected
snippet is marked with its round-2 finding where the change occurs.

**Round-3 DE closure section (revision 4).** The eight HIGH findings from the round-3
DE review are the sole scope of this pass. Each fix is surgical, marked inline with
its finding number, and references the corresponding file:line in this document
at the time the fix was applied. No other prose, no MEDs, no LOWs except the
phantom-ADT bonus bundled with HIGH 6.
| # | Finding | Document fix location | Round-2 closure dependency |
|---|---|---|---|
| 3.1 | MCP handler references undefined symbols; EngineError not Throwable | §6.4 handler rewrite | HIGH 5 adds `EngineError.toErrorDetail`; HIGH 5B adds `DecimalOverflow`, `IncompatibleExprShape(engine)` |
| 3.2 | ResultEncoder.rows returns Iterator instead of bounded sequence | §4.5.4 `PortableQueryResult.rows` | HIGH 1 same finding (DE round-3 finding 1.3) |
| 4.1 | `SparkEngineProvider.available = spark != null` runs after null NPE | §6.4 `SparkEngineProvider(Option)` | new defensive construction; registry's `select` filters before engine allocation |
| 1.1 | Phantom ADT `TransformSpec` carries no portable behavior | §4.4.1 + §4.4.2 v2 JSON drop | bundled with HIGH 6; `Model.transforms` field removed; v2 manifest omits `transforms` |
| 1.3 | `PortableQueryResult.rows: Iterator[ResultRow]` not Serializable | §4.5.4 rows -> `Vector[ResultRow]` with maxRows cap | base invariant §1.3 portable values are transitively serializable |
| 2.1 | `ExecutionPlan[+R] extends Product with Serializable` despite holding non-serializable `SparkPlanHandle` | §4.5.4 base trait stripped; new `ExecutionPlanSummary` | cluster-mode safety restored; cache/audit use the summary |
| 5.1 | `DecimalOverflow` case referenced but not in `EngineError` ADT | §4.5 `EngineError.DecimalOverflow(value, precision, scale)` | typed §5.4 Trino quirk becomes a real return path |
| 5.2 | `IncompatibleExprShape` lacks `EngineIdentity` for adapter diagnostics | §4.5 `EngineError.IncompatibleExprShape(shape, engine)` | previous version had only `String shape` |
| 6.1 | `Model.of` claimed validation but performed none | §4.4.1 smart constructor returns `Either[ModelValidationError, Model]`; `ModelValidator.validate` invoked exactly once | bonus round-3 DE finding 1.1 bundles `TransformSpec` removal |
| 7.1 | `queryToolSchema` patch used Circe `Json.deepMerge`; real type is `McpSchema.JsonSchema` over `LinkedHashMap` (12 → 13 props) | §6.4 actual `props.put("engine", strProp("string"))` one-liner + schema test asserting 13 names | real change ~1 line, doc previously misrepresented it |
| 8.1 | `ExtensionValue` lacks `Null` case; JSON `"field": null` cannot round-trip | §4.4.1 `case object Null extends ExtensionValue`; canonical encoding Jackson `JsonNode.VALUE_NULL` | closes finding 12 follow-up from round-3 DE |
### 1.1 Goals
1. **Preserve the Spark user contract.** Existing fluent chains, DataFrame-returning
   terminals, streaming terminals, Scala lambdas, YAML loading, CLI names, and MCP
   response envelopes continue to work through the compatibility artifact.
2. **Create an actually portable IR.** Portable model and query nodes contain no
   `DataFrame`, `Column`, `SparkSession`, closure, logger, JDBC client, or HTTP client.
3. **Resolve schema before lowering.** A source adapter turns a `SourceRef` into a
   schema-bearing scan or a typed source-resolution failure at query-build time.
4. **Separate features from behavior.** Engine capabilities are stable facts about an
   adapter. Request policies are typed, query-scoped behavior selected by the caller.
5. **Fail closed on semantic gaps.** Missing expression/function/type/cancellation
   support returns a typed error before or during controlled execution. No adapter may
   silently substitute a different aggregate, null rule, timezone, or decimal scale.
6. **Normalize portable results.** Every non-DataFrame engine can expose a stable
   `ResultSchema` and `ResultRow` representation used by MCP and bridges.
7. **Make plans inspectable.** Explain, warnings, required capabilities, normalized
   schema, generated SQL, and bound parameters are observable without executing work.
8. **Version persistent identities.** Engine identity participates in cache/audit;
   catalog identity carries version and digest; manifest schema is explicitly versioned.
9. **Keep distributed state honest.** Portable values are transitively serializable.
   Provider thunks, cancellation handles, clients, sinks, and engine-native plans remain
   driver-local and are referenced by typed identifiers.
10. **Require falsifiable proof before expansion.** Phase 1 ends only when a vertical
    Trino proof executes a representative query, normalizes results, explains it, and
    cancels remote work on timeout.
### 1.2 Non-goals for v0.3.0
- Cross-engine federation within one query.
- Cost-based routing between registered engines.
- A portable streaming semantic model. Existing Spark Structured Streaming remains.
- Portable window-function expressions; reserved for v0.4.0.
- Portable `t.all(name)` / percent-of-total; reserved for v0.4.0.
- Portable Union/Except/Intersect; reserved for v0.4.0.
- Arbitrary native SQL, native lambdas, or native UDFs in the portable model.
- A universal optimizer or a replacement for Catalyst/Trino/Snowflake planners.
- Automatic translation of unrecoverable v1 `<lambda>` manifest expressions.
- Transparent serialization of provider thunks or engine-native execution handles.
### 1.3 Design invariants
- `semanticdf-core` compiles and resolves without Spark on its classpath.
- Every `RelOp.ResolvedScan` has a non-empty resolved schema unless the source itself
  is explicitly zero-column, which must be represented and tested.
- Policy disposition is one of applied, applied-with-warning, or rejected; never silent.
- Engine selection is explicit or server-defaulted; an explicitly unknown engine never
  falls back to another engine.
- Decimal overflow, timezone ambiguity, unsupported null behavior, and cancellation
  failure are typed errors, not warnings.
- Manifest v2 writes canonical JSON for digest and extension-size calculations.
## 2. User contract preservation matrix
The matrix retains every previous entry that remains true. “Compatible” means existing
source still has a supported path. It does not claim the internals or manifest bytes are
unchanged.

| Surface | Today | After v0.3.0 | Compatibility |
|---|---|---|---|
| `SemanticTable.toDataFrame(spark)` | `DataFrame` | delegates to `SparkEngine` | NONE: same JVM signature/result |
| `SemanticTable.toStreamingQuery(spark, opts)` | `StreamingQuery` | stays in `semanticdf-spark` | NONE |
| `SemanticTable.query(...).execute(spark)` | `DataFrame` | explicit Spark overload | NONE |
| `SemanticTable.query(...).execute()` | implicit Spark path | implicit `Engine[DataFrame]` derived from implicit Spark session | source-compatible; locked by `ExecuteAmbiguitySpec` |
| `RollupQuery.execute(spark)` | `DataFrame` | retained in Spark adapter | NONE |
| `toSemanticTable(df, name = ...)` | constructs `SemanticTable` | retained by compatibility facade | NONE |
| `Dimension("x", t => t("x"))` | Spark `Column` factory | retained in `semanticdf-spark` | NONE; not portable |
| `Dimension("x")` | absent | portable field-reference form | additive |
| CLI `semanticdf sql` | Spark-backed SQL | Spark default; optional registered engine flag later | NONE by default |
| CLI `semanticdf introspect` | model/schema report | portable metadata plus engine details when selected | additive fields only |
| CLI `semanticdf lineage` | lineage report | same command and existing fields | NONE |
| MCP `list_models` | reports models | same response; optional `engines` metadata | additive |
| MCP `describe_model` | fields/joins/manifest | same required fields; optional capability summary | additive |
| MCP `query` | Spark execution | optional `engine`; absent selects configured default | additive request member; handler changes |
| MCP `explain` | Spark semantic explain | selected engine explain; same envelope | semantics generalized, envelope preserved |
| MCP `audit_log` | audit events | events include engine identity | additive versioned event member |
| `SemanticManifest.toJson` | v1-ish flat/op-derived shape | writes `ManifestDocument` v2 by default | **WIRE CHANGE**, dual reader and explicit v1 writer during migration |
| `SemanticManifest.fromJson` | requires Spark source | v1 compatibility reader retained; v2 resolves `SourceRef` | source-compatible overload retained |
| `YamlLoader.load(path)` | `ModelRegistry` backed by Spark DataFrames | compatibility overload retained; portable loader emits models plus provider registrations | source-compatible |
| Maven `io.semanticdf:semanticdf_2.13` | one Spark-coupled JAR | compatibility facade depends on `semanticdf-spark_2.13` | dependency coordinate retained |
Manifest compatibility is deliberately not mislabeled as “NONE.” The v1 reader remains
for at least the v0.3.x line; the v2 default writer is a visible, documented change.
## 3. Architectural options
### 3.1 Option A — dispatch-only `Engine` above the current tree
```scala
trait Engine[R] {
  def compile(op: SemanticOp, context: EngineContext): ExecutionPlan[R]
  def execute(plan: ExecutionPlan[R], context: EngineContext): R
}
```
This does not work as the final boundary. `SemanticOp` currently embeds `DataFrame`,
`Column` factories, and join lambdas (`SemanticOp.scala:28-49,343-350,411-420`). Trino
cannot lower those values. A dispatch point without portable source, type, expression,
and relational representations merely relocates the Spark dependency.
**Verdict:** rejected as a complete design; retained only as the adapter-facing shape
after introducing portable data.
### 3.2 Option B — independent Trino implementation beside current Spark code
A separate Trino stack could parse YAML and reproduce query semantics without changing
Spark internals. It shortens time to one demo but duplicates calc dependency ordering,
join safety, predicates, manifests, result encoding, catalog behavior, and errors.
**Verdict:** acceptable only as the Phase-1 vertical spike. Do not make duplicated
semantics the production architecture unless the decision gate disproves Option C.
### 3.3 Option C — portable core plus adapter lowering (chosen Path A)
```text
Scala/YAML/Manifest/MCP request
              |
              v
        portable Model
              |
        QueryBuilder + SourceResolver
              |
       resolved RelOp + Expr
              |
     capability/policy validation
              |
      Engine.compile / execute
       /        |          \
    Spark      Trino     Databricks ...
```
Shared semantics live in `semanticdf-core`; engines lower only a resolved, typed plan.
Legacy Spark lambdas take a compatibility lane directly through `SparkLambdaEval` and
are never presented as portable.
**Verdict:** chosen, subject to the Phase-1 proof gate in §7.2.
### 3.4 Option D — clean-slate universal query language
A universal SQL/Catalyst/Calcite replacement would expand scope into parser design,
optimizer design, federation, cost models, and years of dialect behavior. It is not
needed to support the requested semantic query subset.
**Verdict:** rejected.
### 3.5 Boundary rule
The portable core owns meaning: names, calc dependency order, expression types,
resolved schemas, join cardinality, capability requirements, policies, result schema,
manifest shape, catalog identity, and typed errors. An engine owns native lowering,
native execution, native cancellation, native explain, and native source/catalog I/O.
## 4. The portable IR (the actual change)
The portable flow is `Model/QuerySpec -> SourceResolver -> resolved RelOp/Expr ->
capability and policy validation -> Engine.compile -> Engine.execute -> ResultEncoder`.
Core values contain no Spark type, closure, client, logger, or runtime handle.
### 4.1 Types, literals, and expressions
#### 4.1.1 `SealedDataType`, `Field`, and literals
`Cast`, source schemas, and result schemas use one portable type ADT. This replaces the
Spark-type dependence visible in `Query.scala:323-335` and `SemanticManifest.scala:1025-1118`
(findings 20-21).
```scala
sealed trait SealedDataType extends Product with Serializable
object SealedDataType {
  case object NullType extends SealedDataType
  case object BooleanType extends SealedDataType
  case object ByteType extends SealedDataType
  case object ShortType extends SealedDataType
  case object IntType extends SealedDataType
  case object LongType extends SealedDataType
  case object FloatType extends SealedDataType
  case object DoubleType extends SealedDataType
  case object StringType extends SealedDataType
  case object BinaryType extends SealedDataType
  case object DateType extends SealedDataType
  final case class DecimalType(precision: Int, scale: Int) extends SealedDataType
  final case class TimestampType(timezone: String) extends SealedDataType
  final case class ArrayType(elementType: SealedDataType) extends SealedDataType
  final case class MapType(keyType: SealedDataType, valueType: SealedDataType)
      extends SealedDataType
  final case class StructType(fields: List[Field]) extends SealedDataType
}
final case class Field(
    name: String,
    dataType: SealedDataType,
    nullable: Boolean = true,
) extends Serializable
sealed trait LiteralValue extends Product with Serializable
object LiteralValue {
  final case class IntValue(v: Int) extends LiteralValue
  final case class ByteValue(v: Byte) extends LiteralValue; final case class ShortValue(v: Short) extends LiteralValue
  final case class LongValue(v: Long) extends LiteralValue
  final case class FloatValue(v: Float) extends LiteralValue
  final case class DoubleValue(v: Double) extends LiteralValue
  final case class DecimalValue(v: BigDecimal) extends LiteralValue
  final case class StringValue(v: String) extends LiteralValue
  final case class BoolValue(v: Boolean) extends LiteralValue
  final case class BinaryValue(v: Vector[Byte]) extends LiteralValue
  final case class TimestampValue(v: java.time.Instant) extends LiteralValue
  final case class DateValue(v: java.time.LocalDate) extends LiteralValue
  final case class ArrayValue(values: List[LiteralValue]) extends LiteralValue
  final case class MapValue(values: List[(LiteralValue, LiteralValue)]) extends LiteralValue
  final case class StructValue(fields: List[(String, LiteralValue)]) extends LiteralValue
  case object NullValue extends LiteralValue
}
```
Smart constructors enforce non-empty/unique field names, valid IANA zones,
`1 <= precision <= 38`, `0 <= scale <= precision`, non-null map keys, and literal/type
compatibility. Decimal literals are never inferred from `Double`; timestamps are
`Instant` values interpreted using the declared timezone.
#### 4.1.2 Portable `Expr`
```scala
sealed trait Expr extends Product with Serializable
object Expr {
  final case class ColumnRef(name: String) extends Expr
  final case class MeasureRef(name: String) extends Expr
  final case class Literal(value: LiteralValue, dataType: SealedDataType) extends Expr
  final case class Compare(left: Expr, op: CompareOp, right: Expr) extends Expr
  final case class Not(inner: Expr) extends Expr
  final case class And(parts: List[Expr]) extends Expr
  final case class Or(parts: List[Expr]) extends Expr
  final case class IsNull(inner: Expr, negated: Boolean = false) extends Expr
  final case class In(field: Expr, values: List[Expr], negated: Boolean = false) extends Expr
  final case class Between(value: Expr, lowerInclusive: Expr, upperInclusive: Expr) extends Expr
  final case class Contains(field: Expr, needle: Expr) extends Expr
  final case class StartsWith(field: Expr, prefix: Expr) extends Expr
  final case class EndsWith(field: Expr, suffix: Expr) extends Expr
  final case class ArrayContains(field: Expr, value: Expr) extends Expr
  final case class Add(parts: List[Expr]) extends Expr
  final case class Multiply(parts: List[Expr]) extends Expr
  final case class Subtract(left: Expr, right: Expr) extends Expr
  final case class Divide(left: Expr, right: Expr) extends Expr
  final case class Negate(inner: Expr) extends Expr
  final case class Coalesce(parts: List[Expr]) extends Expr
  final case class CaseWhen(branches: List[(Expr, Expr)], otherwise: Option[Expr]) extends Expr
  final case class Cast(inner: Expr, targetType: SealedDataType) extends Expr
}
sealed trait CompareOp extends Product with Serializable
object CompareOp {
  case object Eq extends CompareOp; case object Ne extends CompareOp
  case object Lt extends CompareOp; case object Le extends CompareOp
  case object Gt extends CompareOp; case object Ge extends CompareOp
  case object NullSafeEq extends CompareOp
}
```
`Between` is inclusive at both ends; adapters may lower it to `>=` and `<=`. Empty
string remains non-null. Null comparisons retain SQL three-valued logic.
#### 4.1.3 Explicit v0.4.0 deferrals
Existing Spark lambda analytic behavior and Spark percent-of-total behavior remain in
the compatibility lane. Portable v0.3.0 contains neither their speculative expression
ADTs nor a lowering contract. `QueryBuilder` rejects requests for these features with
`FeatureDeferred("v0.4.0")`; Trino cannot compute them until a v0.4 RFC defines their
shape, null/order semantics, and capability negotiation. This closes round-2 findings
18-19 without carrying phantom types into the v0.3.0 contract. The reserved names and
all six set-operation variants are listed only in §11, with the explicit removal
rationale required by karpathy-guidelines §2.
### 4.2 Relational IR and schema-bearing scans
The previous `Scan(source, projection)` had no schema. The selected `SourceResolver`
produces fields at query-build time; only then does the builder emit `ResolvedScan`.
The current Spark leaf obtains schema from its embedded DataFrame
(`SemanticOp.scala:28-49,343-350`), which is not portable (finding 5).
```scala
sealed trait RelOp extends Product with Serializable
object RelOp {
  final case class ResolvedScan(
      source: SourceRef,
      schema: List[Field],
      projection: List[Expr],
  ) extends RelOp
  final case class Filter(input: RelOp, predicate: Expr) extends RelOp
  final case class Project(input: RelOp, expressions: List[(Expr, String)]) extends RelOp
  final case class Aggregate(
      input: RelOp,
      groupBy: List[Expr],
      aggregates: List[AggregateCall],
  ) extends RelOp
  final case class Join(left: RelOp, right: RelOp, kind: JoinKind, condition: Expr)
      extends RelOp
  final case class Sort(input: RelOp, keys: List[SortKey]) extends RelOp
  final case class Limit(input: RelOp, count: Long, offset: Long = 0L) extends RelOp
}
sealed trait JoinKind extends Product with Serializable
object JoinKind {
  case object Inner extends JoinKind; case object Left extends JoinKind
  case object Right extends JoinKind; case object Full extends JoinKind
  case object Cross extends JoinKind
}
final case class SortKey(
    expression: Expr,
    direction: SortDirection,
    nullOrdering: NullOrdering,
) extends Serializable
sealed trait SortDirection extends Product with Serializable
object SortDirection { case object Ascending extends SortDirection; case object Descending extends SortDirection }
sealed trait NullOrdering extends Product with Serializable
object NullOrdering { case object First extends NullOrdering; case object Last extends NullOrdering }
```
Aggregate support is per function (finding 23; current rollup closure is only Sum/Count
at `rollup/Rollup.scala:219-262`):
```scala
sealed trait AggregateFn extends Product with Serializable
object AggregateFn {
  case object Sum extends AggregateFn; case object Count extends AggregateFn
  case object CountDistinct extends AggregateFn; case object Avg extends AggregateFn
  case object Min extends AggregateFn; case object Max extends AggregateFn
  case object StddevSample extends AggregateFn; case object StddevPopulation extends AggregateFn
  case object VarianceSample extends AggregateFn; case object VariancePopulation extends AggregateFn
  case object Median extends AggregateFn
  case object PercentileContinuous extends AggregateFn
  case object PercentileDiscrete extends AggregateFn
  case object ApproxPercentile extends AggregateFn
  case object First extends AggregateFn; case object Last extends AggregateFn
}
final case class AggregateCall(
    fn: AggregateFn,
    input: Option[Expr],
    alias: String,
    distinct: Boolean = false,
    arguments: List[LiteralValue] = Nil,
) extends Serializable
```
Percentile/accuracy live in `arguments`; capability ids stay finite. Exact median is
never replaced by approximate percentile.
Set-operation support is absent from the v0.3.0 relational IR. This carries forward
the current node inventory (`SemanticOp.scala:343-1139`) and is not a regression
(round-2 finding 22). The speculative ADT and all six cases were removed in v0.3.0 per
karpathy §2 and review round-2 finding 22; the reservation is recorded only in §11.
### 4.3 Sources, providers, resolver lifecycle, and rollups
#### 4.3.1 Typed references
```scala
sealed trait SourceRef extends Product with Serializable
object SourceRef {
  final case class ByName(
      catalog: Option[String], namespace: Option[String], table: String,
  ) extends SourceRef
  final case class ByPath(
      format: String, path: String, options: Map[String, String] = Map.empty,
  ) extends SourceRef
  final case class ByProvider(provider: ProviderRef) extends SourceRef
}
sealed trait ProviderRef extends Product with Serializable
object ProviderRef {
  final case class DataFrameSource(
      name: String, schemaHint: Option[List[Field]] = None,
  ) extends ProviderRef                 // () => DataFrame
  final case class TableResolver(name: String) extends ProviderRef // String => DataFrame
}
```
There is no `kind: String`. DataFrame-specific provider refs return
`IncompatibleEngine` on non-Spark engines.
#### 4.3.2 Separate `SourceResolver`
```scala
trait SourceResolver extends Serializable {
  def resolve(source: SourceRef, identity: EngineIdentity): ResolvedSource
}
sealed trait ResolvedSource extends Product with Serializable
object ResolvedSource {
  final case class ResolvedScan(
      schema: List[Field], statistics: Option[SourceStats],
  ) extends ResolvedSource
  final case class IncompatibleEngine(reason: String) extends ResolvedSource
  final case class AuthFailed(reason: String) extends ResolvedSource
  case object NotFound extends ResolvedSource
}
final case class SourceStats(
    estimatedRows: Option[Long], estimatedBytes: Option[Long],
) extends Serializable
```
`QueryBuilder` maps this sealed result to `RelOp.ResolvedScan` or typed build errors.
Resolution runs on the driver before lowering. A source digest detects build/execute
schema drift and yields `SourceSchemaChanged`; no adapter silently casts. This closes
finding 15, grounded in `SemanticOp.scala:343-350`.
#### 4.3.3 Registry ownership and provider lookup
```scala
sealed trait LookupResult[+A]
object LookupResult {
  final case class Found[A](thunk: A) extends LookupResult[A]
  case object NotFound extends LookupResult[Nothing]
  final case class Unavailable(reason: String) extends LookupResult[Nothing]
}
final case class SparkProviderRegistry(
    dataFrameSources: Map[String, () => DataFrame] = Map.empty,
    tableResolvers: Map[String, String => DataFrame] = Map.empty,
) {
  def register(ref: ProviderRef.DataFrameSource, f: () => DataFrame) =
    copy(dataFrameSources = dataFrameSources + (ref.name -> f))
  def lookup(ref: ProviderRef.DataFrameSource): LookupResult[() => DataFrame] =
    dataFrameSources.get(ref.name).map(LookupResult.Found(_))
      .getOrElse(LookupResult.NotFound)
}
trait EngineRegistry extends Serializable
final case class SparkRuntimeRegistry(
    providers: SparkProviderRegistry,
    sourceResolvers: Map[String, SourceResolver] = Map.empty,
) extends EngineRegistry
final class TrinoRuntimeRegistry(
    val sourceResolvers: Map[String, SourceResolver],
    val catalogs: Map[String, CatalogAdapter],
    val resultCaches: Map[String, ResultCache],
    val cancellationClients: Map[String, TrinoStatementClient],
) extends EngineRegistry {
  def resolver(name: String): Either[EngineError, SourceResolver] =
    sourceResolvers.get(name).toRight(EngineError.ConnectionFailed("resolver unavailable: " + name))
  def catalog(name: String): Either[CatalogError, CatalogAdapter] =
    catalogs.get(name).toRight(CatalogError.Unsupported("catalog unavailable: " + name))
}
```
Lifecycle (finding 8; current analogue `rollup/Rollup.scala:320-353`):
- owner: engine-side `EngineRegistry`, populated by user/framework (`YamlLoader`);
- concurrency: immutable; every `register` returns a new registry;
- cluster: JVM-local on drivers; provider thunks never ship to executors;
- lookup: exactly `Found(thunk)`, `NotFound`, or `Unavailable(reason)`;
- invocation exceptions become `ProviderInvocationFailed`; fatal JVM errors propagate;
- manifests/MCP may name only deployment-allow-listed refs and cannot register code.
Portable rollups carry no precomputed fields. Registration computes them through the
provider on the engine side; current constructor-time provider/count behavior
(`rollup/Rollup.scala:55-100`) is intentionally removed.
```scala
final case class RollupSpec(
    name: String,
    baseModel: String,
    dimensions: List[String],
    measures: List[RollupMeasureSpec],
    freshness: RollupFreshnessSpec,
) extends Serializable
final case class RollupPrecompute(
    rowCount: Option[Long], columns: Set[String], sourceDigest: Option[String],
)
final case class RollupRegistration(
    spec: RollupSpec, provider: ProviderRef, precomputed: RollupPrecompute,
)
```
`ManifestDocument` stores `RollupSpec` only. Missing registration is `NotFound`, not a
stale portable statistic. `ManualRollupSpec` is rewritten at the §6 registration
boundary into `RollupSpec`; the rewrite copies name/base model/dimensions/measures and
freshness, resolves the provider by ref, and computes `RollupPrecompute` only on the
selected engine. It never puts a provider thunk or engine-native statistic in the
portable model (round-2 finding 8 / M10).
### 4.4 Pure model, manifest v2, and bounded extensions
#### 4.4.1 Model and closed extensions
```scala
final case class CalculatedMeasure(name: String, expr: Expr)
    extends Product with Serializable

// `TransformSpec` is removed from v0.3.0 entirely. The round-1 phantom ADT
// had no cases and no portable behavior; dropping it from `Model` (and from
// `ManifestDocument` in §4.4) means portable models have no `transforms`
// member. A Spark compatibility constructor may retain a legacy
// `SemanticScope => Column` field outside this class. Closes round-3 DE
// finding 1.1.

final case class ModelPolicyDefaults(
    materialize: MaterializePolicy, cache: CachePolicy, audit: AuditPolicy,
) extends Product with Serializable
final class Model private[model] (
    val name: String,
    val description: Option[String],
    val source: SourceRef,
    val dimensions: List[Dimension],
    val measures: List[Measure],
    val calculatedMeasures: List[CalculatedMeasure],
    val joins: List[JoinSpec],
    val filters: List[FilterSpec],
    val version: Int,
    val status: ModelStatus,
    val rollups: List[RollupSpec],
    val defaultPolicies: ModelPolicyDefaults,
    val extensions: Map[String, ExtensionValue],
) extends Serializable
sealed trait ExtensionValue extends Product with Serializable
object ExtensionValue {
  // Canonical encoding: Jackson `JsonNode.VALUE_NULL`. A JSON member written
  // as `"field": null` round-trips to `Null`, never to absence (absence is a
  // different wire state). Without this case, fields explicitly set to null
  // would lose information on read because they share no value with Nil.
  // Closes round-3 DE finding 8.1.
  case object Null extends ExtensionValue
  final case class String(v: scala.Predef.String) extends ExtensionValue
  final case class Bool(v: Boolean) extends ExtensionValue
  final case class Number(v: BigDecimal) extends ExtensionValue
  final case class List(items: scala.List[ExtensionValue]) extends ExtensionValue
  final case class Object(
      fields: scala.collection.immutable.Map[scala.Predef.String, ExtensionValue],
  ) extends ExtensionValue
}

// Smart constructor returns Either and runs validation exactly once. The
// previous one-liner claim (validate names, collisions, references, calc
// cycles, types, policy defaults, extension limits) was documentation only:
// there was no validation. `Model.of` now follows the
// `scala-data-driven-refactor` pattern: a single private validator decides
// validity once at the boundary, and the model is unbuildable otherwise.
// Closes round-3 DE finding 6.1.
sealed trait ModelValidationError extends Product with Serializable
object ModelValidationError {
  final case class InvalidName(reason: String) extends ModelValidationError
  final case class DuplicateMember(kind: String, name: String) extends ModelValidationError
  final case class UnknownReference(referent: String, target: String) extends ModelValidationError
  final case class CalcCycle(names: List[String]) extends ModelValidationError
  final case class CalcDepthExceeded(depth: Int, max: Int) extends ModelValidationError
  final case class InvalidExpr(reason: String) extends ModelValidationError
  final case class ExtensionEnvelopeExceeded(fieldCount: Int, byteCount: Int)
      extends ModelValidationError
  final case class InvalidPolicyDefault(reason: String) extends ModelValidationError
}

object Model {
  def of(
      name: String,
      source: SourceRef,
      dimensions: List[Dimension],
      measures: List[Measure],
      calculatedMeasures: List[CalculatedMeasure] = Nil,
      joins: List[JoinSpec] = Nil,
      filters: List[FilterSpec] = Nil,
      rollups: List[RollupSpec] = Nil,
      defaultPolicies: ModelPolicyDefaults = ModelPolicyDefaults(
        MaterializePolicy.None, CachePolicy.NoCache, AuditPolicy.NoAudit),
      extensions: Map[String, ExtensionValue] = Map.empty,
      version: Int = 1,
      status: ModelStatus = ModelStatus.Draft,
  ): Either[ModelValidationError, Model] =
    ModelValidator.validate(
      name = name, source = source, dimensions = dimensions,
      measures = measures, calculatedMeasures = calculatedMeasures,
      joins = joins, filters = filters, rollups = rollups,
      extensions = extensions, defaultPolicies = defaultPolicies,
    ).map { _ =>
      new Model(name, None, source, dimensions, measures,
        calculatedMeasures, joins, filters, version, status, rollups,
        defaultPolicies, extensions)
    }

  // Lightweight constructor (no validation) for trusted internal callers
  // (e.g. v1 reader after migration). Production callers must use `of`.
  private[model] def unsafe(
      name: String, source: SourceRef, dimensions: List[Dimension],
      measures: List[Measure], calculatedMeasures: List[CalculatedMeasure],
      joins: List[JoinSpec], filters: List[FilterSpec],
      rollups: List[RollupSpec], defaultPolicies: ModelPolicyDefaults,
      extensions: Map[String, ExtensionValue], version: Int, status: ModelStatus,
  ): Model =
    new Model(name, None, source, dimensions, measures, calculatedMeasures,
      joins, filters, version, status, rollups, defaultPolicies, extensions)
}

private[model] object ModelValidator {
  // Single validation pass invoked exactly once by `Model.of`. Order:
  // (1) name is non-blank
  // (2) no duplicate dimension / measure / calc-measure names
  // (3) every calc-measure refers to a declared measure or column reference
  // (4) calc DAG is acyclic; reported depth <= Capability.MaxCalcDepth (caller)
  // (5) extension envelope: inline JSON <= 8 KiB, <= 16 fields (recursive)
  // (6) policy defaults are well-formed (typed refs resolve later, not here)
  def validate(
      name: String, source: SourceRef, dimensions: List[Dimension],
      measures: List[Measure], calculatedMeasures: List[CalculatedMeasure],
      joins: List[JoinSpec], filters: List[FilterSpec],
      rollups: List[RollupSpec], extensions: Map[String, ExtensionValue],
      defaultPolicies: ModelPolicyDefaults,
  ): Either[ModelValidationError, Unit] = {
    val trimmed = Option(name).map(_.trim).getOrElse("")
    if (trimmed.isEmpty)
      Left(ModelValidationError.InvalidName("name is blank"))
    else {
      val declaredMeasures: Set[String] = measures.iterator.map(_.name).toSet
      val declaredDimensions: Set[String] = dimensions.iterator.map(_.name).toSet
      val dupCheck = declaredMeasures
        .iterator.filter(declaredDimensions.contains).toList
      if (dupCheck.nonEmpty)
        Left(ModelValidationError.DuplicateMember(
          "dimension/measure", dupCheck.head))
      else {
        val calcNames: Set[String] =
          calculatedMeasures.iterator.map(_.name).toSet
        // (3) references
        val unresolvedRef = calculatedMeasures.iterator.flatMap { cm =>
          collectMeasureRefs(cm.expr).iterator.filterNot(declaredMeasures.contains)
        }.toList
        if (unresolvedRef.nonEmpty)
          Left(ModelValidationError.UnknownReference(
            "calculatedMeasures", unresolvedRef.head))
        else {
          // (4) cycle + depth (delegated to portable-calc-depth helper).
          // Note: `Capability.MaxCalcDepth` is a type alias (= Int); the
          // runtime cap is the requested policy ceiling, passed by the caller
          // (engine compile time), not the validator. A model with no port
          // rejects depth > the maximum reasonable bound (Int.MaxValue) only
          // when paired with an engine capability later; the constructor
          // itself rejects cycles and unresolved references.
          val maxDepthBound = Int.MaxValue
          CalcGraph.checkAcyclicAndDepth(
            calcNames, calculatedMeasures, maxDepthBound)
            .left.map(ModelValidationError.CalcDepthExceeded(_, maxDepthBound))
            .flatMap(_ =>
              // (5) extension limits
              ExtensionLimits.check(extensions)
                .toEither(e => ModelValidationError.ExtensionEnvelopeExceeded(
                  e.fieldCount, e.byteCount))
                // (6) default policies always present; typed refs checked at
                // use site, not at construction.
                .map(_ => Right(()))
                .flatten
            )
        }
      }
    }
  }

  private def collectMeasureRefs(e: Expr): List[String] = e match {
    case Expr.MeasureRef(n)         => List(n)
    case Expr.ColumnRef(_)          => Nil
    case Expr.Literal(_, _)         => Nil
    case Expr.Not(inner)            => collectMeasureRefs(inner)
    case Expr.Compare(l, _, r)      => collectMeasureRefs(l) ++ collectMeasureRefs(r)
    case Expr.And(parts)            => parts.flatMap(collectMeasureRefs)
    case Expr.Or(parts)             => parts.flatMap(collectMeasureRefs)
    case Expr.IsNull(inner, _)      => collectMeasureRefs(inner)
    case Expr.In(field, vs, _)      =>
      collectMeasureRefs(field) ++ vs.flatMap(collectMeasureRefs)
    case Expr.Between(v, lo, hi)    =>
      collectMeasureRefs(v) ++ collectMeasureRefs(lo) ++ collectMeasureRefs(hi)
    case Expr.Contains(f, n)        => collectMeasureRefs(f) ++ collectMeasureRefs(n)
    case Expr.StartsWith(f, p)      => collectMeasureRefs(f) ++ collectMeasureRefs(p)
    case Expr.EndsWith(f, s)        => collectMeasureRefs(f) ++ collectMeasureRefs(s)
    case Expr.ArrayContains(f, v)   => collectMeasureRefs(f) ++ collectMeasureRefs(v)
    case Expr.Add(parts)            => parts.flatMap(collectMeasureRefs)
    case Expr.Multiply(parts)       => parts.flatMap(collectMeasureRefs)
    case Expr.Subtract(l, r)        => collectMeasureRefs(l) ++ collectMeasureRefs(r)
    case Expr.Divide(l, r)          => collectMeasureRefs(l) ++ collectMeasureRefs(r)
    case Expr.Negate(inner)         => collectMeasureRefs(inner)
    case Expr.Coalesce(parts)       => parts.flatMap(collectMeasureRefs)
    case Expr.CaseWhen(bs, ow)      =>
      bs.flatMap { case (g, t) => collectMeasureRefs(g) ++ collectMeasureRefs(t) } ++
        ow.toList.flatMap(collectMeasureRefs)
    case Expr.Cast(inner, _)        => collectMeasureRefs(inner)
  }
}
object Dimension {
  def field(name: String, dataType: SealedDataType): Dimension =
    Dimension(name, Expr.ColumnRef(name), Some(dataType))
}
object Measure {
  def aggregate(name: String, fn: AggregateFn, expr: Expr): Measure =
    Measure(name, AggregateCall(fn, Some(expr), name), portable = true)
}
```
The portable constructor has no `transforms` field at all: `TransformSpec` was
removed in this revision and the v2 manifest does not carry one
(round-3 DE finding 1.1). A Spark compatibility constructor may retain legacy
lambdas outside this class. `Model.of` returns `Either[ModelValidationError, Model]`
and invokes `ModelValidator.validate` exactly once; validity is enforced at the
boundary, not silently assumed. The validator covers names, dimension/measure
collisions, calc measure references, calc cycle detection, extension envelope
limits, and policy-default presence; type check of typed refs happens at use site.
`CalculatedMeasure` is concrete, and `TransformSpec` no longer exists. Legacy
`SemanticScope => Column` values (`Model.scala:278-281`) remain in
`semanticdf-spark` and are not portable.
`ExtensionValue` is closed—no `Any`, class tag, callback, or engine object (finding 12).
```
```scala
final case class ExternalExtensionBlob(
    digest: String, uri: java.net.URI, byteLength: Long,
    mediaType: String = "application/vnd.semanticdf.extensions+json",
)
final case class ExtensionEnvelope(
    inline: Map[String, ExtensionValue],
    external: Option[ExternalExtensionBlob],
)
object ExtensionLimits {
  val MaxInlineBytes = 8 * 1024
  val MaxFields = 16
}
```
Limits apply to canonical UTF-8 JSON; the 16 fields are counted recursively. Larger
payloads are fully externalized as content-addressed blobs; catalog properties retain
digest, URI, length, and media type. Reads allow-list URI scheme/host and verify size and
digest. Failure rejects publication; data is never truncated (finding 14;
`SemanticManifest.scala:893-1021`).
#### 4.4.2 Acknowledged manifest wire change: concrete before/after
The 1590-line current serializer derives and reconstructs Spark/op-tree state
(`SemanticManifest.scala:1-1590,893-1118`); a flat portable `Model` cannot replicate it.
Current/v1 (abridged):
```json
{
  "schemaVersion": "v0.1.11-manifest",
  "kind": "semanticdf-model-manifest",
  "model": {"name":"orders","version":7,"status":"published",
            "sourceTable":"silver.orders"},
  "digest": {"dimensions":1,"measures":1,"calcMeasures":0,
             "joins":0,"filters":0,"isStreaming":false,"usesTAll":false},
  "dimensions": [{"name":"region","kind":"string","expr":"region"}],
  "measures": [{"name":"amount","kind":"base","expr":"sum(amount)",
                "dependsOn":[]}],
  "joins": [], "filters": [],
  "runtime": {"maxRows":10000,"salt":5}
}
```
V2:
```json
{
  "$schema": "https://semanticdf.io/schemas/manifest-v2.schema.json",
  "schemaVersion": 2, "schemaRevision": 0, "kind": "semanticdf.model",
  "documentId": "com.acme.analytics.orders",
  "model": {
    "name":"orders", "version":7, "status":"published",
    "source":{"type":"by_name","catalog":"hive",
              "namespace":"silver","table":"orders"},
    "dimensions":[{"name":"region","dataType":{"type":"string"},
      "expression":{"type":"column_ref","name":"region"}}],
    "measures":[{"name":"amount","aggregate":"sum",
      "expression":{"type":"column_ref","name":"amount"}}],
    "calculatedMeasures":[], "joins":[], "filters":[],
    "rollups":[],
    "defaultPolicies":{"materialize":{"type":"none"},
      "cache":{"type":"no_cache"},"audit":{"type":"no_audit"}},
    "extensions":{}
  },
  "digest":"sha256:7a..."
}
```
Moved op-derived fields: top-level `dimensions`, `measures`, `joins`, and
`filters` become `model.*`; calculated measures become a typed collection; source
becomes `model.source`; runtime defaults become `model.defaultPolicies`; rollup metadata
becomes `model.rollups`. Providers/precompute/live sinks never enter the wire. The
digest is canonical model identity, not merely counts (finding 4;
`RollupSerializationSpec.scala:1-217` remains metadata purity proof, not wire proof).
#### 4.4.3 Versioned document and JSON Schema
```scala
final case class ManifestDocument(
    schemaVersion: Int,
    schemaRevision: Int,
    kind: String,
    documentId: String,
    generatedAt: java.time.Instant,
    model: Model,
    digest: String,
    unknownFields: Map[String, ExtensionValue] = Map.empty,
) extends Serializable
trait JsonSchema[A] {
  def schemaVersion: Int
  def schemaRevision: Int
  def resourcePath: String
  def validateJson(bytes: Array[Byte]): Either[List[SchemaViolation], Unit]
  def read(bytes: Array[Byte]): Either[ManifestError, A]
  def write(value: A): Array[Byte]
}
object ManifestDocument {
  val V2Schema: JsonSchema[ManifestDocument] =
    new ManifestV2JsonSchema("/schemas/manifest-v2.schema.json")
}
```
Rules: separate v1/v2 readers; unknown major rejected; revision may add optional members
but meaning/removal/closed-enum changes require a new major; unknown members are
preserved, but unknown expression/type discriminators are rejected; JSON Schema runs
before semantic construction; canonical digest excludes time/digest and includes model,
document id, version/status/defaults/rollups/extensions. `fromJson(text, source)` remains
for v1 Spark; v2 resolves its `SourceRef`. V1 writing is temporarily explicit and
rejects unrepresentable values instead of inventing `<lambda>`. Golden migration,
joined-model, rollup, passthrough, schema, and digest tests are required.
### 4.5 Engine contracts, policies, capabilities, plans, and results
```scala
// `EngineError` is a sealed ADT value, NOT a Throwable: it is a structured
// return that flows through `Either[EngineError, T]` and is mapped to
// MCP `ErrorDetail` at the boundary, not thrown. Closes round-3 DE finding 3.1.
sealed trait EngineError extends Product with Serializable
object EngineError {
  final case class UnsupportedCapability(capability: Capability, reason: String)
      extends EngineError
  // Closes round-3 DE finding 5.2: now carries the selected engine identity so
  // diagnostics name the offending adapter, not just the shape.
  final case class IncompatibleExprShape(shape: String, engine: EngineIdentity)
      extends EngineError
  // Closes round-3 DE finding 5.1: bound parameters preserve declared
  // precision/scale; lossless-cast failure becomes a typed error, never a
  // silently rounded value. See §5.4 Trino quirk "decimal overflow."
  final case class DecimalOverflow(
      value: BigDecimal, targetPrecision: Int, targetScale: Int,
  ) extends EngineError
  final case class FeatureDeferred(feature: String, release: String) extends EngineError
  final case class CancellationFailed(reason: String) extends EngineError
  final case class ConnectionFailed(reason: String) extends EngineError
  final case class QueryTimedOut(cancelStatus: String) extends EngineError
  final case class AuditSinkUnavailable(name: String) extends EngineError
  final case class ProviderInvocationFailed(name: String, reason: String) extends EngineError
  final case class SourceSchemaChanged(source: String) extends EngineError
  final case class EngineUnavailable(name: String, available: List[String], wasDefault: Boolean)
      extends EngineError
  // Exhaustive mapper used by `Query.toErrorDetail` to ensure every error case
  // reaches the MCP envelope (see §6.4). Compiler enforces coverage on add.
  def toErrorDetail(err: EngineError): io.modelcontextprotocol.spec.McpSchema.ErrorDetail =
    err match {
      case UnsupportedCapability(c, reason)    => ErrorDetail("unsupported_capability", s"${c.value}: $reason")
      case IncompatibleExprShape(s, e)         => ErrorDetail("incompatible_expr_shape", s"$s [engine=${e.name}]")
      case DecimalOverflow(v, p, sc)           => ErrorDetail("decimal_overflow", s"$v does not fit DECIMAL($p,$sc)")
      case FeatureDeferred(f, r)               => ErrorDetail("feature_deferred", s"$f deferred to $r")
      case CancellationFailed(reason)          => ErrorDetail("cancellation_failed", reason)
      case ConnectionFailed(reason)            => ErrorDetail("connection_failed", reason)
      case QueryTimedOut(cancelStatus)         => ErrorDetail("query_timed_out", cancelStatus)
      case AuditSinkUnavailable(name)          => ErrorDetail("audit_sink_unavailable", name)
      case ProviderInvocationFailed(n, reason)  => ErrorDetail("provider_invocation_failed", s"$n: $reason")
      case SourceSchemaChanged(source)         => ErrorDetail("source_schema_changed", source)
      case EngineUnavailable(name, _, wasDef)  => ErrorDetail("engine_unavailable", s"$name (wasDefault=$wasDef)")
    }
}
sealed trait CatalogError extends Product with Serializable
object CatalogError {
  final case class Conflict(reason: String, current: Option[CatalogRef]) extends CatalogError
  final case class Unauthorized(reason: String) extends CatalogError
  final case class Network(reason: String) extends CatalogError
  final case class Unsupported(reason: String) extends CatalogError
  final case class MalformedManifest(reason: String) extends CatalogError
}
sealed trait Capability extends Product with Serializable
object Capability {
  final case class Named(value: String) extends Capability
  type MaxCalcDepth = Int
  type AggregateFunctions = Set[AggregateFn]
  type SupportedExpr = Set[ExprShape]
  type LateBinding = Boolean
}

final case class EngineIdentity(name: String, nativeVersion: String,
    engineAdapterVersion: String) extends Product with Serializable

final case class ExplainResult(
    engine: EngineIdentity,
    logicalPlan: String,
    nativePlan: Option[String],
    sql: Option[String],
    warnings: List[EngineWarning],
) extends Product with Serializable
```
These closed failures are the missing round-2 H2 definitions. Compile and execute paths
must return `EngineError`; catalog adapters must return `CatalogError`. `Capability.Named`
keeps unsupported-capability diagnostics typed without creating speculative per-feature
classes.
#### 4.5.1 Engine and typed request context
```scala
trait Engine[R] {
  def identity: EngineIdentity
  def capabilities: EngineCapabilities
  def compile(plan: RelOp, ctx: EngineContext): Either[EngineError, ExecutionPlan[R]]
  def execute(plan: ExecutionPlan[R], ctx: EngineContext): Either[EngineError, R]
  def explain(plan: ExecutionPlan[R]): ExplainResult
}
sealed trait StorageLevel extends Product with Serializable
object StorageLevel {
  case object MemoryOnly extends StorageLevel
  case object MemoryAndDisk extends StorageLevel
  case object DiskOnly extends StorageLevel
}
sealed trait MaterializePolicy extends Product with Serializable
object MaterializePolicy {
  case object None extends MaterializePolicy
  final case class Persist(level: StorageLevel) extends MaterializePolicy
  case object Cache extends MaterializePolicy
}
final case class CacheRef(name: String) extends Product with Serializable
final case class AuditSinkRef(name: String) extends Product with Serializable
sealed trait CachePolicy extends Product with Serializable
object CachePolicy {
  case object NoCache extends CachePolicy
  final case class ReadThrough(cache: CacheRef) extends CachePolicy
  final case class WriteThrough(cache: CacheRef) extends CachePolicy
}
sealed trait AuditPolicy extends Product with Serializable
object AuditPolicy {
  case object NoAudit extends AuditPolicy
  final case class EmitEvents(sinkRef: AuditSinkRef) extends AuditPolicy
}
sealed trait JoinStrategy extends Product with Serializable
object JoinStrategy {
  case object Broadcast extends JoinStrategy
  case object ShuffleHash extends JoinStrategy
  case object SortMerge extends JoinStrategy
}
final case class JoinHints(
    broadcastRightBelowBytes: Option[Long] = None,
    skewFactor: Option[Int] = None,
    preferredStrategy: Option[JoinStrategy] = None,
) extends Product with Serializable
sealed trait CancellationCapability extends Product with Serializable
object CancellationCapability {
  final case class Cooperative(requestId: String) extends CancellationCapability
  final case class SparkJobTag(requestId: String) extends CancellationCapability
  final case class RemoteStatement(requestId: String) extends CancellationCapability
  case object Unsupported extends CancellationCapability
}
sealed trait CancellationMode extends Product with Serializable
object CancellationMode {
  case object Cooperative extends CancellationMode
  case object SparkJobTag extends CancellationMode
  case object RemoteStatement extends CancellationMode
  case object Unsupported extends CancellationMode
}
final case class EngineContext(
    materializePolicy: MaterializePolicy,
    cachePolicy: CachePolicy,
    auditPolicy: AuditPolicy,
    joinHints: JoinHints,
    timeout: scala.concurrent.duration.Duration,
    cancellation: CancellationCapability,
) extends Product with Serializable
```
`CancellationCapability` is the request's selected mechanism; `CancellationMode` is the
engine's advertised set. The nested `Kind` hierarchy was removed in v0.3.0 per
karpathy §2 and round-2 finding M3. Streaming output policy ADTs were also removed in
v0.3.0 per karpathy §2 and the round-2 phantom-ADT finding; their reservation is only in
§11.
`ReadThrough` returns a hit or executes and populates a miss; `WriteThrough` bypasses reads and replaces the entry only after successful execution. This models behavior currently spread over `SemanticTableCore.scala:80-220` and `SemanticTable.scala:109-298` (findings 1-2).
Typed refs resolve through the driver registry; runtime objects never enter core data.
#### 4.5.2 Structured capabilities, not booleans
```scala
sealed trait ExprShape extends Product with Serializable
object ExprShape {
  case object References extends ExprShape; case object Literals extends ExprShape
  case object Comparison extends ExprShape; case object BooleanLogic extends ExprShape
  case object NullChecks extends ExprShape; case object StringFunctions extends ExprShape
  case object ArrayFunctions extends ExprShape; case object Arithmetic extends ExprShape
  case object CaseWhen extends ExprShape; case object Cast extends ExprShape
  case object AnalyticFrame extends ExprShape; case object Subquery extends ExprShape
  case object PercentOfTotal extends ExprShape
}
sealed trait EngineFeature extends Product with Serializable
object EngineFeature {
  case object StreamingRead extends EngineFeature; case object StreamingWrite extends EngineFeature
  case object RollupRouting extends EngineFeature; case object TimeGrain extends EngineFeature
  case object NativeUdf extends EngineFeature; case object PlannerHints extends EngineFeature
  case object NativePersistence extends EngineFeature; case object AqeSkewHandling extends EngineFeature
  case object SparkLambdaEval extends EngineFeature // Spark-only, not portable API
}
final case class CapabilitySet(
    maxCalcDepth: Capability.MaxCalcDepth,
    aggregateFunctions: Capability.AggregateFunctions,
    supportedExpr: Capability.SupportedExpr,
    lateBinding: Capability.LateBinding,
    features: Set[EngineFeature],
) extends Product with Serializable
final case class EngineCapabilities(
    query: CapabilitySet,
    cancellationCapability: Set[CancellationMode],
) extends Product with Serializable
```
Depth 0 means no calculated measures. The algorithm is the longest path in the
calculated-measure DAG, counting transitive `Expr.MeasureRef` references: **depth = max
length of `MeasureRef → Expr → MeasureRef → …` chain in a single query's calc DAG.**
SQL engines with subqueries and CTEs publish `maxCalcDepth = Int.MaxValue`; the limit is
still bounded by validation of cyclic graphs. Functions and expression shapes are
individually declared, late binding is explicit, and no v0.3 capability field references
a removed speculative ADT (round-2 findings 7, 22, M4, M8). `NativeLambdas` is dropped.
`SparkLambdaEval` preserves current closures (`Model.scala:278-281`) but a portable query
cannot require it (findings 7,24; calc behavior is grounded in `SemanticOp.scala:1139-1455`).
#### 4.5.3 Policy disposition and cancellation
```scala
sealed trait PolicyDisposition extends Product with Serializable
object PolicyDisposition {
  case object Applied extends PolicyDisposition
  final case class AppliedWithWarning(message: String) extends PolicyDisposition
  final case class Rejected(error: EngineError) extends PolicyDisposition
}
```

| Request policy | Trino | Databricks | Snowflake | Dremio |
|---|---|---|---|---|
| materialize none | apply | apply | apply | apply |
| persist(level) | reject | apply; Connect remap warns | reject | reject |
| engine cache | require configured result cache, else reject | apply | require configured service, else reject | require configured reflection cache, else reject |
| no/read-through/write-through cache | coordinator policies apply; missing ref rejects | same | same | same |
| audit emit | apply; missing sink rejects | apply | apply | apply |
| broadcast hint | warn/omit if unsupported | apply | warn/omit | warn/omit if adapter version lacks support |
| skew hint | reject | apply; Photon may warn | reject | reject |
| streaming request | n/a in v0.3 portable IR | existing Spark lane only | n/a in v0.3 portable IR | n/a in v0.3 portable IR |
Warnings appear on plan/MCP envelope; rejects occur before native execution. There is no
silent ignore. A finite server timeout requires a supported cancellation kind. Timeout
calls native cancellation, waits a bounded acknowledgement, then returns
`QueryTimedOut(cancelStatus)`; it does not only abandon a Future. Spark uses job tags,
Trino remote statement cancellation, and Databricks Spark/Connect cancellation. This
moves Spark-specific handler logic from `Query.scala:75-95,226-299` behind the engine
and closes finding 17.
#### 4.5.4 Inspectable plans and portable results
```scala
sealed trait EngineWarning extends Product with Serializable
object EngineWarning {
  final case class PolicyAdapted(original: String, adapted: String) extends EngineWarning
  final case class ImplicitAssumptionMade(name: String) extends EngineWarning
  final case class CapabilityReluctantlySupported(name: String) extends EngineWarning
}
final class SparkPlanHandle private[spark] (
    val underlying: org.apache.spark.sql.execution.QueryPlan,
) {
  // Engine-local opaque handle. It is deliberately not Serializable.
}
// Deliberately NOT extends Product with Serializable: `SparkPlanHandle`
// wraps a Spark `QueryPlan` which is not cluster-safe. Cluster-mode shipping
// of a live plan handle would NPE or duplicate work; see SparkPlanHandle.
// The base trait only carries portable, case-class-shaped members. Plans are
// driver-local values; they live in `Engine.compile`, return via Either, and
// are not serialized. A separate `ExecutionPlanSummary` (below) is the
// transport/audit shape.
sealed trait ExecutionPlan[+R] {
  def warnings: List[EngineWarning]
  def requiredCapabilities: CapabilitySet
  def normalizedSchema: ResultSchema
}
final case class SparkExecutionPlan(
    native: SparkPlanHandle,
    warnings: List[EngineWarning],
    requiredCapabilities: CapabilitySet,
    normalizedSchema: ResultSchema,
) extends ExecutionPlan[DataFrame]
final case class TrinoExecutionPlan(
    sql: String,
    params: Map[String, Any],
    warnings: List[EngineWarning],
    requiredCapabilities: CapabilitySet,
    normalizedSchema: ResultSchema,
) extends ExecutionPlan[TrinoResult]

// Serializable plan summary for cache/audit/MCP envelope use. Holds only
// portable fields (no `SparkPlanHandle`, no native execution handle).
final case class ExecutionPlanSummary(
    engine: EngineIdentity,
    sql: Option[String],
    logicalPlan: String,
    requiredCapabilities: CapabilitySet,
    warnings: List[EngineWarning],
    normalizedSchema: ResultSchema,
) extends Product with Serializable
final case class ResultSchema(fields: List[Field])
    extends Product with Serializable
final case class ResultRow(values: List[Any], schema: ResultSchema)
    extends Product with Serializable
final case class PortableQueryResult(
    schema: ResultSchema,
    // Bounded by request maxRows. Vector is finite and case-class serializable
    // (Iterator is one-shot runtime state; it cannot round-trip to executors
    // or satisfy the §1.3 transitively-serializable invariant on portable values).
    rows: Vector[ResultRow],
    metadata: Map[String, String],
) extends Product with Serializable
trait ResultEncoder[-R] {
  def schema(result: R): Either[ResultError, ResultSchema]
  // Materialized row list, capped at the request maxRows by the caller. The
  // engine side may use an internal cursor while lowering; the boundary
  // returned to portable code is always a bounded finite sequence.
  def rows(result: R): Either[ResultError, Vector[ResultRow]]
}
final case class TrinoResult(/* native columns/pages */) extends Product with Serializable {
  def toResultRows: List[ResultRow] = TrinoResultEncoder.rows(this).toList
}
```
Plans expose typed warnings, requirements, schema, and native explain inputs; parameters
are redacted by default. `SparkPlanHandle` is driver-local and not serializable, closing
round-2 findings H2 and 10. `ResultSchema` and `ResultRow` are case classes because
conformance tests compare them with `==` (round-2 finding M1). `PortableQueryResult` is
the engine-neutral MCP result container (round-2 finding H2). Result rules (finding 16;
current Spark conversion `Query.scala:94-100,303-335`): null is JVM null and rejected in
non-null fields; decimals preserve declared precision and scale without silent rounding;
timestamps normalize to UTC `Instant` using the declared zone; dates are `LocalDate`;
arrays are recursive `Vector[Any]`; structs are nested `ResultRow`; maps are ordered
`Vector[(Any,Any)]` with non-null keys; MCP emits ISO dates/times and string decimals when
JSON numeric precision would be lost.
#### 4.5.5 Engine identity in cache and audit
```scala
final case class CacheKeyInput(
    engine: EngineIdentity, model: String, modelVersion: Int,
    requestHash: String, resultSchemaDigest: String, maxRows: Int,
)
final case class AuditEventV2(
    engine: EngineIdentity, requestHash: String, planHash: String,
    resultSchemaDigest: String, status: String, elapsedMs: Long,
    rowCount: Long, error: Option[String],
)
```
Identity name/version/implHash enter both canonical hashes, fixing
`CacheKey.scala:39-115` and `AuditEvent.scala:65-143` (finding 11). Rollout intentionally
invalidates all old cache entries; no legacy fallback can return Spark rows for Trino.
Old audit events read as `EngineIdentity("spark","legacy","legacy")`; new writers always
emit identity.
## 5. Backend implementations
### 5.1 Maven graph, scopes, and Spark-free core proof
The current single JAR has Spark `provided` (`pom.xml:1-98`); the parent becomes an
aggregator and the old coordinate a facade (finding 9).
```text
semanticdf-core_2.13
  ^-- semanticdf-spark_2.13 ^-- semanticdf_2.13 compatibility facade
  ^-- semanticdf-trino_2.13 ^-- semanticdf-trino-spark-bridge_2.13 --> spark
  ^-- semanticdf-databricks_2.13 --> spark
  ^-- semanticdf-snowflake_2.13
  ^-- semanticdf-platform-sdk_2.13
  ^-- semanticdf-mcp (engine adapters selected at deployment)
```

| Artifact | Compile | Provided | Test | Optional/runtime |
|---|---|---|---|---|
| core | Scala, Jackson | none | ScalaTest, JSON Schema validator | none |
| spark | core, SnakeYAML | Spark SQL | Spark local/ScalaTest | none |
| compatibility facade | spark | inherited Spark | source/binary facade | none |
| trino | core, catalog HTTP client | none | ScalaTest, container Trino | Trino JDBC driver |
| trino-spark-bridge | trino, spark | Spark SQL | bridge tests | none |
| databricks | core, spark, Databricks SDK | Spark/runtime APIs | REST/integration | Connect |
| snowflake | core | none | contract mocks | Snowflake JDBC |
| MCP | core, MCP SDK | none | registry + engine test scopes | deployment adapters |
The bridge isolates `TrinoResult.toDataFrame`; a Trino-only app resolves no Spark.
Required CI proof (acceptance criteria, not a claim about the unimplemented split):
```bash
grep -r 'org.apache.spark' semanticdf-core/src/main/scala # no output
# Optional: inspect transitive bytecode references after packaging.
jdeps -recursive semanticdf-core/target/*.jar | grep -v 'scala-library' | grep org.apache.spark # no output
``` 
This source grep is the primary check because Spark is `provided` in `pom.xml:56-62`;
Maven dependency scope alone cannot prove that core bytecode has no Spark reference
(round-2 finding H3). Core also has Maven Enforcer `bannedDependencies=org.apache.spark:*`
and `CoreClasspathSpec`, which loads all public core classes without Spark.
### 5.2 Engine sketches
```scala
final class SparkEngine(spark: SparkSession, runtime: SparkRuntimeRegistry)
    extends Engine[DataFrame] {
  val identity = EngineIdentity("spark", spark.version, "semanticdf-spark-adapter-v0.3.0")
  val capabilities = SparkCapabilities.forVersion(spark.version)
  def compile(p: RelOp, c: EngineContext) =
    SparkPolicyValidator.validate(c, capabilities).flatMap(SparkPlanLowering.lower(p, _, spark))
  def execute(p: ExecutionPlan[DataFrame], c: EngineContext) =
    SparkExecution.run(p.asInstanceOf[SparkExecutionPlan], c, runtime)
  def explain(p: ExecutionPlan[DataFrame]) = SparkExplain.render(p)
}
final class TrinoEngine(client: TrinoStatementClient, runtime: TrinoRuntimeRegistry)
    extends Engine[TrinoResult] {
  val identity = EngineIdentity("trino", client.serverVersion, "semanticdf-trino-adapter-v0.3.0")
  val capabilities = TrinoCapabilities.forVersion(client.serverVersion)
  def compile(p: RelOp, c: EngineContext) =
    TrinoPolicyValidator.validate(c, capabilities).flatMap(TrinoSqlLowering.lower(p, _))
  def execute(p: ExecutionPlan[TrinoResult], c: EngineContext) =
    TrinoExecution.run(p.asInstanceOf[TrinoExecutionPlan], c, client, runtime)
  def explain(p: ExecutionPlan[TrinoResult]) = TrinoExplain.render(p, client)
}
```
The adapter version is a manually-set constant, changed whenever lowering or result
semantics change; it replaces the undefined `BuildInfo.gitHash` and closes round-2 M9.

Databricks reuses Spark lowering but owns runtime/Connect identity, Photon validation,
cancellation, and Unity Catalog. Spark legacy lambdas enter only `SparkLambdaEval`.
### 5.3 Catalog identity and publication
```scala
final case class CatalogRef(
    catalog: String, namespace: String, name: String, version: Int, digest: String,
) extends Serializable
sealed trait PublishMode extends Product with Serializable
object PublishMode {
  case object CreateOnly extends PublishMode; case object Upsert extends PublishMode
  final case class CompareAndSet(expectedDigest: String) extends PublishMode
}
sealed trait PublishResult extends Product with Serializable
object PublishResult {
  final case class Inserted(ref: CatalogRef) extends PublishResult
  final case class Updated(previous: CatalogRef, current: CatalogRef) extends PublishResult
  final case class Conflict(current: Option[CatalogRef], reason: String) extends PublishResult
}
trait CatalogAdapter extends Serializable {
  def publish(doc: ManifestDocument, as: CatalogEntity, mode: PublishMode):
      Either[CatalogError, PublishResult]
  def discover(ref: CatalogRef): Either[CatalogError, Option[ManifestDocument]]
  def list(filter: CatalogFilter): Either[CatalogError, List[CatalogEntry]]
}
```
Create-only conflicts if identity exists; upsert atomically increments version; CAS
updates only at expected digest. Discovery verifies catalog ref, manifest, and extension
blob digests. This closes finding 13; the prior design's adapter was unversioned.
### 5.4 Capability targets and dialect quirks

| Capability | Spark portable | Trino | Databricks portable | Snowflake |
|---|---|---|---|---|
| calc depth | 64 | Int.MaxValue | 64 | Int.MaxValue |
| aggregates | Sum, Count, CountDistinct, Avg, Min, Max, both Stddev, both Variance, Median, both exact Percentile, ApproxPercentile, First, Last (version-gated) | Sum, Count, CountDistinct, Avg, Min, Max, both Stddev, both Variance, ApproxPercentile | Spark set, runtime-gated | Sum, Count, CountDistinct, Avg, Min, Max, both Stddev, both Variance, Median, both exact Percentile, ApproxPercentile |
| expr shapes | v0.3 base | base, array version-gated | v0.3 base | v0.3 base |
| late binding | false portable | false | false portable | false |
| set operations | absent from v0.3 IR | absent from v0.3 IR | absent from v0.3 IR | absent from v0.3 IR |
| cancellation | job tag | remote statement | Spark/Connect | JDBC/query id if available |
| SparkLambdaEval | compatibility only | no | compatibility only | no |

`Int.MaxValue` is not a claim that execution is unbounded: for SQL engines with
subqueries and CTEs it means no engine-specific calc-depth cap beyond cycle detection and
resource limits. The published depth remains the longest transitive `MeasureRef` path
specified in §4.5.2 (round-2 finding M8).
Per-engine quirks:
- Trino decimal derivation is cast to declared precision/scale only if lossless;
  overflow/rounding need returns `DecimalOverflow`; precision max is 38.
- `Between` is inclusive; lowerers may emit explicit `>=`/`<=`.
- Empty string is not null; no engine may rewrite it.
- Snowflake emits deterministic GROUP BY expressions in requested order and avoids
  alias/ordinal dependence; result columns follow `ResultSchema` order.
- Databricks Photon may be disabled by legacy lambda/UDF/persistence behavior; the plan
  warns, but semantics/capability acceptance do not change.
- Trino timestamp-with/without-zone values normalize through declared timezone; JVM
  default timezone is never used.
## 6. User-facing interface (preserved)
### 6.1 Existing and portable calls
```scala
implicit val spark: SparkSession = SparkSession.builder().getOrCreate()
val orders = toSemanticTable(ordersDf, name = Some("orders"))
  .withDimensions(Dimension("region", t => t("region")))
  .withMeasures(Measure("amount", t => sum(t("amount"))))
val df1: DataFrame = orders.query(Seq("amount"), Seq("region")).execute()
val df2: DataFrame = orders.query(Seq("amount"), Seq("region")).execute(spark)
val portable = Model.of(
  name = "orders",
  source = SourceRef.ByName(Some("hive"), Some("silver"), "orders"),
  dimensions = List(Dimension.field("region", SealedDataType.StringType)),
  measures = List(Measure.aggregate("amount", AggregateFn.Sum, Expr.ColumnRef("amount"))),
)
```
Portable query building resolves source schema first. Trino results expose
`toResultRows`; `toDataFrame` is an extension in `semanticdf-trino-spark-bridge`, not a
core/Trino Spark dependency. At this boundary, a legacy `ManualRollupSpec` is rewritten
to the portable `RollupSpec` plus a driver-local `RollupRegistration`; its provider is
resolved and precomputed only after engine selection (round-2 M10).
### 6.2 Exact overloads and ambiguity proof
```scala
final class Query(/* state */) {
  def execute[A](implicit engine: Engine[A]): A =
    executeWithContext(EngineContextDefaults.batch, engine)
  def execute(spark: SparkSession): DataFrame = // explicit, not implicit
    execute[DataFrame](SparkEngine.compatibility(spark))
}
object SparkEngineImplicits {
  implicit def engineFromSparkSession(
      implicit spark: SparkSession,
  ): Engine[DataFrame] = SparkEngine.compatibility(spark)
}
```
For `.execute()`, only the implicit-engine overload is applicable; for
`.execute(spark)`, the explicit overload wins. If an implicit engine and session both
exist, the direct engine is more specific than deriving one from the session. The JVM
`execute(SparkSession)` descriptor remains. `ExecuteAmbiguitySpec` covers session only,
engine only, both, explicit Spark, explicit engine, and compile-failure with two
unselected engines (finding 25; `SemanticTableCore.scala:308-311`).
### 6.3 Typed query policies
```scala
val context = EngineContext(
  materializePolicy = MaterializePolicy.None,
  cachePolicy = CachePolicy.ReadThrough(CacheRef("dashboard")),
  auditPolicy = AuditPolicy.EmitEvents(AuditSinkRef("operations")),
  joinHints = JoinHints(broadcastRightBelowBytes = Some(8L * 1024 * 1024)),
  timeout = scala.concurrent.duration.Duration("30s"),
  cancellation = CancellationCapability.RemoteStatement("request-42"),
)
val result: TrinoResult = portable.query(...).executeWithContext(context, trinoEngine)
```
### 6.4 MCP registry and concrete handler change
The current `Query` owns a Spark session and Spark row conversion
(`Query.scala:29-40,75-95`); current `QueryRequest` has no engine
(`Query.scala:601-621`). The server must change (round-2 finding 3 and H5).
```scala
trait AuditSink { // engine-local operational interface; never in portable model data
  def emit(event: AuditEvent): Unit
}
final class AuditSinkRegistry(sinks: Map[String, AuditSink]) {
  def get(name: String): Either[EngineError, AuditSink] =
    sinks.get(name).toRight(EngineError.AuditSinkUnavailable(name))
}
object AuditResolution {
  def resolve(policy: AuditPolicy, registry: AuditSinkRegistry):
      Either[EngineError, Option[AuditSink]] = policy match {
    case AuditPolicy.NoAudit => Right(None)
    case AuditPolicy.EmitEvents(sinkRef) => registry.get(sinkRef.name).map(Some(_))
  }
}

trait MCPEngineProvider {
  def identity: EngineIdentity
  def available: Boolean
  def query(model: Model, request: QueryRequest, context: EngineContext):
      Either[EngineError, PortableQueryResult]
  def explain(model: Model, request: QueryRequest, context: EngineContext):
      Either[EngineError, ExplainResult]
}
// Session is wrapped in `Option` so that `available` is computed strictly
// before any engine allocation. A null Spark session is a deployment-level
// "not configured" state, not a runtime NPE: the registry's `select` must
// short-circuit, and the previous `available = spark != null` read happened
// after `new SparkEngine(spark, runtime)` would have already dereferenced the
// null pointer. `identity` and `available` now derive from the same defensive
// pattern. Closes round-3 DE finding 4.1.
final class SparkEngineProvider(
    sparkOpt: Option[SparkSession],
    runtime: SparkRuntimeRegistry,
    auditSinks: AuditSinkRegistry,
) extends MCPEngineProvider {
  // Eager validation: any well-formed provider must either hold a live
  // session or be explicitly absent from the registry. Configured-but-null
  // is an internal error.
  require(runtime != null, "SparkEngineProvider requires a SparkRuntimeRegistry")
  require(auditSinks != null, "SparkEngineProvider requires an AuditSinkRegistry")
  val available: Boolean = sparkOpt.isDefined
  val identity: EngineIdentity = sparkOpt match {
    case Some(s) => EngineIdentity("spark", s.version, "semanticdf-spark-adapter-v0.3.0")
    case None    => EngineIdentity("spark", "unconfigured", "semanticdf-spark-adapter-v0.3.0")
  }
  // Lazy allocation: only invoke `SparkEngine` when actually selected; the
  // registry's `select` filters on `available` first.
  private lazy val engine: Option[SparkEngine] = sparkOpt.map(new SparkEngine(_, runtime))
  def query(model: Model, request: QueryRequest, context: EngineContext) =
    engine match {
      case None => Left(EngineError.EngineUnavailable("spark", List.empty, wasDefault = true))
      case Some(e) =>
        AuditResolution.resolve(context.auditPolicy, auditSinks).flatMap(_ =>
          SparkMCPExecution.query(e, model, request, context))
    }
  def explain(model: Model, request: QueryRequest, context: EngineContext) =
    engine match {
      case None => Left(EngineError.EngineUnavailable("spark", List.empty, wasDefault = true))
      case Some(e) =>
        AuditResolution.resolve(context.auditPolicy, auditSinks).flatMap(_ =>
          SparkMCPExecution.explain(e, model, request, context))
    }
}
final class TrinoEngineProvider(
    client: TrinoStatementClient,
    runtime: TrinoRuntimeRegistry,
    auditSinks: AuditSinkRegistry,
) extends MCPEngineProvider {
  private val engine = new TrinoEngine(client, runtime)
  val identity: EngineIdentity = engine.identity
  val available: Boolean = client.isHealthy
  def query(model: Model, request: QueryRequest, context: EngineContext) =
    AuditResolution.resolve(context.auditPolicy, auditSinks).flatMap(_ =>
      TrinoMCPExecution.query(engine, model, request, context))
  def explain(model: Model, request: QueryRequest, context: EngineContext) =
    AuditResolution.resolve(context.auditPolicy, auditSinks).flatMap(_ =>
      TrinoMCPExecution.explain(engine, model, request, context))
}
final class MCPEngineRegistry(
    engines: Map[String, MCPEngineProvider],
    default: String,
) {
  require(engines.contains(default),
    s"MCPEngineRegistry default '$default' not in registered engines: ${engines.keys.toList.sorted}")
  def available(): List[String] = engines.collect {
    case (n, p) if p.available => n
  }.toList.sorted
  def select(requested: Option[String]):
      Either[EngineError, (String, MCPEngineProvider)] = {
    val chosen = requested.getOrElse(default)
    engines.get(chosen).filter(_.available).map(chosen -> _).toRight(
      EngineError.EngineUnavailable(chosen, available(), requested.isEmpty))
  }
}
val registry = new MCPEngineRegistry(
  Map("spark" -> new SparkEngineProvider(Option(spark), sparkRuntime, auditSinks),
      "trino" -> new TrinoEngineProvider(trinoClient, trinoRuntime, auditSinks)),
  default = "spark",
)
final case class QueryRequest(
    model: String,
    measures: Seq[String],
    dimensions: Option[Seq[String]] = None,
    /* existing fields unchanged */
    engine: Option[String] = None,
) extends Product with Serializable
```
`Engine.compile()` resolves `AuditPolicy.EmitEvents(AuditSinkRef("operations"))` through
`AuditSinkRegistry.get("operations")` before lowering or execution; a missing sink is a
typed compile failure, never a dropped audit event. This closes round-2 H4 and preserves
the `AuditEvent.scala:65-143` requirement without retaining a sink in `Query`'s
constructor.
Handler change. The previous snippet referenced undefined symbols
(`throw _`, `Query.raise`, `models.portable`, etc.) and assumed
`EngineError extends Throwable`; this revision fixes all of those:
- `EngineError` is a sealed ADT value (see §4.5), never thrown.
- `Models.portable(modelName)` is the dual-registry accessor added below.
- `Query.contextFor`, `Query.toEnvelope`, `Query.toErrorDetail` are defined
  here as module-private helpers.
- The handler uses `Either.fold(...)` with the user-supplied error
  mapper, not `throw`.
Closes round-3 DE finding 3.1.
```scala
// Dual-registry: existing `Models` already maps name -> legacy Spark facade;
// this adds the portable accessor the portable handler path needs. v0.3.0
// loaders populate both maps; the v1 facade is retained for compatibility.
final class Models(
    legacy: Map[String, SemanticTable],
    portableRegistry: Map[String, Model],
) {
  def legacyFor(name: String): Either[EngineError, SemanticTable] =
    legacy.get(name).toRight(
      EngineError.EngineUnavailable(s"model:$name", List.empty, wasDefault = false))
  def portable(name: String): Either[EngineError, Model] =
    portableRegistry.get(name).toRight(
      EngineError.EngineUnavailable(s"model:$name", List.empty, wasDefault = false))
}

object Query {
  // Concrete envelope shape returned by `handle`. Kept as a module-private
  // alias so handlers in other parts of the file can reference it.
  type Data = io.modelcontextprotocol.spec.McpSchema.CallToolResult
  type Envelope[T] = io.modelcontextprotocol.spec.McpSchema.CallToolResult

  // Build a typed EngineContext from the (validated) request and the chosen
  // provider. The provider's identity supplies native version; the timeout
  // comes from the MCP server configuration; the cancellation capability
  // is selected by engine kind (Spark = JobTag, Trino = RemoteStatement,
  // Databricks = SparkJobTag-or-Connect). MaxRows comes from the bound
  // request shape (always <= handler maxRows).
  def contextFor(
      request: QueryRequest,
      provider: MCPEngineProvider,
      timeoutMs: Long,
  ): Either[EngineError, EngineContext] = {
    val requestId = request.requestId.getOrElse(java.util.UUID.randomUUID().toString)
    val cancell: CancellationCapability = provider.identity.name match {
      case "spark" | "databricks" => CancellationCapability.SparkJobTag(requestId)
      case "trino"               => CancellationCapability.RemoteStatement(requestId)
      case _                     => CancellationCapability.SparkJobTag(requestId)
    }
    Right(EngineContext(
      materializePolicy = MaterializePolicy.None,
      cachePolicy       = CachePolicy.NoCache,
      auditPolicy       = AuditPolicy.NoAudit,
      joinHints         = JoinHints(),
      timeout           = scala.concurrent.duration.Duration(timeoutMs, "ms"),
      cancellation      = cancell,
    ))
  }

  // Render the typed execution result into the MCP envelope. Errors are
  // mapped through the exhaustive `EngineError.toErrorDetail` (see §4.5).
  def toEnvelope(
      outcome: Either[EngineError, PortableQueryResult],
      request: QueryRequest,
      identity: EngineIdentity,
      maxRows: Int,
  ): Envelope[Data] = outcome match {
    case Left(err) =>
      val detail = EngineError.toErrorDetail(err)
      io.modelcontextprotocol.spec.McpSchema.CallToolResult.error(detail)
    case Right(result) =>
      val rows = result.rows
      val truncated = rows.length >= maxRows
      io.modelcontextprotocol.spec.McpSchema.CallToolResult.success(
        QueryResponse(result.schema, rows, rows.length, truncated, identity))
  }
}

final case class QueryResponse(
    schema: ResultSchema,
    rows: Vector[ResultRow],
    rowCount: Int,
    truncated: Boolean,
    engine: EngineIdentity,
) extends Product with Serializable

final class Query(engineRegistry: MCPEngineRegistry, maxRows: Int, timeoutMs: Long) {
  def handle(models: Models, request: QueryRequest): Query.Envelope[Query.Data] = {
    // Step 1: registry selection. `select` already filters on availability
    // and returns the typed EngineUnavailable code; no `throw`.
    val providerEither: Either[EngineError, (String, MCPEngineProvider)] =
      engineRegistry.select(request.engine)
    providerEither.flatMap { case (_, provider) =>
      // Step 2: look up the portable model (typed failure for missing).
      models.portable(request.model).flatMap { model =>
        // Step 3: build typed EngineContext.
        Query.contextFor(request, provider, timeoutMs).flatMap { context =>
          // Step 4: execute. The provider returns Either; success becomes
          // a portable result, failure becomes the typed error.
          val outcome: Either[EngineError, PortableQueryResult] =
            provider.query(model, request, context)
          // Step 5: render envelope (error or data).
          Right(Query.toEnvelope(outcome, request, provider.identity, maxRows))
        }
      }
    } match {
      case Right(env) => env
      case Left(err)  =>
        io.modelcontextprotocol.spec.McpSchema.CallToolResult.error(
          EngineError.toErrorDetail(err))
    }
  }
}
```
Absent request engine selects the configured default. Missing/unavailable default returns
`ENGINE_UNAVAILABLE(wasDefault=true)`; explicit unknown/unavailable returns the same
typed code with `wasDefault=false`. Neither case falls back to another engine. Validate
against `available()` before building the query; `explain` shares selection. Existing
response `{columns,rows,row_count,truncated}` remains; metadata adds engine identity.
The `queryToolSchema` patch is additive and closes round-2 M6 plus
round-3 DE finding 7.1. The previous snippet used a Circe `Json.deepMerge`
over a hypothetical `existingQueryToolSchema`, but the real type is
`io.modelcontextprotocol.spec.McpSchema.JsonSchema` built from a mutable
`java.util.LinkedHashMap` of string-keyed properties — the actual one-line
change is a single `props.put` against the existing map. The real
properties list has 12 entries before this change; after the patch it has
13.
```scala
val queryToolSchema: io.modelcontextprotocol.spec.McpSchema.JsonSchema = {
  val props = new java.util.LinkedHashMap[String, Object]()
  def strProp(t: String) =
    java.util.Map.of[String, Object]("type", t): java.util.Map[String, Object]
  // Existing 12 properties (unchanged):
  props.put("model",      strProp("string"))
  props.put("measures",   strProp("array"))
  props.put("dimensions", strProp("array"))
  props.put("where",      strProp("array"))
  props.put("having",     strProp("array"))
  props.put("ast_where",  strProp("object"))
  props.put("ast_having", strProp("object"))
  props.put("order_by",   strProp("array"))
  props.put("limit",      strProp("integer"))
  props.put("time_grain", strProp("string"))
  props.put("time_grains",strProp("array"))
  props.put("time_range", strProp("array"))
  // Additive engine property — the only change for v0.3.0:
  props.put("engine",     strProp("string"))
  new io.modelcontextprotocol.spec.McpSchema.JsonSchema(
    "object",
    props,
    JList.of("model", "measures"),
    java.lang.Boolean.TRUE,       // additionalProperties
    java.util.Map.of(),           // defs
    java.util.Map.of(),           // definitions
  )
}
```
Required fields remain `["model", "measures"]`; `engine` stays optional so
existing clients continue to work without modification. The full diff is one
additive `props.put`. A schema test asserts all 13 property names:
```scala
property("queryToolSchema has 13 properties including engine") {
  val keys = queryToolSchema.properties.keySet.asScala.toSet
  assert(keys.size == 13)
  assert(keys.contains("engine"))
  assert(keys("model") == Map("type" -> "string").asJava)
}
```
```json
{"model":"orders","measures":["amount"],"dimensions":["region"],
 "engine":"trino"}
```
### 6.5 CLI and manifest compatibility entry points
CLI defaults to configured Spark; a later additive `--engine` uses MCP registry rules.
Metadata-only introspection needs no engine; schema/query/explain do.
```scala
object SemanticManifest {
  def toJson(model: SemanticTable): String = ManifestV2Writer.fromLegacySpark(model)
  def toJsonV1(model: SemanticTable): String = LegacyManifestV1.write(model)
  def fromJson(text: String, source: DataFrame): SemanticTable =
    LegacyManifestV1.read(text, source) // retained descriptor
  def fromJson(text: String, resolver: SourceResolver): Either[ManifestError, Model] =
    ManifestReaders.readAny(text, resolver)
}
```
V2 conversion first parses recoverable SQL expressions to portable `Expr`. For an unrecoverable current `<lambda>`, it records a bounded reserved Spark extension plus the same placeholder behavior the current source-required reader uses, and marks the model Spark-only.
Non-Spark resolution returns `IncompatibleEngine`: the existing call still produces a document while portability is not fabricated.
## 7. Implementation phases
Estimates include production code, tests, fixtures, and deployment wiring. They are
planning ranges, not commitments. Generated code and vendored clients are excluded.
### 7.1 Phase 1 — core extraction, compatibility facade, and vertical proof
**Goal:** prove the seam while preserving Spark behavior.
Work breakdown:

| Work package | Estimated LoC |
|---|---:|
| core type/expression/relational nodes and validation | 900-1,200 |
| policies, capabilities, errors, plan/result contracts | 700-900 |
| source/provider refs and resolver contracts | 350-500 |
| manifest v2 model/schema/read/write and v1 migration | 1,000-1,400 |
| Spark lowering/legacy compatibility facade rewiring | 1,200-1,700 |
| cache/audit engine identity migration | 250-400 |
| Maven split/enforcer/build wiring | 300-450 |
| core/Spark/manifest/classpath tests | 1,500-2,000 |
| disposable Trino vertical proof | 800-1,200 |
| **Phase 1 total** | **7,000-9,750** |
Deliverables:
- parent/module POMs and old-coordinate facade;
- Spark-free core proof from §5.1;
- complete core ADTs and typed error contracts;
- v1 reader plus v2 schema/writer/migration tests;
- Spark behavior parity under both Spark 3.5 and Spark 4 profiles;
- `ExecuteAmbiguitySpec`;
- a disposable Trino proof that exercises one representative joined aggregate query.
### 7.2 Phase-1 decision gate — required before full Trino scope
Do not commit to Phase 2 production scope until the proof demonstrates all of:
1. `semanticdf-core` resolves and loads with no Spark dependency/reference.
2. A named Trino source resolves to portable schema and statistics.
3. A query with projection, predicate, equi-join, group, Sum/Count/Avg, sort, and limit
   lowers to parameterized SQL and executes against a containerized Trino.
4. Trino rows normalize to the same `ResultSchema`/`ResultRow` values as the Spark
   conformance fixture, including null, decimal, date, timestamp, array, and struct.
5. `ExecutionPlan` exposes SQL, parameter types, warnings, required capabilities, and
   normalized schema; explain succeeds without query execution.
6. A forced timeout cancels the remote Trino query, not merely the caller Future.
7. Manifest v2 round-trips the proof model and a v1 fixture migrates to the same model.
8. Explicitly unsupported deferred analytic, total, set-operation, aggregate, and policy
   requests return the intended typed errors.
**Gate failure decision tree (round-2 finding M5):** hold Phase 1; produce a 2-page RCA
within 1 week; if the RCA shows a fundamental C-blocking issue, revert Phase 1 to a
Trino-isolated bridge (no `semanticdf-trino` module) and ship v0.3.0 without Trino
support; if the RCA shows an integration-only issue, fix it in Phase 1 with an explicit
timeline. The gate does NOT constitute schedule permission to slip tests.
### 7.3 Phase 2 — production Trino adapter
Realistic range: **4,000-6,000 LoC**, expected midpoint about **5,450 LoC**.
Work breakdown:

| Work package | Estimated LoC |
|---|---:|
| IR requirement analysis and validation | 250-400 |
| SQL lowering, quoting, aliases, params | 900-1,250 |
| source resolver and source digest/stats | 400-650 |
| type/null/decimal/timezone semantics | 400-600 |
| execution client and retry classification | 350-550 |
| result encoding/streaming pages | 250-400 |
| explain and plan diagnostics | 150-250 |
| timeout and remote cancellation | 200-350 |
| catalog adapter | 400-650 |
| conformance/golden/property/integration tests | 900-1,300 |
| deployment/config/docs | 200-350 |
| **Total** | **4,200-6,750; scope target 4,800-5,900** |
Hypothetical spike package budget (used to catch omitted categories, not measured code):

| Package | LoC |
|---|---:|
| `trino/lowering` | 1,120 |
| `trino/source` | 620 |
| `trino/types` | 510 |
| `trino/execution` | 760 |
| `trino/result` | 350 |
| `trino/catalog` | 540 |
| `trino/*Spec` and fixtures | 1,280 |
| deployment/config/docs | 280 |
| **Hypothetical total** | **5,460** |
### 7.4 Phase 3 — Databricks and Unity Catalog
Realistic range: **2,500-3,000 LoC**. Spark lowering is reused, but session/Connect,
identity, cancellation, Photon, catalog concurrency, tests, and deployment remain.

| Work package | Estimated LoC |
|---|---:|
| engine wrapper and capability/version mapping | 180-250 |
| source resolver | 180-260 |
| type/null/Photon validation | 160-230 |
| session/Connect execution lifecycle | 220-320 |
| result/explain/cancellation integration | 180-260 |
| Unity Catalog publish/discover/CAS/blob envelope | 600-800 |
| tests and fixtures | 650-800 |
| deployment/config/docs | 200-280 |
| **Total** | **2,370-3,200; scope target 2,500-3,000** |
Hypothetical package midpoint: engine 220, source 220, types 190, execution 280,
explain/cancel 210, catalog 720, tests 720, deployment/docs 240 = **2,800 LoC**.
Portable streaming remains deferred; only the existing Spark streaming lane is retained.
### 7.5 Phase 4 and Phase 5
**Phase 4 — Snowflake/Dremio:** estimate each independently after a capability and
cancellation spike. Planning placeholder: 4,000-6,000 LoC per SQL engine, including
catalog and integration tests. Do not reuse the old “2,000 LoC each” assumption.
**Phase 5 — custom platform SDK:** publish core conformance fixtures, `MCPEngineProvider`
helpers, typed registry builders, catalog CAS helpers, extension-blob helpers, and a
reference adapter. Estimate 1,500-2,500 LoC after two production engines reveal the
stable seam.
### 7.6 Verification plan and corrected baselines
Baseline counts at this revision are:
- **963 Scala-only tests** for the Scala library scope.
- **1124 cross-project tests** when MCP/platform project suites are included.
Both values are reported because “all tests” previously mixed scopes. The stale value
992 is removed. New v0.3 tests are additional and tracked separately.
Named suites:
- `ExecuteAmbiguitySpec` — exact overload/implicit selection matrix.
- `CoreClasspathSpec` — no Spark class available while loading core public classes.
- `EngineContextPolicySpec` — every policy gets applied/warned/rejected per engine.
- `CapabilityRequirementSpec` — calc depth, aggregate, expr, late-binding, set-op gaps.
- `SourceResolverSpec` — resolved schema plus incompatible/auth/not-found outcomes.
- `ProviderRegistrySpec` — typed closure maps, immutable register, JVM-local failures.
- `ManifestV1MigrationSpec` and `ManifestV2SchemaSpec` — dual-read and schema rules.
- `ExtensionEnvelopeSpec` — 8 KiB, 16-field, canonical digest, blob externalization.
- `CatalogCasSpec` — create-only, upsert, compare-and-set conflict behavior.
- `ResultNormalizationPropertySpec` — decimal/timezone/null/nested values across engines.
- `EngineIdentityCacheSpec` — no cross-engine or pre-identity cache hits.
- `RemoteCancellationPropertySpec` — timeout causes native cancel exactly once.
- `SparkTrinoConformanceSpec` — result/error equivalence over generated portable plans.
Remote cancellation property:
```scala
property("finite timeout cancels remote work and reports acknowledgement") {
  forAll(validLongRunningPlans) { plan =>
    val remote = new RecordingRemoteEngine(plan)
    val result = remote.executeWithTimeout(50.millis)
    assert(result.left.exists(_.isInstanceOf[QueryTimedOut]))
    assert(remote.cancelCalls == 1)
    assert(remote.cancelledRequestId.contains(remote.startedRequestId))
    assert(remote.activeQueries.isEmpty)
  }
}
```
Integration profiles verify Spark job tags, Trino statement cancellation, and
Databricks Connect cancellation where credentials are available. A unit property alone
cannot prove a vendor client actually stops remote work.
### 7.7 Definition of done per adapter
An adapter is not “supported” until it has:
- declared version-specific capabilities;
- source resolution with schema and typed failures;
- complete lowering for every claimed expression/aggregate/type;
- null/decimal/timezone conformance;
- execution, normalized results, explain, and cancellation;
- policy disposition tests;
- catalog identity/CAS/extension-envelope behavior if catalog support is claimed;
- MCP provider wiring and deployment documentation;
- golden and property tests against a real or containerized engine;
- no skipped conformance case for a claimed capability.
## 8. Risk + how debug-mantra mitigates
The migration follows reproduce, trace, falsify, cross-reference, verify:
1. Reproduce Spark behavior with the 963 Scala-only and 1124 cross-project baselines.
2. Trace query build -> source resolution -> capability/policy validation -> lowering ->
   execution -> normalization -> cache/audit/MCP response.
3. Falsify each adapter claim with generated unsupported and edge-case plans.
4. Cross-reference Spark/Trino results, manifest/model digests, native explain, remote
   active-query state, cache keys, audit events, and catalog refs.
5. Verify with real-engine integration tests before capability publication.

| Risk | Probability / impact | Mitigation and falsifier |
|---|---|---|
| Spark behavior changes during extraction | Medium / High | compatibility facade; dual Spark profiles; baseline suites; result/plan snapshots |
| Spark leaks into core transitively | Medium / High | Enforcer ban, dependency tree, `jdeps`, `CoreClasspathSpec` |
| Source schema changes after build | Medium / High | source digest; prepare validation; `SourceSchemaChanged`; mutation integration test |
| Provider exists only in another driver/JVM | Medium / High | document JVM-local ownership; `NotFound`/`Unavailable`; startup registration health check |
| Provider closure serialized to executor | Low / High | refs only in core; registry not serializable; serialization negative tests |
| Unsupported policy is silently ignored | Medium / High | closed disposition ADT; no empty/default branch; per-engine policy matrix test |
| Capability too broad produces wrong SQL | Medium / High | structured function/expr/type sets; generated negative tests; version snapshots |
| Decimal scale/overflow differs by engine | High / High | precision ceiling; explicit result casts; property tests around boundaries |
| Timestamp uses JVM default timezone | Medium / High | declared IANA zone; UTC `Instant`; tests under multiple JVM zones |
| Empty string is normalized to null | Low / High | explicit semantic rule and conformance fixture |
| Result nested values lose shape/order | Medium / High | recursive normalized representation; schema digest; map ordered-pair encoding |
| Timeout abandons caller but remote work continues | Medium / High | cancellation capability; acknowledgement; active-query integration assertion |
| Cancellation races successful completion | Medium / Medium | idempotent request ids; exactly-once cancel property; terminal state machine |
| Cache returns rows from another engine | Medium / Critical | engine identity in key; legacy entries intentionally invalidated |
| Audit dedup merges different engines | Medium / High | identity in event and dedup hash; cross-engine audit test |
| Manifest v1/v2 migration loses semantics | High / High | dual readers; migration goldens; reject unrecoverable lambda; digest comparison |
| Unknown future manifest feature is ignored | Medium / High | reject unknown type/expr discriminators; preserve only unknown members |
| Extension payload exhausts catalog/property limits | Medium / Medium | 8 KiB/16-field envelope; external blob; size/digest verification |
| External extension blob is swapped or SSRF URI used | Low / High | content digest, size, media type, URI allow-list, no embedded credentials |
| Catalog writers overwrite concurrently | Medium / High | versioned `CatalogRef`; create/upsert/CAS; conflict tests |
| MCP explicit engine silently falls back | Medium / High | registry selection rules; `ENGINE_UNAVAILABLE`; no fallback test |
| MCP deployment omits configured default | Low / High | startup health check plus typed default-unavailable response |
| Explain leaks parameter secrets | Medium / Medium | redacted values; types/names only by default; security snapshot |
| Deferred analytic/total shape appears supported | Medium / High | v0.3 rejects deferred analytic, total, and set-operation requests with `FeatureDeferred(v0.4.0)`; absent capabilities |
| Trino/Databricks estimates remain low | Medium / Medium | WBS ranges; Phase-1 gate; package-level tracking; re-estimate after spike |
| Photon/native optimizer changes semantics | Low / High | optimizer is never semantic authority; cross-engine results; warning on fallback |
### 8.1 Falsifiable phase claims
- **Phase 1:** removing Spark from the test runtime still loads every core public class;
  a Spark reference makes `CoreClasspathSpec` fail.
- **Phase 1 gate:** a timed-out Trino proof leaves no matching active query; a mere
  Future timeout fails the gate.
- **Phase 2:** every `AggregateFn` advertised by Trino passes generated result tests;
  removing one lowerer case breaks capability conformance.
- **Phase 3:** concurrent Unity Catalog CAS writers produce one update and one conflict,
  never two successful writes at the same expected digest.
- **All phases:** changing engine identity changes cache and audit hashes.
- **All phases:** a 16-field/8192-byte extension is inline; one byte or field beyond the
  limit is externalized or rejected, never truncated.
## 9. Decisions needing explicit user input
Path A, Trino-first proof, visible optional MCP engine selection, manifest v2, typed
extensions, engine identity, streaming deferral, window/percent-total/set-op deferral,
and policy/capability separation are resolved and therefore removed from this table.

| # | Remaining decision | Options | Proposed default |
|---|---|---|---|
| D1 | Lifetime of the legacy Spark lambda API | coexist through 1.x; or deprecate after portable v0.4 parity | coexist through 1.x |
| D2 | Lifetime of manifest-v1 writing (reading remains longer) | v0.3.x only; through v0.4.x | v0.3.x only, with release-note telemetry request |
| D3 | Trino result materialization strategy for very large MCP results | bounded in-memory pages; spill-to-disk cursor; mandatory pagination | bounded pages in v0.3, design pagination before broad production use |
| D4 | External extension blob schemes | catalog-managed only; deployment allow-list | deployment allow-list defaulting to catalog-managed URI |
None of these decisions blocks the Phase-1 core model or proof. D3 blocks claiming
unbounded production Trino result support; existing MCP `maxRows` remains a hard bound.
## 10. Out of scope (explicitly)
- Cross-engine joins or query federation.
- Cost-based engine selection or automatic fallback to another engine.
- Portable streaming semantics in v0.3.0.
- Portable window functions, subqueries, `t.all`, and set operations in v0.3.0.
- Automatic translation of arbitrary Spark `Column` trees/lambdas to portable `Expr`.
- Arbitrary vendor SQL embedded in the core model.
- User-defined code loading from manifest, catalog property, extension, or MCP request.
- Portable binary row format; v0.3 defines logical normalized values first.
- Exactly-once audit/cache/catalog transactions across independent systems.
- A universal CBO or rewriting native optimizer plans.
- Silent approximation of exact aggregate functions.
- Migration of old cache entries into engine-identified keys.
## 11. Open questions
Resolved questions from the previous draft—opaque extensions, audit engine field,
manifest versioning, MCP engine visibility, provider lifecycle, and cancellation
location—are removed.
**Reserved v0.4.0 RFC items.** `SetOp` (including `UnionDistinct`, `UnionAll`,
`ExceptDistinct`, `ExceptAll`, `IntersectDistinct`, and `IntersectAll`), `Window`,
`WindowFrame`, `TotalRef`, `StreamingSinkPolicy`, `OutputMode.Append`, `OutputMode.Update`,
`OutputMode.Complete`, and `ProviderRef.SinkCallback` are reserved for a v0.4.0 RFC.
Every one was removed from v0.3.0 to honor karpathy-guidelines §2 and review round-2
finding H1; none is a v0.3.0 type, expression case, capability member, or provider
reference. The RFC must define semantics before any of these names return to the IR.
1. Should a future async public API return `Future`, a cancelable effect type, or only a
   driver-local `QueryHandle` while keeping `Engine` synchronous in v0.3?
2. What bounded page/cursor contract should replace `List[ResultRow]` for large remote
   results without changing the existing MCP bounded response?
3. Which Trino versions and connectors form the supported conformance floor? Connector
   behavior affects source types/statistics even when SQL engine version is stable.
4. Should result maps remain ordered pairs in all Scala APIs or expose a convenience
   `Map` only when key types and uniqueness make conversion lossless?
5. What is the v0.4 delivery order for the reserved analytic, total, and set-operation
   features? Their RFCs imply no order today.
6. How should adapter `engineAdapterVersion` be generated or manually governed for
   locally patched/vendor builds so it changes when lowering behavior changes?
These questions must not be answered by string parameters in `EngineContext`.
## 12. Glossary

| Term | Definition |
|---|---|
| `Model` | Pure portable semantic model containing source refs, typed expressions, joins, rollups, defaults, and bounded extensions |
| `Expr` | Portable typed scalar/calculated expression ADT |
| `RelOp` | Portable relational plan ADT built only after source resolution |
| `SealedDataType` | Portable primitive/nested type ADT used by fields, casts, literals, and results |
| `Field` | Name, portable data type, and nullability in a resolved/result schema |
| `ResolvedScan` | Relational scan containing source ref, resolved fields, and projection |
| `SourceRef` | Serializable source identity, never a native source object |
| `ProviderRef` | Typed identifier for one driver-local closure shape (`DataFrameSource`, `TableResolver`) |
| `SourceResolver` | Engine-side adapter that resolves a source to schema/stats or typed failure |
| `ResolvedSource` | Closed result of source resolution: scan, incompatible, auth failed, or not found |
| `Engine[R]` | Adapter contract for compile, execute, explain, identity, and capabilities |
| `MCPEngineRegistry` | Server registry mapping request engine names to available engine providers and a default |
| `MCPEngineProvider` | MCP-facing engine implementation returning portable query/explain results |
| `EngineError` | Closed compile/execute failure ADT, including unsupported capability, connection, timeout, and cancellation failures |
| `CatalogError` | Closed catalog failure ADT: conflict, authorization, network, unsupported operation, or malformed manifest |
| `EngineWarning` | Closed non-fatal plan diagnostic ADT; warnings are typed, never arbitrary strings |
| `Capability` | Typed capability diagnostic name used by unsupported-capability errors |
| `TransformSpec` | Removed in v0.3.0 (round-3 DE 1.1); portable model and v2 manifest carry no `transforms` member |
| `CalculatedMeasure` | Named portable calculated expression, represented as `(name, Expr)` |
| `ExplainResult` | Serializable logical/native explain output with SQL and typed warnings |
| `PortableQueryResult` | Serializable result schema, normalized rows, and string metadata returned by MCP providers |
| `SparkPlanHandle` | Non-serializable, engine-local opaque wrapper around a Spark query plan |
| `TrinoRuntimeRegistry` | Driver-local Trino runtime registry for resolvers, catalogs, caches, and cancellation clients |
| `AuditSinkRegistry` | Driver-local name-to-`AuditSink` resolver used during engine compilation |
| `CancellationMode` | Engine-advertised native cancellation mechanism, distinct from a request capability |
| `ManualRollupSpec` | Legacy rollup declaration rewritten into portable `RollupSpec` at engine registration |
| `EngineIdentity` | Engine name, native version, and adapter implementation hash |
| `EngineContext` | Typed per-query policy/cancellation request, with `auditPolicy: AuditPolicy` resolved at compile time |
| `MaterializePolicy` | Per-query request for no materialization, persistence level, or engine cache |
| `CachePolicy` | Per-query no-cache, read-through, or write-through behavior |
| `AuditPolicy` | Per-query no-audit or typed `AuditSinkRef` (resolved at compile time through `AuditSinkRegistry`) |
| `JoinHints` | Typed, best-effort join planning requests with explicit disposition |
| `CancellationCapability` | Selected per-request native cancellation mechanism (Cooperative, SparkJobTag, RemoteStatement, or Unsupported) |
| `CapabilitySet` | Portable query requirements: calc depth, aggregate functions, expression shapes, late binding, and features |
| `EngineCapabilities` | An engine's query capability set plus supported cancellation modes |
| `EngineFeature` | Static engine-supported feature; never a per-query behavior request |
| `SparkLambdaEval` | Spark-only legacy lambda escape hatch, not exposed by portable API |
| `ExecutionPlan[R]` | Inspectable engine-native plan with typed warnings, requirements, and normalized schema (not Serializable; holds engine-local handles) |
| `ExecutionPlanSummary` | Serializable, cluster/cache/audit-safe mirror of `ExecutionPlan` (no native plan handle, no live execution handle) |
| `ResultSchema` | Portable ordered result fields (case class so conformance tests can use `==`) |
| `ResultRow` | Portable normalized values tied to a result schema (case class) |
| `ResultEncoder[R]` | Bridge from an engine-native result to portable schema/rows |
| `ManifestDocument` | Version-2 persistent document containing a portable model and canonical digest |
| `JsonSchema[A]` | Versioned validation/read/write contract for a wire document |
| `ExtensionValue` | Closed JSON-like extension ADT |
| `ExtensionEnvelope` | Bounded inline extensions plus optional content-addressed external blob |
| `CatalogRef` | Catalog/namespace/name/version/digest identity |
| `PublishMode` | Create-only, upsert, or compare-and-set catalog publication semantics |
| `PublishResult` | Inserted, updated, or conflict publication result |
| `RollupSpec` | Portable rollup metadata with no provider or precomputed engine fields |
| `RollupRegistration` | Engine-side binding of rollup spec, provider ref, and computed source facts |
| `PolicyDisposition` | Applied, applied with warning, or rejected result for one requested policy |
## 13. References
Current implementation grounding:
- `src/main/scala/io/semanticdf/SemanticTableCore.scala:80-220,308-311`
- `src/main/scala/io/semanticdf/SemanticTable.scala:80-298,379-386`
- `src/main/scala/io/semanticdf/SemanticOp.scala:28-49,343-420,991-1139,1139-1518`
- `src/main/scala/io/semanticdf/Scope.scala:12-26,68-94`
- `src/main/scala/io/semanticdf/Model.scala:278-281`
- `src/main/scala/io/semanticdf/rollup/Rollup.scala:13-20,55-100,219-262,320-353`
- `src/main/scala/io/semanticdf/cache/CacheKey.scala:39-115`
- `src/main/scala/io/semanticdf/audit/AuditEvent.scala:65-143`
- `src/main/scala/io/semanticdf/adapters/SemanticManifest.scala:1-1590`
- `src/test/scala/io/semanticdf/RollupSerializationSpec.scala:1-217`
- `semanticdf-mcp/src/main/scala/io/semanticdf/mcp/handlers/Query.scala:29-40,75-100,226-335,601-672`
- `pom.xml:1-206`
Design and contract references:
- `docs/agents/mcp-contract.md`
- `docs/design/manifest-artifact.md`
- `docs/design/rollups.md`
- `docs/tutorial-runtime-tuning.md`
- `schemas/manifest.schema.json` (v1/current; v2 path proposed in §4.4.3)
- `src/test/scala/io/semanticdf/Spec/Post329RedesignSpec.scala`
- `.pi/agent/skills/karpathy-guidelines/SKILL.md`
- `.pi/agent/skills/scala-data-driven-refactor/SKILL.md`
- `.pi/agent/skills/debug-mantra/SKILL.md`
The pre-revision ranges in §0.2 resolve via `git show HEAD:docs/design/multi-engine-design.md`; `HEAD` is the branch version immediately before this uncommitted revision.
## 14. Changelog
- **v0.3.0-revision-4:** final DE pass. Scope-limited to the eight round-3 DE HIGH findings; no MED or LOW edits (except phantom-ADT bonus bundled with HIGH 6).
  - HIGH 1 (3.2/1.3): `PortableQueryResult.rows: Vector[ResultRow]` capped at request `maxRows`; `ResultEncoder.rows` returns a bounded finite sequence; `Iterator` removed.
  - HIGH 2 (2.1): `ExecutionPlan[+R]` no longer extends `Product with Serializable`; new `ExecutionPlanSummary` is the cluster/cache/audit-safe shape.
  - HIGH 3 (3.1): MCP handler rewritten against defined symbols; `EngineError` is a sealed ADT value (not Throwable); `Query.contextFor` / `Query.toEnvelope` / `EngineError.toErrorDetail` defined; `Models.portable` added; handler uses `Either.fold`, never `throw`.
  - HIGH 4 (4.1): `SparkEngineProvider` takes `Option[SparkSession]`; `available` and `identity` derive before any `SparkEngine` allocation; registry call now short-circuits on absence.
  - HIGH 5 (5.1, 5.2): added `EngineError.DecimalOverflow(value, precision, scale)` and `IncompatibleExprShape(shape, engine)` with `EngineIdentity`; added exhaustive `EngineError.toErrorDetail` for MCP.
  - HIGH 6 (6.1): `Model.of` returns `Either[ModelValidationError, Model]` and calls `ModelValidator.validate` exactly once; private `unsafe` constructor retained for trusted internal callers.
  - BONUS (1.1): `TransformSpec` removed from `Model` (field gone) and from v2 manifest JSON; phantom-ADT round-3 finding 1.1 closes.
  - HIGH 7 (7.1): `queryToolSchema` now shown as the actual `McpSchema.JsonSchema` over a `LinkedHashMap`; the change is `props.put("engine", strProp("string"))` (12 → 13 properties); schema test asserts all 13 names.
  - HIGH 8 (8.1): `ExtensionValue.Null` added with canonical encoding Jackson `JsonNode.VALUE_NULL`.
  - Ledger: round-3 closure section added under §0.2 with finding → fix-location table.
- **v0.3.0-revision-3:** final pass. Removed phantom ADTs (`SetOp`, `Window`,
  `WindowFrame`, `TotalRef`, `StreamingSinkPolicy`, `OutputMode.*`,
  `ProviderRef.SinkCallback`) per karpathy §2 and round-2 H1; defined `TransformSpec`,
  `PortableQueryResult`, `ExplainResult`, `EngineError`, `CatalogError`,
  `TrinoRuntimeRegistry`, `SparkPlanHandle`, and `CalculatedMeasure` (H2); corrected
  the Maven/Spark-free verification (H3); wired `AuditSinkRegistry` (H4); added concrete
  `MCPEngineProvider` implementations and registry (H5); collapsed
  `CancellationCapability.Kind` (M3); added `EngineWarning` ADT (M2); defined
  `ResultSchema`/`ResultRow` as case classes (M1); specified calc-depth algorithm and
  changed SQL engines to `Int.MaxValue` (M4, M8); added gate failure decision tree (M5);
  added `queryToolSchema` engine-property patch (M6); defined `Model.of`,
  `Dimension.field`, and `Measure.aggregate` (M7); replaced `BuildInfo.gitHash` with
  `engineAdapterVersion` (M9); added `ManualRollupSpec` rewrite (M10).
- **v0.3.0-revision-2:** Path A revision after DE + Architect falsification passes.
  Added typed policies/cancellation, structured capabilities, resolved scans, provider
  lifecycle, rollup registration split, manifest v2 wire migration, Maven module proof,
  inspectable plans, portable results, engine-identified cache/audit, catalog CAS,
  bounded extensions, MCP engine registry/handler flow, realistic WBS estimates,
  decision gate, corrected test baselines, explicit deferred features, and engine quirks.
- **v0.3.0-draft:** initial multi-engine proposal.
