# semanticdf-trino — Trino engine adapter

This module is the **Trino engine adapter** for the semanticdf library. It
implements the engine-portable `Engine[R]` contract from `semanticdf-core`
against a Trino cluster. The design extracts a portable core and surrounds
it with engine adapters — this is the **first engine adapter** for a
non-Spark engine.

## Current status — Phase 2 + Phase 3 (engine internals complete)

The Trino adapter is **functionally complete at the engine level**:
end-to-end `compile → execute → result` works against a real cluster.
The remaining work is performance / infrastructure (cluster, pool, real
JDBC driver). See [Open items](#open-items) below.

### Implemented (engine methods)

| Method | Mirrors Spark library |
|---|---|
| `compile(model, ctx)` | `model.compile()` |
| `execute(plan, ctx)` | `model.run()` (returns `Dataset[T]` analog) |
| `explain(model, ctx)` | `df.explain()` (returns SQL string; full Trino EXPLAIN needs a real cluster — see Open items) |
| `preview(model, n, ctx)` | `df.limit(n)` |
| `previewAsRows(model, n, ctx)` | `df.take(n).collect()` |
| `count(model, ctx)` | `df.count()` |
| `executeAsRows(model, ctx)` | `df.collect().map(_.getValuesMap(...))` |
| `describeCapabilities` | engine introspection (MCP `describe_model`) |

### Implemented (data shape)

`TrinoResult` is the engine-specific query result shape:

```scala
final case class TrinoResult(
    columns: List[String],
    rows:    List[List[LiteralValue]],
) extends Product with Serializable {
  def rowCount: Int
  def isEmpty: Boolean
  def cell(rowIdx: Int, colIdx: Int): Option[LiteralValue]
  def toJson: String   // mirrors df.toJSON
}
```

### Implemented (boundary contracts)

| Trait / Object | Purpose |
|---|---|
| `TrinoClient` | Source resolution boundary (table → schema) |
| `TrinoTableSchema` | Source resolution data |
| `TrinoSourceResolver` | Concrete `SourceResolver` impl |
| `TrinoConnection` | JDBC boundary (engine-internal trait) |
| `TrinoResult` | Query result data |
| `TrinoResultDecoder` | Raw JDBC → `LiteralValue` translator |
| `TrinoQueryCompiler` | `Model` → parameterized SQL compiler |

## Why a Trino adapter

Per the [multi-engine design](../docs/design/multi-engine-design.md),
the portable core (`semanticdf-core`) gives the engine-portable contract
(`Engine[R]`, `EngineError`, `Capability`, `ExecutionPlan`, `SourceResolver`).
The Trino adapter is the **first concrete non-Spark implementation** of
that contract. It validates the design and serves as a template for
future adapters (Databricks / Snowflake / Dremio).

## Phase 2 — what shipped (13 PRs)

| # | PR | What |
|---|---|---|
| 1 | #351 | Module skeleton + `pom.xml` |
| 2 | #352 | `Engine` trait + `EngineError` + `Capability` ADTs (in `core`) |
| 3 | #353 | `EngineContext` + typed policies (in `core`) |
| 4 | #354 | `TrinoEngine` implements `Engine[Any]` |
| 5 | #355 | `SqlLowerer` for `CorePredicate` |
| 6 | #356 | `SourceRef` + `ProviderRef` ADTs (in `core`) |
| 7 | #357 | `ResolvedSource` ADT (in `core`) |
| 8 | #358 | `SealedDataType` + `Field` + `ResolvedScan` (in `core`) |
| 9 | #359 | `Expr` + `LiteralValue` + `Calculator` (in `core`) |
| 10 | #360 | `RelOp` + relational IR (in `core`) |
| 11 | #361 | Model members + spec types (in `core`) |
| 12 | #362 | Rollup registration + policy defaults (in `core`) |
| 13 | #363 | `Model` + `ModelValidator` (in `core`) |
| 14 | #364 | `EngineIdentity` + `SourceStats` + `SourceResolver` (in `core`) |

## Phase 2 — Trino adapter PRs (10 PRs)

| # | PR | What |
|---|---|---|
| 1 | #365 | Wire `Engine.compile` + `Engine.explain` to `Model` |
| 2 | #366 | `ExecutionPlan[R]` + wire `Engine.execute` |
| 3 | #367 | `TrinoSourceResolver` + `TrinoClient` boundary |
| 4 | #368 | `TrinoQueryCompiler` (Model → SQL) + wire `compile` |
| 5 | #369 | Wire `JOIN` clauses from `JoinSpec` |
| 6 | #370 | Wire `RollupSpec` Track policy |
| 7 | #371 | `ParameterizedSql` + `?` placeholders (SQL injection prevention) |
| 8 | #372 | `TrinoConnection` boundary + `execute` |
| 9 | #373 | End-to-end integration test |
| 10 | #374 | `TrinoResultDecoder` |

## Phase 3 — Spark library mirrors (7 PRs)

The adapter also mirrors common Spark `DataFrame` terminal operations:

| PR | Mirrors | LoC |
|---|---|---|
| #375 | (test fixture) | 215 |
| #376 | `TrinoEngine.describeCapabilities` | 40 |
| #377 | `df.limit(n)` | 149 |
| #378 | `df.count()` | 196 |
| #379 | `df.collect().map(_.getValuesMap(...))` | 184 |
| #380 | `df.toJSON` | 220 |
| #381 | `df.take(n).collect()` | 113 |
| #382 | `df.isEmpty` | 40 |

## Open items

The remaining work is **performance + infrastructure**:

| # | Item | Blocked on |
|---|---|---|
| 1 | **Real Trino cluster integration test** (the *decision gate*) | Docker / CI infra |
| 2 | **Connection pooling** (HikariCP / Apache DBCP / Trino's pool) | Real cluster |
| 3 | **Real JDBC Trino driver** — `JdbcTrinoConnection` impl using `TrinoResultDecoder` | Real cluster |
| 4 | **Full Trino `EXPLAIN (FORMAT JSON)`** — cost estimates + partition pruning | Real cluster |
| 5 | **`executeSql(sql, params, ctx)`** — raw-SQL escape hatch (parked) | Discussion |

Items 1-4 require a real Trino cluster (typically Docker). Item 5 is a
small follow-up PR — parked at the user's request.

## Boundary contract (enforced by `pom.xml`)

The adapter's `pom.xml` has **zero** `org.apache.spark.*` dependencies:

```bash
grep -r 'org.apache.spark' adapters/semanticdf-trino/src/main/scala/
# (empty output)
```

The Trino adapter consumes the engine-portable `io.semanticdf.core.*`
ADTs (never the Spark-bearing `io.semanticdf.predicate.*`). This
enforces the multi-engine boundary at the **build level**: if a future
contributor accidentally adds a Spark import, the Trino artifact would
carry a transitive Spark dependency — which defeats the purpose of
having a Spark-free engine adapter.

## Library version

`0.2.4` — same as `semanticdf-core` and `semanticdf-spark`. No version
bump in any of the Phase 2 / Phase 3 PRs — these are adapter-internal
additions with no user-facing change.

## Why Scala 2.13

The Trino JDBC driver is Java-only; the Scala binding uses Scala 2.13 to
match the rest of the project (per `scala.version=2.13.18` in the
parent pom). Cross-compilation is not needed.

## See also

- `docs/design/multi-engine-design.md` — the full design (especially
  §4 "Adapter contract", §5.2 "Engine sketches", §7.2 "Phase 2 — Trino
  and Presto")
- `semanticdf-core/` — the engine-portable ADTs this adapter consumes
- `adapters/semanticdf-spark/` — the existing Spark adapter (reference
  for what an engine adapter provides)
- `docs/design/sdf-adapter.md` — the legacy adapter pattern (Trino
  follows the modernized version)
