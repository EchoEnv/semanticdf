# semanticdf-duckdb — DuckDB engine adapter

The **second engine adapter** for the semanticdf library. DuckDB
adds:

- **In-process transport** (alongside Trino's JDBC remote cluster)
- **Embedded analytics** (no separate cluster; the engine runs in
  the same JVM as the application)
- **Columnar SQL dialect** (PostgreSQL-compatible) that mirrors
  Spark DataFrame's semantics most closely of any non-Spark engine

## Quick start

```scala
import io.semanticdf.core.engine.{Engine, EngineContext}
import io.semanticdf.duckdb.{DuckDBEngine, JdbcDuckDBConnection}

val engine: Engine[Any] =
  new DuckDBEngine().withConnectionFactory(
    () => JdbcDuckDBConnection.fromUrl("jdbc:duckdb:"),  // in-memory
  )

val plan   = engine.compile(model, EngineContext.defaultContext).toOption.get
val result = engine.execute(plan, EngineContext.defaultContext).toOption.get
```

The DuckDB JDBC driver includes the embedded engine. No separate
server. Sub-second startup.

## Implemented (engine methods)

| Method | Mirrors Spark library |
|---|---|
| `compile(model, ctx)` | `model.compile()` |
| `execute(plan, ctx)` | `model.run()` (returns `Dataset[T]` analog) |
| `explain(model, ctx)` | `df.explain()` (returns SQL string) |
| `explainPlan(model, ctx)` | `df.explain(spark)` (cluster-aware physical plan) |
| `preview(model, n, ctx)` | `df.limit(n)` |
| `previewAsRows(model, n, ctx)` | `df.take(n).collect()` |
| `count(model, ctx)` | `df.count()` |
| `executeAsRows(model, ctx)` | `df.collect().map(_.getValuesMap(...))` |
| `schema(model, ctx)` | `df.schema` (engine-portable `SchemaSummary`) |
| `describeCapabilities` | engine introspection (MCP `describe_model`) |

## Cross-engine composition (per §4.6)

```scala
import io.semanticdf.trino.TrinoEngine
import io.semanticdf.duckdb.{DuckDBEngine, JdbcDuckDBConnection}
import io.semanticdf.unitycatalog.{HttpUnityCatalogClient, UnityCatalogSourceResolver}

// 1. Build the catalog layer (Unity Catalog REST API).
val ucClient   = HttpUnityCatalogClient(baseUrl = "http://uc.example.com:8080")
val ucResolver = UnityCatalogSourceResolver(ucClient, engine.identity)

// 2. Build a DuckDB engine and wire the same UC resolver in.
val duckEngine: Engine[Any] = new DuckDBEngine()
  .withConnectionFactory(() => JdbcDuckDBConnection.fromUrl("jdbc:duckdb:"))
  .withSourceResolver(ucResolver)

// 3. Same Model — different engine. Both engines consult the same UC.
val model = Model.of(
  name     = "orders",
  source   = SourceRef.ByName(Some("unity"), Some("semanticdf"), "orders"),
  // ...
)
// Compile against DuckDB:
val plan = duckEngine.compile(model, EngineContext.defaultContext) match {
  case Right(p)  => p
  case Left(err) => throw new RuntimeException(s"compile failed: $err")
}
// Execute against DuckDB:
val result = duckEngine.execute(plan, EngineContext.defaultContext)
```

## Connection modes

| Mode | JDBC URL | Use case |
|---|---|---|
| **In-memory** | `jdbc:duckdb:` | Tests, ephemeral workloads (zero disk) |
| **File-based** | `jdbc:duckdb:/tmp/x.db` | Persistent analytics on local files (CSV, Parquet) |
| **Server** | `jdbc:duckdb://host:port/db` | Multi-client embedded analytics |

## Pool-backed (production)

```scala
import io.semanticdf.duckdb.{DuckDBConnectionPoolFactory, DuckDBEngine}

val pool = DuckDBConnectionPoolFactory.hikari(
  jdbcUrl     = "jdbc:duckdb:/var/lib/duckdb/analytics.db",
  maxPoolSize = 10,
)
val engine = new DuckDBEngine().withConnectionFactory(pool)
```

HikariCP-backed pool with leak detection (60s threshold).

## Resource budget

| Resource | Where it lives |
|---|---|
| Memory | Bounded by the host JVM's heap (no separate container for DuckDB) |
| Disk | Bound by the file-based DB path (in-memory mode = 0 disk) |
| Engine count | Capped by HikariCP `maxPoolSize` (one engine per connection for embedded mode) |

## Module layout

```
adapters/semanticdf-duckdb/
├── pom.xml                                  (Maven module: depends on semanticdf-core + DuckDB JDBC + HikariCP)
├── README.md                                (this file)
├── src/main/scala/io/semanticdf/duckdb/
│   ├── DuckDBEngine.scala                   (engine + fluent setters + terminal ops)
│   ├── DuckDBConnection.scala               (trait — boundary for testability)
│   ├── JdbcDuckDBConnection.scala           (JDBC impl — in-memory + file + server)
│   ├── DuckDBQueryCompiler.scala            (Model → DuckDB SQL)
│   ├── DuckDBResult.scala                   (columns + rows + accessors)
│   └── DuckDBConnectionPoolFactory.scala    (HikariCP pool builder)
└── src/test/scala/io/semanticdf/duckdb/
    ├── FakeDuckDBConnection.scala           (test fixture)
    ├── DuckDBEngineSpec.scala               (15 unit tests)
    └── DuckDBEngineIntegrationSpec.scala    (8 integration tests vs. real in-memory DuckDB)
```

## Boundary contract

Zero Spark imports. Verifiable by:
```bash
grep -r 'org.apache.spark' adapters/semanticdf-duckdb/src/main/scala/
# (empty output = clean)
```

## Library version

`0.2.4` — same as `semanticdf-core`, `semanticdf-spark`, `semanticdf-trino`,
`semanticdf-unity-catalog`. No bump in this PR (pure new engine, no
user-facing change to existing call sites).

## Why DuckDB (vs. Trino)

| Concern | DuckDB | Trino |
|---|---|---|
| Setup | In-process, zero Docker | Docker cluster required |
| Startup | Sub-second | ~30s for cluster |
| Use case | Embedded analytics | Distributed query |
| SQL semantics | Mirrors Spark DataFrame | Slightly different (Hive-style) |
| Scale-up | One engine per process | Cluster of N workers |

Per the user's standing preference for **local-first** + **mirrors Spark pattern**, DuckDB is the best second engine. It proves the design works for an **entirely different transport** (in-process + JDBC vs. Trino's remote JDBC) while staying compatible with the same `Model.of(...)` user code.

## See also

- `adapters/semanticdf-trino/` — first engine adapter (remote JDBC cluster)
- `adapters/semanticdf-unity-catalog/` — first catalog adapter (REST)
- `docs/design/multi-engine-design.md` — §4.6 layer-separation principle
- `adapters/semanticdf-trino/README.md` — Trino composition pattern (same shape, different engine)