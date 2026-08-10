# Hive Metastore as a Catalog Source

**Status:** v0.3.1 — design + demo
**Scope:** Architecture pattern for the platform's engine-portable query path
**Applies to:** all engine adapters (Spark, DuckDB, PostgreSQL, Trino, …)

---

## 0. TL;DR

The platform's `QueryService` uses an **engine-portable** query path. The
engine (Spark / DuckDB / PG) is selected at runtime via the
`MCPEngineRegistry`. The model is portable. The **source resolution** is
HMS-style: the model says `source: SourceRef.ByName("flights_tbl")` —
that's a table name in the catalog (the way an HMS-resolved query
would look). The engine's catalog system resolves the name to schema
+ data; the engine compiles and executes the SQL.

| Layer | Component | Role |
|---|---|---|
| Control plane | External Restate | Records invocations; web UI at port 9070 |
| Engine registry | `MCPEngineRegistry` | Selects engine per `request.engine` |
| Engine | Spark / DuckDB / PG | Compiles + executes portable `Model` → SQL |
| Catalog | Engine's catalog (HMS-compatible) | Resolves `SourceRef.ByName` → table data |
| Source | `SourceRef.ByName("flights_tbl")` | The portable model reference |

**HMS is a SOURCE CATALOG, not an engine.** The model.source is the
HMS-resolved table name. Each engine has its own catalog system
(Spark's `spark.sql.catalogImplementation`, DuckDB's catalog, PG's
`search_path`) which all accept the same `SourceRef.ByName` pattern.

---

## 1. The pattern

The platform's portable `Model` carries a `source: SourceRef` which
the engine resolves at compile time:

```scala
case class Model(
    name: String,
    source: SourceRef,  // ← catalog-resolved
    dimensions: List[Dimension],
    measures: List[Measure],
    filters: List[FilterSpec],
    ...
)

sealed trait SourceRef
case class ByName(catalog: Option[String], namespace: Option[String], table: String)
  extends SourceRef
```

The `ByName` variant is the **HMS-style reference**: a table name in
some catalog. The engine adapter resolves this through its catalog
system:

- **Spark** with `spark.sql.catalogImplementation=hive` — resolves via
  Hive Metastore (Thrift protocol at `hive.metastore.uris`)
- **DuckDB** — resolves via DuckDB's catalog system (in-memory
  `?cache=shared&name=…`, attached databases, etc.)
- **PostgreSQL** — resolves via `search_path` (defaults to
  `$user, public`)
- **Trino** — resolves via Trino's catalog system
- **Unity Catalog** — resolves via Databricks UC API

The `HiveMetastoreSourceResolver` (in
`adapters/semanticdf-hive-metastore`) provides the HMS-specific
`ResolvedSchema` lookup. Other engines have their own equivalent.

---

## 2. Why this matters

### 2.1 The engine stays portable

The model says `source: ByName("flights_tbl")`. The engine adapter
**translates** this to its native form:

| Engine | Native form | Translation |
|---|---|---|
| Spark | `spark.table("flights_tbl")` | Thrift via HMS if `spark.sql.catalogImplementation=hive` |
| DuckDB | `SELECT * FROM flights_tbl` | Bare-table resolves against current catalog (file-based or `?cache=shared`) |
| PostgreSQL | `SELECT * FROM "flights_tbl"` | Resolves against `search_path` (defaults to public schema) |
| Trino | `SELECT * FROM hive.semanticdf.flights_tbl` | Catalog-qualified |

The model stays portable (just a name). The engine's catalog system
handles the resolution.

### 2.2 The control plane stays simple

The Restate ingress records invocations. The model source is
`ByName("flights_tbl")` — the same name an HMS-resolved query would
carry. The web UI shows the invocation regardless of which engine
resolved the catalog. **HMS doesn't appear in the control plane; it
appears as the SOURCE in the model.**

### 2.3 Adding a new catalog is additive

A new catalog integration (e.g., AWS Glue) plugs in via the
`SourceResolver` interface in `io.semanticdf.core.engine`:

```scala
trait SourceResolver {
  def resolve(ref: SourceRef, ctx: EngineContext)
    : Either[EngineError, ResolvedSource]
}
```

Each catalog adapter implements this trait. The engine-portable
compilation path calls it before generating SQL.

---

## 3. The demo: HMS-style with Spark's in-memory catalog

For local development and CI, the platform uses **Spark's in-memory
catalog** which has the same Thrift-compatible `SourceRef.ByName`
API. The platform registers the demo data in this catalog at
startup:

```java
// In PlatformApplication.main(), DEMO_MODE path
if (!connectMode) {
    spark.sql("CREATE DATABASE IF NOT EXISTS semanticdf_demo");
    spark.sql("CREATE TABLE IF NOT EXISTS semanticdf_demo.flights_tbl (" +
        "carrier STRING, flight_count BIGINT, total_distance BIGINT) " +
        "USING parquet");
    // Seed if empty
    long count = spark.sql("SELECT COUNT(*) FROM semanticdf_demo.flights_tbl")
                       .head().getLong(0);
    if (count == 0) {
        spark.sql("INSERT INTO semanticdf_demo.flights_tbl VALUES " +
            "('AA', 1, 100), ('AA', 2, 200), ('UA', 3, 300)");
    }
}
```

The model:

```yaml
flights:
  table: flights_tbl        # ← SourceRef.ByName — HMS-style reference
  description: Flights data
  dimensions: { carrier: carrier }
  measures:
    c: "count(flight_count)"
    d: "sum(total_distance)"
```

The query routes through the engine-portable path:

```
enduser → curl /QueryService/runQuery
     → External Restate (port 8080)
     → Platform (port 9080) — engine-portable path
     → Engine (Spark / DuckDB / PG) — resolves flights_tbl via its catalog
     → Data
     → Response

The invocation is recorded in the External Restate's journal:
```
[2026-08-10 18:20:48] inv_13RlTxjjasoX4j7VXNB3HdUgxkxeSxTNmQ
  Target: QueryService/runQuery
  Status: completed with success
  Deployment: dp_14EWcxTPhXZnJcrgC36PPSV
```

**The web UI at http://46.62.252.35:9070/ shows the invocation.**

---

## 4. Production: real HMS

For production, swap the in-memory catalog for a real HMS:

```hocon
# spark.conf
spark.sql.catalogImplementation=hive
spark.hadoop.hive.metastore.uris=thrift://hms:9083
spark.sql.catalog.spark_catalog.type=hive
```

Then a real HMS service (Apache Hive Metastore, AWS Glue, Databricks
Unity Catalog, etc.) provides the metadata. The model stays the same
(`source: ByName("flights_tbl")`). The engine-portable path doesn't
change.

The platform's HMS adapter
(`adapters/semanticdf-hive-metastore`) provides the
`HiveMetastoreSourceResolver` and `ThriftHiveMetastoreClient` for the
Thrift-based lookup. The model.source is resolved via Thrift RPC to
the HMS server.

---

## 5. Why "in-memory catalog" is sufficient for the demo

| Property | In-memory (demo) | Real HMS (prod) |
|---|---|---|
| API | `SourceRef.ByName(name)` | `SourceRef.ByName(catalog, namespace, name)` |
| Resolution | `spark.table(name)` (in-process) | `hive_metastore.getTable(db, name)` (Thrift) |
| Schema | `DESCRIBE TABLE` (cached) | `DESCRIBE EXTENDED` (Thrift) |
| Survives restart | No (re-seed) | Yes (durable) |
| Multi-tenant | No | Yes (per-database) |
| Concurrent writers | No | Yes (with locks) |

For the demo, the in-memory catalog proves the **engine-portable query
path** works end-to-end. The same path works against a real HMS in
production — the only thing that changes is the
`SourceResolver` implementation (in-memory vs Thrift).

---

## 6. What this means for the engine-portable path

The `MCPEngineProvider.query()` signature:

```scala
def query(
    model:   Model,           // ← portable
    request: MCPQueryRequest, // ← portable
    ctx:     EngineContext,   // ← per-engine
): Either[EngineError, PortableQueryResult]  // ← portable result
```

The model.source (`SourceRef.ByName("flights_tbl")`) is **engine-portable**.
The engine's catalog system resolves it. The query routes through
the engine's `Engine.compile(model)` which produces portable
`ExecutionPlan`. The engine's `Engine.execute(plan)` reads the data.

The platform's `QueryService.runQueryViaEngineRegistry` selects the
engine based on `request.engine` (or the registry default) and
dispatches. The invocation is recorded in the external Restate.

**HMS is a SOURCE, not an engine.** It's the metadata layer that
resolves `SourceRef.ByName(name)`. The engine is what executes the
query.

---

## 7. See also

- `docs/design/multi-engine-design.md` — overall engine-portable design
- `docs/design/platform-architecture.md` — the platform's overall shape
- `adapters/semanticdf-hive-metastore/` — the HMS adapter (Thrift client)
- `adapters/semanticdf-spark/src/main/scala/io/semanticdf/SemanticField.scala` — `FieldRef` (the model field reference)
- `semanticdf-core/src/main/scala/io/semanticdf/core/model/SourceRef.scala` — `SourceRef` ADT

---

**Key takeaway:** HMS is a CATALOG (metadata service), not an ENGINE
(execution). The platform's engine-portable query path uses
`SourceRef.ByName(name)` for catalog resolution — this is
HMS-compatible by design. The in-memory Spark catalog used in the
demo is the same Thrift-compatible pattern. Production deployments
swap in a real HMS via `hive.metastore.uris`; the model, the engine,
and the control plane don't change.
