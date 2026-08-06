# semanticdf-hive-metastore — Hive Metastore catalog adapter

The **second catalog adapter** for the semanticdf library.
Uses the **Thrift transport** (vs. Unity Catalog's REST) — proving
the §4.6 layer-separation principle is **transport-agnostic**.

## Why this matters

The multi-engine design's §4.6 promises:

> *"The two contracts are transport-agnostic. They can wrap JDBC,
> REST, Thrift, gRPC, in-process libraries, or native clients."*

After this PR, we have **3 transport pairs** proven:

| Catalog | Transport | Adapter |
|---|---|---|
| Unity Catalog | REST | `UnityCatalogSourceResolver` (#394) |
| **Hive Metastore** | **Thrift (binary RPC)** | **`HiveMetastoreSourceResolver` (this PR)** |
| Engine layer | JDBC + in-process | (Trino + DuckDB, #395, #396) |

## Quick start

```scala
import io.semanticdf.core.engine.{Engine, EngineContext}
import io.semanticdf.hivemetastore.{ThriftHiveMetastoreClient, HiveMetastoreSourceResolver}

val engine: Engine[Any] =
  new io.semanticdf.trino.TrinoEngine()
    .withConnectionFactory(() =>
      io.semanticdf.trino.JdbcTrinoConnection.fromUrl("jdbc:trino://..."))
    .withSourceResolver(HiveMetastoreSourceResolver(
      ThriftHiveMetastoreClient.remote("thrift://hms.example.com:9083"),
      engine.identity,
    ))

val plan = engine.compile(model, EngineContext.defaultContext).toOption.get
```

The `SourceRef.ByName(...)` in the model is resolved against HMS
via the Thrift binary protocol. The resolver returns the same
`ResolvedSource.Scan` shape regardless of transport.

## Cross-engine composition (per §4.6)

```scala
import io.semanticdf.trino.TrinoEngine
import io.semanticdf.duckdb.{DuckDBEngine, JdbcDuckDBConnection}
import io.semanticdf.hivemetastore.{ThriftHiveMetastoreClient, HiveMetastoreSourceResolver}

// Same HMS resolver — same Model — different engines:
val hmsClient   = ThriftHiveMetastoreClient.remote("thrift://hms:9083")
val hmsResolver = HiveMetastoreSourceResolver(hmsClient, EngineIdentity("hive", "3.1.3", "0.2.4"))

// Engine 1: Trino (remote JDBC) + HMS (Thrift)
val trinoEngine: Engine[Any] = new TrinoEngine()
  .withConnectionFactory(() => JdbcTrinoConnection.fromUrl("jdbc:trino://..."))
  .withSourceResolver(hmsResolver)

// Engine 2: DuckDB (in-process) + same HMS
val duckdbEngine: Engine[Any] = new DuckDBEngine()
  .withConnectionFactory(() => JdbcDuckDBConnection.fromUrl("jdbc:duckdb:"))
  .withSourceResolver(hmsResolver)

// Same Model compiles against either engine; both consult HMS
// via the Thrift binary protocol.
```

## Module layout

```
adapters/semanticdf-hive-metastore/
├── pom.xml                              (depends on semanticdf-core + hive-metastore 3.1.3)
├── README.md                            (this file)
├── src/main/scala/io/semanticdf/hivemetastore/
│   ├── HiveMetastoreClient.scala         (trait + HmsTableSchema + HmsColumn)
│   ├── ThriftHiveMetastoreClient.scala   (real HMS Thrift client)
│   └── HiveMetastoreSourceResolver.scala (SourceResolver impl using HMS)
└── src/test/scala/io/semanticdf/hivemetastore/
    ├── FakeHiveMetastoreClient.scala     (test fixture)
    └── HiveMetastoreSourceResolverSpec.scala (10 unit tests)
```

## Boundary contract

Zero Spark imports. Verifiable by:
```bash
grep -r 'org.apache.spark' adapters/semanticdf-hive-metastore/src/main/scala/
# (empty output = clean)
```

## Library version

`0.2.4` — same as parent, core, spark, trino, unity-catalog, duckdb.
No bump in this PR (pure new adapter, no user-facing change).

## Why HMS specifically

- **Most-deployed catalog** in the Hadoop/Spark ecosystem
- **Different transport** (Thrift) from UC (REST) — proves §4.6
- **Compatible with Trino** via Trino's Hive connector (cross-engine
  composition: TrinoEngine + HMS resolver = real-world Trino+HMS
  setup)

## Production deployment

```bash
# Real HMS Thrift server (typical Hadoop deployment)
export HMS_URI=thrift://hms-prod-1.example.com:9083
```

```scala
ThriftHiveMetastoreClient.remote(sys.env("HMS_URI"))
```

The adapter does NOT manage the HMS server lifecycle — production
deployments run HMS as a standalone service (often alongside
Spark/Hive). The adapter is purely a consumer.

## Memory + disk monitoring (per user constraint)

- **Memory**: bounded by host JVM heap (no separate container).
  HMS client uses a small fixed-size connection pool internally.
- **Disk**: no local disk usage (HMS is remote; the adapter is
  stateless). HMS itself manages its own Derby/MySQL backend.

## Future work (not in this PR)

- **Integration test against Docker HMS** — per user constraint,
  HMS Docker setup is heavy (similar to Dremio's slow startup).
  Deferred until we have memory headroom or a lighter HMS image.
- **Embedded HMS mode** — `EmbeddedMetaStoreClient` is a real
  in-process HMS option but requires DataNucleus bootstrap.
  Deferred per karpathy §2 ("minimum code that solves the problem").
- **HMS 4.x nullability** — HMS 3.x doesn't carry per-column
  nullability. HMS 4.x does. Future PR can migrate when needed.

## See also

- `adapters/semanticdf-unity-catalog/` — first catalog adapter (REST)
- `adapters/semanticdf-trino/` — first engine adapter
- `adapters/semanticdf-duckdb/` — second engine adapter
- `docs/design/multi-engine-design.md` — §4.6 layer-separation principle