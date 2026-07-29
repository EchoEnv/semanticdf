# semanticdf-platform

Standalone semantic data platform — Restate-native runtime. P1 of the platform-architecture design ([`docs/design/platform-architecture.md`](../docs/design/platform-architecture.md)).

This module is a separate Maven project (not a submodule of the parent `semanticdf`). It depends on the `semanticdf` library as a Maven dependency and uses the Restate SDK for durable async/await.

## Status

Standalone Restate-native runtime, post-v0.2.2. All five services are
wired to their durable Postgres substrate (when the corresponding
environment flags are set) and to the platform's `SparkSession`. The
streaming pipeline runs end-to-end with crash reconciliation; the model
registry persists across restarts; the audit log is replayable; the
query path serves from a result cache.

Flag-gated durability (all default-off today — the platform runs end-
to-end in journal-only mode without any Postgres):

| Env var | When true | Default |
|---|---|---|
| `SEMANTICDF_AUDIT_PERSIST` | `AuditService` writes to Postgres | false |
| `SEMANTICDF_MODELS_PERSIST` | `ModelService.register` writes to Postgres | false |

The streaming catalog (`PostgresStreamCatalog`) is built-in and enabled
whenever `SEMANTICDF_CATALOG_JDBC_URL` is set. See
[`docs/design/platform-architecture.md`](../docs/design/platform-architecture.md)
§2.5 for the storage-tier story.

What remains for v0.2.3+ follow-ups:

- **Caffeine L1 cache** in the platform's REST layer — sub-100µs hits bypass Restate.
- **Engine adapter** (Trino plugin, Spark SDK beyond Connect) — P2 in the roadmap.
- **Auth layer** (mTLS for service-to-service, API-key for REST clients).
- **Multi-node HA** — 3 platform/Restate replicas across AZs (P3).
- **Restate admin client** for ops tooling.

## Architecture

Per [`docs/design/platform-architecture.md`](../docs/design/platform-architecture.md):

| Service | Restate primitive | Key |
|---|---|---|
| `ModelService` | `@VirtualObject` | model name |
| `QueryService` | `@Service` (stateless) | — |
| `StreamingService` | `@Workflow` | stream-id |
| `AuditService` | `@VirtualObject` | tenant |
| `CatalogService` | `@Service` (stateless) | — |

State placement rule: **Restate's journal holds coordination state (recent, recoverable from replay); Postgres holds queryable history (durable, engines read directly).**

## Local dev

### Build

```bash
cd semanticdf-platform
mvn compile
```

### Run with docker-compose (Postgres + Restate server)

```bash
docker-compose -f semanticdf-platform/docker-compose.yml up -d
cd semanticdf-platform
PORT=8080 \
  POSTGRES_HOST=localhost \
  POSTGRES_PORT=5432 \
  POSTGRES_USER=semanticdf \
  POSTGRES_PASSWORD=semanticdf \
  POSTGRES_DB=semanticdf \
  mvn exec:java -Dexec.mainClass=io.semanticdf.platform.PlatformApplication -Plocal
```

The `-Plocal` profile bundles Spark + log4j-core into the runtime classpath (default scope is `provided` for slim production JARs). Spark 3.5.x needs JVM `--add-opens` flags on JDK 17 — the platform ships a `.mvn/jvm.config` so `mvn exec:java -Plocal` works without shell wrappers. The platform's REST surface is on `http://localhost:8080`; the Restate server is on `http://localhost:9070`.

The platform's REST surface is on `http://localhost:8080`; the Restate server is on `http://localhost:9070`.

### Spark engine mode

The platform reads `SEMANTICDF_SPARK_CONNECT_URL` to choose how to obtain its `SparkSession`:

| Mode | When | Where Spark runs |
|---|---|---|
| **Local** (default) | env var unset | In-process Spark driver, master from `SPARK_MASTER` (default `local[*]`) |
| **Connect** (production) | env var set, e.g. `sc://spark-connect:15002` | Long-running Spark Connect cluster (separate JVM) |

Connect mode turns the platform into a pure control plane — the engine's JVM lifetime is decoupled from the platform's, and the platform initiates nothing Spark-related beyond the gRPC client. Requires Spark 4.0+ (Spark Connect ships as a separate artifact on 3.x). Set the env var + restart; no platform code changes.

### Tests

```bash
cd semanticdf-platform
mvn test
```

## Post-v0.2.2 deliverables (still on the roadmap)

1. **Caffeine L1 cache** in the platform's REST layer — sub-100µs hits bypass Restate.
2. **Multi-node HA** — 3 platform/Restate replicas across AZs (P3 in the design doc).
3. **Engine adapter** — Trino plugin + Spark SDK (P2 in the roadmap).
4. **Auth layer** — mTLS for service-to-service (Restate-managed), API-key for REST clients.

## Design decisions specific to this module

- **Java 21.** Restate has no Scala SDK. The platform's services are Java; the `semanticdf` library stays Scala and is consumed as a JAR.
- **Maven.** Matches the parent project. (Gradle would also work; pick one.)
- **Co-located runtime + services in v1.** One process per node. In P3, split if scale demands independent scaling of the HTTP/ingress tier vs the workflow execution tier.
- **Build profiles.** The pom has a `local` profile that bundles Spark + log4j-core into the runtime classpath (needed for `mvn exec:java` on a fresh checkout). Production deployments use neither profile (Spark stays at `provided` scope, used in the cluster). The library's `spark4` profile is independent — they can be composed for local Spark-4 testing.

## Layout

```
semanticdf-platform/
├── pom.xml                                       Maven project, depends on the semanticdf library + Restate 2.8.0
├── docker-compose.yml                            Postgres + Restate server for local dev
├── README.md                                     This file
└── src/
    ├── main/java/io/semanticdf/platform/
    │   ├── PlatformApplication.java              Main entry — boots Restate runtime + HTTP server
    │   ├── model/ModelService.java                @VirtualObject — registration, version bumps
    │   ├── query/QueryService.java                @Service — stateless query routing
    │   ├── streaming/StreamingService.java        @Workflow — long-running stream lifecycle
    │   ├── audit/AuditService.java                @VirtualObject — per-tenant audit log
    │   └── catalog/CatalogService.java            @Service — stateless catalog reads
    └── test/java/io/semanticdf/platform/
        (TBD: smoke test in a follow-up PR using HTTP, not the in-process Client)
```
