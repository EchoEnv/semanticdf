# semanticdf-platform

Standalone semantic data platform — Restate-native runtime. P1 of the platform-architecture design ([`docs/design/platform-architecture.md`](../docs/design/platform-architecture.md)).

This module is a separate Maven project (not a submodule of the parent `semanticdf`). It depends on the `semanticdf` library as a Maven dependency and uses the Restate SDK for durable async/await.

## Status

**P1 skeleton.** Handler bodies are stubs. The full implementation lands in subsequent PRs (per the 6-phase migration roadmap in the design doc).

What's in this skeleton:
- The 5 Restate services with the right Restate primitives
- A `PlatformApplication` main class that boots the runtime + HTTP server
- A `pom.xml` with the right dependency set
- A `docker-compose.yml` for local dev (Postgres + Restate server)

What's NOT in this skeleton:
- Actual `semanticdf.of(spark, model)` integration inside `Restate.run` blocks
- The Caffeine L1 cache (separate PR)
- The engine adapter (Trino plugin, Spark SDK) — P2
- The auth layer (mTLS, API key) — separate PR
- The Restate admin client for ops tooling — separate PR

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
  mvn exec:java -Dexec.mainClass=io.semanticdf.platform.PlatformApplication
```

The platform's REST surface is on `http://localhost:8080`; the Restate server is on `http://localhost:9070`.

### Tests

```bash
cd semanticdf-platform
mvn test
```

## P1 deliverables (the actual implementation work)

Per the design doc, the P1 implementation has these deliverables (separate PRs):

1. **Wire `semanticdf.of(spark, model)` into `ModelService.register`** — the first cross-language boundary inside a `Restate.run` block. Needs a deterministic-purity audit of the library first.
2. **Postgres-backed model registry** — replace the in-process `Models` map (in `semanticdf-mcp/Models.scala`) with `ModelService` reads from Postgres.
3. **Caffeine L1 cache** in the platform's REST layer — sub-100µs hits bypass Restate.
4. **Auth layer** — mTLS for service-to-service (Restate-managed), API-key for REST clients.
5. **One end-to-end smoke test** that boots the runtime + drives a real query.

## Design decisions specific to this module

- **Java 21.** Restate has no Scala SDK. The platform's services are Java; the `semanticdf` library stays Scala and is consumed as a JAR.
- **Maven.** Matches the parent project. (Gradle would also work; pick one.)
- **Co-located runtime + services in v1.** One process per node. In P3, split if scale demands independent scaling of the HTTP/ingress tier vs the workflow execution tier.
- **No custom build profiles yet.** The pom will grow as the platform's needs grow (Spark 4 for the engine adapter, etc.).

## Layout

```
semanticdf-platform/
├── pom.xml                                       Maven project, depends on semanticdf 0.2.0 + Restate 2.8.0
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
