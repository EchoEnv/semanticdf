# Platform Architecture — Standalone Semantic Data Platform (Restate-Native)

**Status:** DRAFT — design review.

This document is the consolidated output of a senior data-architect
and a senior software-architect review, plus the reconciliation of
their disagreements. It pivots the previous "stateless Java daemon"
design onto **Restate.dev** as the platform's runtime substrate.

## 1. Executive summary

The current `semanticdf` is a Scala 2.13 / Spark library that runs
*inside* the user's Spark job — no server, no cluster. The MCP
server is a thin stdio/REST wrapper.

The target state inverts the dependency: **`semanticdf` becomes
the host, and Spark/Trino are compute guests that consume the
platform's REST contract.** The platform is a long-running, fault-
tolerant, engine-agnostic service. The runtime is **Restate.dev**
(distributed durable async/await, journaled in Postgres).

The headline design decisions:

1. **Restate-native platform** — services are Restate `Service` /
   `VirtualObject` / `Workflow`, not a stateless HTTP daemon.
2. **Java 21** for the platform layer. Scala stays only in the
   `semanticdf` library, consumed as a JAR.
3. **5 Restate services** with a clear state-ownership rule:
   *Restate's journal = coordination (recent, recoverable from replay);
   Postgres = record (durable, engines read directly).*
4. **Postgres for everything metadata + Restate's journal + object
   storage for raw data.** No ZooKeeper, no etcd, no Kafka.
5. **REST + Restate protocol** — REST for humans/agents, Restate's
   own protocol for engines (lower latency + exactly-once submission).
6. **Caffeine L1 for cache hits** — the platform's REST layer keeps
   sub-100µs hits by bypassing Restate on the hot read path.

The headline trade-off: the Restate runtime adds operational
overhead (one more process; the team needs journal-replay debugging
skills). In return we get durable multi-step workflows, per-key
serialization, and activity retries — capabilities the previous
design would have had to hand-roll.

## 2. Target architecture

### 2.1 Components

| Component | Responsibility | Implementation |
|---|---|---|
| **Restate runtime** | Hosts the platform's services. Journal in Postgres. mTLS between services. | Restate server binary (Rust), 1+ nodes, 3-AZ. |
| **Platform services** (in the Restate runtime) | `ModelService`, `QueryService`, `StreamingService`, `AuditService`, `CatalogService` | Java 21, defined as Restate `@Service` / `@VirtualObject` / `@Workflow`. |
| **`semanticdf` library** (unchanged) | Pure-data compiler. The platform calls into it inside `Restate.run` blocks. | Scala 2.13 / Spark; consumed as a JAR. |
| **Postgres** | Platform metadata (model registry, lineage, audit, query journal mirror) **and** Restate's journal | Managed (RDS / Cloud SQL), 3-AZ sync replication. |
| **Object storage** (S3 / MinIO / GCS) | Raw data tables (Iceberg) + Restate's WAL/snapshots | Object storage. |
| **Engine adapters** | Per-engine: Trino plugin (Java), Spark SDK (Scala). Consume the platform's REST + optionally the Restate protocol. | Each engine has its own thin adapter. |

### 2.2 The 5 Restate services

Each service is picked for what its lifecycle actually is — not
because the names are neat.

| Service | Restate primitive | Key | Journal state | Postgres (source of truth) |
|---|---|---|---|---|
| `ModelService` | `@VirtualObject` | model name | `currentVersion`, `pendingBuild`, `lastInvalidatedAt`, `manifestHash` | model YAML, versioned definitions, audit-of-registrations |
| `QueryService` | `@Service` (stateless) | — | none | audit events only |
| `StreamingService` | `@Workflow` | stream-id (client-supplied) | `status`, `checkpointLocation`, `lastBatchTs`, `errorCount` | long-retention query journals |
| `AuditService` | `@VirtualObject` | tenant | last write offset, last dedup hash | append-only event log, time-partitioned |
| `CatalogService` | `@Service` (stateless) | — | none | read-replica view of model registry |

**Why these primitives:**
- `ModelService` is a `VirtualObject` keyed by model name because per-model
  registration must be serialized by definition. The `Counter` example
  in `/tmp/sdk-java/examples/.../Counter.java` is the canonical pattern.
- `QueryService` is a `@Service` (stateless) because each query is a
  short-lived stateless Spark action — there is nothing to suspend or
  journal; idempotency is the caller's cache key.
- `StreamingService` is a `@Workflow` because streaming is long-running
  (hours/days) and the cancellation/pause signals map onto
  `DurablePromise`. The `LoanWorkflow` example is the shape.
- `AuditService` is a `VirtualObject` per-tenant because audit writes
  need per-key serialization and audit reads are read-heavy.
- `CatalogService` is a plain `@Service` because `list_models` and
  `describe_model` are read-only and stateless.

### 2.3 State placement rule

> **Restate's journal holds coordination state (recent, recoverable
> from replay). Postgres holds queryable history (durable, engines
> read directly).**

| State | Lives in | Rationale |
|---|---|---|
| Model registry | Postgres | External readers (catalogs, dashboards, lineage tools) query it. The Restate `ModelService` holds only "registration V in progress" + "current version pointer." |
| Lineage | Postgres (durable cache) + Restate (per-version "dirty" flag) | Lineage is queryable. The dirty flag is control-flow. |
| Audit log | Postgres (append-only) | Restate's journal is a replay log, not an audit trail. Mixing them is a critical violation. |
| Plan cache | Restate `VirtualObject` keyed by query-shape hash + optional Redis L2 | Plan cache is read by the platform's own hot path; survives restarts via the journal. |
| Query journal | Restate `Workflow` state (per-query ID) | Per-query in-flight state. Workflow owns the lifecycle. |
| Streaming-query state | Restate `Workflow` state (per-stream ID) | Per-stream in-flight state. Workflow's single-execution-per-ID guarantee is the *whole point*. |
| Plan-cache observability counters | Postgres (optional mirror) | For dashboards only; source of truth is Restate. |

### 2.4 Data flow (one query end-to-end)

```
┌─────────┐    ┌──────────────────┐    ┌──────────────┐
│  Agent  │───▶│  Platform REST   │───▶│  Restate      │───▶ Postgres
│ (LLM,   │    │  (Caffeine L1)   │    │  Services     │       (record)
│  BI)    │    │                  │    │  (Java 21)    │
└─────────┘    └──────────────────┘    └──────────────┘
                                              │
                                              ▼
                                     ┌──────────────┐
                                     │ Engine        │
                                     │ (Spark,       │
                                     │  Trino)       │
                                     └──────────────┘
                                              │
                                              ▼
                                     Object storage (Iceberg)
```

For a query:
1. Agent calls `POST /v1/query` (REST). The platform's HTTP layer
   checks the in-process Caffeine L1 cache. **Hit → return cached
   plan, sub-100µs.** No Restate hop.
2. **Cache miss** → call into Restate's `QueryService` (stateless) or
   the relevant `ModelService` (VirtualObject) for the plan. The
   response is cached in L1 for next time.
3. Restate calls `semanticdf.of(spark, model)` *inside a `Restate.run`
   block* (the boundary that makes non-determinism replay-safe). The
   compiled plan goes to Postgres.
4. Engine reads Iceberg tables from object storage, returns
   Arrow-encoded results.
5. Platform streams results back to agent via Arrow Flight (or REST
   chunks for the BI consumer).

**The engine call is NOT routed through Restate.** Restate is the
coordination/integration protocol, not a data-plane protocol.
Spark Connect (gRPC) and Arrow Flight serve that role for the engine
boundary — see §5 below.

For metadata reads (list models, get_field_lineage,
get_dependencies): the platform reads from the Caffeine L1 (or
Postgres on miss). **No Spark action. Sub-100ms p99.**

### 2.5 HA / fault-tolerance

- **3 platform/Restate replicas across AZs** (single-region for v1).
  Stateless Restate runtime; all state in Postgres.
- **Synchronous PostgreSQL replication** across the 3 AZs. Now
  serving two roles: platform metadata AND Restate's journal.
- **Restate's per-key serialization** replaces the prior
  `SELECT ... FOR UPDATE` round-trips on `ModelService`.
- **Restate's activity retries** with `RetryPolicy` replace the prior
  bespoke `Query.withTimeout` retry logic.
- **Rolling upgrade** (1 node at a time). Restate supports N/N-1
  protocol via `ServiceDefinition.Configurator` knobs
  (`idempotencyRetention`, `journalRetention`, `invocationRetryPolicy`).
- **Journal + WAL + snapshots** persisted in object storage. On a
  region-wide Postgres failure, the platform can rebuild from
  object storage alone (catalog state reconstructable from WAL + last
  snapshot).

### 2.6 Resource budget

| Resource | Per node | Total (3 nodes) |
|---|---|---|
| Container / pod size | 2 GiB | 6 GiB |
| Heap | 1 GiB | 3 GiB |
| CPU | 2 vCPU | 6 vCPU |
| Postgres (RDS, db.r6g.large) | shared | 16 GiB RAM, 4 vCPU, 100 GB SSD |
| Object storage (S3 / MinIO) | shared | grows with data |

**Latency budget**:

| Operation | Target p50 | Target p99 |
|---|---|---|
| Authenticated metadata read (Caffeine hit) | 100 µs | 1 ms |
| Authenticated metadata read (Caffeine miss → Restate) | 1 ms | 5 ms |
| Simple plan compile (cache hit) | 100 µs | 1 ms |
| Simple plan compile (cache miss) | 10 ms | 25 ms |
| Complex lineage lookup | 30 ms | 100 ms |
| Engine query (cache hit) | 5 ms | 50 ms |
| Engine query (uncached, simple) | 200 ms | 1 s |
| Engine query (uncached, complex) | 1 s | 10 s |

**Throughput target per node**: ~1,000 metadata reads/sec, 250 plan
compilations/sec, 500 lineage lookups/sec. The hot read path bypasses
Restate.

## 3. Language and runtime choice

**Java 21 for the platform layer; keep `semanticdf` the library in
Scala; the boundary is a Maven JAR dep, not a language boundary
inside the platform.**

- Restate has Java and Kotlin SDKs only. No Scala SDK.
- The `Restate.run` / `Restate.state` API uses `Class<T>` and
  `TypeTag<T>` for serde. FFI wrappers between Scala and Restate would
  force every journal entry to box through `ThrowingSupplier` and
  obscure the dependency rule.
- The Scala library's value is its typeclass seam
  (`SemanticField[T]`, `SemanticDimension[T]`, `SemanticMeasure[T]`)
  and its Spark `Encoder` integration. Porting would lose both.
- The team writes Java for the platform's Restate services; the
  Scala library is consumed as a JAR. Domain DTOs are Java `record`s
  (the `Counter.CounterUpdateResult` shape in `/tmp/sdk-java/examples/.../Counter.java` is the template).

## 4. Technology stack per layer

| Layer | Recommendation | Why |
|---|---|---|
| **Runtime** | **Restate** (`/tmp/sdk-java/`) | Durable execution, per-key serialization, activity retries — the work the prior design would have hand-rolled. |
| **Metadata store + journal** | **Postgres 16** (managed, 3-AZ sync-replicated) | Already proven; Restate's journal *is* Postgres, no new datastore. |
| **Hot read cache** | **In-process Caffeine** inside the platform's REST layer | Sub-100µs cache hits, bypasses Restate. |
| **L2 cache (v2, optional)** | Redis | Adds one more service. Defer until workload demands. |
| **Storage backend** | **Object storage (S3 / MinIO) + Apache Iceberg** for raw data | Engine-agnostic; Spark/Trino/Flink read Iceberg natively. |
| **REST surface** | Vert.x (Restate SDK ships `sdk-http-vertx`) | Embedded in the platform process. |
| **Observability** | OpenMetrics + structured JSON stdout + health/readiness + optional OTel | Operators supply the observability stack; don't bundle. |
| **Security (v1)** | mTLS for service-to-service (Restate-managed); API-key/JWT for REST clients; namespace-scoped RBAC | Standard. Multi-tenant is logical, not process-level. |
| **Upgrade** | Rolling replacement; N/N-1 protocol via `ServiceDefinition.Configurator` knobs | Zero request-path downtime. |

## 5. Engine interop

**REST for agents, Restate for engine coordination, engine-native
protocol (Spark Connect, Trino client) for the data plane.** (Option C,
revised by PR #240 — the engine call is NOT Restate.)

- **Agents (LLM, BI):** REST. Short-lived, stateless, JSON-shaped. Same
  surface as the prior design.
- **Engine data plane (Spark, Trino):** Native engine protocol.
  - Spark: **Spark Connect (gRPC)** — `sc://host:port` form. The platform
    becomes a thin control-plane JVM that submits queries to a
    long-running Spark cluster. PR #240 added `SEMANTICDF_SPARK_CONNECT_URL`
    env var to `PlatformApplication.main`; the library's `SdfSession.createFromEnv`
    routes through the right factory branch based on the env var.
  - Trino: JDBC (legacy) or Trino Client (newer). Either is engine-native,
    not Restate-shaped.
  - Without the env var, the platform falls back to in-process Spark via
    `master("local[*]")` for tests and quickstart — *not* a production
    topology. Flag is on the env var, not a code path switch, so we
    never fork the runtime shape.
- **What "engine-agnostic" means:** Engines depend on the platform's
  *service surface* (the set of Restate handlers), not on a specific
  engine adapter. The Restate services are the contract.

## 6. The 4 boundaries, with wire shapes

| # | Boundary | Wire shape | Consistency | Hot/cold |
|---|---|---|---|---|
| **B1** | Platform REST ↔ Restate services | Java method call (in-process) | Restate-journal replay | Hot (cache-miss path) |
| **B2** | Platform REST ↔ Agent | JSON over HTTP | Session-coherent | Hot |
| **B3** | Restate services ↔ Engine | REST/JSON for control; Arrow Flight for results | Session-coherent | Hot (per query) |
| **B4** | Restate services ↔ Engine (optional) | Restate gRPC | Exactly-once (Restate-managed) | Hot (engines that opt in) |
| **B5** | Restate services ↔ Postgres | SQL (transactional, both metadata + journal) | Strong | Cold |
| **B6** | Engine ↔ Object storage (Iceberg) | Parquet + Avro | Snapshot isolation (Iceberg guarantee) | Hot (per query) |
| **B7** | Platform → Agent (results) | Arrow Flight (columnar) | Exactly-once result stream | Hot |

The internal boundary B1 is **new** in this design — it's durable and
replayed, so anything crossing it must be a deterministic function of
its inputs from the journal's perspective. The previous design had
no such constraint.

## 7. Migration roadmap

The spine is unchanged from the previous design; the daemon is
replaced by Restate services, the REST surface is preserved, and
`semanticdf` the library is unchanged.

| Phase | Goal | Pivot-specific delta |
|---|---|---|
| P0 (now) | `semanticdf` library + `semanticdf-mcp` thin stdio/REST wrapper | unchanged |
| **P1 (months 1-3)** | **Restate-native platform** | Java 21 module defining the 5 services; `semanticdf` consumed as a JAR; `semanticdf-mcp` becomes a thin client to the platform via REST. Same wire shape — backwards compatible. |
| P2 (months 4-5) | Engine adapters (Trino plugin, Spark SDK) | Adapters become Restate `Client` callers, not REST callers. Demonstrates the new protocol. |
| P3 (months 6-7) | Multi-node HA | 3 platform/Restate nodes across AZs; 3-AZ Postgres. Rolling upgrade with N/N-1 via `ServiceDefinition.Configurator` knobs. |
| P4 (months 8-9) | Iceberg substrate | Object storage becomes the data substrate; engine adapters read/write Iceberg directly. |
| P5 (months 10-12, V2) | Multi-region | Regional active-passive. Restate journal + Postgres per region. |

## 8. Trade-off analysis

The user's design brief flagged: "lightweight / no overhead" vs
"fault-tolerant / long-running." Here's how the Restate-pivot design
balances them:

| Pull toward "lightweight" | Pull toward "fault-tolerant" | Resolution |
|---|---|---|
| One binary, no sidecars | Need replicated state | **Restate + Postgres** — the runtime is one process; the journal is one Postgres. No ZK / no etcd / no Kafka. |
| No Kafka / no service mesh | Need durable audit + query journal | **Postgres serves two roles** — the platform's record store AND Restate's journal. No separate event bus. |
| Sub-millisecond latency | Need durable recovery on restart | **Caffeine L1 bypasses Restate** for cache hits. Restate is the miss path. |
| Minimal services | Multi-node for HA | **3 Restate nodes + 1 Postgres + 1 object storage** = three services. (Adding Redis would be a 4th; deferred.) |
| No consensus system | Need leader election for writes | **Restate's per-key serialization** replaces `SELECT ... FOR UPDATE`. No separate coordinator. |

The unresolved tension: Postgres is now on the platform's critical
path for **both** metadata and the Restate journal. Write
amplification is ~5-10x the request rate (one row per `Restate.run`
step). Sizing must be re-done.

## 9. Risks and things to validate

### 9.1 The 3 biggest risks

1. **The Java-platform → Scala-library boundary is inside a Restate
   journal entry.** Every `Restate.run("compile", () -> semanticdf.of(spark, model))`
   crosses the language boundary, and the result is journaled. If
   the Scala library is non-deterministic (catches a `Throwable` and
   returns a default, depends on a static clock, reads from a non-
   serialized closure capture), the journal replays produce a
   different result than the original run. **Mitigation:** a
   deterministic-purity audit of `semanticdf` before it goes inside
   any `Restate.run` — every public entry point must be a pure
   function of its inputs.
2. **Postgres is now on the critical path for both metadata and the
   journal.** The pre-Restate design sized Postgres for ~1K QPS of
   metadata writes. Restate adds journal rows: ~5-10x request
   amplification. A platform at 1K QPS does 5-10K journal writes/sec.
   A managed Postgres can take it; a self-hosted one cannot.
   Re-sizing is mandatory; assuming "we already have Postgres" is
   the failure mode.
3. **Determinism discipline.** Restate's replay model punishes any
   non-deterministic call outside `Restate.run(...)` —
   `System.currentTimeMillis()`, `UUID.randomUUID()`, third-party
   clients. The prior design had no such constraint. A handler that
   quietly violates it works in tests and breaks under crash-recovery.
   **Mitigation:** a lint rule + test that injects journal-replay
   failures (Restate's testcontainers already supports this).

### 9.2 The 3 things to validate with the team before committing

1. **Workload profile.** Still unanswered from the prior design:
   agent-driven (low-QPS, repeat-heavy → cache wins) vs BI-driven
   (high-QPS, mostly unique → Restate Workflow is the bottleneck, not
   the cache). The hot-path design assumes the former; if the latter
   dominates, Caffeine is the wrong layer.
2. **The Scala library stays Scala, or do we port it to Java?** The
   op tree + lineage + audit code is Scala; porting loses the
   typeclass seam and the Spark `Encoder` integration. Recommend
   *no port*; the platform consumes the library as a JAR. The team
   must agree because the alternative is a 6-month port.
3. **The Restate server is now in the on-call surface.** The pre-
   Restate design was "one binary." Tomorrow it is "one binary + one
   Restate server + a journal schema." The on-call team needs
   Postgres + journal-replay debugging skills. Validate: who carries
   the pager for the Restate server, and do they have a runbook for
   "journal is replaying and we're stuck"?

### 9.3 Open assumptions

- Workload is <500 QPS at steady state, <5K QPS at peak. (If higher,
  re-evaluate Postgres connection pool sizing + journal tier.)
- All engines in scope are JVM-native (Spark, Trino, possibly
  Flink). A non-JVM engine (Rust Python client, Go service) would
  need a thin REST adapter.
- Tenants are logical (namespace-scoped), not process-level.
  Regulated workloads need physical isolation.
- Single-cloud (AWS or GCP) for v1. Multi-cloud is a v3 problem.
- The `semanticdf` library's op tree is the source of truth for
  lineage. The platform never re-implements the compiler.
- Streaming queries are in v2 (P2). v1 is batch + lineage only.
- The platform consumes `semanticdf` as a Maven JAR. FFI is out of
  scope.
