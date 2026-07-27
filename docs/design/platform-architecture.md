# Platform Architecture — Standalone Semantic Data Platform

**Status:** DRAFT — design review.

This document is the consolidated output of a senior data-architect
and a senior software-architect review, plus the reconciliation of
their disagreements. It supersedes the v0.2.1 "library embedded in
Spark" model.

## 1. Executive summary

The current `semanticdf` is a Scala 2.13 / Spark library. It runs
*inside* the user's Spark job — no server, no cluster. The MCP
server is a thin stdio/REST wrapper that creates a local
`SparkSession` on startup.

The target state inverts the dependency: **`semanticdf` becomes
the host, and Spark/Trino are compute guests that consume the
platform's REST contract.** The platform is a long-running, fault-
tolerant, engine-agnostic service that owns the semantic model
registry, lineage, audit, and query planning. Engines are plugins.

The headline design decisions:

1. **Single binary, multi-node** (Option C — modular monolith). One
   artifact runs as a stateless node, replicated 3× across AZs for HA.
2. **Java 21** for the daemon. Keeps Spark/Trino integration smooth
   (both have native Java APIs and the same JVM ecosystem).
3. **Postgres for everything metadata + coordination** (no ZooKeeper,
   no etcd). 3-AZ synchronous replication, advisory locks for
   coordination. The platform inherits Postgres's proven HA.
4. **Object storage (S3 / MinIO) + Iceberg for raw data**. The
   platform doesn't own the table format; engines read/write Iceberg
   directly.
5. **REST as the canonical contract**. Per-engine adapters (Trino
   plugin, Spark SDK, future Flight server) are ergonomic layers on
   top of REST.
6. **No overhead services in v1**: no Kafka, no ZK, no service mesh,
   no sidecar proxies. The only dependencies are Postgres and object
   storage. Redis is deferred.

The headline trade-off: the platform adds a network round-trip
(Treno / Spark → platform REST → results) that the in-process
library does not have. The cost is paid once per uncached query
shape, absorbed by the plan cache and the lineage cache. The
gain is engine-agnostic access and a single source of truth for
lineage, audit, and registration.

## 2. Target architecture

### 2.1 Components

| Component | Responsibility | Implementation |
|---|---|---|
| **Platform daemon** (the binary) | REST + MCP + Arrow Flight endpoints. Catalog, lineage, audit, plan-cache. | Java 21, modular monolith. |
| **Postgres** | All platform metadata: model registry, lineage, audit, query journal, idempotency keys, plan cache (optional). | Managed (RDS / Cloud SQL). 3-AZ sync replication. |
| **Object storage** (S3 / MinIO / GCS) | Raw data tables (Iceberg) + the platform's own WAL/snapshots. | Object storage. |
| **Engine adapters** (per-engine) | Engine-specific translation: Trino plugin, Spark SDK, Flink source. | Each engine has its own thin adapter. |
| **`semanticdf` library** (kept as-is) | Pure-data compiler. Lives *inside* the platform daemon (and as a library, for in-process users). | Scala 2.13 / Spark (unchanged). |
| **Optional Redis** (v2) | L2 cache for query results. | Deferred unless workload demands. |

### 2.2 Data flow (one query)

```
┌─────────┐    ┌──────────────────┐    ┌──────────────┐
│  Agent  │───▶│  Platform daemon  │───▶│ Engine        │
│ (LLM,   │    │  (this PR's       │    │ (Spark,      │
│  BI)    │    │   product)        │    │  Trino)      │
└─────────┘    └──────────────────┘    └──────────────┘
                     │  │
                     │  └─────▶ Postgres (catalog, lineage, audit)
                     │
                     └─────▶ Object storage (Iceberg tables, WAL)
```

For a query:
1. Agent calls `POST /v1/query` (REST) with `model_name`, query shape.
2. Daemon checks plan cache (in-process Caffeine, keyed by query
   shape hash + model version). Hit → return cached plan, no Spark.
3. Miss → load model from Postgres, compile via `semanticdf.of` →
   op tree → submit to engine adapter (Spark or Trino).
4. Engine reads Iceberg tables from object storage, returns
   Arrow-encoded results.
5. Daemon streams results back to agent via Arrow Flight (or
   REST chunks for the BI consumer).

For metadata reads (list models, get_field_lineage,
get_dependencies): the daemon reads from Postgres only. No
engine, no Spark action. Sub-100ms p99.

### 2.3 HA / fault-tolerance

- **3 platform replicas across AZs** (single-region for v1).
  Stateless daemon; all state in Postgres.
- **Synchronous PostgreSQL replication** across the 3 AZs. Each
  platform replica reads/writes to the nearest Postgres replica;
  the cluster's primary serves writes; sync rep means the standby is
  in-sync before the primary acks. (V1: managed Postgres, RDS / Cloud
  SQL. V2: regional active-passive.)
- **Postgres advisory locks** for the few things that need
  coordination: who is compiling this model version, who is running
  this streaming query, who is the registration-writer leader. All
  three are single-row operations with `SELECT ... FOR UPDATE` or
  `pg_try_advisory_lock`. No ZK, no etcd.
- **Rolling upgrade** (1 node at a time, Raft-quorum-style). The
  platform supports wire protocol N and N-1 for at least one release.
  Additive fields are accepted; destructive cleanup is delayed until
  all nodes are upgraded.
- **Snapshot / WAL**: object storage holds the platform's own WAL +
  snapshots. On a region-wide Postgres failure, the platform can
  rebuild from object storage alone (catalog state reconstructable from
  WAL + last snapshot).
- **Engine restart safety**: persisted engine query IDs allow
  restart reconciliation without blind resubmission. If a Spark job
  dies mid-query, the journal can resume from the last checkpoint.

### 2.4 Resource budget

| Resource | Per node | Total (3 nodes) |
|---|---|---|
| Container / pod size | 1 GiB | 3 GiB |
| Heap | 512 MiB | 1.5 GiB |
| CPU | 2 vCPU | 6 vCPU |
| Postgres (RDS, db.r6g.large) | shared | 16 GiB RAM, 4 vCPU, 100 GB SSD |
| Object storage (S3 / MinIO) | shared | grows with data |

**Latency budget**:

| Operation | Target p50 | Target p99 |
|---|---|---|
| Authenticated metadata read | 1 ms | 5 ms |
| Simple plan compile (cached) | 100 µs | 1 ms |
| Simple plan compile (cache miss) | 10 ms | 25 ms |
| Complex lineage lookup | 30 ms | 100 ms |
| Engine query (cached) | 5 ms | 50 ms |
| Engine query (uncached, simple) | 200 ms | 1 s |
| Engine query (uncached, complex) | 1 s | 10 s |

**Throughput target per node**: ~1,000 metadata reads/sec, 250 plan
compilations/sec, 500 lineage lookups/sec. Actual engine-query
concurrency is quota-controlled separately.

## 3. Language and runtime choice

**Java 21** for the platform daemon. Retain Scala 2.13 *only* in the
transitional Spark adapter; `semanticdf` the library stays in
Scala. Do **not** introduce Rust in v1.

Justification:
- **Ecosystem match**. Spark, Trino, and every major data tool
  speak Java. Native interop with the Spark SDK and Trino's
  Plugin API is the path of least friction.
- **Long-running + low GC pauses**. Java 21 + G1/ZGC + bounded
  heaps (512 MiB) make pauses manageable. The 1 GiB container
  footprint is realistic.
- **Team productivity**. The existing team writes Scala. Java is
  the closest cousin. (If we later prove a hot path needs Rust,
  the boundary is clean: Rust sidecar for a specific computation,
  e.g., columnar lineage diff.)
- **What's *not* on the table**: Go (less expressive for data),
  Rust in v1 (operational cost, ecosystem mismatch for Spark/Trino).

## 4. Technology stack per layer

| Layer | Recommendation | Why |
|---|---|---|
| **Metadata store** | **Postgres 16** (managed, sync-replicated 3-AZ) | Already proven at our scale; no need for Iceberg-native catalog until raw tables exist. Defer Polaris / Nessie. |
| **Semantic / catalog layer** | The platform itself; the catalog is `semanticdf` models registered via REST. The platform owns model registry, lineage, audit, plans. | Single source of truth. |
| **Query interface (canonical)** | **REST + JSON** (the contract) | Engine-agnostic. All other interfaces are ergonomic layers on top. |
| **Query interface (ergonomic)** | Trino plugin (per-engine); Spark SDK (per-engine); future Arrow Flight server for low-latency streaming. | One canonical contract, many ergonomics. |
| **Coordination / consensus** | **Postgres advisory locks** (no ZK, no etcd) | The coordination surface is small. Postgres HA already gives us strong consistency. The cost of running ZK/etcd is not justified for the workload. |
| **Caching (v1)** | In-process Caffeine (no Redis) | Sub-100µs cache hits. L1 only. |
| **Caching (v2, optional)** | Add Redis for L2 if workload demands | Adds one more service. Defer until needed. |
| **Storage backend** | **Object storage (S3 / MinIO) + Apache Iceberg** for raw data | Engine-agnostic. Spark/Trino/Flink all read Iceberg natively. |
| **Audit log** | Append-only Postgres table, time-partitioned | Postgres HA + retention policy. |
| **Observability** | OpenMetrics + structured JSON stdout + health/readiness + optional OTel (no Prometheus / Loki / OTel collector bundled) | Operators supply the observability stack. Don't bundle. |
| **Security (v1)** | TLS, local OIDC/JWT validation, mTLS for service identities, namespace-scoped RBAC, tenant-qualified cache keys, delegated per-tenant engine credentials | Standard. Multi-tenant is logical, not process-level. |
| **Upgrade** | Rolling replacement; expand/migrate/contract for catalog changes; blue/green reserved for incompatible engine-adapter changes | Zero request-path downtime. |

## 5. Engine interop

**REST as the protocol contract; per-engine SDKs as the ergonomic
layer.**

| Engine | How it talks to the platform | Notes |
|---|---|---|
| **Spark** | Spark SDK (Scala/Java/Python) | The `semanticdf` library can be linked in-process for hot path; cold calls go via REST. |
| **Trino** | Trino plugin (Java) | Plugin maps a semantic model to a Trino schema. Simple group-by/aggregate pushes down to native Trino SQL. Calc measures fall back to the platform (calls the Spark adapter). |
| **Flink** (future) | Flink source / DataStream connector | Lower priority; defer until there's a real Flink workload. |
| **DuckDB** (future) | Python client + REST | For analyst-side exploration. |

The REST contract is the boundary. Per-engine SDKs are ergonomic
layers — they may embed the `semanticdf` library for fast-path
compilation, but they always go via the platform for state (lineage,
audit, registration).

## 6. The 4 boundaries, each with a wire shape

| # | Boundary | Wire shape | Consistency | Hot/cold |
|---|---|---|---|---|
| **B1** | Postgres ↔ platform daemon | Typed rows (UUID, JSONB, timestamptz) | Strong (single Postgres, transactional) | Cold (registration, version bump) |
| **B2** | Platform daemon ↔ engine | REST/JSON for control; Arrow Flight for data | Session-coherent (read-your-writes within a session) | Hot (per query) |
| **B3** | Engine ↔ object storage (Iceberg) | Parquet + Avro in Iceberg | Snapshot isolation (Iceberg guarantee) | Hot (per query) |
| **B4** | Platform daemon ↔ agent | JSON-RPC for control, Arrow Flight for results | At-least-once control, exactly-once result stream (Flight native) | Hot (per agent request) |

The big cost is B2+B3: a query plan goes JSON → engine plan → Iceberg
snapshot read. The plan cache means the JSON serialization is
paid once per query shape, not per request.

## 7. Migration roadmap

| Phase | Goal | Surface | Rollback |
|---|---|---|---|
| **P0 — Current** | `semanticdf` library embedded in Spark; `semanticdf-mcp` thin REST wrapper. | What we have today. | n/a |
| **P1 — Standalone daemon (months 1-2)** | Extract the platform daemon from `semanticdf-mcp`. Same REST + MCP surface. Postgres-backed registry instead of in-process model loading. Backwards compatible with the current `semanticdf-mcp` clients. | New `semanticdf-platform` repo + artifact. Same `semanticdf` library underneath. | Old `semanticdf-mcp` still works; the platform is additive. |
| **P2 — Engine adapters (months 3-4)** | First-class Trino plugin that exposes semantic models as Trino schemas. Spark SDK for in-process use. Both consume the platform's REST. | New: `trino-semanticdf` plugin, `semanticdf-spark-sdk` for cluster mode. | Trino plugin is opt-in; Spark SDK is opt-in. |
| **P3 — Multi-node HA (months 5-6)** | 3-AZ Postgres replication. Platform daemon runs stateless across 3 nodes. Rolling upgrade. | Operational: same code, more replicas. | Single-node mode still works (dev). |
| **P4 — Object-storage-backed data (months 6-8)** | Iceberg becomes the default raw-data substrate. Platform owns the table-metadata map; engines read/write Iceberg directly. | New: Iceberg integration. Migration: backfill existing parquet. | Engines can still use raw parquet (legacy mode). |
| **P5 — Multi-region (months 9-12, V2)** | Regional active-passive. Object storage is the source of truth; Postgres is regional. | Operational. | Single-region stays supported. |
| **P6 — Optional Redis (deferred)** | Add L2 cache if workload demands (high QPS of repeat queries). | Operational. | n/a (additive) |

## 8. Trade-off analysis

The user's design brief flagged a tension: "lightweight / no
overhead" vs "fault-tolerant / long-running." Here's how the design
balances them:

| Pull toward "lightweight" | Pull toward "fault-tolerant" | Resolution |
|---|---|---|
| One binary, no sidecars | Need replicated state | **Postgres HA is the single replicated state** — no ZK / no etcd. The platform daemon is stateless. |
| No Kafka / no service mesh | Need durable audit + query journal | **Audit + journal live in Postgres** (transactional, WAL-backed). No separate event bus. |
| Sub-millisecond latency | Need durable recovery on restart | **In-process cache + Postgres persistence**: cache hits are µs; cache misses go to Postgres at single-digit ms; engine queries are 10s of ms to seconds. |
| Minimal services | Multi-node for HA | **3 platform replicas** + **1 Postgres cluster** + **1 object storage**. Three services. (Adding Redis would be a 4th; deferred.) |
| No consensus system | Need leader election for writes | **Postgres advisory locks** for the small coordination surface. Avoids the operational cost of ZK. |

The unresolved tension: a Postgres failure pauses writes. A
multi-region upgrade (P5) addresses this. For v1, the user accepts
"single-region, 3-AZ" — the same RPO/RTO as a managed Postgres
deployment.

## 9. Risks and things to validate

### 9.1 The 3 biggest risks

1. **Cross-engine semantic drift.** NULLs, decimals, time zones,
   windows, joins, and function behavior can silently produce
   different answers between Spark and Trino. The platform owns the
   *logical* plan; each engine compiles the *physical* plan. A
   regression in engine semantics breaks the platform's correctness
   promise.
2. **Durable query handoff.** After a partition, ambiguous
   submission outcomes can duplicate expensive work. Adapters must
   provide stable idempotency keys / query IDs.
3. **Migration blast radius.** The existing semantic AST, op tree,
   lineage, and tests are deeply shaped around Spark despite the
   library's pure-data structure. The platform is a thin shell; the
   *library* is where the engine-specific cleverness lives. Any
   refactor of the library's engine surface breaks the platform.

### 9.2 The 3 things to validate with the team

1. **Workload profile.** Is this primarily LLM-agent (low-QPS,
   latency-sensitive, often repeated queries → cache wins big) or
   BI/analytics (high-QPS, latency-tolerant, mostly unique queries →
   cache is less valuable)? The cache architecture and the Trino
   pushdown cut depend on this.
2. **Multi-region or single-region?** If multi-region with
   active-active writes are on the 12-month roadmap, Postgres +
   advisory locks won't carry it; the consensus choice (Postgres
   itself) changes. If single-region is the next 18 months, the
   design is fine as-is.
3. **Will the team accept a separate `semanticdf-platform`
   repository / process**, or does it have to live in the current
   library? The current `semanticdf-mcp` is a thin wrapper in the
   same repo. The platform redesign is *not* a thin wrapper; it
   needs its own deployment unit, its own CI, and its own release
   cadence. If the answer is "no, keep one repo," the platform is
   constrained to a library that *can* be deployed as a service —
   workable but cuts design options (no Arrow Flight server, no
   multi-process caching).

### 9.3 Open assumptions

- Workload is <500 QPS at steady state, <5K QPS at peak. (If higher,
  re-evaluate Postgres connection pool sizing + cache tiers.)
- All engines in scope are JVM-native (Spark, Trino, possibly
  Flink). A non-JVM engine (Rust Python client, Go service) would
  need a thin REST adapter — no special platform work.
- Tenants are logical (namespace-scoped), not process-level.
  Regulated workloads that need physical isolation are out of scope.
- Single-cloud (AWS or GCP) for v1. Multi-cloud is a v3 problem.
- The `semanticdf` library's op tree is the source of truth for
  lineage. The platform never re-implements the compiler.

## 10. Migration: how the current state evolves

The current state is the "P0" of the migration. The platform
redesign (P1-P5) is additive — the library and the current
`semanticdf-mcp` keep working throughout. The risk is in *not*
doing the platform redesign: the library-as-architecture constrains
engine-agnostic access to "write a thin client in each engine's
language" (which is what the current MCP does). The platform makes
the integration path uniform.

Concrete P1 deliverables (months 1-2):
- A new `semanticdf-platform` repository with the Java 21 daemon.
- REST contract versioned as `v1`. Backwards compatible with the
  current `semanticdf-mcp` MCP wire shape.
- Postgres-backed model registry (the platform owns the source
  of truth for model registration).
- The `semanticdf` library consumed as a peer library (the platform
  daemon links it via Maven).
- Deployable as 1 node (dev) or N nodes (prod) with no code change.
- One-line test: a Spark job that calls the platform via REST gets
  the same answer as a Spark job that uses the library in-process.
