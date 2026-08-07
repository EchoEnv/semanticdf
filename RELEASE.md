# Release notes

## v0.3.0 — engine-portable core (in progress)

**Not tagged yet.** Library version still `0.2.4`; this section
tracks the work that will roll up into the v0.3.0 tag when the
remaining migration is complete.

### Headline

- **Engine-portable core (`semanticdf-core/`).** A new module
  holding portable ADTs that have no Spark dependency. Every
  engine adapter (`semanticdf-spark`, `semanticdf-trino`,
  `semanticdf-duckdb`) implements `Engine[R]` against these
  types. Cross-engine types landed: `Model`, `Dimension`,
  `Measure`, `FilterSpec`, `JoinSpec`, `RollupSpec`,
  `RelOp`, `ExecutionPlan`, `PortableQueryResult`,
  `ResultValue`, `ResultRow`, `EngineContext`,
  `EngineCapabilities`, `EngineError` + `ErrorDetail`,
  `AuditEvent`.

- **Catalog identity + CAS publication contract.** New
  `core/catalog/` package: `CatalogRef`, `CatalogIdentity`,
  `PublishMode` (`CreateOnly` / `Upsert` / `CompareAndSet`),
  `PublishResult`, `CatalogError`, `CatalogAdapter`. Per-identity
  atomic publication with explicit conflict visibility
  (`Conflict` vs `StaleConflict(current)`).

- **MCP engine registry.** The MCP server routes queries to the
  chosen engine provider via `MCPEngineRegistry`. Trino, DuckDB,
  and Spark providers implement the same contract.

- **Multi-engine query support.** The `query` MCP tool carries an
  `engine` field; non-empty values route through the registry to
  the chosen provider. Empty falls back to the legacy
  `SemanticTable` path.

### Migration status

| Step | Status |
|---|---|
| Portable ADTs (model, dimension, measure, join, filter, rollup) | ✅ |
| Portable IR (`RelOp`, `RelOp.compile`) | ✅ |
| Portable result (`ResultValue`, `ResultRow`, `PortableQueryResult`) | ✅ |
| Engine contract (`Engine[R]`, `EngineContext`, `ExecutionPlan`) | ✅ |
| Catalog identity + CAS | ✅ |
| MCP engine registry + routing | ✅ |
| Spark adapter implements `Engine[R]` (with predicate-type split) | partial (predicate-type unification pending) |
| Trino adapter implements `Engine[R]` | ✅ |
| DuckDB adapter implements `Engine[R]` | ✅ (basic aggregates only — Sum/Count/CountDistinct/Avg/Min/Max; advanced aggregates in v0.3.1) |
| `SemanticTableCore` emits portable IR + routes through `Engine[R]` | pending |
| Manifest v2 (real `ManifestDocument` ADT) | pending |

**Fail-loud behavior (pre-tag fixes, Gaps 3 + 4):**

- **`ModelBridge.toModel`** returns `Left(ModelValidationError.FilterConversionUnsupported)`
  when the legacy `SemanticTable` carries a `where` or `having`
  predicate. Replaces the v1 silent-drop (the portable Model would
  have run queries against unfiltered rows). Users port filters
  manually to `FilterSpec(name, predicate = Expr)` until the
  predicate-converter PR lands in v0.3.1.
- **`TrinoQueryCompiler.compileRelOp`** returns
  `Left(EngineError.UnsupportedCapability("RelOp.Join", ...))` when
  the portable plan contains a `RelOp.Join`. Replaces the previous
  `-- Joins deferred to a future PR` literal comment that produced
  empty results with no error. Join compile lands in v0.3.1.

For the full feature-parity gap analysis (7 gaps, prioritized P0–P3),
see [`docs/design/v0.3.1-feature-parity-backlog.md`](docs/design/v0.3.1-feature-parity-backlog.md).

### Test count

Was **992** at v0.2.4. Now **2,108** across 7 modules:
`semanticdf-core` (640), `semanticdf-spark` (1,039),
`semanticdf-trino` (9 integration + 196 unit),
`semanticdf-duckdb` (26), `semanticdf-unity-catalog` (10),
`semanticdf-hive-metastore` (no count here; counted in platform),
`semanticdf-mcp` (188).

### PR chain

39 PRs (#351-#389) extracted the portable core and added engine
adapters. 12-PR triage plan (#398-#408) closed the criticals on
the portable contract (serializability, engine identity in cache,
portable result, MCP engine routing, structured error codes,
capability surfaces, ExtensionValue JSON round-trip, EngineContext
policy interaction). PR #409 (SemanticTable → Model partial bridge)
and PR #410 (CatalogIdentity + CAS) closed the last deferred
items from the v0.2.4 design review.

## v0.2.4 — manual rollups feature

9 PRs. **992 library tests green on Spark 3.5.8 + 4.1.1 (was 848).** Ships the
manual rollups feature (long-running design pillar), a pre-existing join
metadata bug fix, and a docs cleanup pass. v0.3.0 will add auto-routing and
freshness enforcement on top of this foundation.

### Headline

- **`Rollup` / `RollupRegistry` / `RollupQuery` — manual rollups, redux**.
  A new `io.semanticdf.rollup` package lets users register pre-aggregated
  rollup tables on a `SemanticTable` via `.withRollup(rollup)` and query
  them via `model.useRollup(name, registry).execute(spark)`. The `Rollup`
  value class is pure metadata (no DataFrame reference); the actual rollup
  source lives in a separate `RollupRegistry` that's threaded through at
  query time. The redesign separates `RollupQuery` from the fluent chain —
  a `useRollup` result has no `withDimensions`/`groupBy`/`join_one`
  methods (compile error), preventing the 19 HIGH-severity bugs found
  across 4 audit cycles of the v1 implementation. Path-2 contract: rollups
  do NOT survive joins (`joined.useRollup` throws) — keeping the contract
  honest about which DataFrame actually ships to executors.

- **Join metadata preservation (H-D1, pre-existing bug)**.
  Every `join_one` / `join_many` / `join_cross` silently reset
  `version` / `sourceTable` / `status` / `postAggPredicates` on the joined
  result. DE reviewer caught it during the rollup audit; root cause was
  two distinct failure modes (hardcoded defaults in the lambda-path
  early-return + unnamed fields in the typed-key fall-through). Fixed at
  all 5 join sites by propagating `this.{metadata...}`. Users who set
  `version` / `status` on a model and then join it now get that metadata
  on the joined result, as expected.

- **Rollup serialization audit-closed** — new `RollupSerializationSpec`
  (8 tests) round-trips `Rollup`, `RollupMeasure`, the aggregator /
  freshness / on-stale-policy singletons, and `SemanticTable.withRollup(...)`
  through Java serialization. The 3 sealed traits (`RollupAggregator`,
  `RollupFreshness`, `OnStalePolicy`) explicitly `extends Serializable` for
  contract clarity (the case classes / case objects were already
  Serializable through the default Scala mixin).

- **Docs / Scaladoc provenance cleanup** — the rollup cycle (PRs
  `#327`–`#335`) accumulated internal `PR #N` / `Phase N` / `ADR #N`
  references in user-facing prose. Stripped from all 11 source files, 4
  modern design docs, and the 30+ test names in `SemanticDFSpec`.
  Historical design docs (`rollups.v0.md`, `rollups.v1.md`) moved to
  `docs/design/archive/` with a README pointing forward to
  `docs/design/rollups.md`. Audit-finding IDs (H-D1, H-1, M-D1, L-D1)
  preserved — they're stable internal review IDs.

### Breaking change to flag

The rollup package is **additive** — no existing API removed. Two
notable behavior changes that may surface in existing tests:

- **`useRollup` returns `RollupQuery`, not `SemanticTable`.** Tests that
  did `assert(result.isInstanceOf[SemanticTable])` after `useRollup`
  must use `assert(result.isInstanceOf[RollupQuery])`. The type-system
  separation is the load-bearing fix for the v1 design's bugs.

- **Joined models have empty rollups.** `model.join_one(other).useRollup(...)`
  throws `IllegalArgumentException` (joined has no rollups to use).
  Re-register via `joined.withRollup(...)` requires a NAMED joined model
  with the rollup's `baseModel` matching the joined model's name. Anonymous
  joined models cannot host rollups at all.

### What stays the same

- `SemanticTable.toDataFrame(...)` and `.toStreamingQuery(...)` — unchanged
- `withAuditSink` / `withResultCache` / `withMaxRows` / `withBroadcastJoinThreshold` / `withMaterialize` / `withSalt` — all six runtime knobs unchanged
- All YAML loading, manifest round-trip, and Semantica-flavored fluent API unchanged

### Test counts (was 848 at v0.2.0)

| Version | Tests | Delta | Notes |
|---|---|---|---|
| v0.2.0 | 848 | — | |
| v0.2.1 | 856 | +8 | streaming registry |
| v0.2.2 | 949 | +93 | services-completion charter |
| v0.2.3 | 915 | -34 | post-charter dedup |
| **v0.2.4** | **992** | **+77** | rollups feature + serialization tests |

77 new tests in this release: 50 rollup-package tests + 8 join metadata regression
tests + 8 rollup serialization round-trip tests + the rest from misc rollup audit
fixes (H1 propagation, H2 WHERE-before-projection, H4 final-class smart
constructor, etc.).

### What's deferred (v0.3.0+)

- **Rollup auto-routing**: `model.query(measures, dims)` should pick the
  best rollup automatically. v0.2.4 only ships `useRollup(...)` — the
  caller names the rollup explicitly. Auto-routing requires the
  `RollupAggregator` algebra extension (Min/Max/Avg/Stddev) plus
  freshness enforcement, which is the v0.3.0 scope.
- **`RollupFreshness.Track` runtime enforcement**: the field is stored
  (and round-trips through Java serialization), but `RollupQuery.execute`
  doesn't yet consult it. Documented in the Scaladoc; the field carries
  the intended policy for downstream consumers (manifest round-trip
  exposes the freshness metadata) until enforcement lands.
- **Manifest rollup round-trip**: v0.2.4 drops rollups at manifest
  serialization (`rollups = Nil`) with a documented "re-register after
  load" instruction. v0.3.0 will round-trip them once the freshness
  schema is finalized.
- **Min / Max / Avg / Stddev aggregators**: only `sum` and `count`
  shipped in v0.2.4 (both exact-additive). Min/Max are partial-additive
  (correct at the rollup's grain only); Avg/Stddev require
  `sum/count` and `sum/sum²/count` state algebra respectively.

### Co-instruction: do not use rollups with a streaming source

The SmartStream Detect is a no-op for streaming joins (Spark's
`ResolveWriteToStream` rule disables auto-routing for streaming
DataFrames with a WARN). The runtime-tuning knobs work for streaming
sources EXCEPT `withSalt` — same Spark limitation. Rollups on
streaming models are not currently supported (manual registration of
a streaming source's DataFrame doesn't round-trip cleanly).

In summary, v0.2.4 ships:

- **Rollup subpackage** — pure-metadata `Rollup` + separated
  `RollupRegistry` + driver-local `RollupQuery`. Manual rollups only —
  no auto-routing.
- **`Semantica-Spec` / `Runtime-Tuning` / `Audit-Cache** co-invariance
  unchanged.
- **Pre-existing H-D1 join bug** fixed at all 5 join sites.
- **Rollup serialization audit** closed (8 new tests, 3 trait
  contracts).
- **Provenance cleanup** across 19 files.

Total test count: **992** library / green on Spark 3.5.8 + 4.1.1.

## v0.2.3 — runtime tuning + audit-cache co-invariance

33 PRs. 915 library tests green on Spark 3.5.8 + 4.1.1 (was 848). Two new
runtime knobs, one enforced invariant, one walk-through doc, two runnable
examples.

### Headline

- **`withMaterialize(level)` — opt-in DataFrame persistence** (`withMaterialize(StorageLevel.MEMORY_AND_DISK)`).
  On the fast path of `toDataFrame` (no audit, no cache), `df.persist(level)`
  is applied so subsequent actions reuse the persisted storage instead of
  re-executing the Spark plan. User manages cleanup via `df.unpersist()`
  on the returned DataFrame. Default `None` (no persist).
- **`withSalt(n)` — opt-in skew-handling via Spark AQE**. Sets
  `spark.sql.adaptive.skewJoin.skewedPartitionFactor = n` (and re-enables
  the parent `adaptive.enabled` flag). AQE splits each skewed shuffle
  partition into smaller sub-partitions and replicates the matching
  partition on the other side. Default `None`. Streaming: no-op
  (Spark's `ResolveWriteToStream` disables AQE for streaming DataFrames).
- **`auditRequest` / `auditSink` / `resultCache` co-invariance is now
  enforced**. Calling `.toDataFrame(spark)` (or `.execute(spark)`) with
  `auditSink` or `resultCache` set but `auditRequest` empty throws
  `IllegalStateException`. Post-query shape-changers (`where`, `having`,
  `orderBy`, `limit`, `atTimeGrain`, `withDimensions`, `withMeasures`,
  `withRowFilter`, `withTransforms`) clear both fields together via
  `invalidateAuditRequest`. Previously the cache was silently bypassed
  and `dedupHash` collapsed to `model+version` only — two distinct
  queries could share a dedup hash, defeating the platform's
  `AuditService.append` dedup contract.
- **`docs/tutorial-runtime-tuning.md`** — walk-through of the six
  runtime knobs. Decision tree, when-to-use matrix, real-world
  customer-analytics-dashboard scenario, anti-patterns.
- **`examples/runtime-tuning/`** and **`examples/skewed-join/`** —
  runnable examples demonstrating all six knobs and focused
  skew-handling, respectively. Run via `mvn exec:java`.

### Breaking change to flag

`withAuditSink` and `withResultCache` now REQUIRE a preceding
`query(measures, dimensions, ...)`. Without it, `.toDataFrame(spark)`
throws. Direct chains like
`model.withResultCache(c).groupBy().aggregate().toDataFrame(spark)`
no longer work — they were silently no-ops before v0.2.3, so this
makes the misuse loud. The throw message includes a working
example call pattern.

### Detailed change list

- **Cache miss cap**: `maxRows` is applied to `.collect()` on the cache
  miss path (and on the audit-only path). The fast path (no audit, no
  cache) skips the cap because the user already has a tight DataFrame.
- **`maxRows` round-trips through manifests and survives fluent chains**
  including the outer envelope of a joined manifest.
- **`broadcastJoinThreshold` propagation**: set BEFORE the join, not
  after. LEFT-wins / RIGHT-fallback precedence at join construction.
- **`auditRequest` joins the round-trip**: outer envelope of a joined
  manifest preserves per-side and outer `auditRequest`.
- **`executedPlan` capture**: `AuditEvent.executedPlan` is populated on
  the success path so operators can inspect the actual Spark plan
  that ran.
- **Schema parity enforcement**: `schemas/manifest.schema.json` (repo)
  and `src/test/resources/manifest.schema.json` (classpath) are
  byte-identical by test.
- **Streaming batchModel** threads `salt` + `broadcastJoinThreshold`
  through `toStreamingQuery` (previously dropped both silently).

### Test counts

- Library: 915 (was 848 at v0.2.2). +67 across the post-v0.2.2 chain.
- Platform: 212 (unchanged).

### Known follow-ups

- Streaming `emitStreamingAudit` `getOrElse` fallback: deferred. When
  a streaming model has `withAuditSink` set without a preceding
  `query()`, the streaming audit emits with empty `measures`/
  `dimensions`. Separate concern (streaming has its own audit
  contract). Future PR.
- A handful of pre-existing example projects still reference
  `semanticdf.version` 0.2.0 (overrides on the parent property).
  Stale, out of scope for this release.
- v0.2.3 did NOT bump the platform module's `0.1.0-SNAPSHOT` —
  platform versioning is independent of the library.

## v0.2.2 — services-completion: durable registries, cache-first query path

This release completes the platform's services-completion charter: every Restate service that previously existed as a P1 skeleton now executes against a durable, replay-safe substrate. The streaming foundation from v0.2.1 is untouched.

### What changed

#### Platform runtime — service wiring

- **`AuditService` → durable Postgres.** The `append` handler now writes through to a partitioned `audit_events` table under flag `SEMANTICDF_AUDIT_PERSIST=true`. The journal-tier `LAST_DEDUP_HASH` / `LAST_WRITE_OFFSET` keeps its coordination role; Postgres adds the durable layer. `Restate.run("audit.append", ...)` ensures replay safety on mid-write crash. Default-off preserves the v0.2.1 behavior.
- **`ModelService.register` → end-to-end.** Five sequenced steps: (1) dedup → (2) compile YAML in handler scope via the library's `YamlLoader` + temp-file (deleted in `finally`) → (3) compute lineage in handler scope via the new library helper `Lineage.workspaceJsonFor(model)` → (4) `Restate.run("model.persist", ...)` against Postgres (the only journal-bounded step — compile and lineage deliberately run OUTSIDE `Restate.run` because the compiled `SemanticTable` carries a Spark `Dataset.rdd` chain that Jackson cannot reconstruct during replay, see PR #249) → (5) journal bookkeeping → `cache.invalidateModel(name)` (outside any `Restate.run`). Same flag-driven opt-in via `SEMANTICDF_MODELS_PERSIST=true`.
- **`CatalogService.listModels` and `describeModel`** read from the same `ModelStore`. `describeModel` returns the latest version's YAML body + lineage JSON + manifest hash + registered-at epoch-millis.
- **`QueryService.runQuery` → cache-first Spark execution.** Cache key built from the wire DTO directly (raw SQL `where` included); cache hit returns cached rows without re-executing the Spark plan; cache miss executes via `Restate.run("query.execute", ...)` so a JVM crash mid-query replays the cached `CachedResult` without re-running the Spark plan. Default cache is `ResultCache.NoOp()`; operators wire `InMemoryResultCache` in a v0.2.3 follow-up.
- **`Library` — six new methods + one fix.** `CacheBridge.executeQuery(...)` (with a 100k-row driver-memory guard) + `CacheBridge.platformCacheKey(...)` (platform-side cache key that includes the raw `where` filter) + `CacheBridge.rowsAsJava` / `schemaFieldsAsJava` / `buildQueryRequest` / `modelNameOrUnknown` Java-friendly facade. `Lineage.workspaceJsonFor(model, prettyPrint)` helper for single-model lineage JSON.

#### Critical fix (post-merge review)

The cache-key helper that landed with the QueryService wiring initially dropped the `where` filter on the floor — two callers with the same model+measures+dimensions but different SQL `where` strings would share the same cache entry, returning one caller's rows for the other's query (silent data corruption). The post-merge `platformCacheKey` fix hashes the full wire DTO directly: model, version, measures, dimensions, and `where`. Pinning is via a regression test that asserts three queries with different `where` strings produce three distinct cache keys.

#### Engine mode

- **`SEMANTICDF_SPARK_CONNECT_URL` is now the production topology.** When set (e.g. `sc://spark-connect:15002`), the platform becomes a pure control plane: a thin JVM that submits queries to a long-running Spark Connect cluster via gRPC. The cluster's lifetime is decoupled from the platform's; the platform initiates nothing Spark-related beyond the gRPC client. Unset keeps the v0.2.1 in-process Spark via `master("local[*]`)`. Flag-gated, no code-path switch — both modes route through the library's `SdfSession.createFromEnv` factory.

#### Environment variables (additions to v0.2.1)

- `SEMANTICDF_SPARK_CONNECT_URL` — switch to Spark Connect control-plane mode (e.g. `sc://spark-connect:15002`). Requires Spark 4.0+ on the platform module.
- `SEMANTICDF_AUDIT_PERSIST` — `true` enables the durable audit log (default `false`).
- `SEMANTICDF_MODELS_PERSIST` — `true` enables the durable model registry (default `false`).
- `SPARK_MASTER` — currently unused (the library hardcodes `local[*]`); reserved for future single-node tuning.

### Test status

- Library: **775 tests, all green** on Spark 3.5.8 and 4.1.1.
- Platform: **138 tests, all green**.
- New tests pin: the cache-poisoning regression (5), `audit.append` replay-safety contracts, `model.compile` / `model.lineage` / `model.persist` step ordering, `where`-in-cache-key end-to-end. (Integration test for `StreamingService` is gated on Docker; see `StreamingServiceIntegrationTest` docstring for scope.)

### What's deferred (v0.2.3+)

- `ResultCache.NoOp()` vs `InMemoryResultCache` env-driven toggle for production deployments.
- Audit emit on `QueryService.runQuery` (today only `streaming.started` / `streaming.restarted` / `model.registered` produce audit events; query events follow in v0.2.3).
- Structured-`Predicate` translation from raw SQL `where` strings into the library's `Predicate` AST.
- Typed `ResultDecoder[T]` rows (wire-shape change deferred).
- Caffeine L1 cache in the platform's REST layer.
- Multi-node HA (3 platform/Restate replicas across AZs).
- Engine adapters beyond Spark Connect (Trino plugin, Spark SDK).

### Migration

For consumers of the `semanticdf` library 0.2.1 → 0.2.2: **no migration required**. The library is consumed by `semanticdf-platform` only; consumers of the user-facing library API see no change.

For operators of `semanticdf-platform`: **opt-in flags** for the new durability (`SEMANTICDF_AUDIT_PERSIST`, `SEMANTICDF_MODELS_PERSIST`, `SEMANTICDF_SPARK_CONNECT_URL`) — all default-off / set-required. Existing v0.2.1 deployments upgrade cleanly; the schemas are pure additive (`audit_events`, `models` tables).

## v0.2.1 — Restate-native platform foundation (P1)

This release lands the foundation of the `semanticdf-platform` module: a Restate-native JVM runtime that hosts the platform's five services (`ModelService`, `QueryService`, `StreamingService`, `AuditService`, `CatalogService`), boots an HTTP ingress, and reconciles streaming queries across JVM crashes. The library `semanticdf` 0.2.1 is unchanged from the user's perspective — this release is the standalone platform module moving from skeleton to feature-complete single-replica.

### What changed

#### Platform runtime (semanticdf-platform/)

- **Engine wiring.** `PlatformApplication.main` constructs the engine adapter stack: `YamlModelRegistry` reads `*.yml` from `MODELS_DIR`, `SparkStreamingQueryLauncher` builds `StreamingQuery` instances via `SemanticTable.toStreamingQuery`, and `StreamingQueryHandleRegistry` provides runtime-local lookup of live queries.
- **`StreamingService` lifecycle.** `run()` initializes the journal, resolves the model through `ModelRegistry`, starts the Spark query inside a journaled `Restate.run`, emits a `dedupHash`-keyed audit event via `AuditService`, then enters an active-monitor loop that observes termination every 5s. `stop()` resolves a `STOP_SIGNAL` `DurablePromise` (annotated `@Shared` so it can fire concurrently with `run`); the loop is the sole owner of physical `query.stop()`.
- **Audit sink abstraction.** A new `AuditSink` interface and `RestateAuditSink` production impl encapsulate `Restate.virtualObject(AuditService.class, ...).append(...)`. The cross-service call is journaled so on retry the cached response is replayed.
- **Shutdown story** (`PlatformApplication` shutdown hook + drain via `drainQueries(handles, perQueryTimeoutMs)`):
  - `drainQueries` iterates the in-memory registry and calls `query.stop()` on each, with a bounded timeout per query (env `SEMANTICDF_DRAIN_TIMEOUT_MS`, default 10s) and an interrupt-safe early-exit if the JVM shutdown hook itself is interrupted. `ExecutorService` is daemon so it can't prevent JVM exit.
  - Bound pool size for parallel drain: `Math.min(16, total)` workers, where `total` is the number of registered queries. Bounds drain time at ~30s for any reasonable P1 workload.
- **Post-crash reconciliation** (`run()` step 5a): after `STATUS=running` is journaled, the handler checks `handles.get(streamId)`. On a cross-JVM replay that came up with an empty registry, `reconcileAfterJvmCrash(streamId, model, request, state)` re-invokes `launcher.start` and re-registers the handle. Spark's checkpoint recovery makes this idempotent for committed offsets.
- **Bulk-startup reconciliation** (daemon thread on platform startup): the platform walks a Postgres-backed `StreamCatalog` (`streaming_streams` table with `stream_id`, `model_name`, `query_shape`, `checkpoint_location`) and POSTs a fire-and-forget `run` invocation to the local Restate ingress for each previously-active stream. Each invocation re-enters the workflow's auto-detect branch and triggers reconciliation. Implemented as a daemon thread with a brief settle delay so Vert.x finishes wiring handlers before the first POSTs.

#### Reliability & circuit-breaker

- **`RECONCILE_BLOCKED` journaled flag** (state key `Boolean`) with three set sites: initial `models.get` failure (step 2 catch), initial `launcher.start` failure (step 3 catch via `startQueryWithFailureTracking`), and post-crash-reconciliation failure (`reconcileAfterJvmCrash` catch). All paths throw `TerminalException` with HTTP 400 so Restate halts retries; the BLOCKED guard at handler entry reads `state.get(RECONCILE_BLOCKED)` and throws TerminalException for any blocked workflow. Prevents the unbounded journal growth on persistent failures (model file deleted, checkpoint path unwritable, etc.).
- **`AUDIT_LOSS_COUNT` journaled counter** (state key `Long`): audit emit failures after successful recreation increment this counter and the workflow continues normally (query is alive and unmonitored-blocking would orphan it). Operators read via `getAuditLossCount()`.
- **Catalog/journal write ordering fix**: the `catalog.registerIfAbsent(...)` call in `run()` step 5b was reordered to BEFORE `state.set(STATUS, "running")`. Closes the microsecond-window drift hazard between the two writes where a JVM crash could leave the journal saying "running" while the catalog row was missing — defeating the entire bulk-startup sweep feature.
- **URL encoding for stream-id in `StartupReconciler`**: `URLEncoder.encode(streamId, UTF_8).replace("+", "%20")` at both ingress URL build sites. Closes the silent cross-stream contamination hazard where stream-id containing `/` would route to wrong workflow key.

#### Public @Handler endpoints

- `StreamingService.run(StreamRunRequest)` — fresh start.
- `StreamingService.stop(Void)` — signal stop (resolves `STOP_SIGNAL`).
- `StreamingService.restart(Void)` — operator-triggered post-crash reconciliation. NOT `@Shared` (writes state). Clears `RECONCILE_BLOCKED` at entry; allows retry from `STATUS=failed` and `STATUS=failed-restart`; refuses `STATUS=stopped` (operator must invoke `run()`); throws `IllegalStateException` for empty journal.
- `StreamingService.clearReconcileBlock(Void)` — manual unblock escape hatch for the auto-retry circuit-breaker.
- `StreamingService.getStatus()`, `getRestartCount()`, `getAuditLossCount()` — `@Shared` operator/dashboard reads.

#### Environment variables

- `MODELS_DIR` (default `./models`), `SPARK_APP_NAME` (default `semanticdf-platform`), `SPARK_MASTER` (default `local[*]`), `PORT` (default 8080) — platform-level.
- `SEMANTICDF_CATALOG_JDBC_URL`, `SEMANTICDF_CATALOG_USER`/`_PASSWORD` (default `semanticdf`) — opt-in bulk-startup reconciliation.
- `SEMANTICDF_DRAIN_TIMEOUT_MS` (default 10000), `SEMANTICDF_DRAIN_MAX_PARALLEL` (default 16), `SEMANTICDF_RECONCILE_TIMEOUT_MS` (default 30000) — shutdown + sweep timeouts.

### Test status

- Library: **775 tests, all green** on Spark 3.5.8 and 4.1.1.
- Platform: **85 tests** covering state-key rotation, BLOCKED-guard logic, RECONCILE_BLOCKED set-sites, sweep happy/skip paths, URL-encoding safety, schema bootstrap idempotency. (Integration test is bounded — see `docs/design/platform-architecture.md` and `StreamingServiceIntegrationTest` docstring for scope.)

### What's deferred (next-wave, not blocking v0.2.1)

- `StreamCatalog.unregister()` for catalog hygiene on `stop()` — stale rows are bounded by the sweep's status pre-check.
- Schema bootstrap retry on Postgres blip (fail-fast currently is intentional for P1 — surfaces config errors immediately).
- `StreamingService` 3-ctor overload collapse to single canonical constructor (Karpathy minimum-code).
- `StreamingServiceIntegrationTest` driving the full `run → tick → stop → reconcileAfterJvmCrash` lifecycle via Testcontainers (currently smoke-only).
- Option C batched ticks (journal cost optimization) and Spark Connect launcher (P2 engine adapter).
- Multi-replica lease in Postgres (P3).

### Migration

For consumers of `semanticdf` library 0.2.0 → 0.2.1: **no migration required**. The library version bump is on the platform-internal `semanticdf_2.13` artifact only (consumed by `semanticdf-platform`). External consumers of `semanticdf` see no API change.

For operators of the new `semanticdf-platform`: deployment changes only — see `semanticdf-platform/docker-compose.yml` and `semanticdf-platform/README.md`.

## v0.2.0 — file organization + cluster-mode safety

This release tightens the package layout, hardens the library for cluster-mode deployment, and introduces a cache auto-invalidation strategy keyed on model version. No behaviour change for existing batch or streaming terminals.

### What changed

- **Predicate DSL in its own sub-package.** The four predicate files (`Predicate`, `PredicateAst`, `PredicateAstWalker`, `PredicateOps`) form a tightly-coupled module — 52 inter-file references between them, more than every other group in the codebase. They are now in `io.semanticdf.predicate.*`. Consumers using FQN imports need to update.
- **Manifest ingestion files consolidated.** `SemanticManifest`, `YamlLoader`, and `DbtManifestReader` are in `io.semanticdf.adapters.*` alongside the other format readers. Consumers using FQN imports need to update.
- **Standalone public types split out of `SemanticTable.scala`.** `SortKey`, `ValidationResult`, `MeasureKind`, `JoinInfo`, and `SemanticFilter` now live in their own files. `SemanticTable.scala` is now just the class declaration.
- **Cluster-mode safety.** `SemanticTable`, `AuditSink`, and `ResultCache` are now `Serializable`. The `PredicateAst.Predicate` cache is `@volatile @transient` so it survives `QueryRequest` capture but is dropped on round-trip. The `OssieReader` `BufferedReader` FD leak is closed.
- **Cache auto-invalidation.** `AuditQueryRequest` and `AuditEvent` carry a `version: Int`. The cache key includes a length-prefixed `mv=<version>` segment; `InMemoryResultCache` invalidates by `(model, version)` pair. After a model rebuild with a new version, old cache entries for that model drop on the next read.
- **Audit event version propagates** through the JSONL stdout sink and the MCP `audit_log` DTO.

### Migration for FQN importers

```diff
-import io.semanticdf.Predicate
-import io.semanticdf.Predicate.Compare
-import io.semanticdf.PredicateOps
-import io.semanticdf.SemanticManifest
-import io.semanticdf.YamlLoader
+import io.semanticdf.predicate.{Predicate, Compare, PredicateOps}
+import io.semanticdf.adapters.{SemanticManifest, YamlLoader}
```

Wildcard imports (`import io.semanticdf._`) keep working because the sub-packages are sub-packages of `io.semanticdf`.

## v0.1.17 — audit log + result cache + Ossie adapter + SDFAdapter + review follow-ups

Twelve PRs landed in this wave. The unifying entry point is now `loadSemanticTables[S, P](source, resolve)` with implicit `SDFAdapter`, `DbtAdapter`, and `OssieReader` instances. A single call works for any of the three supported metadata formats.

### Observability surface

- **Audit log** (`audit/AuditEvent`, `audit/AuditSink`, `JsonlStdoutSink`, `InMemoryAuditSink`). `SemanticTable` gains `auditSink: Option[AuditSink]` + `withAuditSink(...)`. Every `toDataFrame` emits an `AuditEvent` (12 fields: timestamp, model, measures, dimensions, `whereHash`, `havingHash`, rowCount, elapsedMs, status, error, requester, requestId). Default is `NoOp` — zero overhead when not configured.
- **MCP `audit_log` retrieval tool**. The shared `InMemoryAuditSink` is wired to both the `query` writer and the `audit_log` handler reader. Agents can ask "what did I just query?" without a separate observability stack.
- **Result cache** (`cache/ResultCache`, `InMemoryResultCache`, `CachedResult`, `CacheKey`). `SemanticTable.withResultCache(...)` + `toDataFrame`'s cache-check / execute-on-miss / store path. Cache key is a SHA-256 of the **full** request shape: model, measures, dimensions, where hash, having hash, `orderBy` (direction + columns), `limit` (None vs Some). Column order is preserved (not sorted) — it's part of the result contract.
- **Performance baseline + leak tests**. `perf/PerfBaselineSpec` (7 benchmarks, INFO-only numbers published to surefire reports) + `leak/LeakSpec` (9 gates). v0.1.17 baseline: `toDataFrame`=62ms, cache hit=26ms, predicate hash=20μs. See `docs/design/perf-baseline.md`.
- **Cache invalidation hooks**. `ResultCache.putWithModel(key, value, model)` + `invalidateModel(name): Int`. `InMemoryResultCache` has a sidecar `byModel: HashMap[String, Set[String]]` for O(1) lookup. The LRU `removeEldestEntry` callback cleans the sidecar only when actually evicting (`invalidateModel` on 256 entries = 0ms median).

### Adapter surface

- **`SemanticMetadataAdapter[S, P]` typeclass** (`adapters/SemanticMetadataAdapter.scala`). Unifying entry point `loadSemanticTables[S, P](source, resolve)`. Implicit `spark: SparkSession` on `toSemanticTables(projects, resolve)(implicit spark)`. Three instances:
  - `SDFAdapter` (wraps `SemanticManifest.fromJson` / `fromJoinedJson`; the legacy methods are now `@deprecated` pointing at the adapter)
  - `DbtAdapter` (wraps `DbtManifestReader`)
  - `OssieReader` (new; parses both canonical `semantic_model.{datasets,relationships,metrics}` and legacy `ontology_mappings[*].semantic_model` shapes)
- **Ossie adapter perf + leak tests** (`perf/OssieReaderPerfSpec`, `leak/OssieReaderLeakSpec`). v0.1.17 baseline: small=4ms, medium=8ms, large=52ms, regex=26ms. Medium fixture = TPC-DS from `/tmp/ossie/examples/tpcds_semantic_model.yaml`.
- **Examples + README migration**. `examples/joined-manifest-e2e/Query.scala` + `TypedQuery.scala` use `loadSemanticTables(path, resolve)` via `SDFAdapter._`. The unified API is now the documented shape.

### Cache miss returns the rebuilt DataFrame (#184)

The cache miss path used to populate the cache by collecting from the lazy compiled `fresh` DataFrame, then return that lazy DataFrame to the caller. The caller's `collect()` then re-executed the source query — **twice on every miss, once on every hit**. The fix mirrors the cache hit path: collect once for the cache, then rebuild the DataFrame from those exact rows + schema. The returned DataFrame is parallelize-backed and decoupled from the source.

### Review follow-ups (PR #183, #185, #186, #187, #188, #189, #190)

**PR #183 (eight fixes):**
- **Cache key correctness**: `CacheKey` now includes `orderBy` direction + columns + `limit` (None vs Some). A cached `LIMIT 10 ORDER BY x DESC` could previously be returned for an uncapped ascending query — **wrong answer**. `CacheKey` no longer sorts `dimensions`/`measures` — column order is part of the result contract.
- **Cache hygiene**: the `invalidateModel` dead code (the `Option(map.get(k))` block) was deleted. `removeEldestEntry` now runs the `byModel` cleanup **only** when actually evicting (was running unconditionally on every put — a real bug introduced in #182). `rowCount` in audit events no longer reads 0 on cache miss.
- **Audit predicate hasher**: added the missing `Compare` subtypes (`Contains`, `StartsWith`, `EndsWith`, `ArrayContains`) that previously caused a `MatchError` on a valid query. `And`/`Or` now accept 3+ children via varargs.
- **Ossie adapter**: metrics bound to a specific dataset via `dataset.column` are no longer attached to every dataset (`SUM(orders.amount)` lives only on `orders`). Unqualified metrics like `COUNT(1)` in multi-dataset projects are skipped (ambiguous) rather than silently attached to everyone. Composite join keys use the typed multi-key `join_on` overload, not the previous `.head` pair.

**PR #185 (four fixes):**
- **CRITICAL — unified `loadSemanticTables` entry point didn't compile**. Each adapter object now exposes an `implicit val instance: SemanticMetadataAdapter[S, P] = this` so `import SDFAdapter._` brings the implicit into scope. This was the headline feature of the wave and the example `examples/joined-manifest-e2e/` failed to compile without it.
- **`stripTablePrefix` over-stripping fixed**. Now only strips the bound qualifier (passed through to `exprFor`), not every `identifier.` pattern.
- **`byModel` empty-set leak fixed**. Both `removeEldestEntry` and `putWithModel` drop the model entry when the set becomes empty.
- **`Throwable` swallow in cache-miss fallback fixed**. Now uses `scala.util.control.NonFatal`; `collect()` is outside the try-catch so query failures propagate through the outer audit handler.

**PR #186 (five verified bugs):**
- **Cache key `timeGrain` collision** — `None` and `Some("none")` both hashed to the same key; `Map("a"->"b,c:d")` collided with `Map("a"->"b","c"->"d")`. Length-prefixed encoding for the time fields only.
- **Audit predicate hasher gaps** — `In`, `NotIn`, `IsNull`, `IsNotNull` were missing from the `PredicateHasher` match (would `MatchError` on a valid query).
- **Join key probe re-evaluated per `toDataFrame`** — the lambda ran against the joined DataFrames on every compile just to discover keys already known at construction.
- **Audit event `rowCount` is 0 on cache miss** — the cache-fill path emitted the event before the cache was populated, so a miss showed `rowCount=0`.
- **`removeEldestEntry` runs `byModel` cleanup unconditionally** — entry cleanup fired on every `put`, not only on actual eviction (introduced in #182).

**PR #187 (three follow-ups):**
- **Length-prefixed encoding for all time fields** — extends #186 to `timeGrain`, `timeGrains`, `timeRange` (the `timeGrain`/`timeGrains` field was still delimiter-encoded in #186).
- **Cache key collision regression test** — locks in the time-field encoding (no regression in #186's fix).
- **POM `modelVersion` / `connection` sanitization** — `mvn install` warnings silenced.

**PR #188:**
- **Length-prefixed encoding extended to every field** — PR #186 / #187 only covered the time fields; `model`, `measures`, `dimensions`, `orderBy` still used delimiter encoding (admitted collisions when a value contained the delimiter or field separator).
- **MCP `time_grains` test coverage** — three new tests in `QuerySpec` (acceptance, propagation, error path).

**PR #189 (data correctness):**
- **`foreachBatch` now applies the streaming model's transformations**. The pre-fix code constructed a bare `SemanticTableOp(batchDf)` and discarded the rest of the op tree, so filters/transforms applied to a streaming model never reached the `foreachBatch` callback. New `substituteStreamingLeaf` walk replaces the streaming leaf with the batch DataFrame while preserving every intermediate op.

**PR #190 (audit observability):**
- **Per-batch audit events emit the actual `rowCount`**. The windowed-aggregation `foreachBatch` short-circuit bypassed the normal `toDataFrame` audit emit, so a streaming query with `withAuditSink(...)` produced ZERO audit events. The filter-only path emitted events with `rowCount=0` (the "caller will collect" sentinel). Both paths now go through `emitStreamingAudit`, which calls `batchDf.count()` and reports the real per-batch row count.

### Test count

693 library + 125 MCP = 818 green on Spark 3.5.8 and 4.1.1.

## v0.1.16 — structured predicate on the MCP wire + dbt manifest reader

Two independent features, shipped together:

### A. Structured predicate on the MCP `query` wire

The MCP `query` and `explain` tools now accept an optional `ast_where` (and `ast_having`) field with a structured predicate shape. Mirrors the library's `PredicateAst` ops (`eq` / `neq` / `lt` / `lte` / `gt` / `gte` / `and` / `or`). The flat `where` / `having` shape is unchanged.

```json
{
  "model": "flights",
  "measures": ["flight_count"],
  "ast_where": {
    "op": "and",
    "left":  {"op": "gt",  "left": "distance", "right": 500},
    "right": {
      "op": "or",
      "left":  {"op": "eq", "left": "carrier", "right": "AA"},
      "right": {"op": "eq", "left": "carrier", "right": "UA"}
    }
  }
}
```

If both `where` and `ast_where` are present, the server AND-combines them. Either can be omitted.

### B. dbt `manifest.json` reader

A dbt `manifest.json` reader that turns dbt's manifest into a `Map[String, SemanticTable]`. Closes the gap for dbt users who don't want to hand-author a second YAML.

```scala
// Phase 1: read the manifest. Pure, no Spark needed.
val project = DbtManifestReader.read(Paths.get("target/manifest.json"))

// Phase 2: bind to a Spark session.
val tables: Map[String, SemanticTable] =
  DbtManifestReader.toSemanticTables(project, spark, sourceTable =>
    spark.read.format("parquet").load(s"/data/$sourceTable"))
```

A column is a **dimension** by default. To mark a column as a **measure**, the user adds to their dbt `schema.yml`:

```yaml
columns:
  - name: total_revenue
    meta:
      kind: measure
      expr: "sum(amount)"
```

The reader checks for `meta.kind == "measure"` AND a non-empty `meta.expr`. Anything else stays a dimension.

### What's new

- `AstPredicates.scala` — ~80 LOC parser for the structured AST shape on the MCP wire.
- `QueryRequest` DTO — new `ast_where` and `ast_having` fields. `Query.mergedWhere` / `mergedHaving` AND-combine the structured and flat predicate sources.
- `queryToolSchema` — two new properties (`ast_where`, `ast_having`).
- `DbtManifestReader.scala` — ~290 LOC. Two-phase API: `read(manifestPath)` / `read(manifest: Map)` for parse-only; `toSemanticTables(project, spark, resolve)` for Spark binding.
- Source-table resolution: `<database>.<schema>.<alias>` / `<schema>.<alias>` / `<alias>`. Caller controls how to interpret the string.
- 36 new tests: 16 (`AstPredicatesSpec`) + 7 (`QuerySpec`) + 13 (`DbtManifestReaderSpec`).
- `examples/dbt-reader/` — runnable demo: hand-crafted `manifest.json` + CSVs + `Main.scala`.
- `docs/agents/mcp-contract.md` (status v4, new §"Alternative: ast_where / ast_having", updated error-codes table).
- `docs/design/dbt-manifest-reader.md` (new design doc).

### What's NOT new

- No new ops for the AST op set — closed subset the library's `PredicateAst` actually produces. For richer predicates (`in`, `not_in`, `is_null`), use the flat `where` shape.
- dbt **joins** — dbt doesn't record join keys in the manifest. v1 emits the model graph without joins; users add them via the existing `join_one` / `join_many` API.
- dbt **sources / metrics / streaming** — preserved in `DbtProject.rawNodes` for v2; streaming isn't a dbt concept.

### Files

- `semanticdf-mcp/src/main/scala/io/semanticdf/mcp/handlers/AstPredicates.scala` (new)
- `semanticdf-mcp/src/main/scala/io/semanticdf/mcp/handlers/Query.scala` (DTO + handler + schema)
- `semanticdf-mcp/src/test/scala/io/semanticdf/mcp/handlers/AstPredicatesSpec.scala` (new)
- `src/main/scala/io/semanticdf/DbtManifestReader.scala` (new)
- `src/test/scala/io/semanticdf/DbtManifestReaderSpec.scala` (new)
- `src/test/resources/dbt-fixtures/minimal-manifest.json` (new)
- `examples/dbt-reader/` (new example)
- `docs/agents/mcp-contract.md`, `docs/design/dbt-manifest-reader.md`, `docs/feature-roadmap.md`, `examples/README.md` (docs)

## v0.1.15 — SQL-mode CLI

A **first-touch friction** release. Ad-hoc exploration of a YAML model no longer requires writing Scala. The `query` subcommand parses a SQL string and maps it to the existing `SemanticTable.query()` API.

```bash
mvn exec:java \
  -Dexec.args="query --models examples/starter/models/ \
  --sql 'SELECT carrier, total_passengers FROM flights \
  GROUP BY carrier ORDER BY total_passengers DESC LIMIT 10'"
```

### What's new

- `SqlCli.scala` — ~340 LOC tokenizer + recursive-descent parser. No external SQL dependency.
- `query` subcommand in `Main.scala` — `--models <dir-or-file> --sql '<sql>'`.
- Supports `SELECT ... FROM ... WHERE ... AND/OR ... ORDER BY ... ASC/DESC LIMIT n`. `GROUP BY` accepted (and ignored — the model decides grouping from the SELECT items).
- Aliases (`SELECT carrier AS c`), `*` (all dims then all measures), `WHERE` with `AND`/`OR` and parentheses, string literals with SQL `''` escapes.
- Classification: a name that matches a model `measures` key → measure, otherwise → dimension. Unknown field names give a Clear error listing both known dims and known measures.
- 20 new tests (16 unit + 4 end-to-end with a real Spark session). 566/566 total.

### What's NOT new

- No JDBC / ODBC surface. This is a batch CLI, not a server.
- No subqueries, no JOINs, no CTEs. The grammar is intentionally narrow: one model, one SELECT, one WHERE.
- No timezone-aware casting of string literals. Numbers are `Long` if integer, `Double` if fractional. Strings are `String`.

### Files

- `src/main/scala/io/semanticdf/tools/SqlCli.scala`
- `src/main/scala/io/semanticdf/tools/Main.scala` (one new subcommand)
- `src/test/scala/io/semanticdf/tools/SqlCliSpec.scala`
- `src/test/scala/io/semanticdf/tools/SqlCliEndToEndSpec.scala`
- `src/test/resources/sql-cli-fixtures/flights.yml`
- `docs/runtime-quickstart.md` (usage example)
- `docs/feature-roadmap.md` (1.6 SHIPPED entry)

## v0.1.14 — asymmetric join keys

A **asymmetric-key** release. `SemanticTable.join_one` / `join_many` / YamlLoader `joins:` now accept different column names on each side of the join (e.g. `flights.carrier` joined to `carriers.code`). The wire format and runtime already supported the asymmetric shape; only the entry-point guards were blocking the case. No breaking change — existing symmetric joins work unchanged.

Library, MCP server, and CLI consumer are at

```
io.semanticdf:semanticdf_2.13:0.1.14
io.semanticdf:semanticdf-mcp_2.13:0.1.14
com.example:semanticdf-cli_2.13:0.1.14
```

Test count: 544 library + 90 MCP + 18 CLI (652 total), green on Spark 3.5.8 and 4.1.1.

### Library — features

- **`SemanticTable.join_one` / `join_many` accept asymmetric keys** — the typed entries (`join_on(leftKey -> rightKey)`) were already asymmetric-safe; the lambda overloads (`(l, r) => l("x") === r("y")`) now round-trip the asymmetric pair through `SemanticJoinOp.leftKeys` / `rightKeys` correctly.
- **`YamlLoader` accepts asymmetric `left_on` / `right_on`** — `joins: { carriers: { left_on: carrier, right_on: code } }` now parses and executes end-to-end. Previously rejected with a "left_on == right_on" guard.
- **`compileEquiJoin` trusts the constructor's captured keys** — no longer re-probes the lambda at compile time. The probe at construction (via `extractJoinKeys` + AST walker) is the single source of truth for both `leftKeys` and `rightKeys`.

### Performance

Strictly **less work** than v0.1.13:

- Eliminated one re-probe per `toDataFrame` call (the lambda no longer runs against the joined DataFrames at compile time just to discover keys already known from construction).
- The probe at construction runs once and its result is reused across all `toDataFrame` invocations on the same `SemanticTable`.

### Anti-scope (preserved as honest caveats)

- `onExprString` is still emitted as the legacy fallback for non-equi / OR / subquery predicates that don't fit the AST.
- `preAggregateAtGrain` (Many-cardinality fan-out prevention) probes each dimension against the side's DataFrame; user-supplied lambda dims that bypass the scope (`_ => col("x")`) are not filtered by name. Use scope-respecting dims (`t => t("x")`) for fan-out pre-aggregation to work correctly with cross-side dims.

## v0.1.13 — structured predicate AST for joined-manifest

A **predicate-AST** release. The `joined-models-manifest` recipe's last narrow caveat is closed: the joined wire shape now carries predicates as a structured AST (`model.join.predicate_ast`) alongside the legacy opaque `onExprString` SQL form. Tools get a typed view of the join condition; the reader's reconstructed `on` lambda uses the AST when present (zero overhead for the equi case, where the keys lattice already captures the structure).

Library, MCP server, and CLI consumer are at

```
io.semanticdf:semanticdf_2.13:0.1.13
io.semanticdf:semanticdf-mcp_2.13:0.1.13
com.example:semanticdf-cli_2.13:0.1.13
```

Test count: 538 library + 90 MCP + 18 CLI (646 total), green on Spark 3.5.8 and 4.1.1.

### Library — features

- **`PredicateAst` data model** — `Op` (sealed trait + 8 case objects for `eq` / `neq` / range / `and` / `or`), `Operand` (sealed trait + `ColumnRef`), `Predicate` (recursive AND/OR composition). `Predicate.toColumn(leftSide, rightSide)` builds a Spark `Column` from the AST, cached per `(leftSide, rightSide)` pair (typically 1 entry per join).
- **`PredicateAstWalker`** — cross-version reflection walker that turns a Catalyst `Expression` (Spark 3.x) or a `ColumnNode` (Spark 4.x) into a `PredicateAst.Predicate`. Handles `EqualTo` / `LessThan` / etc. via class-name fallback AND `UnresolvedFunction("=")` via `functionName`. Handles both `Seq[_]` and `java.util.List[_]` for the children collection.
- **`SemanticJoinOp.predicateAst: Option[PredicateAst.Predicate]`** — populated eagerly at construction when the keys lattice alone doesn't capture the structure (non-equi / OR / compound). Zero overhead for the canonical equi-join case.
- **`SemanticManifest.toJoinedJson` emits `predicate_ast`** when present; **`fromJoinedJson` prefers the AST** for rebuilding `on` (when prefixes aren't in play) and falls back to `onExprString` for legacy wire shapes.
- **`PredicateAstJson` helpers** — Jackson serialise/deserialise the AST.

### Wire format

The new `predicate_ast` field sits on the join block alongside `leftKeys` / `rightKeys` / `onExprString`:

```json
{
  "op": "lt",
  "left":  { "side": "left",  "col": "date" },
  "right": { "side": "right", "col": "valid_to" }
}
```

Compound predicates (AND / OR) recurse on `left` / `right`. See `examples/joined-manifest/` for an end-to-end demo (equi + non-equi + prefixed in one run).

### Anti-scope (preserved as honest caveats)

- `onExprString` is still emitted (legacy fallback). Older readers that don't know about `predicate_ast` continue to work via the SQL form.
- The structured AST covers the operations the library actually produces in practice (`eq` / `neq` / range / `and` / `or`). Anything more complex (subqueries, UDFs, etc.) falls through to `onExprString`.

### Cross-version

`ColumnSql.expressionOf` now returns `AnyRef` (was `Expression`). On Spark 4.x, when the underlying `ColumnNode` doesn't expose `.expression()` (e.g. for `UnresolvedFunction`), the node itself is returned — `PredicateAstWalker` handles both `Expression` and `ColumnNode` trees via the same reflection-driven shape match.

## v0.1.12 — joined-manifest caveats closed (Path C)

A **joined-manifest-completion** release. The `joined-models-manifest` recipe's last two BLOCK caveats are now closed: the wire shape carries `model.extra_dimensions[]` / `model.extra_measures[]` (caveat §1.2 — alias-prefixed dims round-trip) and the `join` block's `leftPrefix` / `rightPrefix` (caveat §1.3). After this release, the recipe is **ACCEPTED**; the only remaining narrow caveat is non-equi / OR predicates, which fall back to the captured `onExprString` SQL form.

Library, MCP server, and CLI consumer are at

```
io.semanticdf:semanticdf_2.13:0.1.12
io.semanticdf:semanticdf-mcp_2.13:0.1.12
com.example:semanticdf-cli_2.13:0.1.12
```

Test count: 519 library + 90 MCP + 18 CLI (627 total), green on Spark 3.5.8 and 4.1.1.

### Library — features (Path C)

- **`SemanticManifest.toJoinedJson` emits alias-prefixed dims/measures** —
  the joined wire shape now carries `model.extra_dimensions[]` and
  `model.extra_measures[]` blocks for the alias-prefixed dimensions and
  measures that YamlLoader adds at runtime (e.g. `carriers.name`).
  Omitted when empty (canonical post-v0.1.11 producer case).
- **`SemanticManifest.fromJoinedJson` reconstructs them** — wraps the
  base join in a `SemanticTransformsOp` carrying the alias-prefixed
  dims/measures, matching the runtime's exact wiring. The round-trip is
  functional for the typical aliased-join case.
- **`SemanticJoinOp.leftPrefix` / `rightPrefix`** — new optional fields
  on the case class. The wire shape emits them in the `join` block
  (omitted when empty); the reader's reconstructed `on` lambda applies
  them so the predicate reads `l("<leftPrefix>k1") === r("<rightPrefix>k1")`
  when set, with bare column names when empty.
- **`JoinedManifestMeta` gains 4 fields** — `leftPrefix: String`,
  `rightPrefix: String`, `extraDimensions: Int`, `extraMeasures: Int`.
  All default to empty / 0 for legacy manifests.

### Anti-scope (preserved as honest caveats)

- Non-equi / OR predicates — fall back to the captured `onExprString`
  SQL form, which is functional for the wire-round-trip case but
  consumers that need the full predicate semantics should re-load from
  YAML.

See [`RELEASE.md`](https://github.com/EchoEnv/semanticdf/blob/v0.1.12/RELEASE.md)
for the full changelog.

## v0.1.11 — manifest keys, joined-manifest wire shape, recipe denoise

A **joined-manifest + manifest-keys + denoise** release. The library closes both BLOCKed recipes from the v0.1.11 review cycle (`manifest-transforms` and `joined-models-manifest`): `SemanticManifest` now round-trips joined models with embedded per-side single-table manifests, the join key is recovered from the wire shape (typed `join_on` entry / multi-key AND / SQL-form `onExprString` fallback), and `Model.status` carries five new identity fields (`id`, `manifestVersion`, `$schema`, `namespace`, `metadata`) through every surface. The `Transform.exprString` field round-trips through the manifest. Two new typed entry points (`join_on`, `join_many_on`) carry the join key as the source of truth at construction; the legacy lambda overload is preserved for back-compat via a probe that decomposes the AST. Cross-version: the AST probe walks `Column.expr` on Spark 3.5.x and `ColumnNode` on Spark 4.1.x via reflection.

Library, MCP server, and CLI consumer are at

```
io.semanticdf:semanticdf_2.13:0.1.11
io.semanticdf:semanticdf-mcp_2.13:0.1.11
com.example:semanticdf-cli_2.13:0.1.11
```

Test count: 513 library + 90 MCP + 18 CLI (621 total), green on Spark 3.5.8 and 4.1.1.

### Library — features

- **Manifest identity + governance fields** — `SemanticManifest.Identity`
  case class + `toJson(model, identity, prettyPrint)` overload. Five new
  optional top-level fields on the single-table wire shape: `id` (reverse-DNS
  FQN, **required at write time** via `--id` flag), `manifestVersion`
  (semver, defaults to `0.1.0`), `$schema` (URL pointing at the
  `schemas/manifest.schema.json` reference), `namespace` (defaults to
  `default`), `metadata` (free-form `Map[String, String]`). `parseMeta` version
  gate relaxed to a `v0.1.*` prefix match so old manifests continue to parse
  after the schemaVersion string bumps.
- **`tools.Main manifest` gains `--id` (required), `--namespace`,
  `--metadata-K V`** — the CLI now writes the manifest with the new identity
  fields populated. **The `joined-manifest` envelope is no longer BLOCKed**:
  `toJoinedJson` (new) emits `kind: "semanticdf-joined-manifest"` with two
  embedded per-side single-table manifests, a `join` block (cardinality +
  keys), per-side dimension/measure counts, and a SQL-form `onExprString`
  fallback for non-equi predicates. `fromJoinedJson` (new) reconstructs a
  `SemanticTable` rooted at `SemanticJoinOp` with a **functional** `on`
  lambda built from the wire keys (single-column: `l(k)=r(k)`; multi-column:
  `AND over pairs`). `parseJoinedMeta` extracts the joined header without
  loading Spark.
- **Manifest `Transform[]` round-trip** — `Transform.exprString: Option[String]`
  field carries the source SQL through the wire shape. `SemanticManifest`
  emits a `transforms[]` block; `fromJson` reconstructs a
  `SemanticTransformsOp`. The `<lambda>` sentinel path still throws a loud
  runtime error on first query.
- **Manifest `CalcMeasure` round-trip** — `readMeasure` dispatches on the
  manifest's `kind` field: base measures use `F.expr` directly; calc measures
  use `CalcExpr` to walk the calc DSL and substitute `scope(name)` for each
  measure reference, so the post-aggregation `MeasureScope` resolves
  sibling-measure columns correctly.
- **Typed `join_on(other, keys)` entry** — the "core-correct and
  best-practice" way to build a join. The key names are the source of
  truth at construction; the `on` lambda is synthesised internally. Two
  overloads: `join_on(other, (String, String))` for single-key and
  `join_on(other, Seq[String], Seq[String])` for multi-key (positional
  pairing). The corresponding `join_many_on(...)` is the fan-out variant.
- **Legacy `(JoinSide, JoinSide) => Column` lambda still works** — a
  construction-time probe decomposes the AST to recover keys. Capture maps
  tag column names with side prefixes so the walker can distinguish sides
  even after bytecode resolution. The `onExprString` field carries the SQL
  form for predicates the probe can't factor (OR, non-equi, mixed).
- **Cross-version Spark compatibility** — Spark 4.1.x removed `Column.expr`
  in favor of `ColumnNode` (UnresolvedAttribute / UnresolvedFunction). A
  small `ColumnSql` reflection helper abstracts both, and the AST walker
  recognises both `Expression` (Spark 3) and `ColumnNode` (Spark 4) node
  types via `children()` + `sql()` + `functionName()` — same source compiles
  on both. The `JoinKeyProbe` capture-tag trick (`__left__id` /
  `__right__id`) survives the version jump.
- **Tolerant backward compat** — the version-gate in `parseMeta` is
  relaxed to a `v0.1.*` prefix match so v0.1.9 and v0.1.10 manifests still
  parse unchanged. The kind discriminator now accepts both
  `semanticdf-model-manifest` and `semanticdf-joined-manifest`. A new
  `CLI` subcommand `validate-joined-manifest` reads joined manifests
  without loading Spark.

### MCP server

No functional change; the joined-manifest wire shape is library-only.

### CLI

- **`manifest` subcommand** — new flags: `--id` (required, reverse-DNS
  FQN), `--namespace` (default `default`), `--metadata-K V` (repeated
  inline, no separate config file). The `validate-joined-manifest`
  subcommand prints the joined header (kind, cardinality, per-side counts,
  identity, BLOCK warning).
- **No break** for existing manifest subcommand flags.

### Docs

- **Recipe docs denoised** — `docs/design/joined-models-manifest.md` is
  now `SHIPPED cleanly`; `manifest-transforms.md` and
  `manifest-identity-bump.md` removed their "implementation landed in PR
  #NNN" suffixes; `REVIEW-FEEDBACK.md` got a Resolution Status section
  tying BLOCKs to ship-PRs.
- **`docs/manifests-and-joins.md`** — full educational walkthrough of the
  joined-manifest wire shape with the new `toJoinedJson` /
  `fromJoinedJson` API path. The §5 "real path" section uses the canonical
  library primitives; §5.5 (renamed from §5) covers the legacy
  hand-rolled alternative for pre-0.1.11 consumers.

### Examples

- **`manifest-load/`** — refreshed to demonstrate the new
  `SemanticManifest.fromJson` reading a v0.1.11-format manifest end-to-end.
- **`manifest-transforms-load/`** — new worked example showing the
  `transforms[]` round-trip and the `<lambda>` sentinel path.
- **`joined-manifest/`** — new worked example showing the canonical
  `toJoinedJson` / `fromJoinedJson` path. Uses starter's flights +
  carriers models, runs a programmatic join via `join_on`, emits the
  joined manifest, parses the header via `parseJoinedMeta`, and
  round-trips via `fromJoinedJson`.
- **`joined-manifest-split/`** — historical / reference example (the
  legacy hand-rolled envelope path). Remains valid documentation for
  consumers pinned to pre-0.1.11 versions; the README banner now
  redirects to the canonical `joined-manifest/` example.
- All 14 example pom.xml files bumped to 0.1.11. `joined-manifest` and
  `joined-manifest-split` both compile and run end-to-end against
  v0.1.11-SNAPSHOT (verified: `mvn scala:run` → "demo complete" on
  both).

### Anti-scope (preserved as honest caveats)

- Alias-prefixed dim names from the joined runtime (e.g. `carriers.name`
  in the YAML's `joins:` aliasing) don't flow through the joined wire
  shape. The merged-model state has the un-prefixed names; consumers
  needing the full alias surface re-load from YAML or call
  `joined.explainSemantic` for the runtime-resolved names.
- `leftPrefix` / `rightPrefix` on the `join` block (recipe §3) are not
  implemented. `SemanticJoinOp` doesn't carry them today; the recipe
  acknowledges this as a future revision item.

## v0.1.10 — manifest artifact, lifecycle, denoise

A **manifest + lifecycle + docs** release. The library gains a portable JSON-manifest format for shipping a model's static definition independently of YAML, a first-class `Model.status` lifecycle field (Draft / Published / Deprecated), MCP and CLI surfaces that surface lifecycle warnings on every successful envelope, and a denoised docs surface that reflects the current state of every shipped feature. Lifecycle enforcement (warnings vs refusal) is consumer-side, by design.

Library, MCP server, and CLI consumer are at

```
io.semanticdf:semanticdf_2.13:0.1.10
io.semanticdf:semanticdf-mcp_2.13:0.1.10
com.example:semanticdf-cli_2.13:0.1.10
```

Test count: 442 library + 90 MCP + 18 CLI (550 total), green on Spark 3.5.8 and 4.1.1.

### Library — features

- **`SemanticManifest` artifact** — `toJson` / `fromJson` / `parseMeta`.
  Single-table models serialize to a self-contained JSON file carrying
  the model identity (name, version, status, description, sourceTable),
  every dimension, every measure (with `kind: "base" | "calc"` and the
  calc-measure dependency closure), pre-aggregate filters, and a digest
  header. Single-table is the only supported shape — joined models throw
  at `toJson` time (per the recipe §10 anti-scope). Streaming models
  record `isStreaming: true` in the digest; `fromJson` produces a
  `SemanticStreamingTableOp` for streaming sources.
- **Base + calc measures round-trip the persisted `expr` string.**
  `readMeasure` dispatches on the manifest's `kind` field: base measures
  use `F.expr` directly; calc measures use `CalcExpr` to walk the calc
  DSL and substitute `scope(name)` for each measure reference, so the
  post-aggregation `MeasureScope` resolves sibling-measure columns
  correctly. A bare lambda with no `exprString` hint records the
  `<lambda>` sentinel and throws a loud runtime error on first query.
- **`Model.status` lifecycle field** — sealed trait with `Draft`,
  `Published`, `Deprecated` cases. Round-trips through the YAML
  `status:` block (defaults to `Published` for back-compat with v0.1.x
  models). Propagates through every fluent op (`withDimensions`,
  `withMeasures`, `withRowFilter`, `orderBy`, `limit`, `hint`,
  `withTransforms`, `groupBy().aggregate()`). Surfaces in the manifest
  artifact (`model.status`) and in the MCP `describe_model` response
  (`data.status`).
- **Auto-derived time dimensions** — `Dimension.time("ts", ...,
  derive = Seq("year", "month", "day"))` materializes sibling dims
  using Spark date-part functions on the source column. The YAML
  equivalent is `derived_dimensions: [year, month, day]`.
- **Implicit `SparkSession` pass** — `toDataFrame` and `execute` now
  accept an implicit `SparkSession`, matching the Scala DSL ergonomics.
  Backward-compatible: existing explicit-`spark` call sites still work.

### MCP server

- **Lifecycle warnings on every successful envelope** —
  `SemanticManifest` and `toJson` / `fromJson` carry a `warnings:
  List[String]` field. `ManifestMeta.status == "deprecated"` or
  `"draft"` populates the warnings array. Agents see the warning in
  every tool response; downstream consumers can route on it (e.g.
  refuse to query a `deprecated` model).

### CLI

- **Lifecycle warnings on stderr, not stdout** — `sdf describe
  <model>` and `sdf list` print `WARN: model 'X' is deprecated` to
  stderr when the model is `Deprecated` or `Draft`. JSON output
  (`--json`) is unaffected; warnings stay out of the stdout payload.
- **Status column in `sdf list`** — the table gains a `STATUS` column.

### CLI consumer

- **End-to-end integration tests** — `examples/cli-consumer/` now ships
  18 integration tests that exercise every command against an in-process
  `com.sun.net.httpserver.HttpServer`. The tests catch wire-format
  regressions and the `WARN:` plumbing; they run on every PR.
- **Transport failures return exit 3 (don't kill the JVM)** — `sys.exit`
  inside `Client.send` was replaced with a `TransportFailure` exception
  that `Main.run` catches. The CLI's behavior is unchanged for human
  users (same exit code, same stderr message); tests can now exercise
  the transport-error path without killing the test JVM.

### Examples

- **`manifest-load/`** — new worked example for the manifest's
  runtime half. Reads a pre-built streaming-manifest artifact
  (`manifests/usage.json`) via `SemanticManifest.fromJson`, runs
  queries against a streaming source, and surfaces lifecycle warnings.
  Companion to the [manifest-artifact recipe](docs/design/manifest-artifact.md).
- **`streaming-manifest-load/`** — the streaming analog. Reads
  `events.json`, builds a `StreamingConfig`, runs a streaming query for
  12 seconds, prints progress. Demonstrates the streaming manifest
  read path; the manifest carries the model, the operator carries the
  runtime config.
- All 9 example templates updated to the `implicit val spark` form
  and to declare `status: published` explicitly.

### Docs

- **`docs/known-limitations.md` is current** — the summary table
  reflects the v0.1.10 state (auto-derived time dims are shipped;
  streaming lifecycle is documented; the denoise pass removed
  stale version refs and "not implemented" claims that are now
  implemented).
- **`docs/agents/mcp-contract.md` documents the lifecycle warnings**
  added to the contract.
- **3 new design recipes** (`docs/design/joined-models-manifest.md`,
  `docs/design/manifest-transforms.md`, `docs/design/streaming-manifest.md`)
  document the remaining manifest gaps with concrete DRAFT designs.
- **User-facing surface denoised** — PR-level provenance noise removed
  from user-facing docs (the policy: docs describe the WHAT, not the
  WHEN; `RELEASE.md` and `feature-roadmap.md` are the canonical home
  for changelog / historical refs).

### Compatibility

No breaking changes. All 14 PRs since v0.1.9 are additive. The
manifest's new field is optional. `Model.status` defaults to
`Published`. The streaming manifest read path is new (it was a
no-op before). The CLI's lifecycle-warnings surfacing is new.
`tools.Main manifest` is unchanged (it doesn't generate streaming
artifacts — that's an inline script in the new example's README).

### Anti-scope (carried forward, unchanged)

- **Joined models in the manifest** — still throws at `toJson` per
  recipe §10. A `joined-models-manifest.md` recipe was drafted with
  a separate `kind: "semanticdf-joined-manifest"` proposed; needs a
  library change to `SemanticJoinOp` before the implementation lands.
  Deferred to v0.2.
- **`transforms:` block in the manifest** — recipe drafted
  (`manifest-transforms.md`); needs `Transform.exprString` added to
  `Model.scala` first. Deferred.
- **Streaming joined models** — separate recipe if there's demand.
- **N-way joins, non-equi joins, schema negotiation** — out of scope
  for the manifest format. Joined models work via the YAML loader.

## v0.1.9 — Structured Streaming terminal end-to-end

A **streaming + doc + ergonomic** release. The library gains a first-class Structured Streaming terminal that shares the op tree, builders, typed DSL, validator, and implicit-SparkSession ergonomics with the batch terminal. Lifecycle (when to start, how long to run, when to stop) stays with the operator's program — by design, not by accident.

Library, MCP server, and CLI consumer are at

```
io.semanticdf:semanticdf_2.13:0.1.9
io.semanticdf:semanticdf-mcp_2.13:0.1.9
com.example:semanticdf-cli_2.13:0.1.9
```

Test count: 404 library + 72 MCP, green on Spark 3.5.8 and 4.1.1.

### Library — features

- **Streaming terminal** (`SemanticTable.toStreamingQuery(spark, opts)`).
  The streaming counterpart to `toDataFrame`. Same DSL builders
  (`groupBy` / `where` / `join_one` / `groupByDimensions` /
  `aggregateMeasures` / typed `Predicate` factories) work on
  streaming-rooted models. `StreamingValidator` rejects the patterns
  the streaming engine can't handle (limit, orderBy, stream-stream
  joins, groupBy+aggregate without a window, `t.all(...)` without a
  window) before the query starts, naming the offending pattern.
- **`SemanticStreamingTableOp`** — the streaming counterpart to
  `SemanticTableOp`. The op tree walks it transparently; the
  streaming terminal + the YAML loader + the typed DSL all
  converge on the same op node shape.
