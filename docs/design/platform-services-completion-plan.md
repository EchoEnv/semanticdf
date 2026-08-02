# Platform Services Completion Plan (v0.2.2 Charter)

**Status:** DRAFT — pending user sign-off before any code is written.
**Authors:** Synthesized from the senior-data-engineer + senior-software-architect
review dispatch (post (see version history), post SparkConnect control plane).
**Scope:** Wire the four remaining `semanticdf-platform` Restate-service stubs
(`ModelService`, `QueryService`, `AuditService`, `CatalogService`) to the
`semanticdf` library + a durable Postgres audit/registry substrate.

---

## Executive summary

After PRs #220–#241, the platform's **streaming story** is end-to-end wired:
`StreamingService` ↔ `SparkStreamingQueryLauncher` ↔ `SemanticTable.toStreamingQuery`
↔ library `StreamingSupport`. **Four Restate services remain explicit P1 skeletons**
with `TODO` comments and placeholder bodies:

| Service | Current state | Library integration | Postgres durable? |
|---|---|---|---|
| `AuditService` | dedup-hash-only journal writes; ignores payload | none | **no** |
| `ModelService` | versions + manifest-hash in journal; never compiles YAML | none | **no** |
| `QueryService` | echoes back stub `QueryResult` (`List.of()`, `0L`) | none | **no** |
| `CatalogService` | returns `List.of()`, `null` | none | **no** |

This plan closes the four stubs across **three focused PRs**, prioritized by
**user-visible blast radius** (smallest first) and **architectural risk
density** (most error-prone last, with one per-PR planning step).

| Order | PR | Why this order | Approx LOC |
|---|---|---|---|
| 1 | `AuditService` (write+read) | already on the streaming hot path via `RestateAuditSink`; smallest blast; sets the durable-Postgres pattern the other PRs copy. | ~180 + ~280 tests |
| 2 | `ModelService` + `CatalogService` (bundled) | both stubs read/write the same `models` table; cannot test `ModelService` without `CatalogService` as a witness. Cache-invalidation hook is the key design gate. | ~450 + ~360 tests |
| 3 | `QueryService` | stateless, single-call site, `ResultCache` already in library; biggest blast once wired but only becomes live when the REST surface is added. | ~250 + ~280 tests |

**Total**: ~880 LOC impl + ~920 LOC tests across 3 PRs. All additive — no
breaking changes to existing public API.

---

## Design rules locked before any code

These are non-negotiable; the design-first PRs set them in stone.

### 1. Reuse, don't reinvent

Every library abstraction the platform already has **MUST** be reused. The
senior reviewers flagged these confirmed-existing seams:

| Library type | File:line | Reuse site |
|---|---|---|
| `io.semanticdf.SemanticTable` | `src/main/scala/io/semanticdf/SemanticTable.scala` | `ModelService.register` (compile target), `QueryService.runQuery` (lookup target) |
| `io.semanticdf.adapters.YamlLoader.loadDir` | `src/main/scala/io/semanticdf/adapters/YamlLoader.scala:82` | `ModelService.register` (compile step) |
| `io.semanticdf.lineage.Lineage.of` / `workspaceOf` / `toJson` / `fromJson` | `src/main/scala/io/semanticdf/lineage/Lineage.scala` | `ModelService.register` (lineage durable cache), `CatalogService.describeModel` |
| `io.semanticdf.cache.ResultCache` + `CacheKey.forRequest` + `invalidateByModelAndVersion` | `src/main/scala/io/semanticdf/cache/{ResultCache,CacheKey}.scala` (`:48`, `:103`) | `QueryService.runQuery` (cache), `ModelService.bumpVersion` (auto-invalidation) |
| `io.semanticdf.audit.AuditEvent.dedupHashOf` | `src/main/scala/io/semanticdf/audit/AuditEvent.scala:102` | `AuditService.append` (dedup key) — NOT `StreamingDedupHash` (which is for streaming events) |
| `io.semanticdf.audit.Clock` (`() => Restate.instantNow()`) | `src/main/scala/io/semanticdf/audit/Clock.scala` | All Restate handlers that produce `AuditEvent.ts` |
| `io.semanticdf.audit.AuditEvent` | `src/main/scala/io/semanticdf/audit/AuditEvent.scala` | `RestateAuditSink.emit` (construct canonical event), `AuditService.append` (persisted row) |
| `io.semanticdf.result.ResultDecoder` | `src/main/scala/io/semanticdf/result/ResultDecoder.scala` | `QueryService.runQuery` (decode rows typed) |

**Forbidden new types**: do **NOT** create a new `ModelRegistry`,
`AuditSink`, `ResultCache`, `Lineage`, `ResultDecoder`, `Clock`, or
`AuditEvent` shadow type in the platform. The platform's `streaming.AuditSink`
interface is correct as-is — it's the Restate-side seam to `AuditService`,
distinct from the library's `AuditSink` (which is the in-process emit seam
inside `SemanticTable.toDataFrame`). Don't conflate them.

### 2. State placement — locked

Per `docs/design/platform-architecture.md` §2.3:

> **Restate journal holds coordination state (recent, recoverable from
> replay). Postgres holds queryable history (durable, engines read directly).**

Concretely per service:

| State | Lives in | Where |
|---|---|---|
| `currentVersion`, `manifestHash`, `pendingBuild`, `lastInvalidatedAt` | journal | `ModelService` |
| `LAST_DEDUP_HASH`, `LAST_WRITE_OFFSET` | journal | `AuditService` (fast-path dedup) |
| `STATUS`, `STOP_SIGNAL`, `RECONCILE_BLOCKED`, `RESTART_COUNT`, `ERROR_COUNT`, `LAST_RESTART_AT`, `AUDIT_LOSS_COUNT` | journal | `StreamingService` (already shipped) |
| model YAML, versioned lineage, durable audit events | Postgres | `semanticdf_catalog.{models, model_lineage, audit_events}` |

### 3. Determinism inside `Restate.run(...)`

The senior-architect review flagged finding #1: *"any code inside Restate.run
must be a pure function of its inputs — no System.currentTimeMillis(),
UUID.randomUUID(), etc."* Apply the `Clock` seam.

For every `ts` and `compiledAt` field produced inside a Restate handler:

```java
import io.semanticdf.audit.Clock;
// ...
final java.time.Instant ts = Restate.instantNow();      // replay-stable
// pass to library as a thunk: Clock: () => Instant = () -> Restate.instantNow();
```

### 4. Replay-safe Postgres writes

Every side-effecting Postgres write goes inside a `Restate.run` block to
make replay idempotent:

```java
@Handler
public void onSomething(SomeRequest req) {
    Long persistedOffset = Restate.run("audit-append", () -> {
        return store.append(...);   // Postgres INSERT — replayed as cached value, not re-run
    });
    var state = Restate.state();
    state.set(LAST_WRITE_OFFSET, persistedOffset);
}
```

The journal is the dedup boundary for replay; the SQL `UNIQUE` constraint
on the dedup-hash is the durable durable boundary for cross-restart
operator-replay (e.g., `purgestate` then submit the same call).

### 5. Test-seam convention (5-constructor overload pattern)

The wired `StreamingService` set the template (`StreamingService.java:163-209`):
no-arg constructor → 3-arg → 4-arg `AuditSink` → 4-arg `StreamCatalog` → 5-arg
both. Each adds one seam without breaking existing test code. The three new
service implementations MUST mirror this — *no `new` inside handlers, only
injected constructors.*

---

## PR-A: `feat(platform): wire AuditService to Postgres`

**Goal**: `AuditService.append` writes audit events to a partitioned Postgres
table on the streaming hot path that already invokes it. Adds a `queryRecent`
read handler for retrieval.

### Tasks

#### A.1. New `audit/AuditEventStore` interface (mirrors `streaming/StreamCatalog`)

```java
package io.semanticdf.platform.audit;

import io.semanticdf.audit.AuditEvent;

public interface AuditEventStore extends java.io.Closeable {
  long append(AuditEvent event) throws SQLException;
  java.util.List<AuditEvent> queryRecent(String tenant, java.time.Instant since,
      java.time.Instant until, int limit);
  void ensureSchema() throws SQLException;  // CREATE SCHEMA/TABLE/PARTITION IF NOT EXISTS
}
```

#### A.2. New `audit/PostgresAuditEventStore` impl

Mirrors `streaming/PostgresStreamCatalog.java:1-200` exactly:
- HikariCP pool size 4, `connectionTimeout` 2000ms
- `CREATE SCHEMA/TABLE/PARTITION IF NOT EXISTS` at boot
- `INSERT ... ON CONFLICT DO NOTHING`
- `try-with-resources` on every connection borrow

Schema:

```sql
CREATE SCHEMA IF NOT EXISTS semanticdf_catalog;

CREATE TABLE IF NOT EXISTS semanticdf_catalog.audit_events (
  tenant        TEXT        NOT NULL,
  ts            TIMESTAMPTZ NOT NULL,
  event_type    TEXT        NOT NULL,
  dedup_hash    TEXT        NOT NULL,
  payload       TEXT        NOT NULL,
  offset_value  BIGSERIAL,
  PRIMARY KEY (tenant, ts, dedup_hash)
) PARTITION BY RANGE (ts);

-- monthly partitions created on demand (a future helper)
CREATE TABLE IF NOT EXISTS semanticdf_catalog.audit_events_YYYY_MM
  PARTITION OF semanticdf_catalog.audit_events
  FOR VALUES FROM (...) TO (...);

CREATE INDEX IF NOT EXISTS audit_events_tenant_ts_idx
  ON semanticdf_catalog.audit_events (tenant, ts DESC);

CREATE INDEX IF NOT EXISTS audit_events_tenant_event_type_ts_idx
  ON semanticdf_catalog.audit_events (tenant, event_type, ts DESC);
```

#### A.3. Rewrite `AuditService.append`

```java
@Handler
public void append(AuditService.AppendRequest request) {
  var state = Restate.state();
  String currentHash = state.get(LAST_DEDUP_HASH).orElse("");
  if (currentHash.equals(request.dedupHash())) return;  // fast path — journal hit

  final AuditEvent event = new AuditEvent(
      request.tenant(),
      request.eventType(),
      Restate.instantNow(),                   // replay-stable ts
      request.dedupHash(),
      request.payload());

  Long newOffset = Restate.run("audit-append", () -> store.append(event));
  state.set(LAST_DEDUP_HASH, request.dedupHash());
  state.set(LAST_WRITE_OFFSET, newOffset);
}

@Shared
@Handler
public List<AuditEvent> queryRecent(QueryRecentRequest req) {
  return Restate.run("query-recent",
      () -> store.queryRecent(req.tenant(), req.since(), req.until(), req.limit()));
}

public record AppendRequest(/* existing fields */) {}
public record QueryRecentRequest(String tenant, Instant since, Instant until, int limit) {}
public record AuditEventRow(/* mirror AuditEvent fields */) {}
```

**Cross-service caller update**: `streaming/RestateAuditSink` already
constructs an `AuditEventRequest` and calls `append`. The wire shape is
unchanged. Library `AuditEvent.dedupHashOf(...)` is called once at the
`RestateAuditSink` construction site (constant per call shape).

#### A.4. Tests (`AuditServiceTest.java` + `PostgresAuditEventStoreTest.java`, Testcontainers PG)

| Test | What |
|---|---|
| `append_writesToPostgresWithReplayStableTs` | Insert one event, verify row + that `ts` is replay-stable across same-call replay (Restate test harness) |
| `append_idempotentOnDedupHashCollision` | Insert same `(tenant, dedup_hash)` twice → second is no-op (single row) |
| `append_doesNotBreakOnHikariFailure` | Mock `store.append` to throw → exception bubbles to `Restate.run`, REPLAY retries, AuditService does not propagate to `RestateAuditSink.emit` (which already swallows) — verify streaming does not abort |
| `append_autoCreatesMonthlyPartition` | First insert at month X → partition X is created; second insert at month Y → partition Y is created |
| `queryRecent_honorsTimeRangeAndLimit` | Insert 5 events across t1..t5; query (t2, t4, limit=2) → returns exactly the 2 in window |
| `queryRecent_partitionPruning` | Same query against a 90-day range — verify EXPLAIN uses partition pruning (or document it as PG optimizer's guarantee) |
| `append_doesNotUseSystemCurrentTimeMillis` | Structural assertion: `AuditService.java` contains no `System.currentTimeMillis` / `Instant.now()` calls (mirror of `PlatformApplicationStartupSparkHookTest`) |
| `append_dedupHashUsesLibrary` | Structural assertion: dedup-hash path includes `dedupHashOf` call into library, not `StreamingDedupHash.compute` |

#### A.5. Flag-gated rollout

Per the user's directive *"flag-gated transitions, not cut-overs"*:

```
SEMANTICDF_AUDIT_PERSIST=true|false   (default: false)
```

- `false` (default for v0.2.2): `AuditEventStore` is a `NoOpAuditEventStore`; behavior matches today's broken-but-safe state — events reach `LAST_DEDUP_HASH` in the journal but not Postgres. Streaming is unaffected.
- `true`: `PostgresAuditEventStore`. Durable audit across restart. Requires `SEMANTICDF_CATALOG_JDBC_URL` to be set.

Decision point for the operator: flip the env var on production rollout.
Default flip target: **v0.2.3**.

### Files
- NEW `audit/AuditEventStore.java`, `audit/PostgresAuditEventStore.java`, `audit/NoOpAuditEventStore.java`
- NEW `audit/PostgresAuditEventStoreTest.java` (Testcontainers PG, ~5 tests)
- NEW `audit/AuditServiceTest.java` (Restate SDK TestKit, ~4 tests, includes replay-safety)
- MOD `audit/AuditService.java` (append rewritten + `queryRecent` added)
- MOD `PlatformApplication.java` (HikariCP + wiring)
- MOD `streaming/RestateAuditSink.java` (call `AuditEvent.dedupHashOf` instead of constructing the hash shape by hand)
- MOD `docs/design/platform-architecture.md` §5 (mention AuditEventStore durability)

### Risk classification (per `clean-architecture-refactor` skill §3)
**Auto-safe**: structural — replacing placeholder bodies with real wiring. No
contract change visible to operators (events still reach the journal).
Behavior opt-in via env var.

**Needs-confirmation** *(none — no data semantics change)*.

---

## PR-B: `feat(platform): wire ModelService + CatalogService to Postgres`

**Goal**: Models registered through the Restate API compile via the library,
persist durably, and are queryable via `CatalogService`. Cache invalidation
hooks into `ResultCache.invalidateByModelAndVersion`.

### Tasks

#### B.1. New `model/ModelStore` interface (mirrors `streaming/StreamCatalog`)

```java
package io.semanticdf.platform.model;

import io.semanticdf.lineage.ModelLineage;

public interface ModelStore extends java.io.Closeable {
  /** Idempotent insert. Returns the persisted definition (or current if exists). */
  ModelDefinition registerIfAbsent(String modelName, int version, String yaml,
      String manifestHash, java.time.Instant registeredAt) throws SQLException;
  java.util.List<ModelDefinition> listAll(String namespace) throws SQLException;
  ModelDefinition loadByName(String modelName, int version) throws SQLException;
  void ensureSchema() throws SQLException;

  /** DTOs */
  record ModelDefinition(String modelName, int version, String yaml,
                         String manifestHash, java.time.Instant registeredAt,
                         String lineageJson) {}
}
```

#### B.2. New `model/PostgresModelStore` impl

Schema:

```sql
CREATE TABLE IF NOT EXISTS semanticdf_catalog.models (
  model_name    TEXT NOT NULL,
  version       INT  NOT NULL,
  status        TEXT NOT NULL DEFAULT 'active',  -- active | deprecated | failed
  manifest_yaml TEXT NOT NULL,
  manifest_hash TEXT NOT NULL,
  registered_at TIMESTAMPTZ NOT NULL,
  lineage_json  TEXT NOT NULL DEFAULT '',
  PRIMARY KEY (model_name, version)
);

CREATE INDEX IF NOT EXISTS models_name_idx ON semanticdf_catalog.models (model_name);

-- L1 cache invalidation hook: per-model "dirty since version" record
CREATE TABLE IF NOT EXISTS semanticdf_catalog.model_invalidation (
  model_name      TEXT PRIMARY KEY,
  invalidated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  version_at      INT NOT NULL
);
```

`INSERT ... ON CONFLICT (model_name, version) DO NOTHING` for idempotency.

#### B.3. Rewrite `ModelService.register`

```java
@Handler
public void register(RegisterRequest request) {
  var state = Restate.state();

  // STEP 1: journal in_progress (visible to other handlers in same key)
  state.set(REGISTRATION_STATUS, "in_progress");

  try {
    // STEP 2: compile + lineage inside Restate.run (replay-safe, deterministic clock)
    final Clock clock = () -> Restate.instantNow();
    final SemanticTable model = Restate.run(
        "compile-model",
        () -> YamlLoader.loadModel(request.yaml(), spark));

    final ModelLineage lineage = Restate.run(
        "compute-lineage",
        () -> Lineage.of(model));
    final String lineageJson = Lineage.toJson(lineage.workspaceOf(Map.of(model.name(), model)));

    // STEP 3: write to Postgres idempotently inside Restate.run
    final int nextVersion = state.get(CURRENT_VERSION).orElse(0) + 1;
    final String manifestHash = computeManifestHash(request.yaml());

    Restate.run(
        "register-model",
        () -> store.registerIfAbsent(
            request.modelName(), nextVersion, request.yaml(),
            manifestHash, clock.apply(), lineageJson));

    // STEP 4: journal bookkeeping
    state.set(CURRENT_VERSION, nextVersion);
    state.set(MANIFEST_HASH, manifestHash);
    state.set(LAST_INVALIDATED_AT, clock.apply());
    state.set(REGISTRATION_STATUS, "idle");

    // STEP 5: invalidate downstream cache — NON-journaled (cache is observable state, not coord)
    cache.invalidateByModelAndVersion(request.modelName(), nextVersion);

  } catch (Exception e) {
    state.set(REGISTRATION_STATUS, "failed");
    throw e;
  }
}

@Shared
@Handler
public ModelDetail describeModel(String modelName) {
  return Restate.run("describe-model",
      () -> store.loadByName(modelName, currentVersion(modelName)))
      .withLineage(loadLineage(...))
      ...
}
```

**Stub comment fix**: `ModelService.java:54` references `semanticdf.of(...)` —
this method does not exist. Replace with the actual entry points:
`YamlLoader.loadModel(yaml, spark)` and `Lineage.of(model)`. Trivial
documentation fix in the same PR.

#### B.4. Rewrite `CatalogService` (read-only)

`CatalogService.listModels(ListModelsRequest)` and `describeModel(...)` both
read from `ModelStore`. No `Restate.run` (no replay — Postgres reads are
deterministic for a given point-in-time snapshot; Postgres' MVCC handles
concurrent writers).

#### B.5. Cache hook plumbing

`ResultCache` lives in the library; the platform's cache is constructed once
in `PlatformApplication.main` (alongside the existing
`YamlModelRegistry.load(...)`). The cache is **library-owned** — the
platform just calls `invalidateByModelAndVersion(name, version)` from
`ModelService.bumpVersion`.

A default no-op `ResultCache` (`InMemoryResultCache` or a no-op impl in the
library — TBD per cache.md) suffices for the PR. Real Caffeine L1 is a
follow-up.

#### B.6. Tests

| Test | What |
|---|---|
| `register_idempotentOnSameManifestHash` | Register model twice with same hash → single row, version stays at 1 |
| `register_bumpsVersionOnDifferentHash` | Register, change YAML, register again → version=2 in journal + Postgres |
| `register_persistsLineageJson` | Register model → `model_lineage.lineage_json` populated and round-trippable via `Lineage.fromJson` |
| `register_doesNotUseSystemCurrentTimeMillis` | Structural assertion (mirror) |
| `register_triggerCacheInvalidation` | Mock `ResultCache.invalidateByModelAndVersion` → verify called with (name, version) after success |
| `register_doesNotInvalidateOnFailure` | Make `store.registerIfAbsent` throw → cache.invalidateByModelAndVersion NOT called |
| `register_recoversFromJvmCrash` | Restate SDK TestKit: crash mid-`Restate.run`, replay → model exists in Postgres, version correct |
| `listModels_returnsAllRegisteredModels` | Register 3 models → `CatalogService.listModels` returns 3 with correct versions |
| `describeModel_returnsLineageWithColumns` | Register model with calc measure → `describeModel(lineage)` contains `pct_of_total` with `dependsOn: [revenue, count]` |

#### B.7. Flag-gated rollout

```
SEMANTICDF_MODELS_PERSIST=true|false   (default: false)
```

- `false` (default): `ModelStore` is `NoOpModelStore`; `register()` only updates
  the journal. Behavior matches today (broken but safe — `CatalogService`
  returns empty).
- `true`: `PostgresModelStore`. Models durable across restart. Required for
  PR-A's `queryRecent` to show `model_definition_registered` audit events.

`YamlModelRegistry.load(modelsDir, spark)` at startup continues to work
independently — the env var governs the **runtime registration path**, not
the **startup-time YAML load**.

### Files
- NEW `model/ModelStore.java`, `model/PostgresModelStore.java`, `model/NoOpModelStore.java`
- NEW `model/PostgresModelStoreTest.java` (~5 tests, Testcontainers)
- NEW `model/ModelServiceTest.java` (~6 tests, Restate TestKit)
- MOD `model/ModelService.java` (register rewritten; fix `semanticdf.of` comment)
- MOD `catalog/CatalogService.java` (read against `ModelStore`)
- MOD `PlatformApplication.java` (wire `ModelStore` + `ResultCache`)
- NEW `catalog/CatalogServiceTest.java` (~3 tests, against `NoOpModelStore`)

### Risk classification

**Auto-safe**: structural & opt-in. Same as PR-A.

**Needs-confirmation** *(one — collaboration with the user)*: the choice of
`ResultCache` no-op default in the platform vs. instantiating the library
`InMemoryResultCache`. The cache is hot-path; conservative default is
no-op; operators wire it explicitly via DI config.

---

## PR-C: `feat(platform): wire QueryService with cache-first ResultCache`

**Goal**: `QueryService.runQuery` looks up the model in the registry,
serves from the cache when possible, compiles + executes via
`SemanticTable.toDataFrame` on miss.

### Tasks

#### C.1. `cache/QueryPlanCache` interface (small) — wraps library `ResultCache`

The platform's `QueryService` receives an `Option<ResultCache>` (or null)
from its constructor. Misses fall through to compilation. The library's
`ResultCache` trait is the implementation; the platform doesn't add a
layer.

Actually — **the platform doesn't need its own `QueryPlanCache` interface**.
It can depend directly on `io.semanticdf.cache.ResultCache` — that's
already a library trait. Constructor injects
`ResultCache` (with a `NoOpResultCache` fallback when DI is null).

#### C.2. Rewrite `QueryService.runQuery`

```java
@Service
public class QueryService {

  private final ModelRegistry models;
  private final ResultCache cache;        // library trait
  private final Clock clock;              // () -> Instant (Restate.instantNow())

  // 5-arg constructor for tests, 4-arg etc. mirrors StreamingService pattern

  @Handler
  public QueryResult runQuery(QueryRequest req) {
    final SemanticTable model = models.get(req.modelName());  // deterministic
    final io.semanticdf.audit.QueryRequest libReq = toLibReq(req, model);
    final Option<String> cacheKey = CacheKey.forRequest(libReq);

    // STEP 1: cache lookup (deterministic, outside Restate.run)
    if (cacheKey.isDefined()) {
      Option<...> cached = cache.get(cacheKey.get());
      if (cached.isDefined()) {
        return toQueryResult(cached.get());  // hot path: <1ms p50
      }
    }

    // STEP 2: compile + execute inside Restate.run (Spark execution is non-det, replay-safe here)
    final Dataset<Row> df = Restate.run("query-execute",
        () -> model.query(libReq).toDataFrame(spark));
    // ... decode rows via ResultDecoder
    final List<List<Object>> rows = decodeRows(df);
    final long rowCount = df.count();

    // STEP 3: cache populate (L2 of Caffeine L1 outside Restate)
    if (cacheKey.isDefined()) {
      cache.put(cacheKey.get(), ...);
    }

    // STEP 4: audit (best-effort, non-blocking)
    try {
      auditSink.emit(new AuditEvent(
          req.tenant(), "query.executed", clock.apply(),
          AuditEvent.dedupHashOf(...), payload));
    } catch (Exception e) { /* sink is non-throwing by contract */ }

    return new QueryResult(req.modelName(), req.measures(), rows, false, rowCount);
  }
}
```

#### C.3. Wire shape — keep `QueryResult` or evolve?

The current `QueryResult(String model, List<String> measures, List<List<Object>> rows,
boolean truncated, long rowCount)` is *untyped* (the senior-architect review
flagged this). The library's `ResultDecoder[T]` exists for typed decoding.

**Decision (locked)**: keep the existing wire DTO shape (`List<List<Object>>`).
Adding `ResultDecoder` requires a generic `T` type parameter that doesn't
serialize cleanly across the Restate wire. v0.2.3 can revisit with Arrow
Flight / columnar types. The PR doesn't change the wire.

#### C.4. Tests

| Test | What |
|---|---|
| `runQuery_cacheHit` | Pre-populate `ResultCache` with a known key → `runQuery` returns cached result without compile |
| `runQuery_cacheMiss_compilesAndExecutes` | Empty cache → `runQuery` compiles, executes, populates cache, returns result |
| `runQuery_invalidatesOnModelBump` | Wire cache + a `ModelService.bumpVersion` invalidation hook → `runQuery` after bump goes through full compile (cache returned `None`) |
| `runQuery_auditEventEmitted` | After successful query → `auditSink` snapshot contains one `query.executed` event with dedupHash computed from library's `AuditEvent.dedupHashOf` |
| `runQuery_unknownModel` | `models.get("nonexistent")` throws `ModelNotFoundException` → surfaces as Restate `TerminalException(BAD_REQUEST_CODE)` (Restate halts retries, clients see 400) |

#### C.5. Flag-gated rollout — extends library's existing pattern

The library's `ResultCache` is independently opt-in via library code; the
platform's query path inherits this. No new env var: if `SEMANTICDF_CACHE`
is set in the future, it instantiates a different cache. PR-C ships with
**default `InMemoryResultCache`** (library trait, no platform config needed).

### Files
- MOD `query/QueryService.java` (rewritten runQuery)
- NEW `query/QueryServiceTest.java` (~5 tests against the streaming test fixtures — same model YAML used by `StreamingServiceIntegrationTest`)
- MOD `PlatformApplication.java` (wire `ResultCache`)

### Risk classification

**Auto-safe**: stateless service, one handler. No state-placement change.
The wire shape is unchanged.

**Needs-confirmation** *(one)*: whether to use `ResultDecoder` typed-decoding
in `runQuery`'s row projection (would change the `QueryResult.rows` shape).
**Locked as**: keep `List<List<Object>>` for v0.2.2; typed decoding is
v0.2.3+ Arrow Flight work.

---

## Cross-cutting concerns

### What NOT to touch (per senior-architect review)

These boundaries are already correct and the new PRs preserve them:

- The `5-constructor seam pattern` in `StreamingService.java:163-209`.
- The state-placement rule (journal = coordination, Postgres = record).
- `Restate.instantNow()` over `System.currentTimeMillis()` everywhere.
- `Restate.run(...)` discipline wrapping every side-effecting library call.
- `ON CONFLICT DO NOTHING` semantics.
- `dedupHash` excludes `ts`/`elapsedMs` — `AuditEvent.dedupHashOf`
  (library) is the contract; `StreamingDedupHash` (platform) is a separate
  concern for streaming events and stays.

### Blast radius summary

Per the `clean-architecture-refactor` skill's mantra #6:

- **PR-A scope**: streaming-only. Reachable surface today: `StreamingService.run`
  → `RestateAuditSink.emit` → `AuditService.append`. Blast is bounded.
- **PR-B scope**: registration API. No external consumer today (zero
  callers per `socraticode_codebase_impact` depth 3). Goes live when MCP
  adds `register_model` tool.
- **PR-C scope**: query API. No external consumer today. Goes live when
  MCP adds `query_model` tool.

### Risk order (per `clean-architecture-refactor` skill §1)

1. PR-A is **least risky** — small surface, opt-in, library primitives
   reused, deterministic clock already wired in `StreamingService`.
2. PR-B is **medium** — caches-as-witness is the design gate.
3. PR-C is **least new-risk, but highest blast-when-live** — no callers
   today, but every external agent query hits it when the REST surface
   ships.

### Migration / rollback

All three PRs are flag-gated. Rollback path:
- Set `SEMANTICDF_AUDIT_PERSIST=false` → reverts PR-A's write path
- Set `SEMANTICDF_MODELS_PERSIST=false` → reverts PR-B's write path
- (PR-C has no env; rollback is revert-the-PR — single file)

Migrations of the Postgres tables are pure additive (`IF NOT EXISTS`).
Existing deployments with the `streaming_streams` table (from (see version history))
upgrade cleanly.

---

## Approvals and ordering

This plan MUST be approved by the user before any of the three PRs start.
Each PR then gets its own focused PR description, complete with
test-by-test breakdown.

The single highest-priority follow-up PR is **PR-A** (AuditService) — it
removes a real bug (audit events currently vanish), reuses the existing
Postgres pattern from (see version history), and has the smallest blast radius of the
three. After PR-A lands, PR-B follows. PR-C lands last.

---

## Out of scope (deliberately deferred to v0.2.3+)

- **Caffeine L1 cache hot path** (per `platform-architecture.md:121-130`):
  the platform's REST layer caches in-process to bypass Restate on
  cache-hit. Needs the REST layer to exist. v2.
- **Arrow Flight result streaming** (per `platform-architecture.md:80`):
  typed `ResultDecoder` integration. Requires wire shape change to
  `QueryService.runQuery` response. v2.
- **Multi-tenant catalog partitioning**: namespace → physical schema
  mapping. v3 (per `platform-architecture.md:296` "Tenants are logical,
  not process-level").
- **Async/queue-based audit sink** (per `audit-log.md:166`): not on the
  v0.2.2 critical path.
- **`platform-architecture.md` says streaming is P2 (not v1)**:
  *streaming is already shipped in v0.2.1 ((see version history)+)* — this design doc is stale and should be updated as part of PR-B's doc-update.

---

**Open question for the user before PR-A starts:**

1. **CatalogService in PR-B or separate?** Design decision: bundle with
   `ModelService` (cannot test without) vs. separate PR (focused PRs).
   *Recommendation*: bundle.
2. **`ResultCache` default in PR-C.** `InMemoryResultCache` (library
   no-op-friendly) vs. wire a future Caffeine L1. *Recommendation*:
   `InMemoryResultCache` (library) for v0.2.2; Caffeine deferred.
3. **Test profile for Testcontainers.** New `*IT` profile (mvn-failsafe)
   vs. existing `*Test` suffix (maven-surefire default). Existing
   `PostgresStreamCatalogTest` uses `*Test` — match it for consistency.
   *Recommendation*: match; no profile change.

Once these three are confirmed, the plan is frozen and PR-A can start.
