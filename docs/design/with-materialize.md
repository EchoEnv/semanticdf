# `withMaterialize(level)` — opt-in DataFrame persistence

**Status:** REVISED DRAFT (post-#313, post-2-subagent review). Awaiting user sign-off.
**Scope:** Library-only, additive, opt-in feature. No breaking changes.
**Field shape (proposed):** `materializeLevel: Option[org.apache.spark.storage.StorageLevel] = None`

## Problem

Spark's `DataFrame.persist(level)` caches the *plan output* in the chosen
storage tier (memory, disk, or both, with optional serialization). Without
explicit persistence, every `toDataFrame()` action re-runs the query plan
from scratch.

Common case this fixes: a user runs the same query multiple times in a
session — e.g., a Jupyter notebook iterating over the result, an agent
loopping on the same shape, or a streaming `foreachBatch` micro-batch
re-emitting the same aggregate. The `withResultCache` flag ((see version history))
catches the *rows-after-collect* case; `withMaterialize` catches the
*DataFrame-before-collect* case. They are complementary, not redundant.

The library should not force persist on every query — most users
don't re-ask the same shape within a single SparkSession, and
uncontrolled persistence can OOM the cluster. Hence opt-in.

## Design (REVISED)

### Two layers, two knobs

```
.withMaxRows(n)       — caps the rows returned by `df.limit(n).collect()` (post-#294)
.withResultCache(c)   — caches the *rows* after `collect()` (post-#295)
.withMaterialize(l)   — caches the *DataFrame* before `collect()` (proposed)
```

- `withMaxRows`     → driver-memory cap (always on by default; the safety net)
- `withResultCache` → cache after the cap (opt-in, off by default)
- `withMaterialize` → cache before the cap (opt-in, off by default)

A user who sets both `withResultCache` and `withMaterialize` gets:
1. Compile the query plan
2. `df.persist(level)` (if `materializeLevel` is set)
3. `df.limit(maxRows).collect()` — serves from the cache
4. `cache.put(key, rows, schema)` (if `resultCache` is set)

### Field shape

```scala
final class SemanticTable private[semanticdf] (
    // ... existing fields ...
    val materializeLevel: Option[org.apache.spark.storage.StorageLevel] = None,
) extends Serializable with ...
```

`Option[StorageLevel]` follows the same shape as `auditSink`,
`resultCache`, `broadcastJoinThreshold` (sentinel `None` = "not set").
`StorageLevel` is Spark's standard enum; operators who know Spark
already know it. The library does not curate a subset — the Spark
enum is the API surface.

### Fluent setter

```scala
def withMaterialize(level: org.apache.spark.storage.StorageLevel): SemanticTable =
  new SemanticTable(
    root, postAggPredicates, version, sourceTable, status,
    auditSink, auditRequest, resultCache, maxRows,
    broadcastJoinThreshold,
    materializeLevel = Some(level),
  )
```

Symmetric with `withBroadcastJoinThreshold`, `withResultCache`, etc.

### Lifecycle (REVISED — no `unpersist()` on the table)

The original design proposed a `@volatile var cached: DataFrame` and an
`unpersist()` method on `SemanticTable`. The 2-subagent review caught
three HIGH-severity issues with that approach:

- **Memory leak across N `toDataFrame()` calls**: each call would create
  a new persisted `DataFrame` without unpersisting the old. The
  volatile ref is overwritten; Spark's `BlockManager` does not
  auto-evict; N calls → N RDDs in cluster memory.
- **Race condition on the volatile ref**: thread A persists `dfA`,
  thread B persists `dfB` and overwrites the volatile, thread A's
  `unpersist()` would unpersist `dfB` (wrong one).
- **`@transient` requirement**: `DataFrame` is NOT serializable, but
  `SemanticTable extends Serializable`. A non-transient `DataFrame`
  field would break cluster-mode closures (e.g., `foreachBatch`).

**REVISED design**: drop the `unpersist()` method and the retained ref
entirely. The user gets a `DataFrame` from `toDataFrame()` and calls
`df.unpersist()` on it themselves. This:
- Removes the memory leak (no retained ref to overwrite)
- Removes the race (no shared mutable state)
- Removes the `@transient` requirement (no `DataFrame` field)
- Removes API surface (`unpersist()` no longer on `SemanticTable`)

The user already holds a `DataFrame` reference after `toDataFrame()`. They
can `df.unpersist()` it. The library doesn't need to track it.

```scala
// User code:
val df = myTable.withMaterialize(MEMORY_ONLY).toDataFrame(spark)
df.count()        // served from cache (or computes + caches)
df.count()        // served from cache
df.unpersist()    // user-managed cleanup
```

### Where to insert `df.persist(level)`

In `SemanticTableCore.toDataFrameInternal`, apply the persist **inside
the fast-path branch** (no audit, no cache). The current gate is:

```scala
private def toDataFrameInternal(spark, clock): DataFrame = {
  if (auditSink.isEmpty && resultCache.isEmpty) {
    // Fast path: no audit, no cache. Apply `materializeLevel` here
    // if set — subsequent actions on the returned DataFrame reuse
    // the persisted storage instead of re-executing the Spark
    // plan. The audit/cache branch (below) does NOT honour
    // `materializeLevel`: it returns a `parallelize`-based
    // DataFrame built from cached rows, which is effectively
    // MEMORY_ONLY for the call's duration. Persisting the compiled
    // DataFrame there would leak cluster storage (the user never
    // sees the compiled DataFrame). See `SemanticTable.materializeLevel`.
    val compiled = root.compile(spark)
    materializeLevel match {
      case Some(level) => compiled.persist(level)
      case None       => compiled
    }
  } else {
    // Audit + cache path (unchanged — see "Why not also persist on the
    // audit/cache branch?" below).
    // ...
  }
}
```

**Why not also persist on the audit/cache branch?** The
audit/cache branch returns a `parallelize(rows.toSeq, schema)`
DataFrame built from the row cache. The user never sees the
compiled DataFrame `fresh`; it's dereferenced after `rebuilt` is
built. If we called `fresh.persist(level)` here, the cluster's
`BlockManager` would register a new RDD ID with no caller able to
unpersist it (no `@volatile var cached: DataFrame` — we
deliberately dropped that pattern per the design review). The
cluster memory would leak across N `toDataFrame()` calls.

The row cache already provides the within-DataFrame optimization
on this path: `parallelize`-based DataFrames are effectively
`MEMORY_ONLY` for the call's duration. Spark serves subsequent
actions from memory without re-executing the source plan. So the
user's "I want my multi-action DataFrame to be fast" goal is
already met on the audit/cache path without `withMaterialize`.

If a future requirement emerges for cross-call persistence on the
audit/cache path (e.g., a 10-call agent loop where each call hits
the row cache miss path and re-compiles), revisit this — but the
N-call storage leak must be solved first (probably via
`BlockManager.unpersistRdd(id, blocking=true)` keyed by a stable
hash of the request, not via a `DataFrame` ref).

### Order: `compile → persist → return` (fast path)

The fast-path branch compiles the DataFrame first, then applies
`persist(level)`, then returns it. The `limit` step from the
original design ("limit → persist → collect") does NOT apply here
for two reasons:

1. **The fast path doesn't `collect()`**. It returns the lazy
   DataFrame to the caller. The caller decides when to apply
   `limit()` or `count()`. The library doesn't apply `limit`
   on the fast path (this matches the pre-#174 code's behavior
   — the cap is a driver-memory safety net for the
   audit/cache path's `collect()`).

2. **The fast-path user gets the full plan output, not a limited
   one**. If the user sets both `withMaterialize(MEMORY_ONLY)`
   and `withMaxRows(100)`, the persisted DataFrame is the FULL
   result; the user is expected to apply `limit(100)` themselves
   (or set the row cap via the row cache on the audit/cache path).
   The persisted DataFrame's storage cost is the full result, not
   the limited version — this is the documented trade-off of the
   fast path.

For the audit/cache path, the existing order is `compile → limit →
collect → cache.put` (see `SemanticTableCore.scala`). The
`materializeLevel` flag does not change this — it does not apply
on the audit/cache path.

### Propagation through the join wrapper

The post-join wrapper (`SemanticTableMutation`) currently propagates
5 fields via helper methods (`joinMaxRows`, `joinAuditSink`, etc.).
Add a 6th:

```scala
private[semanticdf] def joinMaterializeLevel(other: SemanticTable):
    Option[org.apache.spark.storage.StorageLevel] =
  this.materializeLevel.orElse(other.materializeLevel)
```

Precedence: LEFT wins when both set, RIGHT is the fallback. Matches
the broadcastJoinThreshold pattern ((see version history)/#307).

### Propagation through `new SemanticTable(...)` callsites

The 2-subagent review found **89 `new SemanticTable(...)` callsites**
in `src/main` (the doc originally estimated "3 wrapper sites"). Every
callsite that needs the new field to propagate must thread the
positional/named arg. Beyond `joinMaterializeLevel`, the relevant
sites are:

- `SemanticTableCore` passthroughs: `withRowFilter`, `version`,
  `status`, `withAuditSink`, `withResultCache`, `withMaxRows`,
  `withBroadcastJoinThreshold`, `copyAuditRequest`,
  `invalidateAuditRequest`, `where`, `having`, `orderBy`, `limit`,
  `withHint`
- `SemanticTableMutation` passthroughs: 18 `withDimensions`,
  `withMeasures`, `withTransforms` cases
- `SemanticTableStreaming.batchModel` (line 237): already uses named
  args for the fields it sets; `materializeLevel` defaults to `None`
  via the constructor
- `SemanticGroupBy.aggregate` (needs its own `materializeLevel`
  field, parallel to `resultCache`)
- `SemanticManifest.{fromJson, toJoinedJson}` (manifest round-trip)
- `package.scala` (helper constructors)

**The cleanest path**: add a private `@volatile @transient var
cached: DataFrame = null` only IF we keep `unpersist()` on the table.
Since we're dropping `unpersist()`, we don't need the field. The
field-propagation question is then: does each callsite need
`materializeLevel = materializeLevel` added? **Yes**, for any callsite
that creates a new `SemanticTable` derived from `this` (otherwise the
new table defaults to `None` and silently drops the user's setting).

For the simplest implementation: have a single `copy` method on
`SemanticTable` that takes the overrides, and replace all `new
SemanticTable(...)` callsites with `this.copy(...)`. This is a
mechanical refactor (out of scope for the feature PR, but the right
direction).

For the feature PR: just add the new field to all 89 callsites. A
test (`MaterializeSpec`) verifies the field is preserved across
`withDimensions`, `withMeasures`, `withTransforms`, etc.

### Streaming path

`SemanticTableStreaming.batchModel` (line 237) creates a per-micro-batch
`SemanticTable` using **named args** for the fields it sets. Other
fields (including the new `materializeLevel`) default to the
constructor's default (`None`). The streaming path therefore
**automatically** doesn't persist per batch — no code change needed.

The doc's "no-op" claim is correct BY CONSTRUCTION (via default
args). To prevent regression, the test plan adds a test that asserts
`materializeLevel = None` on the batchModel even when the parent has
`materializeLevel = Some(MEMORY_ONLY)`.

### Manifest round-trip

`withMaterialize` is a `StorageLevel` enum, which is serializable in
both directions. Unlike `broadcastJoinThreshold` (which is
documented as lossy in the round-trip), `materializeLevel` should
round-trip losslessly. The implementation plan:
- Add `materializeLevel` to the runtime block in the JSON wire format
- Add it to `SemanticManifest.{toJson, fromJson, fromJoinedJson}`
- Add a round-trip test (`MaterializeManifestRoundtripSpec`)

## Trade-offs (REVISED)

| Choice | Why this | Alternative |
|---|---|---|
| `Option[StorageLevel]` field | Matches the existing pattern; sentinel `None` is the default | A custom ADT — but the existing `resultCache`, `auditSink`, `broadcastJoinThreshold` are all `Option`-typed |
| Expose Spark's `StorageLevel` enum | Operators who know Spark know `StorageLevel` | A curated subset — but the library already exposes Spark's broadcast hint; matching the level of abstraction is consistent |
| **No retained `DataFrame` ref** (REVISED) | Removes the memory leak, the race, the `@transient` requirement, and the API surface | Retain the ref, manage `unpersist()` — but the agent review found 3 HIGH bugs in that approach |
| **No `unpersist()` on `SemanticTable`** (REVISED) | User unpersists via the returned `DataFrame` reference; simpler API | Add `unpersist()` method on the table — but this requires the retained ref |
| `limit → persist → collect` order (REVISED) | Persist the LIMITED result; safety cap is preserved | `persist → limit → collect` — materializes the full DataFrame, defeats `maxRows` |
| LEFT-wins precedence on joins | Matches broadcastJoinThreshold pattern ((see version history)/#307) | Other rule — not applicable, no "tighter" concept |
| Streaming path no-op (REVISED: by default-args) | The default constructor sets `materializeLevel = None`; the batchModel uses named args and doesn't override | Explicit reset — but the default-args approach is correct BY CONSTRUCTION |
| No `BlockManager.unpersistRdd` cleanup | The user is responsible for unpersist; the library doesn't track persist | Active cleanup — but the library doesn't have the RDD ID to unpersist |

## Open questions (REVISED)

1. **Field name**: `materializeLevel` (recommended) vs. `persistLevel`?
2. **Streaming path handling**: confirmed no-op via default-args. Add a regression test (recommended: yes).
3. **Hot-path test**: include in v1 (recommended: yes, listener-based on `BlockManagerMaster`).
4. **`unpersist()` in audit event**: N/A — no `unpersist()` on the table.
5. **Interaction with `executedPlan`**: capture the plan AFTER `persist` is applied (move the capture to after the persist call).
6. **`unpersist()` on never-persisted table**: N/A — no `unpersist()` on the table.

## Implementation plan (REVISED)

Single PR, ~6 files:

1. **Field + setter** (`src/main/scala/io/semanticdf/SemanticTable.scala`,
   `SemanticTableCore.scala`, `SemanticTableMutation.scala`):
   - Add `materializeLevel: Option[StorageLevel] = None` field
   - Add `withMaterialize(level): SemanticTable` setter
   - Move `df.persist(level)` BEFORE the audit/cache fast-path gate
   - Change order to `limit → persist → collect`
   - Update the `executedPlanCapture` to be set AFTER persist (so the plan reflects the persist step)
   - Add `joinMaterializeLevel` helper to the wrapper helpers

2. **Callsite updates** (89 sites in `src/main`):
   - Add `materializeLevel = materializeLevel` to all `new SemanticTable(...)` callsites that derive from `this` (preserves the field through the fluent chain)
   - The streaming `batchModel` doesn't need a change (default-args pattern)

3. **`SemanticGroupBy`** (`src/main/scala/io/semanticdf/SemanticTableCollection.scala`):
   - Add `materializeLevel: Option[StorageLevel] = None` field (parallel to `resultCache`)

4. **Manifest round-trip** (`src/main/scala/io/semanticdf/adapters/SemanticManifest.scala`,
   `schemas/manifest.schema.json`, `src/test/resources/manifest.schema.json`):
   - Add `materializeLevel` to the runtime block
   - Update `toJson`, `fromJson`, `fromJoinedJson`
   - Update `manifest.schema.json` (both the repo copy and the test classpath copy)
   - Add a round-trip test

5. **Tests** (`src/test/scala/io/semanticdf/cache/MaterializeSpec.scala`,
   `src/test/scala/io/semanticdf/audit/MaterializeManifestRoundtripSpec.scala`):
   - Fluent setter propagates through `withDimensions`, `withMeasures`, etc.
   - Join wrapper preserves the field (LEFT-wins, RIGHT-fallback)
   - `compile()` calls `df.persist(level)` when field is set
   - `compile()` does NOT call `df.persist(level)` when field is `None`
   - The fast-path gate honors `materializeLevel` (no audit + no cache + persist)
   - Streaming path: `materializeLevel` defaults to `None` on `batchModel`
   - Manifest round-trip preserves the field
   - Hot-path test: `BlockManagerMaster` shows the persisted RDD

6. **Update field Scaladoc** on `resultCache` (in `SemanticTable.scala`):
   - Mention the persistence vs cache distinction

Estimated diff: 6 files, ~150 lines of production code, ~120 lines of test code.

## Out of scope

- **`df.persist` for streaming `foreachBatch` with override**: would
  require a separate field, different lifecycle. Defer.
- **Auto-tuning of `StorageLevel` based on dataset size**: out of
  scope; the user knows their data.
- **Cross-session persistence (cluster-level cache)**: not Spark's
  default; would need distributed cache config. Out of scope.
- **Plan-level persistence (`.persist` on a `QueryPlan`)**: the
  library operates on `DataFrame`; let Spark handle plan-level.

## What I want from you (per the karpathy + data-driven + debug-mantra discipline)

Before I start coding, please sign off on the **revised design** (especially the 4 things the 2-subagent review caught):

1. **Drop `unpersist()` on `SemanticTable`** — user unpersists via the returned `DataFrame`. Eliminates the memory leak, the race, the `@transient` requirement, and the API surface.
2. **Move `df.persist(level)` BEFORE the audit/cache gate** — so `withMaterialize` works even without audit/cache.
3. **Change order to `limit → persist → collect`** — preserves the `maxRows` safety cap.
4. **89 callsite updates** (not "3 wrapper sites" as the original doc said).

If you sign off, this is a focused PR. If you want a different design (e.g., keep `unpersist()` with the volatile ref), I can revise again. If you want to defer until there's a concrete user need, that's also fine.
