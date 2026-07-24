# Result cache

**Status:** ACCEPTED — `io.semanticdf.cache.*` shipped in the post-v0.1.16
release.

## Problem

LLM-agent loops re-ask the same question while reasoning, and several
agents in the same session hit the same semantic table. Without a
result cache, every query is a fresh Spark job — the second identical
query costs as much as the first.

The audit log (post-v0.1.16) already gives us a stable
`whereHash` / `havingHash` for the filter part. The cache uses the
same hash machinery — and the same request-shape capture — to
build its key.

## Design

```
SemanticTable.query(...)  →  toDataFrame(spark)
                            │
                            ├── cache.get(key)  ──hit──▶  rebuild DataFrame
                            │                          from cached rows
                            │                          (no Spark plan)
                            │
                            └── miss──▶  compile + collect
                                        │
                                        └── cache.put(key, rows+schema)
```

### `CacheKey` (object)

Stable SHA-256 of the canonicalised request shape:

```
m=<model>|me=<sorted measures>|dim=<sorted dimensions>|
w=<whereHash>|h=<havingHash>
```

Two equivalent queries produce the same key. The hash machinery
(`PredicateHasher`) is the same one the audit log uses, so an
audit-log entry and a cache key are always consistent.

### `ResultCache` (trait)

```scala
trait ResultCache {
  def get(key: String): Option[CachedResult]
  def put(key: String, value: CachedResult): Unit
}
```

Default implementations:

- `ResultCache.NoOp` — drops every put, returns `None` on every get.
- `ResultCache.inMemory(maxEntries = 256)` — bounded LRU cache.
  `LinkedHashMap` with `accessOrder = true`; on overflow the
  least-recently-accessed entry is evicted. Thread-safe via
  `synchronized` on the internal map.

### `CachedResult` (case class)

```scala
case class CachedResult(
    rows:   Array[Row],
    schema: StructType,
)
```

The materialised form of a query result. A `DataFrame` is a lazy
operator tree, not a value — to put one in a cache we have to
materialise it. The materialised form (rows + schema) is the smallest
representation that can rebuild a `DataFrame` on a cache hit.

### Wiring

The cache hook is opt-in via a fluent setter:

```scala
val t = toSemanticTable(df, name = Some("flights"))
  .withDimensions(...)
  .withMeasures(...)
  .withResultCache(ResultCache.inMemory(maxEntries = 256))  // <-- opt-in

t.query(measures = ..., dimensions = ..., where = ...)
  .toDataFrame(spark)  // <-- cache check, then execute-or-rebuild
```

The cache survives the fluent chain (`where`, `having`, `orderBy`,
`limit`, `groupBy`, `aggregate`, `join_*`) because the chainable
methods preserve `resultCache` the same way they preserve
`auditSink` and `auditRequest`.

The default is `None` (no cache), so existing call sites are
zero-overhead: a single `if (auditSink.isEmpty && resultCache.isEmpty)`
guard short-circuits both paths.

### Cost

| | Cache miss | Cache hit |
|---|---|---|
| Spark plan compile | yes | **no** |
| Spark job (collect) | yes | **no** |
| Memory cost | O(rows) for the row array | O(rows) at hit time (rebuild) |
| Cache put | one `put` call | — |

The row array is the dominant memory cost. For a 100-row query
with 10 columns it's a few KB; for a 1M-row query it can be
hundreds of MB. The cache is bounded by `maxEntries`; pick it
according to the workload.

## What's NOT in v1

- **TTL.** A cached result is "the answer to query X, computed
  once." No expiry. If the source data changes, the cache becomes
  stale. v2 would add a TTL or a publisher/subscriber invalidation
  hook.
- **Cross-process / cross-restart durability.** The cache is
  in-memory only. A restart wipes it. A file-backed or Redis-backed
  sink is a v2 conversation.
- **Per-row invalidation.** The cache key is the request, not the
  source rows. A row-level change doesn't invalidate the cache.
  v2 would integrate with the data-source's commit log.
- **`orderBy` / `limit` / `timeGrain` / `timeRange` in the key.**
  The current key only covers `model + measures + dimensions + where + having`.
  A query with `limit=10` and a query with no limit have the same
  cache key. v2 would add these.

## Tests

19 tests in `ResultCacheSpec`:

- `CacheKey` (8 tests): same model/measures/dimensions, different
  model/measures, order-insensitivity, where hash, where with
  equivalent shape, None model.
- `InMemoryResultCache` (4 tests): empty get, put/get, LRU
  eviction, clear, NoOp.
- End-to-end (7 tests): cache miss executes + stores, cache hit
  returns same rows, different filter = different key, same
  filter = single key, no-cache regression, cache + audit sink
  both fire.

## Interaction with the audit log

The two features share the same `auditRequest` capture. When both
are configured:

- The audit event fires for every `toDataFrame` call, with the
  request shape.
- On a cache hit, the audit event's `rowCount` is the cached row
  count (not 0 as on a miss). `elapsedMs` is small (no compile,
  no collect).
- On a miss, the audit event's `rowCount` is 0 (we don't collect
  to know). `elapsedMs` is the compile + collect time.

So a `mcp audit_log` consumer can see "this query was a cache
hit" by `elapsedMs < 1ms` and `rowCount > 0`.
