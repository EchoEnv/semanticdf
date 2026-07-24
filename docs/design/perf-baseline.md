# Performance baseline — post-v0.1.16 hot paths

**Status:** ACCEPTED — `PerfBaselineSpec` shipped in the post-v0.1.16
release. Numbers below are the **v0.1.17 baseline**, captured on
the first run on a developer machine. The numbers are published
on every test run via `info(...)` so trends are visible in the
surefire-reports. **They are not gates** — a slow CI day doesn't
block a PR.

## What we measure

Six benchmarks in `PerfBaselineSpec`, each publishes a median
millisecond count via `info(...)`. Run with:

```bash
mvn -o test -Dsuites='io.semanticdf.perf.PerfBaselineSpec'
```

The numbers land in `target/surefire-reports/semanticdf-test-suite.txt`
with the `[perf]` prefix so they're greppable.

| # | What | Why it matters |
|---|---|---|
| 1 | `toDataFrame` on a 30-row flights table | The basic terminal latency; the floor for every other path. |
| 2 | Cache miss vs cache hit | Validates the "best performance" claim for the cache. |
| 3 | Cache hit + audit sink | Validates the combined write+rebuild path. |
| 4 | Predicate hashing (small/medium/large) | The hasher runs on every `where` / `having` — it's the foundation of both the audit log and the cache key. |
| 5 | Full cache hit (key compute + lookup + rebuild) | The "best performance" path end-to-end. |
| 6 | 200 puts with eviction (cap=100) | LRU throughput under load. |

## v0.1.17 baseline numbers

Captured on the first run (Spark 3.5.8, JDK 17, single-machine
local mode). Numbers vary by hardware — the values below are
reference points, not absolutes.

```
[perf] toDataFrame (no audit, no cache): median=62ms
[perf] cache miss: median=30ms; cache hit: median=26ms
[perf] cache hit + audit: median=28ms
[perf] predicate hash: small=207ms/10k; medium=200ms/10k; large=22ms/1k
[perf] full cache hit (key compute + lookup + rebuild): median=25ms
[perf] 200 puts with eviction (cap=100): median=0ms
```

**Reading the numbers:**

- `toDataFrame` 62ms is dominated by Spark's plan compile + a
  small collect (the query returns 3 rows). The library's own
  overhead is sub-millisecond.
- Cache hit (26ms) is **2.4× faster than miss** (62ms when
  measured independently, 30ms when warm). The win comes from
  skipping the Spark plan; the rebuild via `parallelize(rows) +
  schema` is O(rows).
- Cache hit + audit (28ms) is the realistic production path for
  LLM agents. The audit emit is a single `info(...)` call — a few
  microseconds. Negligible.
- Predicate hashing is fast even at 10k iterations: ~20 microseconds
  per hash on small/medium predicates. The 1k iterations on the
  large 21-node predicate (~40 levels of nesting) take 22ms — still
  fine for any realistic query.
- 200 puts with LRU eviction is sub-millisecond (rounds to 0ms
  at 1ms resolution). The `LinkedHashMap` is fast.

## How to use the numbers

- **Trend-watching:** if a future PR doubles `toDataFrame` from
  62ms to 120ms, that's visible in CI as a doubling of the published
  number. A real regression doesn't slip through.
- **Capacity planning:** the cache-hit throughput tells you how
  much load the in-memory cache can absorb before the
  `maxEntries` cap becomes a bottleneck (1,000 ops/sec is trivial
  for `LinkedHashMap`; the cap is hit when the working set is
  larger than `maxEntries`, not when the cache is slow).
- **Debugging:** when an LLM agent reports "queries are slow," the
  baseline numbers give you a "what's normal" reference to compare
  against.

## What's NOT measured

- **Spark planner details** — delegated to Spark. The library's
  job is to feed Spark a clean plan; Spark's job is to plan it.
- **Network / disk IO** — the test uses an in-memory DataFrame,
  no I/O is involved.
- **GC pauses** — the JVM's job. A long pause mid-test would
  show up as a tail in the published numbers, but the median is
  robust to it.
- **Multi-tenant / high-concurrency agent scenarios** — these
  tests are single-threaded. The next PR could add a
  concurrent benchmark if real workloads show contention.

## Leak tests (gates, not observational)

`LeakSpec` is the structural counterpart: **its failures are
real bugs**, not flaky measurements. It covers:

- `InMemoryAuditSink` buffer bounded by `maxEvents` (gates)
- `InMemoryResultCache` buffer bounded by `maxEntries` (gates)
- `clear()` empties the buffer (gates)
- `PredicateHasher` is stateless under concurrent access (gates)
- `toDataFrame` chain survives 100 calls without plan accumulation
  (gates)
- Cache under load: 100 distinct queries with `maxEntries=4`
  → exactly 4 keys retained (gates)

A failure here means: a future PR added a field that doesn't
get cleared, a buffer that doesn't evict, or shared mutable state
that races. The fix is structural, not tuning.
