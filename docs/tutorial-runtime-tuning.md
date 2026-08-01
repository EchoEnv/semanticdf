# Tutorial — runtime tuning: the six knobs that change query behavior

This walk-through covers the runtime knobs that tune how a compiled
DataFrame behaves at execution time. You set them with fluent
`.withXxx(...)` calls on a `SemanticTable`, and they travel with the
model through the join / manifest round-trip lifecycle.

By the end you should be able to:

- Choose the right knob for a given problem (cache vs persist vs
  broadcast vs skew handling).
- Know which knobs compose with which (and which silently override).
- Read the Scaladoc on any single knob without going in circles.

## Why these knobs exist

A `SemanticTable` is a *definition*, not a query. When you call
`.toDataFrame(spark)`, it compiles to a Spark `DataFrame` and any
number of follow-up actions (`.collect()`, `.show()`, `.write...`)
may run on that DataFrame. The knobs below control what happens
*between compile and action*, in three layers:

1. **Safety** — bound the work a single query can do (e.g. row cap).
2. **Performance** — skip work that's already been done (cache) or
   avoid work that would be slow without help (broadcast, skew).
3. **Observability** — record what ran, when, and how (audit).

Some knobs apply at *compile time* (broadcast, skew), some at *action
time* (materialize), some at *terminal time* (cache, maxRows). The
table below is your map.

| Knob | Phase | Sets `SparkSession` conf? | Returns / side-effect |
|---|---|---|---|
| `withMaxRows(n)` | action | no | caps rows collected |
| `withResultCache(c)` | terminal | no | rows cached by SHA-256 of request |
| `withAuditSink(s)` | terminal | no | emits `AuditEvent` per query |
| `withBroadcastJoinThreshold(b)` | compile | no | `broadcast(right)` if size < b |
| `withMaterialize(l)` | compile | no | `df.persist(l)` on the compiled DF |
| `withSalt(n)` | compile | **yes** | enables AQE skew handling |

Only `withSalt` mutates session-global config. Everything else is
local to the table instance and the joins it participates in.

## When to use each — the decision tree

```
Is the query expensive to repeat?  ──── yes ──▶ withResultCache
                              no
                              │
Is the user going to do multiple actions on the same DF?  ── yes ──▶ withMaterialize
                                                         no
                                                         │
Is the result count bounded?  ──── no ────▶ withMaxRows
                              yes
                              │
Is the data warehouse going to query it many times?  ──── yes ──▶ audit (withAuditSink)
                                                          no
                                                          │
Is the join's right side small?  ──── yes ──▶ withBroadcastJoinThreshold
                              no
                              │
Are any join keys heavily skewed?  ──── yes ──▶ withSalt
                                  no
```

If two knobs conflict (e.g. `withMaxRows(0)` (no cap) and
`withResultCache(...)`), the result is the union — the cap is
disabled, but caching still happens.

## The six knobs, one section each

### 1. `withMaxRows(n)` — driver-memory safety cap

Caps the number of rows any single query can return. The cap is
applied *after* the query compiles and *before* Spark collects
results, so it bounds driver memory even when the underlying source
has millions of rows.

```scala
import io.semanticdf._

val m = toSemanticTable(ordersDf, name = Some("orders"))
  .withDimensions(Dimension("region", _ => ordersDf("region")))
  .withMeasures(Measure("total", _ => sum(ordersDf("amount"))))
  .withMaxRows(10_000)   // never return more than 10K rows per query
  .withResultCache(ResultCache.inMemory())  // cap fires on cache miss

m.query(measures = Seq("total")).execute(spark).count()
// Long = 10000 (or fewer if the underlying data has fewer rows)
```

**Sentinel**: `n = 0` disables the cap entirely (escape hatch for
audits / dumps that must see every row). The setter stores `0`
literally; the compile path checks `if (maxRows > 0)` and skips
`df.limit(maxRows)` when `0` (so the cap doesn't fire). This is
asymmetric to `withBroadcastJoinThreshold(0)` and `withSalt(0)`,
which DO convert `0` to `None` at the setter — the cap uses
compile-path branching because the no-cap semantic is meaningful
even after the model is constructed (a model built with
`maxRows = 0` is "no cap, ever" by intent).

**Default**: 100,000 (see `CacheKey.DefaultMaxRows`).

**Where the cap fires**: the cache-miss and audit-only branches of
`toDataFrameInternal`. The fast path (no audit, no cache) skips
the cap because the user already has a tight `DataFrame` to work
with — the cap is a safety net for the cache/audit paths where
the library builds a `parallelize`-based DataFrame from cached rows
or audit-driven compilation. To make the cap demonstrably fire on
a query, include `withResultCache` (so the path is cache-miss on
first call) or `withAuditSink`.

**Use case**: production models exposed to an LLM agent or BI tool,
where an accidentally-unbounded query would OOM the driver.

### 2. `withResultCache(c)` — identical queries return cached rows

Caches query *results* (rows + schema) by SHA-256 of the request
shape (model + version + measures + dimensions + `where` + `having`
+ `maxRows`). On a hit, no Spark job runs — the cached rows are
rebuilt into a `DataFrame` directly.

```scala
import io.semanticdf.cache.ResultCache

val cache = ResultCache.inMemory(maxEntries = 256)
val m = toSemanticTable(ordersDf)
  .withDimensions(Dimension("region", _ => ordersDf("region")))
  .withMeasures(Measure("total", _ => sum(ordersDf("amount"))))
  .withResultCache(cache)

// First query: miss → runs Spark job, stores rows.
m.query(measures = Seq("total")).execute(spark).count()

// Second identical query: hit → no Spark job, returns cached rows.
m.query(measures = Seq("total")).execute(spark).count()
```

**What's cached**: `Array[Row]` + `StructType`. **What's the key**:
the audit request shape (so two queries that differ only in `where`
are different cache entries).

**Use case**: agent loops that re-query the same shape every
reasoning turn; BI dashboards that re-render the same widget.

**Anti-pattern**: caching results that change every call (e.g. a
`timeRange` filter that always uses "now"). Cache hits are based
on the request *shape* including `where`, so the keys differ — but
you'll waste cache slots on stale results that no one queries again.

### 3. `withAuditSink(s)` — log every query execution

Emits an `AuditEvent` per query (model, request shape, elapsed
millis, row count, status, executed Spark plan). Use the in-memory
sink for tests, a JSONL stdout sink for local debugging, or wire
your own (Postgres, Kafka, anything).

```scala
import io.semanticdf.audit.AuditSink

val sink = AuditSink.inMemory(maxEvents = 1024)
val m = toSemanticTable(ordersDf)
  .withDimensions(Dimension("region", _ => ordersDf("region")))
  .withMeasures(Measure("total", _ => sum(ordersDf("amount"))))
  .withAuditSink(sink)

m.query(measures = Seq("total")).execute(spark).show()

sink.snapshot().foreach(println)
// AuditEvent(model=orders, version=1, measures=List(total),
//            dimensions=List(region), rowCount=4, elapsedMs=87,
//            status=ok, executedPlan=..., dedupHash=...)
```

**Use case**: observability — "who ran what, when, with what shape,
in how many milliseconds, and what Spark plan did it compile to?"

**De-duplication**: each `AuditEvent` carries a `dedupHash` (SHA-256
of the *query shape*, not the result). Repeated identical queries
produce events with the same `dedupHash` — useful for grouping in
log search.

**Audit/cache branch behavior**: when the audit/cache branch fires
(i.e. a `parallelize` rebuild from cached rows on cache hit), no
Spark plan runs — the audit event still fires, but `rowCount`
comes from the cached row count and `executedPlan` is `None`. The
`maxRows` cap does not apply on a cache hit: the cap was already
applied when the producer cached those rows, and cache hits
replay those rows verbatim without re-capping. The cap does
apply on a cache miss (first time the query runs) and on the
audit-only path (cache disabled but audit enabled).

### 4. `withBroadcastJoinThreshold(b)` — auto-broadcast small dimensions

Hints Spark to `broadcast(right)` when the right side of an equi-join
is smaller than `b` bytes. The hint overrides Spark's cost-based
`autoBroadcastJoinThreshold` for this specific query.

```scala
val orders = toSemanticTable(ordersDf, name = Some("orders"))
  .withDimensions(Dimension("customer_id", _ => ordersDf("customer_id")))
  .withMeasures(Measure("total", _ => sum(ordersDf("amount"))))

val customers = toSemanticTable(customersDf, name = Some("customers"))
  .withDimensions(Dimension("id", _ => customersDf("id")))
  .withMeasures(Measure("count", _ => count(lit(1))))
  .withBroadcastJoinThreshold(10L * 1024 * 1024)  // 10 MB

// The 10 MB hint wins over Spark's default auto-broadcast (also 10 MB,
// but the user's intent is explicit). For an equi-join on customer_id,
// the right side gets broadcast.
val joined = orders.join_one(customers, (l, r) => l("customer_id") === r("id"))
```

**Sentinel**: `b = 0` disables the hint (escape hatch). The setter
converts `0` to `None`.

**Precedence**: LEFT-wins, RIGHT-fallback. If both sides set the
threshold, the LEFT's value is used (the user on the LEFT is the
"primary" — they're typically the fact table; the user on the RIGHT
is the dimension table).

**Where it fires**: `compileEquiJoin` only. Cross joins
(`join_cross`) and the streaming foreachBatch path do not honor this
threshold — broadcasting a dimension table in a streaming join needs
a different code path (not yet supported).

### 5. `withMaterialize(l)` — persist the compiled DataFrame

Calls `df.persist(level)` on the compiled DataFrame before returning
it from the fast path of `toDataFrame`. Subsequent actions on the
same DataFrame reuse the persisted storage instead of re-executing
the Spark plan.

```scala
import org.apache.spark.storage.StorageLevel

val m = toSemanticTable(ordersDf)
  .withDimensions(Dimension("region", _ => ordersDf("region")))
  .withMeasures(Measure("total", _ => sum(ordersDf("amount"))))
  .withMaterialize(StorageLevel.MEMORY_ONLY)

val df = m.toDataFrame(spark)
df.count()       // first action: triggers Spark job, persists to memory
df.count()       // second action: served from in-memory storage
df.unpersist()   // user releases the storage
```

**Lifecycle**: the library does NOT retain a `DataFrame` reference
on the table. Unpersist is the caller's responsibility: call
`df.unpersist()` on the `DataFrame` you got back from `toDataFrame`.
The library deliberately does not expose an `unpersist()` method on
`SemanticTable` to avoid a multi-thread race on the volatile-ref
pattern and a cluster-memory leak across N `toDataFrame()` calls.

**Where it fires**: the **fast path only** (no audit, no cache). On
the audit/cache path, the user gets a `parallelize`-based DataFrame
built from cached rows — the compiled DataFrame is not visible to
the caller, so persisting it would leak cluster storage.

**Use case**: agent loops that do `.count()`, `.show()`, `.collect()`
on the same `DataFrame` across multiple reasoning turns; notebooks
that iterate on a stable intermediate result.

**Trade-off**: `MEMORY_ONLY` on a 10M-row query can OOM the cluster.
The library can't paper over that — storage level choice is the
operator's responsibility.

### 6. `withSalt(n)` — skew-handling hint (via Spark AQE)

When set, the next `toDataFrame` (or `toStreamingQuery`) call configures
Spark's Adaptive Query Execution to handle skewed joins: sets
`spark.sql.adaptive.enabled = true` (parent AQE), the skew-join
child config, and the `skewedPartitionFactor` threshold.

```scala
val orders = toSemanticTable(ordersDf, name = Some("orders"))
  .withDimensions(Dimension("customer_id", _ => ordersDf("customer_id")))
  .withMeasures(Measure("total", _ => sum(ordersDf("amount"))))
  .withSalt(5)   // a partition is skewed if size > 5 × median

val customers = toSemanticTable(customersDf)
  .withDimensions(Dimension("id", _ => customersDf("id")))
  .withMeasures(Measure("count", _ => count(lit(1))))

orders.join_one(customers, (l, r) => l("customer_id") === r("id"))
  .toDataFrame(spark)
  .show()
```

**What AQE actually does**: divides each skewed shuffle partition
into smaller sub-partitions AND replicates the matching partition on
the other side of the join — they run as parallel tasks. It is NOT
auto-broadcast semantics (a common misconception). For a full
explanation, see [`DESIGN.md`](../DESIGN.md) § 5.3 "Joins" and the
`withSalt` Scaladoc on `SemanticTable`.

**Why not a custom salt column**: the obvious "add `(rand() * n)` to
both sides, join on `concat(key, "|", salt)`" approach produces
**wrong results** in shuffled joins because the LEFT and RIGHT sides
run on different executors with different RNG sequences, so the
salt values do not match across sides for the same key. Spark AQE
handles skew correctly at the shuffle stage by splitting each
skewed partition into smaller sub-partitions and replicating the
matching partition on the other side (see
`OptimizeSkewedJoin.scala` in Spark) — without a custom salt
column.

**Sentinel**: `n = 0` disables the hint (escape hatch).

**Streaming limitation**: Spark's `ResolveWriteToStream` rule
disables AQE for streaming DataFrames automatically. `withSalt` on a
streaming model cannot enable skew handling at the streaming query
level — the AQE config is applied, but Spark's rule overrides
`adaptive.enabled = false` during streaming plan construction. The
hint is therefore only effective for batch joins.

**Use case**: star-schema joins where one customer_id accounts for
millions of rows; queries against social graphs where one node has
10× the average degree.

## A real-world scenario: customer analytics dashboard

Suppose you're building a customer analytics dashboard. The data
analyst queries the same shapes repeatedly (each widget refresh).
Some widgets show top customers (small result, expensive
aggregation), some show daily totals (small result, cheap
aggregation), some show per-region rollups (mid-size result).

```scala
import io.semanticdf._
import io.semanticdf.cache.ResultCache
import io.semanticdf.audit.AuditSink
import org.apache.spark.storage.StorageLevel

val cache   = ResultCache.inMemory(maxEntries = 512)
val audit   = AuditSink.inMemory(maxEvents = 8192)
val storage = StorageLevel.MEMORY_AND_DISK_SER

// Per-widget models share the cache + audit + persist knobs.
val customers = toSemanticTable(customersDf, name = Some("customers"))
  .withDimensions(
    Dimension("id",       _ => customersDf("id")),
    Dimension("region",   _ => customersDf("region")),
  )
  .withMeasures(
    Measure("ltv",       _ => sum(customersDf("lifetime_value"))),
    Measure("count",     _ => count(lit(1))),
  )
  .withMaxRows(10_000)
  .withResultCache(cache)
  .withAuditSink(audit)
  .withMaterialize(storage)
  .withSalt(5)

val orders = toSemanticTable(ordersDf, name = Some("orders"))
  .withDimensions(
    Dimension("customer_id", _ => ordersDf("customer_id")),
    Dimension("ordered_at",  _ => ordersDf("ordered_at")),
  )
  .withMeasures(
    Measure("amount", _ => sum(ordersDf("amount"))),
    Measure("count",  _ => count(lit(1))),
  )
  .withResultCache(cache)
  .withAuditSink(audit)
  .withMaterialize(storage)
  .withSalt(5)
  .withBroadcastJoinThreshold(50L * 1024 * 1024)

// Widget 1: "top 10 customers by LTV"
customers
  .query(measures = Seq("ltv"), dimensions = Seq("id", "region"))
  .orderBy("ltv desc")
  .limit(10)
  .execute(spark)
  .show()

// Widget 2: "orders per region, last 30 days"
orders
  .query(measures = Seq("amount", "count"))
  .execute(spark)
  .show()

// Widget 3: "LTV + orders per customer"
orders.join_one(customers, (l, r) => l("customer_id") === r("id"))
  .query(measures = Seq("customers.ltv", "orders.amount"))
  .execute(spark)
  .show()
```

What's happening:

- Every query is **cached by shape** (widget 1 cached separately
  from widget 2). Refreshing the dashboard re-runs Spark only on
  cache miss.
- Every query is **audited** — `audit.snapshot()` tells you which widgets
  are slow, which users are heavy queriers, what the actual Spark
  plans look like.
- The compiled `DataFrame` is **persisted in memory + disk serialized**
  — first action is expensive, subsequent actions are cheap.
- The join in widget 3 gets **skew handling** via AQE — heavy customer
  rows don't cause straggler tasks.
- The customers dimension (small) gets **broadcast** to each executor
  for the join in widget 3 — no shuffle on the customer side.
- Each query result is **capped at 10K rows** so a misbehaving
  widget doesn't OOM the driver.

## Knob composition — what works with what

| | `maxRows` | `resultCache` | `auditSink` | `broadcast` | `materialize` | `salt` |
|---|---|---|---|---|---|---|
| `maxRows` | — | ✓ | ✓ | ✓ | ✓ | ✓ |
| `resultCache` | ✓ | — | ✓ | ✓ | ✓ (cache hit returns parallelize DF, no persist) | ✓ |
| `auditSink` | ✓ | ✓ | — | ✓ | ✓ | ✓ |
| `broadcast` | ✓ | ✓ | ✓ | — | ✓ | ✓ (combined: broadcast + skew) |
| `materialize` | ✓ | ✓ (audit/cache path skips persist) | ✓ | ✓ | — | ✓ |
| `salt` | ✓ | ✓ | ✓ | ✓ | ✓ | — |

A few non-obvious interactions:

- **`materialize` + `resultCache`**: on a cache hit, the
  `parallelize`-based DataFrame is returned — the persisted DF is
  not visible. The user gets the cached rows directly.
- **`materialize` + `auditSink`**: on the audit/cache path, the
  audit event still fires (with rowCount = cached rows), but the
  persisted DataFrame is irrelevant to the user.
- **`broadcast` + `salt`**: both are join optimizations; they
  compose. AQE's `skewJoin` can broadcast skewed partitions, and
  the user's `withBroadcastJoinThreshold` broadcasts based on size.
- **`salt` + cross join**: cross joins don't honor `salt` (no key
  to skew on). The setter doesn't throw — it's a silent no-op.

## Anti-patterns

- **Setting `withMaterialize(MEMORY_ONLY)` on a 10M-row table**.
  Pick `MEMORY_AND_DISK` or `MEMORY_AND_DISK_SER` if the cluster
  is tight on RAM.
- **Setting `withResultCache` but never querying the same shape
  twice**. Wastes memory.
- **Setting `withMaxRows(0)` (disable) in production**. The cap
  exists for a reason; disabling it in production is an OOM waiting
  to happen. Keep the cap.
- **Setting `withBroadcastJoinThreshold(10L * 1024 * 1024 * 1024)`**
  (1 GB). Broadcasting a 1 GB table to every executor burns memory
  for marginal speedup. Pick a threshold you know your right side
  stays under.
- **Setting `withSalt(1)`**. Spark's default is 5; values below the
  default add no skew sensitivity.

## Where to go next

- **Scaladoc**: every setter has full Scaladoc explaining the
  semantics, the sentinel conventions, and the propagation rules.
  Hover over the setter in IntelliJ, or read `SemanticTable.scala`
  in the source.
- **Examples**:
  - [`examples/runtime-tuning/`](../examples/runtime-tuning/) —
    a multi-knob scenario similar to the dashboard above. Run it,
    read the README, modify the knobs, see what changes.
  - [`examples/skewed-join/`](../examples/skewed-join/) — focused
    skew-handling walkthrough (1M events, 90/10 split, same total
    with and without `withSalt`).
- **Tests**: `MaterializeSpec`, `BroadcastJoinThresholdSpec`,
  `SaltSpec` each cover one knob end-to-end. Read the spec to see
  the exact behavior the library guarantees.
- **Manifest round-trip**: the runtime knobs (`maxRows`,
  `broadcastJoinThreshold`, `materializeLevel`, `salt`) all
  round-trip through the manifest wire format. See
  [`docs/manifests-and-joins.md`](manifests-and-joins.md) for the
  wire shape.
