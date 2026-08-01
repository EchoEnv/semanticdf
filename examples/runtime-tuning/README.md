# Example — runtime tuning (all six knobs)

This example walks through the **six runtime knobs** that tune how a
compiled `SemanticTable` behaves at execution time:

| Knob | What it does |
|---|---|
| `withMaxRows(n)` | Caps rows returned by any single query (safety) |
| `withResultCache(c)` | Caches query results by request shape (performance) |
| `withAuditSink(s)` | Emits `AuditEvent` per query (observability) |
| `withBroadcastJoinThreshold(b)` | `broadcast(right)` when right side < b bytes (performance) |
| `withMaterialize(l)` | `df.persist(level)` on the compiled DataFrame (performance) |
| `withSalt(n)` | Enables Spark AQE skew handling (performance) |

The scenario: a **customer analytics dashboard**. Three widgets query
the same shape repeatedly; some are expensive aggregations, some are
joins. Each widget benefits from a different combination of knobs.

## Run

```bash
# From the parent semanticdf project root:
mvn install -pl . -am -DskipTests   # build and install the library jar

# From this example directory:
cd examples/runtime-tuning
mvn exec:java
```

Expected output (the numbers vary by machine; the *structure* is
deterministic):

```
=== Building sample data ===
Wrote 1000 customers
Wrote 50000 orders

=== Building customer analytics models ===
  customers model: 2 dimensions, 2 measures
  orders model: 2 dimensions, 2 measures

=== Widget 1: top customers by LTV, per region ===
+-------+--------+-----+
| region|     ltv|count|
+-------+--------+-----+
|   East|854421.0|  333|
|   West|850279.0|  334|
|Central|848800.0|  333|
+-------+--------+-----+

  elapsed: ~1000ms

=== Widget 2: orders per region ===
+-----------+------------------+-----+
|   category|            amount|count|
+-----------+------------------+-----+
|      books|~3200000         |12500|
|   clothing|~3200000         |12500|
|       food|~3200000         |12500|
|electronics|~3200000         |12500|
+-----------+------------------+-----+

  elapsed: ~650ms

=== Widget 3: LTV + orders per customer (join) ===
+-----------+--------------------+
|        ltv|              amount|
+-----------+--------------------+
|  ~9.8e7   |       ~1.3e7      |
+-----------+--------------------+

  elapsed: ~800ms

=== Audit events emitted ===
  3 events captured:
    [ok] model=customers rows=3 elapsed=~800ms dedupHash=...
    [ok] model=orders rows=4 elapsed=~570ms dedupHash=...
    [ok] model=unknown rows=1 elapsed=~680ms dedupHash=...

=== Verifying cache hits (run widgets 1+2 again) ===
  Widget 1 second call: ~70ms (cache hit)
  Widget 2 second call: ~70ms (cache hit)

=== Salt hint verification ===
  adaptive.enabled=true (set by withSalt)
  skewJoin.enabled=true
  skewJoin.skewedPartitionFactor=5

Done. Try modifying the knobs in this file and re-running.
```

## What to change

Try modifying `Main.scala` to:

1. Remove `.withMaxRows(10_000)` — observe that the cap no longer
   fires (cache misses will return whatever the source has).
2. Remove `.withResultCache(cache)` — observe that every query
   re-runs Spark instead of hitting the cache.
3. Remove `.withAuditSink(sink)` — observe that the audit events
   block disappears; the cache-hit path also no longer applies the
   `maxRows` cap (see the audit/cache branch note in the tutorial
   doc).
4. Change `withBroadcastJoinThreshold(10 MB)` to `withBroadcastJoinThreshold(1 KB)`
   on customers — observe the broadcast hint switching off because
   customers (~100 KB) is below the threshold anyway. With a
   `1 KB` threshold, the auto-broadcast still triggers (the check
   is on the right side of the join, not the model's own size).
5. Change `.withMaterialize(StorageLevel.MEMORY_AND_DISK)` to
   `.withMaterialize(StorageLevel.MEMORY_ONLY)` — observe the
   cluster spilling to disk less often, at the cost of dropping
   cached partitions on executor loss. The user manages cleanup
   via `df.unpersist()` (the library does not retain a reference).
6. Set `.withSalt(1)` — observe that the AQE skew detection
   becomes the most conservative (a partition is split only if
   size > 1 × median).

Then re-run and compare.

## Files

```
examples/runtime-tuning/
├── pom.xml                                          Maven build, deps on semanticdf + spark-sql
├── README.md                                        this file
├── .mvn/jvm.config                                  JDK 17 module-system access flags
└── src/main/scala/com/example/runtime/Main.scala     the runnable example
```

The example does not write any files to disk — all data lives in
memory. To see how to feed CSV files into a model, see
[`examples/pipeline/`](../pipeline/).

## See also

- [`docs/tutorial-runtime-tuning.md`](../../docs/tutorial-runtime-tuning.md)
  — the walk-through doc that this example implements
- [`examples/skewed-join/`](../skewed-join/) — focused
  skew-handling walkthrough (1M events, 90/10 split)
- [`examples/customer-analytics/`](../customer-analytics/) — a
  similar customer model without the runtime knobs (good for
  comparing "before / after" runtime tuning)