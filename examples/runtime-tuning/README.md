# Example — runtime tuning (all six knobs)

This example walks through the **six runtime knobs** that tune how a
compiled `SemanticTable` behaves at execution time:

| Knob | What it does |
|---|---|
| `withMaxRows(n)` | Caps rows returned by any single query |
| `withResultCache(c)` | Caches query results by request shape |
| `withAuditSink(s)` | Emits `AuditEvent` per query |
| `withBroadcastJoinThreshold(b)` | `broadcast(right)` when right side < b bytes |
| `withMaterialize(l)` | `df.persist(level)` on the compiled DataFrame |
| `withSalt(n)` | Enables Spark AQE skew handling |

The scenario: a **customer analytics dashboard**. Three widgets query
the same shape repeatedly; some are expensive aggregations, some are
joins. Each widget benefits from a different combination of knobs.

## Run

```bash
mvn install the parent semanticdf project (so the local jar is available)
mvn exec:java
```

Expected output (approximately):

```
=== Building sample data ===
Wrote 1000 customers to ./output/customers.csv
Wrote 50000 orders to ./output/orders.csv

=== Building customer analytics models ===
  customers model: 3 dimensions, 2 measures
  orders model: 3 dimensions, 2 measures

=== Querying with all knobs enabled ===
Widget 1: top customers by LTV
+--------------+---------+--------+
|region        |ltv      |count   |
+--------------+---------+--------+
|East          |485023.50|167     |
|West          |462100.00|158     |
|Central       |445800.25|165     |
+--------------+---------+--------+

Widget 2: orders by region
...

=== Audit events emitted ===
  3 events in the in-memory sink
  shape=region,region rows=4 elapsedMs=87 plan=...

=== Verifying cache hits ===
  Widget 1 second call (cache hit): 0ms
  Widget 2 first call (cache miss): 87ms
  Widget 2 second call (cache hit): 0ms
```

## What to change

Try modifying `Main.scala` to:

1. Remove `.withMaxRows(10_000)` — observe that all rows are returned.
2. Remove `.withResultCache(cache)` — observe that every query re-runs Spark.
3. Increase `withMaterialize(StorageLevel.MEMORY_ONLY)` — observe higher
   memory usage with no real benefit (already cheap query).
4. Set `.withSalt(1)` — observe that the AQE skew detection becomes
   more conservative (a partition is skewed only if size > 1 × median).

Then re-run and compare.

## Files

```
examples/runtime-tuning/
├── pom.xml                                          Maven build, deps on semanticdf + spark-sql
├── README.md                                        this file
├── src/main/scala/com/example/runtime/Main.scala     the runnable example
└── src/main/resources/sample-data/                  generated at runtime by Main.scala
    ├── customers.csv                                1K rows
    └── orders.csv                                   50K rows
```

## See also

- [`docs/tutorial-runtime-tuning.md`](../../docs/tutorial-runtime-tuning.md)
  — the walk-through doc that this example implements
- [`examples/pipeline/`](../pipeline/) — a real ETL pipeline scenario
- [`examples/customer-analytics/`](../customer-analytics/) — simpler
  customer model without the runtime knobs
