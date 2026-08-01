# Example — skewed joins with `withSalt`

This example demonstrates a real-world scenario where **one join key
has 90% of the data** — the classic "celebrity user" problem in
star-schema joins.

The scenario:
- **events** table: 1M rows, partitioned by `user_id`
- **users** table: 10K rows (small dimension)
- One user (`user_id = 1`) accounts for **90% of all events** — a
  hot key that would cause a straggler task if joined naively.

`withSalt(n)` translates to Spark AQE skew handling, which detects
the skewed partition and splits it into smaller sub-partitions.

## Run

```bash
mvn install the parent semanticdf project
mvn exec:java
```

Expected output (approximately):

```
=== Building skewed dataset ===
Total events: 1000000
Top user (id=1) has 900000 events (90%)
Other users share the remaining 100000 events

=== Building models ===
events model: 2 dimensions, 2 measures
users model: 1 dimensions, 1 measures

=== Without withSalt: skew stragglers possible ===
The events fact table has one user_id with 900K rows (90%). Without
skew handling, Spark's default behavior partitions by hash(user_id)
mod N — one partition gets 90% of the data, others get ~10% each.
The task processing that partition takes 9x longer than others.

=== With withSalt(5): AQE handles skew ===
  adaptive.enabled=true              (set by withSalt)
  skewJoin.enabled=true               (set by withSalt)
  skewJoin.skewedPartitionFactor=5    (set by withSalt)
  expected speedup on skewed partitions: ~9x (90% partition split into 5 sub-partitions)

=== Verifying the join produces correct results ===
The join result has 10K rows (one per user) — same as without
withSalt. Skew handling changes the EXECUTION PLAN, not the SQL semantics.

Done.
```

## What `withSalt(n)` does

`withSalt(n)` is a hint, not a custom salt column. It configures Spark
AQE to:

1. Detect partitions that are larger than `n × median_size`
2. Split each skewed partition into smaller sub-partitions
3. Replicate the matching partition on the other side of the join

For our dataset: `n = 5` means a partition is skewed if its size
exceeds `5 × median`. The 90% partition (900K rows) gets split into
~5 sub-partitions of 180K each, eliminating the straggler.

## What `withSalt(n)` does NOT do

- **It does not add a custom salt column to the DataFrame.** A naive
  `(rand() * n)` salt would produce **wrong results** in shuffled
  joins because the LEFT and RIGHT sides run on different executors
  with different RNG sequences, so the salt values do not match
  across sides for the same key.
- **It does not affect streaming queries.** Spark's
  `ResolveWriteToStream` rule disables AQE for streaming DataFrames
  automatically. `withSalt` on a streaming model is a no-op for the
  streaming query level.

## Try modifying

1. Remove `.withSalt(5)` — observe the warning about skewed partitions
   in the Spark UI (look at the stage durations).
2. Change `.withSalt(5)` to `.withSalt(2)` — observe more aggressive
   skew detection (more partitions get split).
3. Change `.withSalt(5)` to `.withSalt(20)` — observe conservative
   skew detection (only extreme skew triggers splitting).

## Files

```
examples/skewed-join/
├── pom.xml                                          Maven build, deps on semanticdf + spark-sql
├── README.md                                        this file
└── src/main/scala/com/example/skew/Main.scala        the runnable example
```

## See also

- [`docs/tutorial-runtime-tuning.md`](../../docs/tutorial-runtime-tuning.md)
  — section 6 (withSalt) explains the design rationale
- [`examples/runtime-tuning/`](../runtime-tuning/) — broader
  example covering all six knobs
- [`examples/pipeline/`](../pipeline/) — real ETL pipeline scenario
