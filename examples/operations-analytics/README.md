# semanticdf operations-analytics

Operations analytics on top of semanticdf — **order fulfillment time**, **on-time rate**, **anomaly detection** (z-score).

This template shows two patterns that ops and supply-chain teams ask constantly. It complements the [starter template](../starter/README.md) (basics) and the [pipeline template](../pipeline/README.md) (full lifecycle).

## What you get

```
semanticdf-operations-analytics/
├── README.md                   ← you are here
├── pom.xml
├── data/
│   └── orders.csv               ← 35 orders with order_date + shipped_at
├── models/
│   └── orders.yml               ← per-row ship_days + on-time flag; calc rates
└── src/main/scala/com/example/operationsanalytics/
    └── Main.scala                ← 2 queries
```

## Run it (5 minutes)

### Prerequisites
- JDK 17, Maven 3.9+, Spark 3.5.x or 4.x

### Step 1: install semanticdf locally
```bash
cd /path/to/semanticdf
mvn install -DskipTests
```

### Step 2: run
```bash
cd examples/operations-analytics
mvn scala:run -DmainClass=com.example.operationsanalytics.Main
```

You'll see:
1. **Q1 — Order fulfillment (avg ship_days + on-time rate)** — average days from order to shipment, and the fraction of orders shipped within 2 days.
2. **Q2 — Anomaly detection (z-score, 2-step)** — orders whose `amount` is more than 2 standard deviations from the mean.

## What it demonstrates

| Concept | Where it shows up |
|---|---|
| Per-row time-delta measure (`datediff`) | Q1 — `ship_days` (base) |
| Per-row conditional measure (`case when`) | Q1 — `on_time_flag` (base) |
| Calc measure = ratio of two base measures | Q1 — `avg_ship_days`, `on_time_rate` |
| Per-row aggregate measure (`avg`, `var_samp`) as group-invariant | Q2 — `mean_amount`, `var_amount` (Scala-side) |
| Infix typed predicate `ref > value` (or `===`, `=!=`, `>=`, `<=`, `isin`, ...) | Q2 — `amount > threshold` |
| Two-step pipeline: compute global stats → filter rows | Q2 |

## How Q2's anomaly detection works (2-step pattern)

Anomaly detection in semanticdf is a **two-step pipeline** — per-row z-score
doesn't fit in a single base-measure lambda, so we compute the global stats
first, then filter rows whose `amount` exceeds the threshold.

```scala
// Step 1: global stats — compute the dataset-wide mean and sample variance.
val stats = orders
  .withMeasures(
    Measure(meanAmount.name, t => avg(t("amount"))),
    Measure(varAmount.name,  t => var_samp(t("amount"))),
  )
  .groupBy()
  .aggregateMeasures(meanAmount, varAmount)
  .execute(spark)
  .collect().head
val mean    = stats.getAs[Double]("mean_amount")
val stddev  = math.sqrt(stats.getAs[Double]("var_amount"))
val threshold = mean + 2 * stddev

// Step 2: filter orders using the computed threshold. The infix form
// `amount > threshold` is the typed-predicate DSL (import PredicateOps._)
// — same compile-time check as the verbose `Predicate.Gt(amount, threshold)`,
// shorter syntax.
orders
  .where(amount > threshold)
  .groupByDimensions(orderId)
  .aggregateMeasures(orderAmount)
  .orderBy(SortKey.desc(orderAmount))
  .execute(spark)
  .show(10, false)
```

The two-step pattern generalises to any "flag rows that deviate from
the group by more than N standard deviations" use case (fraud detection,
ops alerting, data-quality checks).

## Related templates

- [`examples/starter`](../starter/README.md) — 5 basic queries.
- [`examples/pipeline`](../pipeline/README.md) — full BI lifecycle.
- [`examples/window-analytics`](../window-analytics/README.md) — top-N, MoM, running total.
- [`examples/customer-analytics`](../customer-analytics/README.md) — RFM + cohort activity.
