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
2. **Q2 — Anomaly detection (z-score)** — for each order, whether its `amount` is more than 2 standard deviations from the mean.

## What it demonstrates

| Concept | Where it shows up |
|---|---|
| Per-row time-delta measure (`datediff`) | Q1 — `ship_days` (base) |
| Per-row conditional measure (`case when`) | Q1 — `on_time_flag` (base) |
| Calc measure = ratio of two base measures | Q1 — `avg_ship_days`, `on_time_rate` |
| Per-row aggregate measure (`avg`, `stddev`) as group-invariant | Q2 — `mean_amount`, `stddev_amount` |
| Per-row classification using two group-invariant measures | Q2 — `is_outlier` (calc, auto-pulls deps) |
| `abs(col - col) > k * col` z-score pattern | Q2 |

## How Q2's anomaly detection works

The model is extended in Scala with two **base measures** (`mean_amount`, `stddev_amount`) and one **calc measure** (`is_outlier`):

```scala
Measure("mean_amount",   t => avg(t("amount")))            // base: group-invariant
Measure("stddev_amount", t => stddev(t("amount")))         // base: group-invariant
Measure("is_outlier",
  t => when(abs(t("amount") - t("mean_amount")) > lit(2.0) * t("stddev_amount"),
            lit(1)).otherwise(lit(0)))                       // calc: depends on both
```

When you request `is_outlier` in a query, the framework's transitive-closure walk sees the `t("mean_amount")` and `t("stddev_amount")` references and **auto-pulls them into the aggregation** — you don't have to list them manually. The `is_outlier` measure then evaluates per-row against the group-mean and group-stddev.

This pattern generalises to any "flag rows that deviate from the group by more than N standard deviations" use case (fraud detection, ops alerting, data-quality checks).

## Related templates

- [`examples/starter`](../starter/README.md) — 5 basic queries.
- [`examples/pipeline`](../pipeline/README.md) — full BI lifecycle.
- [`examples/window-analytics`](../window-analytics/README.md) — top-N, MoM, running total.
- [`examples/customer-analytics`](../customer-analytics/README.md) — RFM + cohort activity.
