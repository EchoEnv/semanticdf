# semantica window-analytics

Window-function analytics on top of semantica — **top-N per group**, **period-over-period**, **running total**.

This template shows three common window-function patterns that are typical in BI dashboards. It complements the [starter template](../starter/README.md) (which covers the basics) and the [pipeline template](../pipeline/README.md) (which shows the full ETL + semantic lifecycle).

## What you get

```
semantica-window-analytics/
├── README.md                   ← you are here
├── pom.xml
├── data/
│   └── flights.csv             ← 32 flights over 3 months (Jan–Mar 2024)
├── models/
│   └── flights.yml             ← base measures; window measures added in Scala
└── src/main/scala/com/example/windowanalytics/
    └── Main.scala               ← 3 queries
```

## Run it (5 minutes)

### Prerequisites
- JDK 17, Maven 3.9+, Spark 3.5.x or 4.x

### Step 1: install semantica locally
```bash
cd /path/to/semantica
mvn install -DskipTests
```

### Step 2: run
```bash
cd examples/window-analytics
mvn scala:run -DmainClass=com.example.windowanalytics.Main
```

You'll see:
1. **Q1 — Top-5 origins per carrier by total passengers** — `row_number() over (partition by carrier order by total_passengers desc)` filtered to `rank <= 5`.
2. **Q2 — Monthly passengers with MoM % change** — `lag(total_passengers, 1) over (...)` plus a `safeDivide` calc for the percent change.
3. **Q3 — Running total of passengers over time** — `sum(...) over (order by flight_date rows between unbounded preceding and current row)`.

## What it demonstrates

| Concept | Where it shows up |
|---|---|
| `row_number() over (partitionBy ... orderBy ...)` | Q1 — top-N per group |
| `lag(col, 1) over (orderBy ...)` | Q2 — period-over-period |
| `sum(col) over (orderBy ... rows between ...)` | Q3 — running total |
| `safeDivide(num, denom, defaultValue)` | Q2 — guards div-by-zero in the pct-change calc |
| `where(Predicate.Compare(...))` after window | Q1 — filter on the rank measure |
| Window measure classified as **calc** (not base) | All — because they reference other measures via `t(...)`, the framework detects the dep and runs them in Pass 2 |

## Why window measures are added in Scala, not in YAML

The YAML `calculated_measures:` parser is intentionally minimal — arithmetic, parens, and `all(name)` only. Window syntax (`OVER (PARTITION BY ... ORDER BY ...)`) isn't supported there. The standard pattern is:

1. Define base + simple calc measures in YAML (the 90% case).
2. Add window measures programmatically in Scala via `withMeasures(Measure("rank", t => ...))`.

The framework's `ClassificationScope` records which measure names the lambda touches via `t(...)`. When the window references another measure (e.g. `t("total_passengers")` inside an `orderBy`), the framework correctly classifies the window as a **calc** measure and runs it in Pass 2 against the post-aggregation DataFrame. You don't need to tell it the type — it figures it out from the dependency.

## Related templates

- [`examples/starter`](../starter/README.md) — 5 basic queries (group-by, pct-of-total, joins, time-grain, filter).
- [`examples/pipeline`](../pipeline/README.md) — full BI lifecycle from raw CSV to declarative queries.
- [`examples/customer-analytics`](../customer-analytics/README.md) — RFM segmentation + cohort activity.
- [`examples/operations-analytics`](../operations-analytics/README.md) — order fulfillment + anomaly detection.
