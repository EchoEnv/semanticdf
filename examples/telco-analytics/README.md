# semanticdf telco-analytics

Telco analytics on top of semanticdf — **mobile services, charges, packages, promotions**. The canonical use case: a mobile carrier running analytics over customer usage, plan tiers, and active promotional offers.

This template complements the other consumer templates ([starter](../starter/README.md), [pipeline](../pipeline/README.md), [window-analytics](../window-analytics/README.md), [customer-analytics](../customer-analytics/README.md), [operations-analytics](../operations-analytics/README.md)) by showing the telco-specific patterns.

## What you get

```
semanticdf-telco-analytics/
├── README.md                   ← you are here
├── pom.xml
├── data/
│   ├── plans.csv               ← 4 plans (Basic, Standard, Premium, Family)
│   ├── promotions.csv          ← 3 active promotions
│   └── usage.csv               ← 40 usage events across 9 customers, 2 months
├── models/
│   ├── plans.yml               ← plans reference model
│   ├── promotions.yml          ← promotions reference model
│   └── usage.yml               ← denormalized usage fact table (the main model)
└── src/main/scala/com/example/telcoanalytics/
    └── Main.scala                ← 3 queries
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
cd examples/telco-analytics
mvn scala:run -DmainClass=com.example.telcoanalytics.Main
```

You'll see:
1. **Q1 — Monthly ARPU per plan** (Average Revenue Per User, the canonical telco KPI)
2. **Q2 — Promotion effectiveness** (revenue + customer count per promo, with % of total)
3. **Q3 — Roaming revenue contribution** (% of total revenue from roaming)

## What it demonstrates

| Concept | Where it shows up |
|---|---|
| Denormalized fact table (single-model analytics) | All queries — avoids 3-way joins |
| `atTimeGrain(dim, "month")` on a time dimension | Q1 — monthly ARPU |
| Calc measure = ratio of two base measures (`total_revenue / active_customers`) | Q1 — `arpu` |
| `countDistinct` for distinct customer counting | Q1 — `active_customers` (Scala-side) |
| `t.all(measure)` for percent-of-total | Q2 — `pct_of_revenue`, Q3 — `pct_roaming` |
| Per-row classification via `case-when` | Q3 — `total_roaming_revenue` (YAML base measure) |
| `safeDivide(num, denom, defaultValue)` for div-by-zero guards | Q1 — `arpu` |
| Multiple `withMeasures` calls (additive) | Q2 — both `pct_of_revenue` and `customers_on_promo` |

## Why a denormalized model

The ideal data model for this is a normalized star schema (usage ⨝ customers ⨝ plans).
A multi-hop join chain is possible in semanticdf but isn't a production-grade
guarantee yet — see [`docs/known-limitations.md`](../../docs/known-limitations.md)
for the current state. We work around this here by denormalizing: the
`usage` CSV includes `plan_name` and `monthly_fee` as extra columns, so a
single `usage` model carries enough context for all 3 queries.

For a production warehouse, you can chain joins in semanticdf; the
query patterns shown here work the same way on a normalized schema.

## Related templates

- [`examples/starter`](../starter/README.md) — 7 basic queries (group-by, pct-of-total, joins, time-grain, filter, top-N window, MoM window)
- [`examples/pipeline`](../pipeline/README.md) — full BI lifecycle from raw CSV to declarative queries
- [`examples/window-analytics`](../window-analytics/README.md) — top-N, MoM, running total
- [`examples/customer-analytics`](../customer-analytics/README.md) — RFM + cohort activity
- [`examples/operations-analytics`](../operations-analytics/README.md) — fulfillment + anomaly detection
