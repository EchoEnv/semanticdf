# semantica customer-analytics

Customer segmentation analytics on top of semantica — **RFM** (Recency, Frequency, Monetary) and **cohort activity**.

This template shows two patterns that are staples in CRM and marketing analytics. It complements the [starter template](../starter/README.md) (basics) and the [pipeline template](../pipeline/README.md) (full lifecycle).

## What you get

```
semantica-customer-analytics/
├── README.md                   ← you are here
├── pom.xml
├── data/
│   ├── customers.csv           ← 15 customers with signup dates
│   └── orders.csv              ← 38 orders across those customers
├── models/
│   ├── customers.yml           ← customer master
│   └── orders.yml              ← orders + join to customers
└── src/main/scala/com/example/customeranalytics/
    └── Main.scala               ← 2 queries
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
cd examples/customer-analytics
mvn scala:run -DmainClass=com.example.customeranalytics.Main
```

You'll see:
1. **Q1 — RFM per customer** — for each customer, the recency (days since last order), order count, total spend, and a simple segment classification (High Value / Active / At Risk / Lapsed).
2. **Q2 — Customer activity by signup-month cohort** — for each signup-month cohort, the total orders and total spend.

## What it demonstrates

| Concept | Where it shows up |
|---|---|
| Joined model (`orders ⨝ customers` via YAML `joins:`) | Both queries |
| Calc measures on a joined model (per-customer aggregates) | Q1 — `recency_days`, `segment` |
| `when(...) / otherwise(...)` classification | Q1 — `segment` calc |
| `atTimeGrain(dim, "month")` on a joined-model dimension | Q2 — `customers.signup_date` |
| `datediff(literal, max(col))` calc | Q1 — `recency_days` |

## RFM segment thresholds

The example uses simple thresholds:

| Segment | Rule |
|---|---|
| **High Value** | `recency <= 60` AND `orders >= 3` AND `spend >= 200` |
| **Active** | `recency <= 30` |
| **At Risk** | `recency <= 90` |
| **Lapsed** | everything else |

Production RFM models are usually percentile-based or use logistic regression on the R/F/M values. This template shows the structure; the thresholds are placeholder.

## Known limitation: the recency cutoff is hardcoded

Q1 uses `datediff('2024-04-01', max(order_date))` so the example is deterministic. Production code would use `current_date` or a parameter — change the literal in the `recency_days` measure to switch.

## Related templates

- [`examples/starter`](../starter/README.md) — 5 basic queries.
- [`examples/pipeline`](../pipeline/README.md) — full BI lifecycle.
- [`examples/window-analytics`](../window-analytics/README.md) — top-N, MoM, running total.
- [`examples/operations-analytics`](../operations-analytics/README.md) — fulfillment + anomaly detection.
