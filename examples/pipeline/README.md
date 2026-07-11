# semantica pipeline template

The ETL + semantic-layer workflow — from raw CSV to parquet to declarative queries.

This template shows the **full BI lifecycle**:

```
raw/         ← bronze layer: messy CSV from upstream systems
  ↓
ETL pipeline: clean, dedupe, enrich, validate
  ↓
output/      ← silver layer: clean parquet tables
  ↓
semantic models (YAML)   ← gold layer: declarative metric definitions
  ↓
queries                   ← the BI / dashboard / API surface
```

It's the data-engineering companion to the [starter template](../starter/README.md) — that one started at the gold layer; this one shows how to *get there* from raw data.

---

## What you get

```
semantica-pipeline/
├── README.md                 ← you are here
├── pom.xml
├── raw/                      ← BRONZE: messy source data
│   ├── orders_raw.csv        ← 33 rows with nulls, dupes, bad data
│   └── customers_raw.csv     ← 12 rows with dupes + case issues
├── output/                   ← SILVER: created by pipeline (gitignored in prod)
│   ├── orders/               ← parquet, partitioned by order_year
│   └── customers/            ← parquet
├── models/                   ← GOLD: semantic models on the parquet
│   ├── orders.yml            ← orders + customer_name (joined)
│   └── customers.yml
└── src/main/scala/com/example/pipeline/
    └── Main.scala            ← the 7-step pipeline
```

---

## Run it (5 minutes)

### Prerequisites
- JDK 17, Maven 3.9+, Spark 3.5.x or 4.x

### Step 1: install semantica locally
```bash
cd /path/to/semantica
mvn install -DskipTests
```

### Step 2: run the pipeline
```bash
cd examples/pipeline
mvn scala:run -DmainClass=com.example.pipeline.Main
```

You'll see the 7 steps run in sequence:
1. INGEST — 33 raw orders, 12 raw customers
2. CLEAN — drops nulls, dedupes, casts types → ~30 clean rows
3. ENRICH — adds `total_amount`, `order_year`
4. VALIDATE — drops qty=0, invalid statuses
5. WRITE — saves parquet partitioned by year
6. CATALOG — registers as temp views
7. SEMANTIC — runs YAML-defined queries against the parquet

---

## What the pipeline does

| Step | What | Why |
|---|---|---|
| **INGEST** | Read CSV with `PERMISSIVE` mode | Bad rows become nulls instead of failing the whole job |
| **CLEAN** | Drop nulls, dedupe, cast types, trim/lowercase strings | Bronze is dirty. Silver must be trustworthy. |
| **ENRICH** | `total_amount = qty * price`, `order_year = year(date)` | Business columns for analytics |
| **VALIDATE** | Drop `qty=0`, drop invalid `status`, drop non-positive totals | Enforce business rules. Better to fail a row than produce wrong numbers. |
| **WRITE** | Save as parquet, partitioned by year | Silver layer. Partitioning lets downstream queries prune. |
| **CATALOG** | Re-read parquet, register temp views | The bridge: parquet on disk → queryable table |
| **SEMANTIC** | Load YAML models, run queries | Gold layer. Declarative metrics on top of silver. |

---

## Customizing it

### Change the data
Drop your own CSV into `raw/orders_raw.csv` with the same column names. The pipeline handles common messiness automatically.

### Change the cleaning rules
Edit `Main.scala` → `cleanOrdersDf`, `cleanCustomersDf`, `validateOrdersDf`. Each is a pure function: `(DataFrame) => DataFrame`. Easy to swap, easy to test.

### Change the output format
Replace `parquet(ordersPath)` with `.saveAsTable("orders")` for a Hive/Delta managed table, or `.format("delta").save(...)` for Delta.

### Change the semantic layer
Edit `models/orders.yml` or `models/customers.yml`. The YAML is the contract between data engineers and analysts — engineers change the pipeline, analysts change the models, neither blocks the other.

### Schedule it
Wrap `Main.main()` in an Airflow DAG, a cron job, or a Databricks workflow. The pipeline is idempotent (parquet is overwritten each run with `SaveMode.Overwrite`).

---

## Querying the semantic models

The YAML models use `total_amount`, `qty`, `order_date` etc. — the **silver layer** columns, not raw `price_usd` or `order_date` strings. This is the value of the layered architecture: analysts query clean, enriched, validated data.

```scala
// From Main.scala:
orders
  .groupBy("customer_name", "country")
  .aggregate("total_revenue", "order_count", "avg_order_value")
  .toDataFrame(spark)
  .orderBy(col("total_revenue").desc)
  .show()
```

→

```
+---------------+-------+-------------+-----------+----------------+
|customer_name  |country|total_revenue|order_count|avg_order_value |
+---------------+-------+-------------+-----------+----------------+
|Alice Smith    |USA    |424.97       |3          |141.66          |
|Bob Jones      |USA    |92.0         |3          |30.66           |
|...
```

---

## What this teaches

- **Bronze/silver/gold layering** — each layer has a clear contract and audience
- **Idempotent ETL** — re-running produces the same output (parquet overwrite)
- **Schema validation** — drop nulls, dedupe, cast types — the basics of any pipeline
- **Partitioning** — `partitionBy("order_year")` for query pruning
- **Parquet as the trusted layer** — columnar, typed, compressed, Spark-native
- **Semantic layer on top** — analysts get clean YAML, not raw parquet columns

---

## Going to production

For a real deployment:

1. **Replace local parquet with Delta Lake or Apache Iceberg** — gives you ACID, time travel, schema evolution
2. **Use a real catalog** — Unity Catalog, Hive Metastore, AWS Glue — instead of temp views
3. **Version your raw data** — keep bronze immutable; rebuild silver when logic changes
4. **Add data quality checks** — Great Expectations, Soda, or a simple custom validator
5. **Monitor the pipeline** — track row counts, null rates, schema drift
6. **Parameterize paths** — pass input/output paths as Spark job args, not hardcoded

---

## Related

- [starter template](../starter/README.md) — for the semantic-layer-only path (no ETL)
- [README](../../README.md) — main semantica documentation
- [DESIGN](../../DESIGN.md) — architecture and design decisions
- [known-limitations](../../docs/known-limitations.md) — what semantica does NOT support yet
