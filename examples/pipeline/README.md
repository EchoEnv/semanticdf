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
mvn exec:java -Dexec.mainClass=com.example.pipeline.Main
```

You'll see the 7 steps run in sequence:
1. INGEST — 33 raw orders, 12 raw customers
2. CLEAN — drops nulls, dedupes, casts types → ~30 clean rows
3. ENRICH — adds `total_amount`, `order_year`
4. VALIDATE — drops qty=0, invalid statuses
5. WRITE — saves parquet partitioned by year
6. CATALOG — registers as temp views
7. SEMANTIC — runs YAML-defined queries against the parquet

> **Tip:** generate browsable HTML docs from the YAML models:
> ```bash
> cd /path/to/semantica
> mvn exec:java -Dexec.args="docsgen --path examples/pipeline/models/ --out docs/pipeline.html"
> open docs/pipeline.html   # macOS; for Linux: xdg-open
> ```

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
| **PERSIST** | Write `model.schema(spark)` to parquet | Catalog the gold layer's metadata. Anyone can query "what models exist?" without parsing YAML. |

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
- **Gold has two parts** — the YAML model is the *definition*; the catalog table (`_semantica_catalog/`) is the *queryable metadata*. Both live alongside the data.
- **Gold data vs gold metadata** — see the next section for the distinction
- **Idempotent ETL** — re-running produces the same output (parquet overwrite)
- **Schema validation** — drop nulls, dedupe, cast types — the basics of any pipeline
- **Partitioning** — `partitionBy("order_year")` for query pruning
- **Parquet as the trusted layer** — columnar, typed, compressed, Spark-native
- **Semantic layer on top** — analysts get clean YAML, not raw parquet columns

## Gold data vs gold metadata

This template produces **silver data + gold metadata**, not **gold data**. That distinction matters:

| Layer | What | Example |
|---|---|---|
| **Silver** | Cleaned, validated, normalized fact data | `orders_clean.parquet` (33 rows, deduped, typed) |
| **Gold metadata** | Catalog of what models exist | `_semantica_catalog.parquet` (15 rows: every field, owner, description) |
| **Gold data** | Pre-aggregated tables for hot queries | `daily_revenue_by_country.parquet` (small, dashboard-ready) |

**This template has silver + gold metadata, but NOT gold data.** For most analytical workloads, the semantic layer compiles silver into the right shape on-the-fly — no materialization needed. But for production hot paths:

- **Dashboards refreshing every minute** → pre-aggregate daily/hourly
- **Same query running 1000x/day** → cache or materialize the result
- **Sub-second latency required** → pre-compute and point the dashboard at the materialized table

### When to add gold data materialization

Add a materialization step when you observe:

```sql
-- The same query runs thousands of times per day
SELECT customer_id, sum(amount)
FROM orders
WHERE date >= current_date - INTERVAL '30 days'
GROUP BY 1
```

Materialize it as `gold.daily_revenue_by_customer`, refresh hourly. The dashboard reads the materialized table directly — no semantic compilation, no silver scan.

### How to add it to the pipeline

```scala
// Step 9 (optional): materialize hot queries
val dailyByCountry = orders
  .groupBy("country", "order_date")
  .aggregate("total_revenue", "order_count")
  .toDataFrame(spark)

dailyByCountry
  .withColumnRenamed("order_date", "snapshot_date")
  .write
  .format("delta")                       // ACID + time travel
  .mode(SaveMode.Overwrite)
  .partitionBy("snapshot_date")
  .saveAsTable("gold.daily_revenue_by_country")
```

Point your dashboard at `gold.daily_revenue_by_country` instead of running the semantic query each refresh.

### Materialization trade-offs

| Pro | Con |
|---|---|
| Sub-second dashboard latency | Storage cost (one table per materialized query) |
| Decouples dashboard from source data | Refresh lag (data is N hours old) |
| Reduces load on silver | Schema changes require rebuild |
| Easy to backfill | Hard to choose *which* queries to materialize |

**The semantic layer + on-demand compilation is the default.** Materialization is the optimization for hot paths. Don't materialize everything — that's a data warehouse, and warehouses are expensive.

### A materialization spec (future work)

This template doesn't implement materialization. The design would be:

```yaml
# models/orders.yml (extended)
materializations:
  - name: daily_revenue_by_country
    grain: [country, order_date]
    measures: [total_revenue, order_count]
    refresh: daily
    destination: gold.daily_revenue_by_country
  - name: monthly_revenue_by_product
    grain: [product, month]
    measures: [total_revenue, total_units]
    refresh: hourly
    destination: gold.monthly_revenue_by_product
```

The pipeline reads the materialization specs, generates the aggregated tables, and writes them. The semantic model stays available for ad-hoc queries; the materialized tables serve dashboards.

This is a future enhancement, gated on real consumer demand. See `docs/first-consumer-plan.md` for when to add it.

## Querying the gold catalog (demo mode)

After the pipeline runs, `output/_semantica_catalog/` contains every field of every model as a row. Anyone in the org can query it like any other table:

```scala
// Find all fields owned by a specific team
spark.read.parquet("output/_semantica_catalog")
  .filter(col("metadata_values").contains("finance-team"))
  .select("model_name", "field_name", "description")
  .show(false)

// Find all PII fields across all models
spark.read.parquet("output/_semantica_catalog")
  .filter(col("metadata_keys").contains("pii"))
  .select("model_name", "field_name")
  .show(false)

// Audit: which dimensions are time dimensions?
spark.read.parquet("output/_semantica_catalog")
  .filter(col("is_time_dimension") && col("field_type") === "dimension")
  .select("model_name", "field_name", "smallest_grain")
  .show(false)
```

**This is a demo**, not enterprise-grade. See the next section for production patterns.

---

## Production-grade catalog patterns

The demo uses `SaveMode.Overwrite` on a local parquet path. That works for showing the concept but fails enterprise requirements:

| Requirement | Demo (what we have) | Production (what you need) |
|---|---|---|
| **Schema evolution** | Breaks when YAML changes | Delta Lake / Iceberg auto-handles |
| **Time travel / history** | `Overwrite` destroys history | Delta `VERSION AS OF` / Iceberg snapshots |
| **Concurrent writers** | One wins, one fails | Optimistic concurrency control |
| **Atomicity** | Partial writes possible | Delta transactions (commit or rollback) |
| **Access control** | Anyone with FS access | Unity Catalog / IAM / table ACLs |
| **Lineage** | Just a `source_path` string | Unity Catalog column-level lineage |
| **Git SHA tracking** | Missing | Add `git_sha` column from `git rev-parse HEAD` in CI |
| **Notifications** | None | CDC events → Kafka → downstream subscribers |
| **Audit trail** | `loaded_at` only | Full audit log (who/when/what changed) |

### Recommended: Delta Lake + Unity Catalog

For a real deployment, replace Step 8 with:

```scala
// Append-only Delta table, partitioned by date for time travel
newSnapshot
  .withColumn("git_sha", lit(gitSha))      // from CI: `git rev-parse HEAD`
  .withColumn("snapshot_date", current_date())
  .write
  .format("delta")
  .mode("append")
  .partitionBy("snapshot_date")
  .saveAsTable("main._semantica.catalog")

// Register in Unity Catalog for IAM, audit, and discovery
```

Now your org can do:

```sql
-- Latest snapshot
SELECT * FROM main._semantica.catalog
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main._semantica.catalog)

-- What changed since yesterday?
SELECT * FROM main._semantica.catalog WHERE snapshot_date = current_date()
EXCEPT
SELECT * FROM main._semantica.catalog WHERE snapshot_date = current_date() - INTERVAL 1 DAY

-- Time travel to last week's schema
SELECT * FROM main._semantica.catalog TIMESTAMP AS OF '2024-03-15'

-- Audit: who loaded which model when
SELECT model_name, git_sha, loaded_at, source_path
FROM main._semantica.catalog
ORDER BY loaded_at DESC
```

Unity Catalog adds column-level lineage automatically (you can see which pipeline step produces each field) and enforces IAM so only authorized users can read the catalog.

### For non-Databricks stacks

| Catalog tool | Pattern |
|---|---|
| **AWS Glue / Hive Metastore** | `CREATE EXTERNAL TABLE` over parquet on S3/HDFS |
| **Apache Polaris** | Iceberg REST catalog, open-standard |
| **DataHub** (LinkedIn) | Push metadata via REST or Kafka emitter |
| **Amundsen** (Lyft) | Push via metadata service API |
| **Apache Atlas** | Push via Kafka notification |

All of these work with semantica's `model.schema(spark)` output — it's just a DataFrame. The integration pattern is the same: write the DataFrame to your catalog backend, register it, and let your BI tools discover it.

---

## Going to production

For a real deployment:

1. **Replace local parquet with Delta Lake or Apache Iceberg** — gives you ACID, time travel, schema evolution
2. **Use a real catalog** — Unity Catalog, Hive Metastore, AWS Glue — instead of temp views
3. **Replace the Step 8 demo with Delta Lake write** — append-only, partitioned by date, with git_sha
4. **Version your raw data** — keep bronze immutable; rebuild silver when logic changes
5. **Add data quality checks** — Great Expectations, Soda, or a simple custom validator
6. **Monitor the pipeline** — track row counts, null rates, schema drift
7. **Parameterize paths** — pass input/output paths as Spark job args, not hardcoded
8. **Wire to CI/CD** — Airflow DAG, Databricks workflow, GitHub Actions

---

## Related

- [starter template](../starter/README.md) — for the semantic-layer-only path (no ETL)
- [README](../../README.md) — main semantica documentation
- [DESIGN](../../DESIGN.md) — architecture and design decisions
- [known-limitations](../../docs/known-limitations.md) — what semantica does NOT support yet
