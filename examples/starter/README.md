# semanticdf starter template

A complete, working example of [semanticdf](https://github.com/earendil/semanticdf) — a declarative semantic layer on Apache Spark.

This template shows how to define your data models in **YAML** (no Scala required) and run typed queries against them. It's the fastest way to evaluate semanticdf.

---

## What you get

```
semanticdf-starter/
├── README.md                 ← you are here
├── pom.xml                   ← Maven build (depends on semanticdf)
├── data/
│   ├── flights.csv          ← sample flight data (~50 rows)
│   └── carriers.csv         ← carrier lookup table
├── models/                   ← YOUR semantic layer lives here
│   ├── flights.yml          ← flight facts model with dims, measures, calcs, joins
│   └── carriers.yml         ← carriers lookup model
└── src/main/scala/com/example/starter/
    └── Main.scala            ← the app: loads models, runs queries
```

---

## Run it (5 minutes)

### Prerequisites

- JDK 17 (semanticdf requires Java 17+)
- Maven 3.9+
- Apache Spark 3.5.x or 4.x (pulled in automatically by Maven)

### Step 1: install semanticdf locally

The starter depends on semanticdf, which (for now) isn't on Maven Central. Build and install it to your local Maven repo from the parent project:

```bash
cd /path/to/semanticdf
mvn install -DskipTests
```

This puts `io.semanticdf:semanticdf_2.13:0.1.7` in `~/.m2/repository`.

### Step 2: run the starter

```bash
cd /path/to/semanticdf/examples/starter
mvn scala:run -DmainClass=com.example.starter.Main
```

You should see output like:

```
======================================================================
Loaded 2 models: flights, carriers
======================================================================

--- Q1: Top carriers by total passengers ---
+------+------------------+------------+---------------+
|carrier|total_passengers|flight_count|avg_passengers |
+------+------------------+------------+---------------+
|AA    |1833             |12          |152.75         |
|DL    |1867             |12          |155.58         |
...
```

---

## What the queries show

| Query | What it demonstrates |
|---|---|
| **Q1** | Basic group-by + 3 measures (base + base + calc) |
| **Q2** | Percent-of-total — `all(total_passengers)` correctly avoids the `pct=100%` trap |
| **Q3** | Joined query — `flights ⨝ carriers` via the YAML `joins:` block |
| **Q4** | Time-grain — `.atTimeGrain("flight_date", "month")` |
| **Q5** | Filter + group-by — `Predicate` API (note `===` not `==`) |
| **Q6** | Window function: `row_number() over (partition by carrier ...)` for top-N per group |
| **Q7** | Window function: `lag(total_passengers, 1) over (... order by flight_date)` for MoM change |
| **Q8** | Typed query — `groupByDimensions / aggregateMeasures` (compile-time field-ref safety) |
| **Q9** | Typed predicate — `Predicate.Gt(pax, 500)` (operator kind in the type, not a string) |
| **Q10** | `queryAs[T]: Dataset[T]` — typed-bundled-query terminal, one-shot case-class result |
| **Q11** | `Measure.typed[Double]` + `TypedArithmetic.divide` — calc measure with compile-time type-checked arithmetic (zero runtime overhead, no memory leak) |
| Schema | `model.schema(spark)` — every dimension and measure as a DataFrame |

See [`examples/window-analytics`](../window-analytics/README.md) for a deeper walkthrough of window functions.

---

## Customize it

### Change the data

Replace `data/flights.csv` with your own data. As long as the column names match what the YAML references, no code changes needed.

### Add a measure

Edit `models/flights.yml`:

```yaml
measures:
  total_passengers:
    expr: "sum(passengers)"
    description: "Total passengers across all flights in the group"
    metadata:
      owner: analytics-team
      unit: count
```

Add a new measure:

```yaml
  max_single_flight_passengers:
    expr: "max(passengers)"
    description: "Largest single-flight passenger count in the group"
    metadata:
      owner: analytics-team
      unit: count
```

Then re-run. No code changes required.

### Add a calc

```yaml
calculated_measures:
  revenue_per_flight:
    expr: "total_passengers * 200 / flight_count"
    description: "Estimated revenue per flight at $200/ticket"
    metadata:
      owner: finance-team
      unit: USD
```

### Add a dimension

```yaml
dimensions:
  carrier: carrier
  origin:  origin
  dest:    dest          # ← added: destination airport
```

---

## Model files: what they produce

Each `models/*.yml` becomes a `SemanticTable` — an immutable op tree that compiles to a Spark `DataFrame` at query time. You can:

```scala
val models = YamlLoader.loadDir("models/", tables)
models("flights").groupBy("carrier").aggregate("total_passengers").toDataFrame(spark)
```

To see the compiled Spark plan for any query:

```scala
println(flights.groupBy("carrier").aggregate("pct_of_total").explain(spark))
```

---

## Inspect your model

```scala
// Every dimension + measure as a DataFrame
flights.schema(spark).show()

// Catalog-style: filter by owner, find PII fields, etc.
flights.schema(spark)
  .filter(col("metadata_values").contains("analytics-team"))
  .show()
```

This is the same shape you'd write to a Delta catalog table for a multi-team setup.

---

## Use the Scala DSL (optional)

If you prefer code over YAML, semanticdf's Scala DSL produces the same objects:

```scala
import io.semanticdf._
import org.apache.spark.sql.functions.sum

val flights = toSemanticTable(flightsCsv, name = Some("flights"))
  .withDimensions(
    Dimension("carrier", t => t("carrier")),
    Dimension("origin",  t => t("origin")),
  )
  .withMeasures(
    Measure("total_passengers", t => sum(t("passengers")),
      description = Some("Total passengers")),
    Measure("pct_of_total",
      t => t("total_passengers") / t.all("total_passengers"),
      description = Some("Fraction of all passengers")),
  )

flights.groupBy("carrier").aggregate("pct_of_total").toDataFrame(spark).show()
```

The DSL and YAML are equivalent — choose whichever fits your team's style.

---

## Going to production

For a real deployment:

1. **Publish semanticdf to your internal artifact repo** (Nexus, Artifactory, etc.) so the starter can pull it normally instead of `mvn install`.
2. **Version your YAML** in git. Each commit = a versioned semantic layer.
3. **Persist `schema(spark)`** to a Delta/Iceberg table for catalog queries across teams:
   ```scala
   flights.schema(spark)
     .withColumn("loaded_at", current_timestamp())
     .write.format("delta").mode("append")
     .save("_semanticdf/model_schema")
   ```
4. **Deploy** via spark-submit:
   ```bash
   spark-submit \
     --class com.example.starter.Main \
     --master yarn \
     target/semanticdf-starter_2.13-0.1.7-jar-with-dependencies.jar
   ```

---

## What's next

- Read [`docs/known-limitations.md`](../../docs/known-limitations.md) for current scope, guardrails, and roadmap hints
- Read [`DESIGN.md`](../../DESIGN.md) for the architecture and design decisions
- Try the existing [`src/main/scala/io/semanticdf/examples/`](../../src/main/scala/io/semanticdf/examples) for more advanced patterns (joins, time series, filters)
- For compile-time safety on field references, see [the typed-queries section](../../README.md#typed-queries-compile-time-safety) in the main README and [Phase E](../../docs/phase-E-plan.md) for the rationale.

---

## Get help

- Issues: [github.com/earendil/semanticdf/issues](https://github.com/earendil/semanticdf/issues)
- Discussions: [github.com/earendil/semanticdf/discussions](https://github.com/earendil/semanticdf/discussions)
