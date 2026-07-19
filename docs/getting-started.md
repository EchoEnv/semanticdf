# Getting Started

A 5-minute setup that takes you from `mvn install` to your first
semanticdf query. If you're new to semanticdf, **start here**.

This page is the canonical "run the library" guide — it should
work end-to-end on a fresh macOS / Linux box with JDK 17 installed.

---

## What you'll have at the end

A Java/Scala project that imports `io.semanticdf._`, builds a small
in-memory DataFrame, declares one semantic model on top of it, runs
one aggregate query, and prints the result. Five minutes, six steps.

If any step fails, jump to [§ Troubleshooting](#troubleshooting) at the bottom.

---

## 1. Prerequisites

| Tool | Required version | How to check |
|---|---|---|
| **JDK** | 17 (or newer) | `java -version` |
| **Maven** | 3.9+ (any 3.x works) | `mvn -version` |
| **Spark** | 3.5.x or 4.x | bundled with semanticdf at test time; you only need it if you ship a Spark app to a cluster |

That's it. No Python, no extra services, no GPU. Semanticdf is a pure JVM library; everything you need ships in the Maven coords.

---

## 2. Add the Maven dependency

In your project's `pom.xml`:

```xml
<properties>
    <semanticdf.version>0.1.6</semanticdf.version>
</properties>

<dependencies>
    <dependency>
        <groupId>io.semanticdf</groupId>
        <artifactId>semanticdf_2.13</artifactId>
        <version>${semanticdf.version}</version>
    </dependency>
</dependencies>
```

For `sbt` users: `"io.semanticdf" %% "semanticdf" % "0.1.6"`.

> **Upgrading?** Bump the version and re-run. Semanticdf is
> additive-only between minor versions — your existing code keeps
> compiling. See [`RELEASE.md`](../RELEASE.md) for what's in each release.

---

## 3. Create a SparkSession

semanticdf compiles each query on demand, so the first thing it needs is an active `SparkSession`. For local development, the minimal setup is `local[2]`:

```scala
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
  .master("local[2]")           // any local[N] works; [1] is fine but slow
  .appName("my-semanticdf-app")
  .config("spark.ui.enabled", "false")
  .getOrCreate()
```

For production: use whatever SparkSession your cluster provides.
semanticdf reads/writes the same `SparkSession` Spark applications already use — no special configuration.

---

## 4. Create a sample DataFrame

In production this would be `spark.read.parquet("s3://...")`. For this example, build a small in-memory table:

```scala
import spark.implicits._  // brings in .toDF for Seq[CaseClass]

val flights = Seq(
  ("AA", "JFK", "LAX", 100, 5),
  ("AA", "LAX", "JFK", 120, 4),
  ("UA", "ORD", "LAX",  80, 3),
  ("DL", "ATL", "JFK", 150, 6),
).toDF("carrier", "origin", "dest", "distance", "passengers")
```

That's five rows, three carriers, two airports. Enough to make the queries below interesting.

---

## 5. Build a semantic model

A **semantic model** is the immutable description of *what* you want to compute, separate from *how* it's computed. Three steps:

```scala
import io.semanticdf._

val flightsModel = toSemanticTable(flights, name = Some("flights"))
  .withDimensions(
    Dimension("carrier", t => t("carrier")),     // groupable field
    Dimension("origin",  t => t("origin")),
  )
  .withMeasures(
    Measure("flight_count",   t => count(lit(1))),                    // base
    Measure("total_passengers", t => sum(t("passengers"))),           // base
    // Calc measure — references other measures BY NAME. The framework
    // auto-pulls `total_passengers` and `flight_count` when you ask for
    // `avg_passengers`:
    Measure("avg_passengers",   t => t("total_passengers") / t("flight_count")),
  )
```

Read this as a contract: *"for the `flights` table, let me group by `carrier` or `origin`, and let me compute `flight_count`, `total_passengers`, and the derived `avg_passengers`."*

This **does not run anything yet**. It just builds the description.

---

## 6. Run your first query

Compile and execute the query against the SparkSession:

```scala
flightsModel
  .groupBy("carrier")
  .aggregate("flight_count", "total_passengers", "avg_passengers")
  .execute(spark)
  .show()
```

Expected output (sums match the data above):

```
+-------+-------------+------------------+----------------+
|carrier|flight_count |total_passengers |avg_passengers  |
+-------+-------------+------------------+----------------+
|     AA|            2|                 9|             4.5|
|     UA|            1|                 3|             3.0|
|     DL|            1|                 6|             6.0|
+-------+-------------+------------------+----------------+
```

The compilation is the interesting part. semanticdf:
- Sees `flight_count` and `total_passengers` need base aggregation → emits `groupBy(carrier).agg(...)`
- Sees `avg_passengers` references two base measures → emits a second `select` layer
- All by inspecting the **lambdas**, not by parsing SQL

---

## What to read next

You just ran a semantic model end-to-end. From here, the recommended order is:

| If you want to... | Read |
|---|---|
| **Understand how the compilation worked under the hood** | [`docs/guide.md`](guide.md) → *How a query compiles* |
| **Write YAML-defined models instead of Scala DSL** | [`docs/guide.md`](guide.md) → *Notebook escape hatch — raw SQL via a temp view* (and any [`examples/*/README.md`](../examples/README.md) for YAML examples) |
| **Add joins, calc measures, or `t.all` percent-of-total** | [`docs/guide.md`](guide.md) → *Calc measures*, *Joins*, *Percent-of-total* |
| **Understand the API surface and the typeclass layer** | [`DESIGN.md`](../DESIGN.md) and [`docs/guide.md`](guide.md) → *The typed layer* |
| **Pick the right example for your use case** | [`examples/README.md`](../examples/README.md) (central index of 8 templates) |

The recommended newcomer path is:
> **`docs/getting-started.md` (this page) → [`docs/guide.md`](guide.md) → [`examples/starter/`](../examples/starter/)`**

---

## Troubleshooting

**`compile` fails with "cannot find symbol: flightsDf" / "cannot find symbol: spark"** — You're pasting from somewhere that uses placeholder names. Replace `flightsDf` with your actual DataFrame variable (e.g., the `flights` from step 4), and use the `spark` from step 3. The complete code in § 3–§ 6 of this page is self-contained and runs.

**`NoClassDefFoundError: org/apache/spark/sql/SparkSession`** — Spark isn't on the runtime classpath. Either run via `spark-submit` (your cluster provides Spark) or include Spark as a runtime dep in `pom.xml` for local dev:
```xml
<dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-sql_2.13</artifactId>
    <version>3.5.8</version>
    <scope>runtime</scope>
</dependency>
```

**`mvn compile` works but the example output is empty** — check that you actually called `.execute(spark)` (or `.execute` with implicit `spark`) and that your `groupBy` keys exist in the data. A `.show()` with no preceding terminal returns an empty DataFrame.

**`Exception in thread "main" java.lang.IllegalAccessError: class org.apache.spark.storage.StorageUtils$ ... cannot access class sun.nio.ch.DirectBuffer`** — JDK module access. Run with the standard flags:
```bash
java --add-opens=java.base/java.lang=ALL-UNNAMED \
     --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
     ...
```
The example templates include these flags in their `pom.xml`. For a quick try, `mvn exec:java` from one of the `examples/*/` directories handles this for you.

**A typo in a measure name** — `IllegalArgumentException: Unknown measure 'pax'` with a `"Did you mean: 'flight_count'?"` suffix. semanticdf's validator catches typos via Levenshtein distance.

---

## Coming from a notebook?

If you prefer SQL, semanticdf lets you query a model via Spark SQL by registering it as a temp view:

```scala
flightsModel.execute(spark).createOrReplaceTempView("flights_view")
spark.sql("SELECT carrier, flight_count FROM flights_view").show()
```

For Databricks / Jupyter / Zeppelin users, this means BI tools, dashboards, and SQL-first consumers can query a semantic model directly. See [`docs/guide.md` → *Notebook escape hatch*](guide.md#notebook-escape-hatch--raw-sql-via-a-temp-view) for the full pattern.
