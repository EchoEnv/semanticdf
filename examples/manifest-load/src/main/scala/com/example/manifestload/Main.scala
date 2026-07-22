package com.example.manifestload

import io.semanticdf.SemanticManifest

import scala.io.Source

import org.apache.spark.sql.SparkSession

/** Demo: load a pre-built `SemanticManifest` JSON artifact and query it.
  *
  * This is the "second half" of the manifest workflow:
  *
  *   1. **Build (CI):** `tools.Main manifest --yaml X --out Y` writes a
  *      JSON artifact capturing the model's static definition.
  *   2. **Load (runtime):** this `Main.scala` reads the JSON via
  *      `SemanticManifest.fromJson`, reconstructs a `SemanticTable`, and
  *      runs queries — including a calc measure that depends on two
  *      base measures.
  *
  * The checked-in artifact at `manifests/usage.json` was built from
  * `examples/telco-analytics/models/usage.yml`. It carries 5 measures:
  *   - 4 base: event_count, total_revenue, total_roaming_revenue, avg_event_amount
  *   - 1 calc: revenue_per_event = total_revenue / event_count
  *
  * The CSV data is re-derived at runtime — the manifest only carries the
  * model's STATIC definition, not the data.
  *
  * To run:
  *   1. `mvn install` the parent semanticdf project
  *   2. From this directory: `mvn scala:run -DmainClass=com.example.manifestload.Main`
  *
  * What this demonstrates:
  *   - Loading a manifest JSON from disk via `SemanticManifest.fromJson`
  *   - Inspecting the loaded model via `ManifestMeta` (no Spark needed)
  *   - Running a base-measure query (Q1)
  *   - Running a calc-measure query (Q2) — the calc `revenue_per_event`
  *     references two base measures, and the post-aggregation MeasureScope
  *     resolves them correctly
  *   - Lifecycle surfacing: the manifest's `status` field is exposed
  *     so consumers can route on `Draft` / `Published` / `Deprecated` */
object Main {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("semanticdf-manifest-load")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try {
      // -- 1. Load the manifest JSON from the classpath (manifests/) ------
      val manifestJson = Source.fromResource("manifests/usage.json").mkString

      // -- 2. Inspect the manifest metadata (no Spark needed) --------------
      val meta = SemanticManifest.parseMeta(manifestJson)
      println(s"Manifest schema: ${meta.schemaVersion}, kind: ${meta.kind}")
      println(s"Model: ${meta.modelName.getOrElse("?")}  v${meta.version}  status=${meta.status}")
      println(s"Digest: ${meta.dimensions} dims, ${meta.measures} measures " +
        s"(${meta.calcMeasures} calc), ${meta.filters} filters, joins=${meta.joins}")
      println()

      // -- 3. Load the source data (operator's concern) --------------------
      // The manifest only carries the model's static definition. The actual
      // data lives in the operator's pipeline — here we read the same CSV
      // the manifest was built from, so the example is self-contained.
      // (The usage model joins plans + promotions + usage via three CSVs.)
      val plans      = spark.read.option("header", "true").option("inferSchema", "true")
        .csv("../telco-analytics/data/plans.csv")
      val promotions = spark.read.option("header", "true").option("inferSchema", "true")
        .csv("../telco-analytics/data/promotions.csv")
      val usage      = spark.read.option("header", "true").option("inferSchema", "true")
        .csv("../telco-analytics/data/usage.csv")

      // -- 4. Reconstruct the SemanticTable from the manifest -------------
      // Manifest holds a single-table model (no joins). The source DF is
      // the `table:` declared in the manifest's `model.sourceTable`.
      val sourceDf = usage  // `usage_csv` is the manifest's `table:`
      val model = SemanticManifest.fromJson(manifestJson, sourceDf)

      // -- 5. Run two queries ---------------------------------------------
      // Q1: a base-measure aggregate. Confirms the manifest recorded the
      // base measure's expr and that fromJson builds a working Column.
      println("Q1 — event_count + total_revenue (base measures):")
      val q1 = model.groupBy().aggregate("event_count", "total_revenue")
        .execute(spark)
      q1.show(false)
      println()

      // Q2: a CALC-measure aggregate. The calc `revenue_per_event` references
      // two base measures (`total_revenue`, `event_count`). The aggregation
      // framework computes the base measures first, then evaluates the calc
      // against the post-aggregated DataFrame via CalcExpr — the persisted
      // expr string is walked and each measure name resolves to a Column
      // through the scope.
      println("Q2 — revenue_per_event (calc measure = total_revenue / event_count):")
      val q2 = model.groupBy().aggregate("revenue_per_event")
        .execute(spark)
      q2.show(false)
      println()

      // Q3: a dim + measure query — exercises a dimension with a measure
      // grouped by event_type.
      println("Q3 — total_revenue by event_type (dim + measure):")
      val q3 = model.groupBy("event_type").aggregate("total_revenue")
        .execute(spark)
      q3.show(false)

      // -- 6. Lifecycle surfacing (PR #136) --------------------------------
      // Every successful envelope carries `warnings`. The library surfaces
      // them via `ManifestMeta.status` so the operator can route on it.
      if (meta.status == "deprecated") {
        System.err.println(s"WARN: model '${meta.modelName.get}' is deprecated")
      }
    } finally {
      spark.stop()
    }
  }
}
