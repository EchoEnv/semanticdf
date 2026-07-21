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
  *      runs a query.
  *
  * The checked-in artifact at `manifests/orders.json` was built from
  * `examples/operations-analytics/models/orders.yml` and represents the
  * model that the operations-analytics example uses. The CSV data is
  * re-derived at runtime — the manifest only carries the model's
  * STATIC definition, not the data.
  *
  * To run:
  *   1. `mvn install` the parent semanticdf project
  *   2. From this directory: `mvn scala:run -DmainClass=com.example.manifestload.Main`
  *
  * What this demonstrates:
  *   - Loading a manifest JSON from disk via `SemanticManifest.fromJson`
  *   - Inspecting the loaded model via `ManifestMeta` (no Spark needed)
  *   - Running a query against the reconstructed `SemanticTable`
  *   - The source `DataFrame` is the operator's concern — here we read
  *     the same CSV the manifest was built from; in production it would
  *     be a Kafka topic, a Parquet table, etc. */
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
      val manifestJson = Source.fromResource("manifests/orders.json").mkString

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
      val sourceDf = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("../operations-analytics/data/orders.csv")
        .cache()
      println(s"Source loaded: ${sourceDf.count()} rows, columns: ${sourceDf.columns.mkString(", ")}")

      // -- 4. Reconstruct the SemanticTable from the manifest -------------
      val model = SemanticManifest.fromJson(manifestJson, sourceDf)

      // -- 5. Run two queries and print the results -----------------------
      // Q1: aggregation by status — exercises a base measure + a dim.
      // The `exprString` on the manifest's measure (`count(1)`,
      // `sum(amount)`) is parsed back into a working Column at
      // SemanticManifest.fromJson time, so the loaded model produces
      // the same numbers as the original YAML model.
      println("Q1 — order count + total amount by status:")
      val q1 = model.groupBy("status").aggregate("order_count", "order_amount")
        .execute(spark)
      q1.show()
      println()

      // Q2: dim-only query (no aggregate). Exercises dimensions only.
      // Note: calc measures (`avg_ship_days`, `on_time_rate`) are NOT
      // round-trippable from a manifest today — the persisted `expr`
      // references sibling measures, and the manifest loader only
      // resolves base-measure exprs. Calc-measure consumers must
      // re-load the model from YAML to recover full behavior. The
      // library surfaces the calc-measure metadata (name, expr,
      // dependsOn) in `ManifestMeta` for inspection.
      println("Q2 — dim-only projection by order_date (top 5):")
      val q2 = model.groupBy("order_date")
        .aggregate("order_count")
        .execute(spark)
      q2.show(5)

      // -- 6. Lifecycle surfacing (PR #136) --------------------------------
      // Every successful envelope carries `warnings`. Not a real
      // HTTP envelope here, but the library surfaces them via
      // `ManifestMeta.status` so the operator can route on it.
      val warnings = Seq(s"model '${meta.modelName.getOrElse("orders")}' is ${meta.status}")
        .filter(_.contains("deprecated"))
      if (warnings.nonEmpty) warnings.foreach(w => System.err.println(s"WARN: $w"))
    } finally {
      spark.stop()
    }
  }
}