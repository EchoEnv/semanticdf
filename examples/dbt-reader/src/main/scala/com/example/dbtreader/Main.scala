package com.example.dbtreader

import io.semanticdf.DbtManifestReader

import org.apache.spark.sql.SparkSession

/** Demo: load a dbt `manifest.json` and turn it into a `Map[String, SemanticTable]`.
  *
  * This shows the dbt reader's two-phase API in action:
  *
  *   1. `DbtManifestReader.read(manifestPath)` parses the manifest
  *      into a `DbtProject` — pure, no Spark.
  *   2. `DbtManifestReader.toSemanticTables(project, spark, resolve)`
  *      binds each model to a real DataFrame and produces the
  *      `SemanticTable` graph. The `resolve` callback is the caller's
  *      contract for how to load a source table — here we use the
  *      alias as a CSV file path.
  *
  * Run with: `mvn scala:run` from this directory. */
object Main {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("dbt-reader-demo")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try {
      val modelDir = "models"
      val manifestPath = java.nio.file.Paths.get(s"$modelDir/manifest.json")

      // Phase 1: read the manifest (no Spark needed).
      val project = DbtManifestReader.read(manifestPath)
      println(s"Parsed dbt manifest v${project.manifestVersion.getOrElse("?")}")
      println(s"Models: ${project.models.keys.mkString(", ")}")
      println()

      // Phase 2: bind to Spark. We resolve each source table by reading
      // a CSV file from the same directory as the manifest. The dbt
      // reader formats sourceTable as `<db>.<schema>.<alias>`; we use
      // the last segment (the alias) as the CSV file name. In a real
      // dbt project the resolver would point at the warehouse (e.g.
      // `spark.table(model.sourceTable)`).
      val tables = DbtManifestReader.toSemanticTables(project, spark, sourceTable =>
        spark.read.option("header", "true").option("inferSchema", "true")
          .csv(s"$modelDir/${sourceTable.split('.').last}.csv"))

      // Query: total revenue per customer (orders joined to customers).
      // The dbt reader doesn't add joins in v1; users add them via
      // the existing API. Here we just show two queries against the
      // `orders` model.
      val orders = tables("orders")
      println("=== orders by total_revenue (top 5) ===")
      orders.query(
        measures   = Seq("total_revenue", "order_count"),
        dimensions = Seq("customer_id"),
        orderBy    = Seq(io.semanticdf.SortKey.desc("total_revenue")),
        limit      = Some(5),
      ).toDataFrame(spark).show(truncate = false)

      println("=== orders filtered to customer 100 ===")
      orders.query(
        measures   = Seq("total_revenue", "order_count"),
        dimensions = Seq("order_id"),
        where      = Some(io.semanticdf.Predicate.Compare.Eq("customer_id", 100)),
        orderBy    = Seq(io.semanticdf.SortKey.asc("order_id")),
        limit      = Some(10),
      ).toDataFrame(spark).show(truncate = false)

      println("=== customers schema ===")
      println(tables("customers").dimensions.keys.mkString(", "))
    } finally spark.stop()
  }
}
