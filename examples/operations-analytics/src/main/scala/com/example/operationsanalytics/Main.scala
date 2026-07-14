package com.example.operationsanalytics

import io.semanticdf._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/** Operations analytics on top of semanticdf.
  *
  * Demonstrates two operational patterns:
  *
  *   Q1 — Order fulfillment time: average ship_days + on-time rate.
  *        "How long does shipping take?" and "what % of orders ship on time?"
  *        Per-row ship_days and on_time_flag are pre-computed in the loader
  *        (because the YAML CalcExpr parser doesn't support function calls).
  *        The YAML then aggregates them with sum() to derive avg and rate.
  *
  *   Q2 — Anomaly detection via z-score: flag orders whose amount is more
  *        than 2 standard deviations from the mean. Implemented as a 2-step
  *        pattern (global stats → per-order filter) because per-row z-score
  *        using a single aggregate query is awkward in semanticdf's
  *        Pass-1 base-measure / Pass-2 calc-measure split.
  *
  * Run:
  *   1. mvn install the parent semanticdf project
  *   2. mvn scala:run -DmainClass=com.example.operationsanalytics.Main
  */
object Main {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("semanticdf-operations-analytics")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    try {
      // ---------------------------------------------------------------------
      // 1. Load source data, pre-compute per-row columns, load YAML model
      // ---------------------------------------------------------------------
      // The YAML's calc_measures (avg_ship_days, on_time_rate) reference
      // total_ship_days and total_on_time (sum of per-row ship_days and
      // The per-row ship_days and on_time_flag columns are computed in the
      // YAML's `transforms:` block — they're applied to the source DataFrame
      // at model-load time, so the Main.scala doesn't need to pre-compute
      // them in Scala. This is the "single source of truth" property: the
      // YAML declares the per-row logic; the Scala DSL just loads and queries.
      val ordersCsv = spark.read.option("header", "true").option("inferSchema", "true")
        .csv("data/orders.csv")
        .withColumn("order_date", col("order_date").cast("date"))
        .withColumn("shipped_at", col("shipped_at").cast("date"))
      val tables = Map("orders_csv" -> ordersCsv)
      val orders = YamlLoader.loadDir("models/", tables)("orders")

      println("=" * 70)
      println("Operations analytics template — fulfillment time + anomaly detection")
      println("=" * 70)

      // ---------------------------------------------------------------------
      // 2. Q1: Order fulfillment time + on-time rate (from YAML calc measures)
      // ---------------------------------------------------------------------
      println("\n--- Q1: Order fulfillment (avg ship_days + on-time rate) ---")
      orders
        .groupBy()
        .aggregate("avg_ship_days", "on_time_rate", "order_count")
        .toDataFrame(spark)
        .show(false)
      println("  (avg_ship_days and on_time_rate are calc measures in the YAML model)")

      // ---------------------------------------------------------------------
      // 3. Q2: Anomaly detection (z-score) — 2-step pattern
      // ---------------------------------------------------------------------
      // Per-row z-score against a global mean/stddev is hard to express in a
      // single semanticdf query. The cleanest pattern is two queries:
      //
      //   Step 1: aggregate the entire dataset to get global mean + variance
      //   Step 2: filter orders using the computed threshold (mean + 2*stddev)
      println("\n--- Q2: Anomaly detection (2-step) — orders > 2σ from mean ---")

      // Step 1: global stats
      val stats = orders
        .withMeasures(
          Measure("mean_amount",   t => avg(t("amount"))),
          Measure("var_amount",    t => var_samp(t("amount"))),
        )
        .groupBy()
        .aggregate("mean_amount", "var_amount")
        .toDataFrame(spark)
        .collect()
        .head
      val mean = stats.getAs[Double]("mean_amount")
      val stddev = math.sqrt(stats.getAs[Double]("var_amount"))
      val threshold = mean + 2 * stddev
      println(f"  mean=$mean%.2f  stddev=$stddev%.2f  threshold(2σ)=$threshold%.2f")

      // Step 2: per-order classification using the global threshold.
      // where() is on SemanticTable (pre-aggregate), so the chain is
      // where().groupBy().aggregate() — not groupBy().aggregate().where().
      orders
        .where(Predicate.Compare("gt", "amount", threshold))
        .groupBy("order_id")
        .aggregate("order_amount")
        .toDataFrame(spark)
        .orderBy(col("order_amount").desc)
        .show(10, false)
    } finally spark.stop()
  }
}
