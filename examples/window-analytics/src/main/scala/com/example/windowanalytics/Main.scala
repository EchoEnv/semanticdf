package com.example.windowanalytics

import io.semantica._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/** Window-function analytics on top of semantica.
  *
  * This template demonstrates three common window-function patterns that are
  * typical in BI dashboards:
  *
  *   Q1 — Top-N per group:    "top 5 origins per carrier by total passengers"
  *   Q2 — Period-over-period: "monthly passengers with MoM % change" (lag)
  *   Q3 — Running total:      "cumulative passengers per carrier over time"
  *
  * Window-function measures are added in Scala (not in YAML) because the YAML
  * CalcExpr parser is intentionally minimal (arithmetic + all() only) — it
  * doesn't support window syntax. Window measures are also classified as
  * calc measures (Pass 2) by the framework because their lambdas reference
  * other measures via t(...), which the ClassificationScope records.
  *
  * Run:
  *   1. mvn install the parent semantica project
  *   2. mvn scala:run -DmainClass=com.example.windowanalytics.Main
  */
object Main {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("semantica-window-analytics")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    try {
      // ---------------------------------------------------------------------
      // 1. Load source data and YAML model
      // ---------------------------------------------------------------------
      val flightsCsv = spark.read.option("header", "true").option("inferSchema", "true")
        .csv("data/flights.csv")
        .withColumn("flight_date", col("flight_date").cast("date"))
      val tables = Map("flights_csv" -> flightsCsv)
      val flights = YamlLoader.loadDir("models/", tables)("flights")

      println("=" * 70)
      println("Window-function analytics template — top-N, MoM, running total")
      println("=" * 70)

      // ---------------------------------------------------------------------
      // 2. Add window-function measures in Scala (not in YAML)
      // ---------------------------------------------------------------------
      val flightsWithWindows = flights.withMeasures(
        // rank by total passengers within each carrier (top-N per group):
        Measure("rank_within_carrier",
          t => row_number().over(Window.partitionBy(t("carrier")).orderBy(t("total_passengers").desc))),
        // previous month's passengers (period-over-period):
        Measure("prev_month_passengers",
          t => lag(t("total_passengers"), 1).over(
            Window.partitionBy().orderBy(t("flight_date")))),
        // running total of passengers over time (across all carriers).
        // partitionBy(lit(1)) forces a single window partition so the running
        // sum crosses Spark task boundaries (otherwise each partition computes
        // its own running total independently).
        Measure("running_total",
          t => sum(t("total_passengers")).over(
            Window.partitionBy(lit(1)).orderBy(t("flight_date")).rowsBetween(
              Window.unboundedPreceding, Window.currentRow))),
      )

      // ---------------------------------------------------------------------
      // 3. Q1: Top-5 origins per carrier by total passengers
      // ---------------------------------------------------------------------
      println("\n--- Q1: Top-5 origins per carrier by total passengers (window + filter) ---")
      flightsWithWindows
        .groupBy("carrier", "origin")
        .aggregate("total_passengers", "flight_count", "rank_within_carrier")
        .where(Predicate.Compare("le", "rank_within_carrier", 5))
        .toDataFrame(spark)
        .orderBy("carrier", "rank_within_carrier")
        .show(20, false)

      // ---------------------------------------------------------------------
      // 4. Q2: Monthly passengers with MoM % change (lag window)
      // ---------------------------------------------------------------------
      println("\n--- Q2: Monthly passengers with MoM % change (lag) ---")
      // pct_change is added BEFORE the aggregate (must be a model measure,
      // not something composed after aggregation). withMeasures() does not
      // accept SemanticAggregateOp as root.
      val withPctChange = flightsWithWindows.withMeasures(
        Measure("pct_change",
          t => CalcHelpers.safeDivide(
            t("total_passengers") - t("prev_month_passengers"),
            t("prev_month_passengers"),
            defaultValue = 0.0)),
      )
      val monthly = withPctChange
        .atTimeGrain("flight_date", "month")
        .groupBy("flight_date")
        .aggregate("total_passengers", "prev_month_passengers", "pct_change")
        .toDataFrame(spark)
        .orderBy("flight_date")
      monthly.show(false)
      println("  (first row's pct_change is 0.0 because there's no prior month — safeDivide default)")

      // ---------------------------------------------------------------------
      // 5. Q3: Running total of passengers over time
      // ---------------------------------------------------------------------
      println("\n--- Q3: Running total of passengers over time ---")
      flightsWithWindows
        .groupBy("flight_date")
        .aggregate("total_passengers", "running_total")
        .toDataFrame(spark)
        .orderBy("flight_date")
        .show(10, false)
      println("  (running_total is partition-sensitive; the order shown may not")
    } finally spark.stop()
  }
}
