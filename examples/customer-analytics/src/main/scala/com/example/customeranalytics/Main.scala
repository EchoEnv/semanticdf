package com.example.customeranalytics

import io.semanticdf._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/** Customer segmentation analytics on top of semanticdf.
  *
  * Demonstrates two real-world segmentation patterns:
  *
  *   Q1 — RFM (Recency, Frequency, Monetary) per customer.
  *        R = days since last order; F = total orders; M = total spend.
  *        Classic CRM segmentation: identify high-value, at-risk, and lapsed
  *        customers without a separate data-warehouse query layer.
  *
  *   Q2 — Customer activity by signup month (cohort volume).
  *        For each signup-month cohort, the total orders and total spend
  *        from that cohort. A simple "how big is each cohort" view that
  *        works as a starting point for fuller retention analysis.
  *
  * Run:
  *   1. mvn install the parent semanticdf project
  *   2. mvn scala:run -DmainClass=com.example.customeranalytics.Main
  */
object Main {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("semanticdf-customer-analytics")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    try {
      // ---------------------------------------------------------------------
      // 1. Load source data and YAML models
      // ---------------------------------------------------------------------
      val customersCsv = spark.read.option("header", "true").option("inferSchema", "true")
        .csv("data/customers.csv")
        .withColumn("signup_date", col("signup_date").cast("date"))
      val ordersCsv = spark.read.option("header", "true").option("inferSchema", "true")
        .csv("data/orders.csv")
        .withColumn("order_date", col("order_date").cast("date"))
      val tables = Map(
        "customers_csv" -> customersCsv,
        "orders_csv"    -> ordersCsv,
      )
      val models = YamlLoader.loadDir("models/", tables)
      val orders = models("orders")

      println("=" * 70)
      println("Customer-segmentation analytics template — RFM + cohort activity")
      println("=" * 70)

      // ---------------------------------------------------------------------
      // 2. Q1: RFM per customer
      // ---------------------------------------------------------------------
      // The joined model groups by `customers.customer_id` (the join key —
      // shared column, not prefixed). Per-customer measures then aggregate
      // the orders linked to that customer.
      println("\n--- Q1: RFM per customer (Recency, Frequency, Monetary) ---")
      orders
        .withMeasures(
          // R: days since last order. Fixed cutoff ("2024-04-01") so the
          // example is deterministic; production would use current_date.
          // orders.order_date → order_date (left side of join, no prefix).
          Measure("recency_days",
            t => datediff(lit(java.sql.Date.valueOf("2024-04-01")),
                          max(t("order_date")))),
          // F: order_count (base) and M: order_amount (base) — no calc
          // needed, use directly in the segment.
          // RFM segment: simple threshold-based classification.
          Measure("segment",
            t => when(t("recency_days") <= 60 && t("order_count") >= 3 && t("order_amount") >= 200, lit("High Value"))
                 .when(t("recency_days") <= 30, lit("Active"))
                 .when(t("recency_days") <= 90, lit("At Risk"))
                 .otherwise(lit("Lapsed"))),
        )
        // Join key (customer_id) is shared between orders and customers, so
        // it appears unprefixed in the joined model. Group by customer_id
        // only — customers.name / customers.city can be added in production.
        .groupBy("customer_id")
        .aggregate("recency_days", "order_count", "order_amount", "segment")
        .toDataFrame(spark)
        .orderBy(col("order_amount").desc)
        .show(20, false)

      // ---------------------------------------------------------------------
      // 3. Q2: Customer activity by signup-day cohort
      // ---------------------------------------------------------------------
      // We group by signup_date at day-level. The YamlLoader's join handler
      // re-creates joined dimensions without the isTimeDimension flag, so
      // atTimeGrain on a joined dim isn't supported. For month-truncation
      // in production, add a Scala-side Dimension.time("customers.signup_month",
      // t => date_trunc("month", t("signup_date"))).
      //
      // Note: the resulting DataFrame has a column literally named
      // `customers.signup_date`. Spark's DataFrame.orderBy(String) interprets a
      // name containing a `.` as a qualified `table.column` reference (which
      // fails because there's no `customers` table — it's just a single joined
      // DataFrame). Backtick-escaping the name makes Spark treat it as one
      // identifier that matches the column name verbatim.
      println("\n--- Q2: Customer activity by signup-day cohort ---")
      orders
        .groupBy("customers.signup_date")
        .aggregate("order_count", "order_amount")
        .toDataFrame(spark)
        .orderBy("`customers.signup_date`")
        .show(20, false)
    } finally spark.stop()
  }
}
