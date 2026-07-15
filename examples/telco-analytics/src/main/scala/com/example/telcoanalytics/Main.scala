package com.example.telcoanalytics

import io.semanticdf._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/** Telco analytics on top of semanticdf.
  *
  * Demonstrates three real-world telco queries:
  *
  *   Q1 — Monthly ARPU per plan (Average Revenue Per User, the canonical
  *        telco KPI), broken down by plan and month. Uses atTimeGrain +
  *        countDistinct (Scala-side) + ARPU calc.
  *   Q2 — Promotion effectiveness: for each promotion, how many customers
  *        and what total revenue, plus the share of total revenue.
  *        Uses t.all() percent-of-total.
  *   Q3 — Roaming revenue contribution: total revenue, roaming revenue, and
  *        % of total that's roaming. The premium-service pattern.
  *
  * Run:
  *   1. mvn install the parent semanticdf project
  *   2. mvn scala:run -DmainClass=com.example.telcoanalytics.Main
  */
object Main {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("semanticdf-telco-analytics")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    try {
      // ---------------------------------------------------------------------
      // 1. Load source data and YAML models
      // ---------------------------------------------------------------------
      val plansCsv = spark.read.option("header", "true").option("inferSchema", "true")
        .csv("data/plans.csv")
      val promotionsCsv = spark.read.option("header", "true").option("inferSchema", "true")
        .csv("data/promotions.csv")
      val usageCsv = spark.read.option("header", "true").option("inferSchema", "true")
        .csv("data/usage.csv")
        .withColumn("event_date", col("event_date").cast("date"))
      val tables = Map(
        "plans_csv"      -> plansCsv,
        "promotions_csv" -> promotionsCsv,
        "usage_csv"      -> usageCsv,
      )
      val models = YamlLoader.loadDir("models/", tables)
      val usage = models("usage")

      println("=" * 70)
      println("Telco analytics template — ARPU, promotion effectiveness, roaming")
      println("=" * 70)

      // ---------------------------------------------------------------------
      // 2. Q1: Monthly ARPU per plan
      // ---------------------------------------------------------------------
      // ARPU = total revenue / distinct active customers in the period.
      // countDistinct isn't in the YAML keyword blocklist, so the measure
      // is added in Scala.
      println("\n--- Q1: Monthly ARPU per plan (Average Revenue Per User) ---")
      val usageWithArpu = usage.withMeasures(
        Measure("active_customers", t => countDistinct(t("customer_id"))),
        Measure("arpu",
          t => CalcHelpers.safeDivide(
            t("total_revenue"), t("active_customers"), defaultValue = 0.0)),
      )
      usageWithArpu
        .atTimeGrain("event_date", "month")
        .groupBy("plan_name")
        .aggregate("total_revenue", "active_customers", "arpu")
        .toDataFrame(spark)
        // Spark's DataFrame.orderBy has two overloads — (String, String*) and
        // (Column*) — but no mixed form. The next query below uses the Column
        // form, so we match that here for consistency.
        .orderBy(col("arpu"), col("plan_name"))
        .show(false)
      println("  (ARPU = total_revenue / active_customers; active = countDistinct customer_id)")

      // ---------------------------------------------------------------------
      // 3. Q2: Promotion effectiveness
      // ---------------------------------------------------------------------
      // Per promo: how many customers, total revenue, and share of total.
      // Uses t.all() for the percent-of-total.
      println("\n--- Q2: Promotion effectiveness (revenue + customer count per promo) ---")
      val usageWithPromoPct = usage.withMeasures(
        Measure("pct_of_revenue",
          t => t("total_revenue") / t.all("total_revenue")),
      )
      usageWithPromoPct
        .withMeasures(
          Measure("customers_on_promo", t => countDistinct(t("customer_id"))),
        )
        .groupBy("promo_code")
        .aggregate("total_revenue", "customers_on_promo", "pct_of_revenue")
        .toDataFrame(spark)
        .orderBy(col("total_revenue").desc)
        .show(false)
      println("  (pct_of_revenue uses t.all() to re-evaluate total_revenue at zero grain)")

      // ---------------------------------------------------------------------
      // 4. Q3: Roaming revenue contribution
      // ---------------------------------------------------------------------
      // The premium-service pattern: % of total revenue that comes from
      // roaming. The YAML already defines total_roaming_revenue as a base
      // measure; the pct_roaming calc uses t.all() for the denominator.
      println("\n--- Q3: Roaming revenue contribution (% of total) ---")
      val usageWithRoamingPct = usage.withMeasures(
        Measure("pct_roaming",
          t => t("total_roaming_revenue") / t.all("total_revenue")),
      )
      usageWithRoamingPct
        .groupBy()
        .aggregate("total_revenue", "total_roaming_revenue", "pct_roaming")
        .toDataFrame(spark)
        .show(false)
      println("  (pct_roaming shows what fraction of all revenue is from roaming events)")

      println("\n" + "=" * 70)
      println("All queries ran. Edit models/*.yml and re-run to modify the semantic layer.")
      println("=" * 70)
    } finally spark.stop()
  }
}
