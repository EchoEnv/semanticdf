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
  * ==Typed field references (v0.1.x typed API)==
  *
  * See the [[Refs]] object — phantom-typed witnesses for every dimension
  * and measure. Downstream calls use the typed refs (groupByDimensions,
  * aggregateMeasures, SortKey). A typo in a ref name is a compile error.
  * See `examples/starter/Main.scala` and `docs/phase-E-plan.md` for the
  * full story.
  *
  * Run:
  *   1. mvn install the parent semanticdf project
  *   2. mvn scala:run -DmainClass=com.example.telcoanalytics.Main
  */
/** Narrative logger for this template.
  *
  * Uses java.util.logging.Logger (JDK built-in). The public API
  * (`info` / `warn` / `error` / `debug`) is logger-agnostic — swap the
  * underlying implementation for SLF4J / log4j2 in production by
  * changing only the body of these four methods. Callsites stay stable.
  *
  * For spark-heavy projects that want logging routed through Spark's
  * log4j infrastructure, `io.semanticdf.SemanticLogger` is available —
  * but using it from a consumer template couples the template to a
  * library internal; this template-local logger is the recommended
  * pattern.
  */
object Logger {
  import java.util.logging.{Level, Logger => JulLogger}
  private val jul: JulLogger = JulLogger.getLogger("com.example.telcoanalytics")
  jul.setLevel(Level.INFO)

  def info(msg: => String): Unit  = jul.info(msg)
  def warn(msg: => String): Unit  = jul.warning(msg)
  def error(msg: => String): Unit = jul.severe(msg)
  def debug(msg: => String): Unit = jul.fine(msg)
}

object Main {

  // -----------------------------------------------------------------------
  // Phantom-typed field references. Name strings appear ONCE per field
  // (in the implicit val below); downstream calls use typed refs.
  // -----------------------------------------------------------------------
  object Refs {
    // Dimensions
    sealed trait PlanName
    sealed trait PromoCode
    sealed trait EventDate
    // Time-grain dimensions (typed via atTimeGrain API)

    // Measures (declared in the YAML)
    sealed trait EventCount
    sealed trait TotalRevenue
    sealed trait TotalRoamingRevenue
    sealed trait AvgEventAmount
    // Calc measure (declared in the YAML)
    sealed trait RevenuePerEvent
    // Temporary measures (added in Scala for Q1)
    sealed trait ActiveCustomers
    sealed trait Arpu
    // Temporary measures (added in Scala for Q2)
    sealed trait PctOfRevenue
    sealed trait CustomersOnPromo
    // Temporary measure (added in Scala for Q3)
    sealed trait PctRoaming

    implicit val planName:          SemanticDimension[PlanName]            = SemanticDimension.of[PlanName]("plan_name")
    implicit val promoCode:         SemanticDimension[PromoCode]           = SemanticDimension.of[PromoCode]("promo_code")
    implicit val eventDate:         SemanticDimension[EventDate]           = SemanticDimension.of[EventDate]("event_date")
    implicit val eventCount:        SemanticMeasure[EventCount]            = SemanticMeasure.of[EventCount]("event_count")
    implicit val totalRevenue:      SemanticMeasure[TotalRevenue]          = SemanticMeasure.of[TotalRevenue]("total_revenue")
    implicit val totalRoamingRevenue: SemanticMeasure[TotalRoamingRevenue] = SemanticMeasure.of[TotalRoamingRevenue]("total_roaming_revenue")
    implicit val avgEventAmount:    SemanticMeasure[AvgEventAmount]        = SemanticMeasure.of[AvgEventAmount]("avg_event_amount")
    implicit val revenuePerEvent:   SemanticMeasure[RevenuePerEvent]       = SemanticMeasure.of[RevenuePerEvent]("revenue_per_event")
    // Temporary measures — names come from typed refs
    implicit val activeCustomers:   SemanticMeasure[ActiveCustomers]      = SemanticMeasure.of[ActiveCustomers]("active_customers")
    implicit val arpu:              SemanticMeasure[Arpu]                  = SemanticMeasure.of[Arpu]("arpu")
    implicit val pctOfRevenue:      SemanticMeasure[PctOfRevenue]          = SemanticMeasure.of[PctOfRevenue]("pct_of_revenue")
    implicit val customersOnPromo:  SemanticMeasure[CustomersOnPromo]     = SemanticMeasure.of[CustomersOnPromo]("customers_on_promo")
    implicit val pctRoaming:        SemanticMeasure[PctRoaming]            = SemanticMeasure.of[PctRoaming]("pct_roaming")
  }

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

      // Bring the typed refs into scope
      import Refs._

      Logger.info("=" * 70)
      Logger.info("Telco analytics template — ARPU, promotion effectiveness, roaming")
      Logger.info("=" * 70)

      // ---------------------------------------------------------------------
      // 2. Q1: Monthly ARPU per plan
      // ---------------------------------------------------------------------
      // ARPU = total revenue / distinct active customers in the period.
      // countDistinct isn't in the YAML keyword blocklist, so the measure
      // is added in Scala via withMeasures (typed ref → no string duplicate).
      Logger.info("--- Q1: Monthly ARPU per plan (Average Revenue Per User) ---")
      val usageWithArpu = usage.withMeasures(
        Measure(activeCustomers.name, t => countDistinct(t("customer_id"))),
        Measure(arpu.name,
          t => CalcHelpers.safeDivide(
            t("total_revenue"), t("active_customers"), defaultValue = 0.0)),
      )
      usageWithArpu
        .atTimeGrain(eventDate.name, "month")
        .groupByDimensions(planName)
        .aggregateMeasures(totalRevenue, activeCustomers, arpu)
        .orderBy(SortKey.asc(arpu), SortKey.asc(planName))
        .execute(spark)
        .show(false)
      Logger.info("  (ARPU = total_revenue / active_customers; active = countDistinct customer_id)")

      // ---------------------------------------------------------------------
      // 3. Q2: Promotion effectiveness
      // ---------------------------------------------------------------------
      // Per promo: how many customers, total revenue, and share of total.
      // Uses t.all() for the percent-of-total. The two temporary measures
      // (pct_of_revenue, customers_on_promo) are added in two separate
      // withMeasures calls so each typed ref's name is sourced from the
      // witness.
      Logger.info("--- Q2: Promotion effectiveness (revenue + customer count per promo) ---")
      val usageWithPromoPct = usage.withMeasures(
        Measure(pctOfRevenue.name,
          t => t("total_revenue") / t.all("total_revenue")),
      )
      usageWithPromoPct
        .withMeasures(
          Measure(customersOnPromo.name, t => countDistinct(t("customer_id"))),
        )
        .groupByDimensions(promoCode)
        .aggregateMeasures(totalRevenue, customersOnPromo, pctOfRevenue)
        .orderBy(SortKey.desc(totalRevenue))
        .execute(spark)
        .show(false)
      Logger.info("  (pct_of_revenue uses t.all() to re-evaluate total_revenue at zero grain)")

      // ---------------------------------------------------------------------
      // 4. Q3: Roaming revenue contribution
      // ---------------------------------------------------------------------
      // The premium-service pattern: % of total revenue that comes from
      // roaming. The YAML already defines total_roaming_revenue as a base
      // measure; the pct_roaming calc uses t.all() for the denominator.
      // Q3 aggregates over all rows (no groupBy), so we use string-based
      // groupBy() since the typed groupByDimensions requires at least one
      // dim ref.
      Logger.info("--- Q3: Roaming revenue contribution (% of total) ---")
      val usageWithRoamingPct = usage.withMeasures(
        Measure(pctRoaming.name,
          t => t("total_roaming_revenue") / t.all("total_revenue")),
      )
      usageWithRoamingPct
        .groupBy()
        .aggregateMeasures(totalRevenue, totalRoamingRevenue, pctRoaming)
        .execute(spark)
        .show(false)
      Logger.info("  (pct_roaming shows what fraction of all revenue is from roaming events)")

      Logger.info("=" * 70)
      Logger.info("All queries ran. Edit models/*.yml and re-run to modify the semantic layer.")
      Logger.info("=" * 70)
    } finally spark.stop()
  }
}
