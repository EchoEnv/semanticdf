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
  * ==Typed field references (v0.1.x typed API)==
  *
  * Every dimension/measure name is declared once in the [[Refs]] object below
  * as a phantom-typed witness. Downstream calls use the typed refs:
  * `groupByDimensions(customerId)`, `aggregateMeasures(orderCount, ...)`,
  * `SortKey.desc(orderAmount)`. A typo in a ref name is a compile error, not
  * a runtime error. See `examples/starter/Main.scala` and `docs/phase-E-plan.md`
  * for the full story.
  *
  * Run:
  *   1. mvn install the parent semanticdf project
  *   2. mvn scala:run -DmainClass=com.example.customeranalytics.Main
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
  private val jul: JulLogger = JulLogger.getLogger("com.example.customeranalytics")
  jul.setLevel(Level.INFO)

  def info(msg: => String): Unit  = jul.info(msg)
  def warn(msg: => String): Unit  = jul.warning(msg)
  def error(msg: => String): Unit = jul.severe(msg)
  def debug(msg: => String): Unit = jul.fine(msg)
}

object Main {

  // -----------------------------------------------------------------------
  // Phantom-typed field references. The name string appears ONCE per field
  // (in the implicit val below); every downstream call uses the typed ref
  // (e.g. `customerId`, `orderCount`). The compiler enforces kind: passing a
  // measure ref to `groupByDimensions` is a compile error.
  // -----------------------------------------------------------------------
  object Refs {
    // Dimensions
    sealed trait CustomerId
    sealed trait CustomerSignupDate

    // Measures
    sealed trait OrderCount
    sealed trait OrderAmount
    // Derived measures (added via Scala withMeasures)
    sealed trait RecencyDays
    sealed trait Segment

    implicit val customerId: SemanticDimension[CustomerId]            = SemanticDimension.of[CustomerId]("customer_id")
    implicit val signupDate:  SemanticDimension[CustomerSignupDate]    = SemanticDimension.of[CustomerSignupDate]("customers.signup_date")
    implicit val orderCount:  SemanticMeasure[OrderCount]             = SemanticMeasure.of[OrderCount]("order_count")
    implicit val orderAmount: SemanticMeasure[OrderAmount]            = SemanticMeasure.of[OrderAmount]("order_amount")
    // Note: the names below must match the withMeasures(...) calls below
    implicit val recencyDays: SemanticMeasure[RecencyDays]             = SemanticMeasure.of[RecencyDays]("recency_days")
    implicit val segment:     SemanticMeasure[Segment]                 = SemanticMeasure.of[Segment]("segment")
  }

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

      // Bring the typed refs into scope
      import Refs._

      Logger.info("=" * 70)
      Logger.info("Customer-segmentation analytics template — RFM + cohort activity")
      Logger.info("=" * 70)

      // ---------------------------------------------------------------------
      // 2. Q1: RFM per customer
      // ---------------------------------------------------------------------
      // The joined model groups by `customers.customer_id` (the join key —
      // shared column, not prefixed). Per-customer measures then aggregate
      // the orders linked to that customer.
      //
      // The typed `withMeasures(measure, expr)` overload (v0.1.1) reads the
      // measure name from the SemanticMeasure witness — no string duplicated
      // at the call site. Recency is added as a new measure; segment is a
      // calc measure that references recency + the existing base measures.
      Logger.info("\n--- Q1: RFM per customer (Recency, Frequency, Monetary) ---")
      orders
        .withMeasures(
          // R: days since last order. Fixed cutoff ("2024-04-01") so the
          // example is deterministic; production would use current_date.
          // orders.order_date → order_date (left side of join, no prefix).
          // The measure name comes from the typed ref (`recencyDays.name`).
          Measure(recencyDays.name, t => datediff(lit(java.sql.Date.valueOf("2024-04-01")), max(t("order_date")))),
          // RFM segment: simple threshold-based classification. References
          // the recency_days measure we just defined (via t) plus the
          // existing order_count and order_amount base measures.
          Measure(segment.name, t => when(t("recency_days") <= 60 && t("order_count") >= 3 && t("order_amount") >= 200, lit("High Value")).when(t("recency_days") <= 30, lit("Active")).when(t("recency_days") <= 90, lit("At Risk")).otherwise(lit("Lapsed"))),
        )
        // groupByDimensions: typed; only dimension refs are accepted
        // (a measure ref here would be a compile error).
        .groupByDimensions(customerId)
        // aggregateMeasures: typed; only measure refs are accepted
        // (a dimension ref here would be a compile error).
        .aggregateMeasures(recencyDays, orderCount, orderAmount, segment)
        .orderBy(SortKey.desc(orderAmount))
        .execute(spark)
        .show(20, false)

      // ---------------------------------------------------------------------
      // 3. Q2: Customer activity by signup-day cohort
      // ---------------------------------------------------------------------
      // The YamlLoader's join handler re-creates joined dimensions without
      // the isTimeDimension flag, so atTimeGrain on a joined dim isn't
      // supported. For month-truncation in production, add a Scala-side
      // Dimension.time("customers.signup_month",
      // t => date_trunc("month", t("signup_date"))).
      //
      // Note: the resulting DataFrame has a column literally named
      // `customers.signup_date`. The typed SortKey backtick-quotes names
      // containing dots, so the join-prefixed dimension sorts correctly.
      Logger.info("--- Q2: Customer activity by signup-day cohort ---")
      orders
        .groupByDimensions(signupDate)
        .aggregateMeasures(orderCount, orderAmount)
        .orderBy(SortKey.asc(signupDate))
        .execute(spark)
        .show(20, false)
    } finally spark.stop()
  }
}
