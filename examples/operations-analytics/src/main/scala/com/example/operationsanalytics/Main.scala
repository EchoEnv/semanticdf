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
  *        Per-row ship_days and on_time_flag are pre-computed in the YAML
  *        `transforms:` block (the CalcExpr parser doesn't support function
  *        calls). The YAML then aggregates them with sum() to derive
  *        avg and rate.
  *
  *   Q2 — Anomaly detection via z-score: flag orders whose amount is more
  *        than 2 standard deviations from the mean. Implemented as a 2-step
  *        pattern (global stats → per-order filter) because per-row z-score
  *        using a single aggregate query is awkward in semanticdf's
  *        Pass-1 base-measure / Pass-2 calc-measure split.
  *
  * ==Typed field references (v0.1.x typed API)==
  *
  * See the [[Refs]] object — phantom-typed witnesses for every dimension
  * and measure. Downstream calls use the typed refs (groupByDimensions,
  * aggregateMeasures, SortKey, Predicate.Eq/Gt). A typo in a ref name is
  * a compile error. See `examples/starter/Main.scala` and
  * `docs/phase-E-plan.md` for the full story.
  *
  * Run:
  *   1. mvn install the parent semanticdf project
  *   2. mvn scala:run -DmainClass=com.example.operationsanalytics.Main
  */
object Main {

  // -----------------------------------------------------------------------
  // Phantom-typed field references. Name strings appear ONCE per field
  // (in the implicit val below); downstream calls use typed refs.
  // -----------------------------------------------------------------------
  object Refs {
    // Dimensions
    sealed trait OrderId
    sealed trait CustomerId

    // Measures (declared in the YAML)
    sealed trait OrderAmount
    sealed trait OrderCount
    sealed trait TotalShipDays
    sealed trait TotalOnTime
    // Calc measures (declared in the YAML)
    sealed trait AvgShipDays
    sealed trait OnTimeRate
    // Temporary measures (added in Scala for Q2)
    sealed trait MeanAmount
    sealed trait VarAmount
    // The `amount` column is a per-row source column (not a catalog dim/measure
    // because it's not aggregated). We declare it as a SemanticDimension so
    // Predicate.Gt can take a typed ref — same name, typed wrapper.
    sealed trait Amount

    implicit val orderId:       SemanticDimension[OrderId]       = SemanticDimension.of[OrderId]("order_id")
    implicit val customerId:    SemanticDimension[CustomerId]    = SemanticDimension.of[CustomerId]("customer_id")
    implicit val amount:        SemanticDimension[Amount]        = SemanticDimension.of[Amount]("amount")
    implicit val orderAmount:   SemanticMeasure[OrderAmount]     = SemanticMeasure.of[OrderAmount]("order_amount")
    implicit val orderCount:    SemanticMeasure[OrderCount]      = SemanticMeasure.of[OrderCount]("order_count")
    implicit val totalShipDays: SemanticMeasure[TotalShipDays]   = SemanticMeasure.of[TotalShipDays]("total_ship_days")
    implicit val totalOnTime:   SemanticMeasure[TotalOnTime]     = SemanticMeasure.of[TotalOnTime]("total_on_time")
    implicit val avgShipDays:   SemanticMeasure[AvgShipDays]     = SemanticMeasure.of[AvgShipDays]("avg_ship_days")
    implicit val onTimeRate:    SemanticMeasure[OnTimeRate]      = SemanticMeasure.of[OnTimeRate]("on_time_rate")
    // The temporary measures are added via Scala withMeasures in Q2.
    implicit val meanAmount:   SemanticMeasure[MeanAmount]      = SemanticMeasure.of[MeanAmount]("mean_amount")
    implicit val varAmount:    SemanticMeasure[VarAmount]       = SemanticMeasure.of[VarAmount]("var_amount")
  }

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

      // Bring the typed refs into scope
      import Refs._

      println("=" * 70)
      println("Operations analytics template — fulfillment time + anomaly detection")
      println("=" * 70)

      // ---------------------------------------------------------------------
      // 2. Q1: Order fulfillment time + on-time rate (from YAML calc measures)
      // ---------------------------------------------------------------------
      // The YAML's calculated_measures (avg_ship_days, on_time_rate) reference
      // total_ship_days and total_on_time. The Scala DSL just asks for them by
      // typed ref — no string copy.
      //
      // Q1 aggregates over all rows (no group-by). The typed groupByDimensions
      // requires at least one dim ref, so we use the string-based groupBy()
      // for this case. The aggregate measures are still typed.
      println("\n--- Q1: Order fulfillment (avg ship_days + on-time rate) ---")
      orders
        .groupBy()
        .aggregateMeasures(avgShipDays, onTimeRate, orderCount)
        .execute(spark)
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

      // Step 1: global stats. The two temporary measures (mean_amount,
      // var_amount) are added in Scala via withMeasures. Typed refs mean the
      // names come from the witnesses, not duplicated strings. The string
      // groupBy() aggregates over all rows.
      val stats = orders
        .withMeasures(
          Measure(meanAmount.name, t => avg(t("amount"))),
          Measure(varAmount.name, t => var_samp(t("amount"))),
        )
        .groupBy()
        .aggregateMeasures(meanAmount, varAmount)
        .execute(spark)
        .collect()
        .head
      val mean = stats.getAs[Double]("mean_amount")
      val stddev = math.sqrt(stats.getAs[Double]("var_amount"))
      val threshold = mean + 2 * stddev
      println(f"  mean=$mean%.2f  stddev=$stddev%.2f  threshold(2σ)=$threshold%.2f")

      // Step 2: per-order classification using the global threshold.
      // Predicate.Gt is the typed predicate factory — accepts a typed ref.
      orders
        .where(Predicate.Gt(amount, threshold))
        .groupByDimensions(orderId)
        .aggregateMeasures(orderAmount)
        .orderBy(SortKey.desc(orderAmount))
        .execute(spark)
        .show(10, false)
    } finally spark.stop()
  }
}
