package com.example.windowanalytics

import io.semanticdf._
import io.semanticdf.PredicateOps._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/** Window-function analytics on top of semanticdf.
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
  * ==Typed field references (v0.1.x typed API)==
  *
  * See the [[Refs]] object — phantom-typed witnesses for every dimension
  * and measure. Downstream calls use the typed refs (groupByDimensions,
  * aggregateMeasures, SortKey, Predicate.Le). A typo in a ref name is a
  * compile error. See `examples/starter/Main.scala` and
  * `docs/phase-E-plan.md` for the full story.
  *
  * Run:
  *   1. mvn install the parent semanticdf project
  *   2. mvn scala:run -DmainClass=com.example.windowanalytics.Main
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
  private val jul: JulLogger = JulLogger.getLogger("com.example.windowanalytics")
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
    sealed trait Carrier
    sealed trait Origin
    sealed trait FlightDate

    // Measures (declared in the YAML)
    sealed trait TotalPassengers
    sealed trait FlightCount
    sealed trait TotalDistance
    // Window measures (added in Scala via withMeasures)
    sealed trait RankWithinCarrier
    sealed trait PrevMonthPassengers
    sealed trait RunningTotal
    // Calc measure (added in Scala, depends on prev_month_passengers)
    sealed trait PctChange

    implicit val carrier:              SemanticDimension[Carrier]              = SemanticDimension.of[Carrier]("carrier")
    implicit val origin:               SemanticDimension[Origin]               = SemanticDimension.of[Origin]("origin")
    implicit val flightDate:           SemanticDimension[FlightDate]           = SemanticDimension.of[FlightDate]("flight_date")
    implicit val totalPassengers:      SemanticMeasure[TotalPassengers]        = SemanticMeasure.of[TotalPassengers]("total_passengers")
    implicit val flightCount:          SemanticMeasure[FlightCount]            = SemanticMeasure.of[FlightCount]("flight_count")
    implicit val totalDistance:        SemanticMeasure[TotalDistance]          = SemanticMeasure.of[TotalDistance]("total_distance")
    implicit val rankWithinCarrier:    SemanticMeasure[RankWithinCarrier]     = SemanticMeasure.of[RankWithinCarrier]("rank_within_carrier")
    implicit val prevMonthPassengers:  SemanticMeasure[PrevMonthPassengers]    = SemanticMeasure.of[PrevMonthPassengers]("prev_month_passengers")
    implicit val runningTotal:         SemanticMeasure[RunningTotal]           = SemanticMeasure.of[RunningTotal]("running_total")
    implicit val pctChange:            SemanticMeasure[PctChange]               = SemanticMeasure.of[PctChange]("pct_change")
  }

  def main(args: Array[String]): Unit = {
        //  so call sites can write  /  without
    // passing spark positionally. Backward-compatible: explicit
    //  still works (PR #81).
    // `implicit` so call sites can write `.execute` / `.toDataFrame` without
    // passing spark positionally. Backward-compatible: explicit
    // `.execute(spark)` still works (PR #81).
    implicit val spark = SparkSession.builder()
      .master("local[*]")
      .appName("semanticdf-window-analytics")
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

      // Bring the typed refs into scope
      import Refs._

      Logger.info("=" * 70)
      Logger.info("Window-function analytics template — top-N, MoM, running total")
      Logger.info("=" * 70)

      // ---------------------------------------------------------------------
      // 2. Add window-function measures in Scala (not in YAML)
      // ---------------------------------------------------------------------
      // Typed refs read the measure name from the witness — no string
      // duplicated at the call site.
      val flightsWithWindows = flights.withMeasures(
        // rank by total passengers within each carrier (top-N per group):
        Measure(rankWithinCarrier.name,
          t => row_number().over(Window.partitionBy(t("carrier")).orderBy(t("total_passengers").desc))),
        // previous month's passengers (period-over-period):
        Measure(prevMonthPassengers.name,
          t => lag(t("total_passengers"), 1).over(
            Window.partitionBy().orderBy(t("flight_date")))),
        // running total of passengers over time (across all carriers).
        // partitionBy(lit(1)) forces a single window partition so the running
        // sum crosses Spark task boundaries (otherwise each partition computes
        // its own running total independently).
        Measure(runningTotal.name,
          t => sum(t("total_passengers")).over(
            Window.partitionBy(lit(1)).orderBy(t("flight_date")).rowsBetween(
              Window.unboundedPreceding, Window.currentRow))),
      )

      // ---------------------------------------------------------------------
      // 3. Q1: Top-5 origins per carrier by total passengers
      // ---------------------------------------------------------------------
      Logger.info("--- Q1: Top-5 origins per carrier by total passengers (window + filter) ---")
      flightsWithWindows
        .groupByDimensions(carrier, origin)
        .aggregateMeasures(totalPassengers, flightCount, rankWithinCarrier)
        // Infix typed predicate — `rankWithinCarrier <= 5` is the typed form of
        // `Predicate.Le(rankWithinCarrier, 5)`. Brought into scope by
        // `import io.semanticdf.PredicateOps._` above.
        .where(rankWithinCarrier <= 5)
        .orderBy(SortKey.asc(carrier), SortKey.asc(rankWithinCarrier))
        .execute
        .show(20, false)

      // ---------------------------------------------------------------------
      // 4. Q2: Monthly passengers with MoM % change (lag window)
      // ---------------------------------------------------------------------
      Logger.info("--- Q2: Monthly passengers with MoM % change (lag) ---")
      // pct_change is added BEFORE the aggregate (must be a model measure,
      // not something composed after aggregation). withMeasures() does not
      // accept SemanticAggregateOp as root.
      val withPctChange = flightsWithWindows.withMeasures(
        Measure(pctChange.name,
          t => CalcHelpers.safeDivide(
            t("total_passengers") - t("prev_month_passengers"),
            t("prev_month_passengers"),
            defaultValue = 0.0)),
      )
      withPctChange
        .atTimeGrain(flightDate.name, "month")
        .groupByDimensions(flightDate)
        .aggregateMeasures(totalPassengers, prevMonthPassengers, pctChange)
        .orderBy(SortKey.asc(flightDate))
        .execute
        .show(false)
      Logger.info("  (first row's pct_change is 0.0 because there's no prior month — safeDivide default)")

      // ---------------------------------------------------------------------
      // 5. Q3: Running total of passengers over time
      // ---------------------------------------------------------------------
      Logger.info("--- Q3: Running total of passengers over time ---")
      flightsWithWindows
        .groupByDimensions(flightDate)
        .aggregateMeasures(totalPassengers, runningTotal)
        .orderBy(SortKey.asc(flightDate))
        .execute
        .show(10, false)
      Logger.info("  (running_total is partition-sensitive; the order shown may not")
    } finally spark.stop()
  }
}
