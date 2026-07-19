package com.example.starter

import io.semanticdf._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, current_timestamp, lag, lit, row_number}

/** Starter template for semanticdf — declarative semantic layer on Spark.
  *
  * This is the entire "main" file: load YAML models from a directory, run a few
  * example queries, then show the model schema as a DataFrame.
  *
  * What this demonstrates:
  *   1. Loading all YAML models from a directory (loadDir)
  *   2. Basic group-by + aggregate
  *   3. Percent-of-total (the classic BI trap, correctly avoided)
  *   4. Joined query (flights ⨝ carriers)
  *   5. Time-grain aggregation
  *   6. Filter + aggregate, window functions + lag
  *   7. Typed queries with compile-time field-reference safety (the typeclass pattern)
  *   8. Schema introspection (model.schema(spark))
  *   9. Schema export for catalog persistence (e.g. Delta table)
  *
  * To run:
  *   1. `mvn install` the parent semanticdf project (so the local jar is available)
  *   2. From this directory: `mvn scala:run -DmainClass=com.example.starter.Main`
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
  private val jul: JulLogger = JulLogger.getLogger("com.example.starter")
  jul.setLevel(Level.INFO)

  def info(msg: => String): Unit  = jul.info(msg)
  def warn(msg: => String): Unit  = jul.warning(msg)
  def error(msg: => String): Unit = jul.severe(msg)
  def debug(msg: => String): Unit = jul.fine(msg)
}

object Main {

  def main(args: Array[String]): Unit = {
        //  so call sites can write  /  without
    // passing spark positionally. Backward-compatible: explicit
    //  still works (PR #81).
    // `implicit` so call sites can write `.execute` / `.toDataFrame` without
    // passing spark positionally. Backward-compatible: explicit
    // `.execute(spark)` still works (PR #81).
    implicit val spark = SparkSession.builder()
      .master("local[*]")
      .appName("semanticdf-starter")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    try {
      // ---------------------------------------------------------------------
      // 1. Load source data
      // ---------------------------------------------------------------------
      val flightsCsv = spark.read.option("header", "true").option("inferSchema", "true")
        .csv("data/flights.csv")
        .withColumn("flight_date", col("flight_date").cast("date"))
      val carriersCsv = spark.read.option("header", "true").option("inferSchema", "true")
        .csv("data/carriers.csv")

      val tables = Map(
        "flights_csv"  -> flightsCsv,
        "carriers_csv" -> carriersCsv,
      )

      // ---------------------------------------------------------------------
      // 2. Load all YAML models from the models/ directory
      // ---------------------------------------------------------------------
      val models = YamlLoader.loadDir("models/", tables)
      val flights = models("flights")
      val carriers = models("carriers")

      Logger.info("=" * 70)
      Logger.info(s"Loaded ${models.size} models: ${models.keys.mkString(", ")}")
      Logger.info("=" * 70)

      // ---------------------------------------------------------------------
      // 3. Query 1: Top carriers by total passengers
      // ---------------------------------------------------------------------
      Logger.info("--- Q1: Top carriers by total passengers ---")
      flights
        .groupBy("carrier")
        .aggregate("total_passengers", "flight_count", "avg_passengers")
        .toDataFrame
        .orderBy(col("total_passengers").desc)
        .show(false)

      // ---------------------------------------------------------------------
      // 4. Query 2: Percent of total passengers per carrier
      // ---------------------------------------------------------------------
      Logger.info("--- Q2: Percent of total passengers (correctly avoids pct=100% trap) ---")
      flights
        .groupBy("carrier")
        .aggregate("total_passengers", "pct_of_total")
        .toDataFrame
        .orderBy(col("pct_of_total").desc)
        .show(false)

      // ---------------------------------------------------------------------
      // 5. Query 3: Joined query — carrier names + passengers
      // ---------------------------------------------------------------------
      Logger.info("--- Q3: Joined query (flights ⨝ carriers) — carrier names with stats ---")
      flights
        .groupBy("carrier", "name", "hub")
        .aggregate("total_passengers", "avg_distance")
        .toDataFrame
        .orderBy(col("total_passengers").desc)
        .show(false)

      // ---------------------------------------------------------------------
      // 6. Query 4: Time-grain aggregation (monthly)
      // ---------------------------------------------------------------------
      Logger.info("--- Q4: Monthly aggregation using the time-grain API ---")
      flights
        .atTimeGrain("flight_date", "month")
        .groupBy("flight_date")
        .aggregate("total_passengers", "flight_count")
        .toDataFrame
        .orderBy("flight_date")
        .show(false)

      // ---------------------------------------------------------------------
      // 7. Query 5: Filter + aggregate
      // ---------------------------------------------------------------------
      Logger.info("--- Q5: Filter to a specific carrier, then aggregate ---")
      import Predicate._
      flights
        .where("carrier" === "AA")
        .groupBy("origin")
        .aggregate("total_passengers", "flight_count")
        .toDataFrame
        .orderBy(col("total_passengers").desc)
        .show(false)

      // ---------------------------------------------------------------------
      // 5b. Typesafe field references — phantom-typed ref per field.
      // ---------------------------------------------------------------------
      // The Refs object is the only place a field name is hard-coded; all
      // downstream consumers (groupByDimensions, aggregateMeasures, where,
      // SortKey.asc/desc) are type-checked at compile time. See README and
      // docs/phase-E-plan.md for the full story.
      object Refs {
        // Phantom tags — one per field you want to reference by type.
        sealed trait Carrier
        sealed trait Origin
        sealed trait TotalPassengers
        sealed trait FlightCount
        sealed trait AvgPassengers
        sealed trait RankWithinCarrier
        sealed trait PrevMonthPassengers

        // Implicit typeclass witnesses — the *only* place a field name is hard-coded.
        implicit val carrier: SemanticDimension[Carrier]           = SemanticDimension.of[Carrier]("carrier")
        implicit val origin:  SemanticDimension[Origin]            = SemanticDimension.of[Origin]("origin")
        implicit val pax:     SemanticMeasure[TotalPassengers]     = SemanticMeasure.of[TotalPassengers]("total_passengers")
        implicit val count:   SemanticMeasure[FlightCount]         = SemanticMeasure.of[FlightCount]("flight_count")
        implicit val avg:     SemanticMeasure[AvgPassengers]       = SemanticMeasure.of[AvgPassengers]("avg_passengers")
        // Q6 (window/rank) — name must match the withMeasures(rank, ...) below.
        implicit val rank:    SemanticMeasure[RankWithinCarrier]    = SemanticMeasure.of[RankWithinCarrier]("rank_within_carrier")
        // Q7 (lag) — name must match the withMeasures(prev, ...) below.
        implicit val prev:    SemanticMeasure[PrevMonthPassengers]   = SemanticMeasure.of[PrevMonthPassengers]("prev_month_passengers")
      }
      import Refs._

      // ---------------------------------------------------------------------
      // 6. Window function: rank within group (added in Scala, not YAML)
      // ---------------------------------------------------------------------
      // Top-5 origins per carrier by total passengers. Window functions are
      // added in Scala because the YAML CalcExpr parser supports only
      // arithmetic + all() — no OVER (...) syntax. See examples/window-analytics
      // for a fuller walkthrough of this pattern.
      //
      // Typesafe variants: the measure name comes from the typed `rank` ref
      // (no string duplicated at the call site). All consumers
      // (groupByDimensions, aggregateMeasures, where, orderBy) are
      // type-checked — a measure-into-groupByDimensions is a compile error.
      Logger.info("--- Q6: Top-5 origins per carrier (typed row_number window + filter) ---")
      val flightsWithWindow = flights.withMeasures(
        rank,
        t => row_number().over(
          Window.partitionBy(t("carrier")).orderBy(t("total_passengers").desc)
        )
      )
      flightsWithWindow
        .groupByDimensions(carrier, origin)                                 // typed
        .aggregateMeasures(pax, rank)                                        // typed
        .where(Predicate.Le(rank, 5))                                       // typed: Predicate.Le via the typed factory
        .orderBy(SortKey.asc(carrier), SortKey.asc(rank))                   // typed SortKey
        .toDataFrame
        .show(20, false)

      // ---------------------------------------------------------------------
      // 7. Window function: month-over-month change (lag)
      // ---------------------------------------------------------------------
      // Monthly passengers with the previous month as a side-by-side column
      // (via lag), then a calc-measure pct_change. Window functions are
      // added in Scala; the calc measure is added at the SemanticTable
      // level (must be defined before aggregate()).
      //
      // Typesafe variant: `prev` uses the typed withMeasures(prev, expr)
      // overload. `pct_change` is still a string-named calc measure — it
      // references other measures by name (not via a typed ref), and the
      // typed overload requires a SemanticMeasure witness, so calc measures
      // use the string varargs form. This query shows BOTH overloads.
      Logger.info("--- Q7: Monthly passengers with MoM % change (typed lag window) ---")
      // Typed withMeasures(prev, expr) for the lag window measure. The calc
      // measure `pct_change` has no SemanticMeasure witness (it references
      // other measures by name, not via a typed ref), so it uses the string
      // varargs form. This shows both overloads in one query:
      val flightsWithLag = flights
        .withMeasures(
          prev,
          t => lag(t("total_passengers"), 1).over(
            Window.partitionBy().orderBy(t("flight_date"))))
        .withMeasures(
          Measure(
            "pct_change",
            t => CalcHelpers.safeDivide(
              t("total_passengers") - t("prev_month_passengers"),
              t("prev_month_passengers"),
              defaultValue = 0.0
            )
          )
        )
      flightsWithLag
        .atTimeGrain("flight_date", "month")
        .groupBy("flight_date")
        .aggregate("total_passengers", "prev_month_passengers", "pct_change")
        .orderBy("flight_date")                  // flight_date isn't in the typed Refs above
        .toDataFrame
        .show(false)
      Logger.info("  (pct_change is 0.0 for the first month — safeDivide default, no prior month)")

      // ---------------------------------------------------------------------
      // 8. Typed queries (compile-time safety)
      // ---------------------------------------------------------------------
      // Same shape as Q1, but using the typed API. Field refs are phantom-typed,
      // so a measure-into-groupByDimensions is a COMPILE error, not a runtime
      // one — typos happen once at the implicit-val declaration, not per call.
      //
      // Compare to Q1 (above): zero ref-string typos are possible here because
      // every dimension/measure is a typed handle. See README and
      // docs/phase-E-plan.md for the full story.

      Logger.info("--- Q8 (typed): Top carriers (parallel to Q1 with compile-time ref safety) ---")
      flights
        .groupByDimensions(carrier)                         // dimension-only — measure refs are a compile error
        .aggregateMeasures(pax, count, avg)                  // measure-only  — dimension refs are a compile error
        .orderBy(SortKey.desc(pax))                             // typed SortKey
        .toDataFrame
        .show(false)

      // Typed predicate factory: `Predicate.Gt(pax, 500)` produces a `Compare.Gt`
      // predicate internally — operator kind is in the type, not a runtime string.
      Logger.info("--- Q9 (typed predicate): Total-passengers threshold via Predicate.Gt ---")
      flights
        .where(Predicate.Gt(pax, 500))                       // typed: `pax: FieldRef[TotalPassengers]`
        .groupByDimensions(carrier)
        .aggregateMeasures(pax, count)
        .orderBy(SortKey.desc(pax))                             // typed SortKey
        .toDataFrame
        .show(false)

      // ---------------------------------------------------------------------
      // 9. Schema introspection — every dimension and measure as a DataFrame
      // ---------------------------------------------------------------------
      Logger.info("--- Model schema (every field, every model) ---")
      val schema = flights.schema(spark)
      schema.show(false)
      Logger.info(s"Total fields in schema: ${schema.count()}")

      // ---------------------------------------------------------------------
      // 9. Optional: export schema as a Delta/Iceberg/Parquet catalog table
      // ---------------------------------------------------------------------
      // Uncomment to persist the schema to a local parquet file. In production,
      // you'd write to a Delta table in your lake and query it like any other.
      //
      //   schema
      //     .withColumn("loaded_at", current_timestamp())
      //     .write.mode("append").parquet("catalog/model_schema.parquet")
      //
      // Then: SELECT * FROM <lake>._semanticdf.model_schema WHERE model_name = 'flights'
      Logger.info("--- Schema exported (commented out — see source) ---")

      Logger.info("=" * 70)
      Logger.info("All queries ran. Edit models/*.yml and re-run to modify the semantic layer.")
      Logger.info("=" * 70)
    } finally spark.stop()
  }
}
