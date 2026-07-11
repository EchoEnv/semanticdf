package com.example.starter

import io.semantica._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, current_timestamp, lit}

/** Starter template for semantica — declarative semantic layer on Spark.
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
  *   6. Schema introspection (model.schema(spark))
  *   7. Schema export for catalog persistence (e.g. Delta table)
  *
  * To run:
  *   1. `mvn install` the parent semantica project (so the local jar is available)
  *   2. From this directory: `mvn scala:run -DmainClass=com.example.starter.Main`
  */
object Main {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("semantica-starter")
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

      println("=" * 70)
      println(s"Loaded ${models.size} models: ${models.keys.mkString(", ")}")
      println("=" * 70)

      // ---------------------------------------------------------------------
      // 3. Query 1: Top carriers by total passengers
      // ---------------------------------------------------------------------
      println("\n--- Q1: Top carriers by total passengers ---")
      flights
        .groupBy("carrier")
        .aggregate("total_passengers", "flight_count", "avg_passengers")
        .toDataFrame(spark)
        .orderBy(col("total_passengers").desc)
        .show(false)

      // ---------------------------------------------------------------------
      // 4. Query 2: Percent of total passengers per carrier
      // ---------------------------------------------------------------------
      println("\n--- Q2: Percent of total passengers (correctly avoids pct=100% trap) ---")
      flights
        .groupBy("carrier")
        .aggregate("total_passengers", "pct_of_total")
        .toDataFrame(spark)
        .orderBy(col("pct_of_total").desc)
        .show(false)

      // ---------------------------------------------------------------------
      // 5. Query 3: Joined query — carrier names + passengers
      // ---------------------------------------------------------------------
      println("\n--- Q3: Joined query (flights ⨝ carriers) — carrier names with stats ---")
      flights
        .groupBy("carrier", "name", "hub")
        .aggregate("total_passengers", "avg_distance")
        .toDataFrame(spark)
        .orderBy(col("total_passengers").desc)
        .show(false)

      // ---------------------------------------------------------------------
      // 6. Query 4: Time-grain aggregation (monthly)
      // ---------------------------------------------------------------------
      println("\n--- Q4: Monthly aggregation using the time-grain API ---")
      flights
        .atTimeGrain("flight_date", "month")
        .groupBy("flight_date")
        .aggregate("total_passengers", "flight_count")
        .toDataFrame(spark)
        .orderBy("flight_date")
        .show(false)

      // ---------------------------------------------------------------------
      // 7. Query 5: Filter + aggregate
      // ---------------------------------------------------------------------
      println("\n--- Q5: Filter to a specific carrier, then aggregate ---")
      import Predicate._
      flights
        .where("carrier" === "AA")
        .groupBy("origin")
        .aggregate("total_passengers", "flight_count")
        .toDataFrame(spark)
        .orderBy(col("total_passengers").desc)
        .show(false)

      // ---------------------------------------------------------------------
      // 8. Schema introspection — every dimension and measure as a DataFrame
      // ---------------------------------------------------------------------
      println("\n--- Model schema (every field, every model) ---")
      val schema = flights.schema(spark)
      schema.show(false)
      println(s"Total fields in schema: ${schema.count()}")

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
      // Then: SELECT * FROM <lake>._semantica.model_schema WHERE model_name = 'flights'
      println("\n--- Schema exported (commented out — see source) ---")

      println("\n" + "=" * 70)
      println("All queries ran. Edit models/*.yml and re-run to modify the semantic layer.")
      println("=" * 70)
    } finally spark.stop()
  }
}
