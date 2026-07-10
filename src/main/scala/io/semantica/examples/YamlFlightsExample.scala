package io.semantica.examples

import io.semantica._
import YamlLoader._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/** YAML-driven example — defines semantic models in `flights_model.yml` and queries
  * them the same way as the Scala DSL examples.
  *
  * Run: `mvn scala:run -DmainClass=io.semantica.examples.YamlFlightsExample`
  *
  * This demonstrates that YAML and Scala DSL produce identical results — the YAML
  * loader builds the exact same Dimension / Measure / SemanticTable objects.
  */
object YamlFlightsExample {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("semantica-yaml-example")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.sql.ansi.enabled", "false")
      .getOrCreate()
    try {
      import spark.implicits._

      // --- Source tables (in a real app: spark.read.parquet("s3://...")) ---
      val flightsTbl = spark.createDataFrame(Seq(
        ("AA", "LAX", "JFK", 100, 5),
        ("AA", "JFK", "LAX", 120, 4),
        ("UA", "LAX", "ORD",  80, 3),
        ("UA", "ORD", "LAX",  90, 2),
        ("DL", "ATL", "JFK", 150, 6),
      )).toDF("carrier", "origin", "dest", "distance", "passengers")

      val carriersTbl = spark.createDataFrame(Seq(
        ("AA", "American Airlines"),
        ("UA", "United Airlines"),
        ("DL", "Delta Air Lines"),
      )).toDF("code", "name")

      // --- Load models from YAML ---
      val yamlPath = YamlFlightsExample.getClass.getResource("/flights_model.yml").getPath
      val models = YamlLoader.load(yamlPath, Map(
        "flights_tbl"  -> flightsTbl,
        "carriers_tbl" -> carriersTbl,
      ))

      val flights = models("flights")

      println("=" * 60)
      println("YAML-loaded model — dimensions & measures")
      println("=" * 60)
      println(s"  dimensions: ${flights.dimensions.keys.mkString(", ")}")
      println(s"  measures:    ${flights.measures.keys.mkString(", ")}")

      // --- Query: group-by + base measures ---
      println("\n--- groupBy carrier → total_passengers, flight_count ---")
      flights.groupBy("carrier")
        .aggregate("total_passengers", "flight_count")
        .orderBy("carrier")
        .execute(spark).show()

      // --- Query: calc measures ---
      println("\n--- groupBy carrier → avg_passengers, avg_distance ---")
      flights.groupBy("carrier")
        .aggregate("avg_passengers", "avg_distance")
        .orderBy("carrier")
        .execute(spark).show()

      // --- Query: percent-of-total ---
      println("\n--- groupBy carrier → pct_of_total (percent-of-total) ---")
      flights.groupBy("carrier")
        .aggregate("total_passengers", "pct_of_total")
        .orderBy("carrier")
        .execute(spark).show()

    } finally {
      spark.stop()
    }
  }
}
