package io.semanticdf.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{count, lit, sum}
import io.semanticdf._

/** Example 01 — Basic group-by and calc measures
  *
  * Run with: mvn scala:run -DmainClass=io.semanticdf.examples.FlightsBasic
  * Or submit to Spark: spark-submit target/semanticdf_2.13-*.jar
  */
object FlightsBasic {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    try {
      val flights = spark.createDataFrame(Seq(
        ("AA", "LAX", "JFK", 100, 5),
        ("AA", "JFK", "LAX", 120, 4),
        ("UA", "LAX", "ORD",  80, 3),
        ("UA", "ORD", "LAX",  90, 2),
        ("DL", "ATL", "JFK", 150, 6),
      )).toDF("carrier", "origin", "dest", "distance", "passengers")

      // Build the semantic model
      val model = toSemanticTable(flights, name = Some("flights"))
        .withDimensions(
          Dimension("carrier", t => t("carrier")),
          Dimension("origin",  t => t("origin")),
        )
        .withMeasures(
          Measure("total_passengers", t => sum(t("passengers"))),
          Measure("flight_count",    t => count(lit(1))),
          Measure("total_distance",  t => sum(t("distance"))),
          Measure("avg_passengers",   t => t("total_passengers") / t("flight_count")),
          Measure("avg_distance",   t => t("total_distance")    / t("flight_count")),
        )

      // Query A: group by carrier
      val byCarrier = model
        .groupBy("carrier")
        .aggregate("total_passengers", "flight_count", "avg_passengers")

      SemanticLogger.info("=== Flights by carrier ===")
      byCarrier.execute(spark).show(truncate = false)

      // Query B: group by carrier and origin
      val byCarrierAndOrigin = model
        .groupBy("carrier", "origin")
        .aggregate("total_passengers", "avg_distance")

      SemanticLogger.info("=== Flights by carrier and origin ===")
      byCarrierAndOrigin.execute(spark).show(truncate = false)

      // Query C: totals across all carriers (no group key)
      val totals = model
        .groupBy()
        .aggregate("total_passengers", "flight_count", "total_distance")

      SemanticLogger.info("=== Totals across all flights ===")
      totals.execute(spark).show(truncate = false)

      // Also show the semanticdf plan (no Spark compile)
      SemanticLogger.info("=== SemanticDF plan (explain) ===")
      println(byCarrier.explain())

    } finally {
      spark.stop()
    }
  }
}
