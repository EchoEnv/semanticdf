package io.semanticdf.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{count, col, lit, sum}
import io.semanticdf._

/** Example 05 — Time series with atTimeGrain()
  *
  * Dimension.time() declares a time dimension. atTimeGrain() truncates it for grouping.
  *
  * Key distinction:
  *   - Filtering (where): applied on the RAW column (pre-truncation)
  *   - Grouping (atTimeGrain): applied on the TRUNCATED expression
  *
  * Run with: mvn scala:run -DmainClass=io.semanticdf.examples.TimeSeries
  */
object TimeSeries {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    try {
      val flights = spark.createDataFrame(Seq(
        ("2024-01-15", "AA", 100, 5),
        ("2024-01-28", "AA", 120, 4),
        ("2024-02-03", "UA",  80, 3),
        ("2024-02-20", "UA",  90, 2),
        ("2024-03-10", "DL", 150, 6),
        ("2024-03-25", "DL", 180, 3),
      )).toDF("flight_date", "carrier", "distance", "passengers")
        .withColumn("flight_date", col("flight_date").cast("timestamp"))

      val model = toSemanticTable(flights, name = Some("flights"))
        .withDimensions(
          // smallestTimeGrain = "day" prevents grouping finer than day
          Dimension.time("flight_date", t => t("flight_date"), smallestTimeGrain = Some("day")),
          Dimension("carrier", t => t("carrier")),
        )
        .withMeasures(
          Measure("total_passengers", t => sum(t("passengers"))),
          Measure("flight_count",    t => count(lit(1))),
        )

      // Group by month (truncate day → month)
      println("\n=== Flights by month ===")
      model
        .atTimeGrain("flight_date", "month")
        .groupBy("flight_date", "carrier")
        .aggregate("total_passengers", "flight_count")
        .execute(spark)
        .show(truncate = false)

      // Filter Jan–Feb, then group by quarter
      println("\n=== Jan–Feb flights by quarter (should show only Q1) ===")
      model
        .where(
          Predicate.Compare("ge", "flight_date", "2024-01-01") and
          Predicate.Compare("le", "flight_date", "2024-02-29")
        )
        .atTimeGrain("flight_date", "quarter")
        .groupBy("flight_date", "carrier")
        .aggregate("total_passengers")
        .execute(spark)
        .show(truncate = false)

      // Query API: one-shot bundle
      println("\n=== query() one-shot: by month, top 10 ===")
      model
        .query(
          measures    = Seq("total_passengers"),
          dimensions  = Seq("flight_date", "carrier"),
          timeGrain   = Some("month"),
          orderBy     = Seq("flight_date", "carrier"),
          limit       = Some(10),
        )
        .execute(spark)
        .show(truncate = false)

      println("\n=== SemanticDF plan for by-month query ===")
      println(model
        .atTimeGrain("flight_date", "month")
        .groupBy("flight_date", "carrier")
        .aggregate("total_passengers")
        .explain())

      println("\n=== Key concepts ===")
      println("flight_date timestamps truncated: 2024-01-15 → 2024-01, 2024-02-03 → 2024-02, etc.")
      println("time_range filter applied to RAW column before truncation")
      println("Valid grains: year, quarter, month, week, day, hour, minute")

    } finally {
      spark.stop()
    }
  }
}
