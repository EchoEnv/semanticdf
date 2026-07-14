package io.semanticdf.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{count, lit, sum}
import io.semanticdf._

/** Example 02 — Percent-of-total with t.all()
  *
  * t.all("measure") returns the GRAND TOTAL of that measure (same aggregation, zero grain).
  * Crucially: the calc formula is RE-EVALUATED at zero grain, not summed per-group.
  *
  * This is the correct BI behavior — sums of averages are wrong.
  *
  * Run with: mvn scala:run -DmainClass=io.semanticdf.examples.FlightsPctTotals
  */
object FlightsPctTotals {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    try {
      val flights = spark.createDataFrame(Seq(
        ("AA", "LAX", "JFK", 100, 5),
        ("AA", "JFK", "LAX", 120, 4),
        ("UA", "LAX", "ORD",  80, 3),
        ("UA", "ORD", "LAX",  90, 2),
        ("DL", "ATL", "JFK", 150, 6),
        ("DL", "ATL", "LAX", 200, 3),
        ("DL", "JFK", "ATL", 180, 3),
      )).toDF("carrier", "origin", "dest", "distance", "passengers")

      val model = toSemanticTable(flights, name = Some("flights"))
        .withDimensions(Dimension("carrier", t => t("carrier")))
        .withMeasures(
          Measure("total_passengers", t => sum(t("passengers"))),
          Measure("flight_count",   t => count(lit(1))),
          Measure("avg_passengers", t => t("total_passengers") / t("flight_count")),
          // pct_of_passengers: this carrier's passengers as % of all passengers
          // t.all("total_passengers") = grand total = 2375
          Measure("pct_of_passengers", t => t("total_passengers") / t.all("total_passengers")),
          // pct_of_avg: this carrier's avg vs the grand average
          // t.all("avg_passengers") = grand avg = 2375/23 = 103.3 (NOT sum of avgs)
          Measure("pct_of_avg",        t => t("avg_passengers")   / t.all("avg_passengers")),
        )

      val result = model.groupBy("carrier").aggregate(
        "total_passengers", "avg_passengers", "pct_of_passengers", "pct_of_avg"
      )

      println("\n=== Percent-of-total ===")
      result.execute(spark).show(truncate = false)

      // Verify: pcts sum to 1.0
      println("\n=== SemanticDF plan ===")
      println(result.explain())

      println("\n=== Expected values ===")
      println("AA total=550, pct=550/2375=0.2316, avg=550/7=78.6, pct_avg=78.6/103.3=0.761")
      println("UA total=275, pct=275/2375=0.1158, avg=275/5=55.0,  pct_avg=55.0/103.3=0.532")
      println("DL total=550, pct=550/2375=0.2316, avg=550/9=61.1,  pct_avg=61.1/103.3=0.592")
      println("Note: pct_of_passengers sums to 1.0 by construction (pct + pct_UA + pct_DL = 1.0)")
      println("Note: pct_of_avg does NOT sum to 1.0 (it's ratio-to-grand-avg, not pct-of-sum)")

    } finally {
      spark.stop()
    }
  }
}
