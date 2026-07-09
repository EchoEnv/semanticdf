package io.semantica.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{count, lit, sum}
import io.semantica._
import io.semantica.Predicate._

/** Example 04 — Filters: WHERE / HAVING auto-routing
  *
  * Semantica routes filter predicates automatically:
  *   - Dimension predicates → pre-aggregation (WHERE)
  *   - Measure predicates  → post-aggregation (HAVING)
  *   - And compounds: each child routed independently
  *   - Or/Not: if ANY child references a measure → whole → HAVING
  *
  * Run with: mvn scala:run -DmainClass=io.semantica.examples.FiltersRouting
  */
object FiltersRouting {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    try {
      val flights = spark.createDataFrame(Seq(
        ("AA", "LAX", 100, 5),
        ("AA", "JFK", 120, 4),
        ("UA", "LAX",  80, 3),
        ("UA", "ORD",  90, 2),
        ("DL", "ATL", 150, 6),
        ("DL", "JFK", 180, 3),
      )).toDF("carrier", "origin", "distance", "passengers")

      val model = toSemanticTable(flights, name = Some("flights"))
        .withDimensions(
          Dimension("carrier", t => t("carrier")),
          Dimension("origin",  t => t("origin")),
        )
        .withMeasures(
          Measure("total_passengers", t => sum(t("passengers"))),
          Measure("total_distance",   t => sum(t("distance"))),
          Measure("flight_count",    t => count(lit(1))),
        )

      // 1. DIMENSION filter → WHERE (pre-aggregation)
      println("\n=== 1. Dimension filter (WHERE): carrier = AA ===")
      model
        .where("carrier" === "AA")
        .groupBy("carrier")
        .aggregate("total_passengers", "flight_count")
        .execute(spark)
        .show(truncate = false)

      // 2. MEASURE filter → HAVING (post-aggregation)
      println("\n=== 2. Measure filter (HAVING): total_passengers > 10 ===")
      model
        .groupBy("carrier")
        .aggregate("total_passengers")
        .where("total_passengers" > 10)
        .execute(spark)
        .show(truncate = false)

      // 3. COMPOUND AND: split into WHERE + HAVING
      println("\n=== 3. Compound AND: splits into WHERE + HAVING ===")
      model
        .where(("carrier" === "AA") and ("total_passengers" > 3))
        .groupBy("carrier")
        .aggregate("total_passengers")
        .execute(spark)
        .show(truncate = false)

      // 4. OR with measure: WHOLE predicate → HAVING
      println("\n=== 4. OR with measure → HAVING ===")
      model
        .groupBy("carrier")
        .aggregate("total_passengers")
        .where(("carrier" === "AA") or ("total_passengers" > 10))
        .execute(spark)
        .show(truncate = false)

      // 5. NOT predicate
      println("\n=== 5. NOT dimension → WHERE ===")
      model
        .where(not("carrier" === "AA"))
        .groupBy("carrier")
        .aggregate("total_passengers")
        .execute(spark)
        .show(truncate = false)

      // 6. Explicit HAVING
      println("\n=== 6. Explicit .having() ===")
      model
        .where("carrier" === "AA")
        .groupBy("carrier")
        .aggregate("total_passengers")
        .having("total_passengers" > 5)
        .execute(spark)
        .show(truncate = false)

      println("\n=== Routing summary ===")
      println("carrier = AA         → WHERE  (dimension)")
      println("total_passengers > 10 → HAVING (measure)")
      println("(carrier=AA) AND (total>3) → WHERE + HAVING (split per condition)")
      println("(carrier=AA) OR  (total>10) → HAVING (OR with measure → whole)")
      println("NOT(carrier=AA)    → WHERE  (dimension)")
      println("")

    } finally {
      spark.stop()
    }
  }
}
