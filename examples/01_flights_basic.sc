// Example 01 — Basic group-by and calc measures
// Run with: scala-cli example.sc
// or submit to Spark: spark-submit --class <class> example.jar
// ---------------------------------------------------------------------------

// Replace this with your real DataFrame
val flights = spark.createDataFrame(Seq(
  ("AA", "LAX", "JFK", 100, 5),
  ("AA", "JFK", "LAX", 120, 4),
  ("UA", "LAX", "ORD",  80, 3),
  ("UA", "ORD", "LAX",  90, 2),
  ("DL", "ATL", "JFK", 150, 6),
)).toDF("carrier", "origin", "dest", "distance", "passengers")

import io.semantica._
import org.apache.spark.sql.functions.{count, lit, sum}

// ---------------------------------------------------------------------------
// Step 1 — Build the semantic model
// ---------------------------------------------------------------------------

val model = toSemanticTable(flights, name = Some("flights"))
  .withDimensions(
    Dimension("carrier", t => t("carrier")),
    Dimension("origin",  t => t("origin")),
  )
  .withMeasures(
    // Base measures — aggregate a column directly
    Measure("total_passengers", t => sum(t("passengers"))),
    Measure("flight_count",    t => count(lit(1))),
    Measure("total_distance",  t => sum(t("distance"))),
    // Calc measure — references OTHER measures by NAME
    // Compiled AFTER total_passengers and flight_count
    Measure("avg_passengers",  t => t("total_passengers") / t("flight_count")),
    Measure("avg_distance",    t => t("total_distance")   / t("flight_count")),
  )

// ---------------------------------------------------------------------------
// Step 2 — Query it
// ---------------------------------------------------------------------------

// Query A: group by carrier
val byCarrier = model
  .groupBy("carrier")
  .aggregate("total_passengers", "flight_count", "avg_passengers")

println("=== Flights by carrier ===")
byCarrier.execute(spark).show(truncate = false)

// Query B: group by carrier AND origin
val byCarrierAndOrigin = model
  .groupBy("carrier", "origin")
  .aggregate("total_passengers", "avg_distance")

println("=== Flights by carrier and origin ===")
byCarrierAndOrigin.execute(spark).show(truncate = false)

// Query C: total across all carriers (no group key)
val totals = model
  .groupBy()  // ← no keys = aggregate over all rows
  .aggregate("total_passengers", "flight_count", "total_distance")

println("=== Totals across all flights ===")
totals.execute(spark).show(truncate = false)

// ---------------------------------------------------------------------------
// Key concepts demonstrated:
//   - base measures (sum, count) vs calc measures (ratio of sums)
//   - groupBy().aggregate() fluent chain
//   - zero-grain aggregation (groupBy with no keys)
//   - calc-of-calc: avg_passengers and avg_distance both depend on flight_count
// ---------------------------------------------------------------------------
