// Example 04 — Filters: WHERE / HAVING auto-routing
// ---------------------------------------------------------------------------
// Semantica routes filter predicates automatically:
//   - Dimension predicates → pre-aggregation (WHERE)
//   - Measure predicates → post-aggregation (HAVING)
//   - And compounds: each child is routed independently
//   - Or/Not compounds: if ANY child references a measure, whole predicate → HAVING
//
// You can also use .having() to force a post-aggregation filter.
// ---------------------------------------------------------------------------

import io.semantica._
import io.semantica.Predicate._
import org.apache.spark.sql.functions.{count, lit, sum}

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

// ---------------------------------------------------------------------------
// Routing examples
// ---------------------------------------------------------------------------

// 1. DIMENSION filter → WHERE (pre-aggregation)
//
//   "carrier" is a dimension → filter applied to base rows before aggregation
//   Only AA flights are counted.
println("=== Dimension filter (WHERE): carrier = AA ===")
model
  .where("carrier" === "AA")
  .groupBy("carrier")
  .aggregate("total_passengers", "flight_count")
  .execute(spark)
  .show(truncate = false)

// 2. MEASURE filter → HAVING (post-aggregation)
//
//   "total_passengers" is a measure → filter applied to aggregated rows
//   Only groups with total_passengers > 10 appear.
println("=== Measure filter (HAVING): total_passengers > 10 ===")
model
  .groupBy("carrier")
  .aggregate("total_passengers")
  .where("total_passengers" > 10)   // ← HAVING
  .execute(spark)
  .show(truncate = false)

// 3. COMPOUND AND: split per condition
//
//   "carrier = AA" AND "total_passengers > 3" is split:
//     "carrier = AA"  → WHERE (pre-agg, on base rows)
//     "total_passengers > 3" → HAVING (post-agg, on grouped rows)
println("=== Compound AND: splits into WHERE + HAVING ===")
model
  .where(("carrier" === "AA") and ("total_passengers" > 3))
  .groupBy("carrier")
  .aggregate("total_passengers")
  .execute(spark)
  .show(truncate = false)

// 4. OR with measure: WHOLE predicate goes to HAVING
//
//   "carrier = AA" OR "total_passengers > 10" cannot be split
//   (you can't evaluate "OR" with one condition pre-agg and one post-agg).
//   The whole predicate is applied post-aggregation.
println("=== OR with measure → HAVING only ===")
model
  .groupBy("carrier")
  .aggregate("total_passengers")
  .where(("carrier" === "AA") or ("total_passengers" > 10))
  .execute(spark)
  .show(truncate = false)

// 5. Explicit HAVING
//
//   .having() forces a predicate to post-aggregation regardless of routing.
println("=== Explicit HAVING ===")
model
  .where("carrier" === "AA")
  .groupBy("carrier")
  .aggregate("total_passengers")
  .having("total_passengers" > 5)
  .execute(spark)
  .show(truncate = false)

// 6. NOT predicate
//
//   "NOT (carrier = AA)" → pre-agg filter excluding AA rows.
println("=== NOT dimension → WHERE ===")
model
  .where(not("carrier" === "AA"))
  .groupBy("carrier")
  .aggregate("total_passengers")
  .execute(spark)
  .show(truncate = false)

// ---------------------------------------------------------------------------
// Key concepts:
//   - Dimension predicates: pre-aggregation (WHERE)
//   - Measure predicates: post-aggregation (HAVING)
//   - AND compounds: split condition-by-condition
//   - OR/Not compounds: if ANY measure reference → whole → HAVING
//   - .having() forces post-aggregation filter
//
// DSL note: use === (not ==) for equality. Scala's == cannot be overridden.
//          use =!= (not !=) for inequality.
// ---------------------------------------------------------------------------
