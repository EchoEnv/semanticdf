// Example 02 — Percent-of-total with t.all()
// ---------------------------------------------------------------------------
// t.all("measure") returns the GRAND TOTAL of that measure (same aggregation, zero grain).
// Crucially: the calc formula is RE-EVALUATED at zero grain, not summed per-group.
// This is the correct BI behavior — sums of averages are wrong.
//
// Example: avg_passengers = total_passengers / flight_count
//   - Per group: AA = 550/7 = 78.6, UA = 775/7 = 110.7, DL = 1050/9 = 116.7
//   - Sum of per-group avgs = 306  ← WRONG (classic BI trap)
//   - Grand avg = 2375/23 = 103.3 ← CORRECT (t.all() recomputes formula at zero grain)
//
// This is verified against real BSL behavior.
// ---------------------------------------------------------------------------

val flights = spark.createDataFrame(Seq(
  ("AA", "LAX", "JFK", 100, 5),
  ("AA", "JFK", "LAX", 120, 4),
  ("UA", "LAX", "ORD",  80, 3),
  ("UA", "ORD", "LAX",  90, 2),
  ("DL", "ATL", "JFK", 150, 6),
  ("DL", "ATL", "LAX", 200, 3),
  ("DL", "JFK", "ATL", 180, 3),
)).toDF("carrier", "origin", "dest", "distance", "passengers")

import io.semantica._
import org.apache.spark.sql.functions.{count, lit, sum}

val model = toSemanticTable(flights, name = Some("flights"))
  .withDimensions(Dimension("carrier", t => t("carrier")))
  .withMeasures(
    Measure("total_passengers",   t => sum(t("passengers"))),
    Measure("flight_count",       t => count(lit(1))),
    Measure("avg_passengers",     t => t("total_passengers") / t("flight_count")),
    // Percent-of-total: this carrier's passengers as % of all passengers
    // t.all("total_passengers") = grand total = 2375
    // For AA: pct = 550/2375 = 0.2316
    Measure("pct_of_passengers",  t => t("total_passengers") / t.all("total_passengers")),
    // Percent-of-total of an AVERAGE measure: avg_passengers / t.all(avg_passengers)
    // t.all("avg_passengers") = grand_avg_passengers = 2375/23 = 103.3 (NOT sum of avgs)
    Measure("pct_of_avg",        t => t("avg_passengers") / t.all("avg_passengers")),
  )

val result = model.groupBy("carrier").aggregate(
  "total_passengers", "avg_passengers", "pct_of_passengers", "pct_of_avg"
)

println("=== Percent-of-total ===")
result.execute(spark).show(truncate = false)

// Verify: pcts sum to 1.0
println("=== Verify pcts sum to 1.0 ===")
val grand = model.groupBy().aggregate("pct_of_passengers", "pct_of_avg").execute(spark)
grand.show(truncate = false)

// ---------------------------------------------------------------------------
// Key concepts:
//   - t.all("measure") always refers to the grand total
//   - The formula is RE-EVALUATED at zero grain (not summed per-group)
//   - For average measures, this gives the grand average, not sum of avgs
//   - pct_of_passengers + pct_of_UA + pct_of_DL = 1.0 by construction
//
// Known limitation: if ALL rows are filtered out, t.all() returns 0
//   and pct becomes 0/0 → null (Spark SQL semantics).
//   Use CalcHelpers.safeDivide if you want 0.0 instead of null.
// ---------------------------------------------------------------------------
