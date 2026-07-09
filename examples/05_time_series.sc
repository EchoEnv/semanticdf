// Example 05 — Time series with atTimeGrain()
// ---------------------------------------------------------------------------
// Dimension.time() declares a time dimension with a smallest grain.
// atTimeGrain() truncates it to a coarser grain for grouping.
//
// Important distinction:
//   - Filtering (where): applied on the RAW column (pre-truncation)
//   - Grouping (atTimeGrain): applied on the TRUNCATED expression
//
// This means time_range filters are applied BEFORE truncation —
// a filter for "2024-01-01 to 2024-01-31" on a "day" grain removes January data
// before it can be grouped into months.
// ---------------------------------------------------------------------------

import io.semantica._
import org.apache.spark.sql.functions.{count, lit, sum}

val flights = spark.createDataFrame(Seq(
  ("2024-01-15", "AA", 100, 5),
  ("2024-01-28", "AA", 120, 4),
  ("2024-02-03", "UA",  80, 3),
  ("2024-02-20", "UA",  90, 2),
  ("2024-03-10", "DL", 150, 6),
  ("2024-03-25", "DL", 180, 3),
)).toDF("flight_date", "carrier", "distance", "passengers")
  .withColumn("flight_date", $"flight_date".cast("timestamp"))

// ---------------------------------------------------------------------------
// Step 1 — Declare time dimension
// ---------------------------------------------------------------------------

val model = toSemanticTable(flights, name = Some("flights"))
  .withDimensions(
    // smallestTimeGrain = "day" means you can't group finer than day
    Dimension.time("flight_date", t => t("flight_date"), smallestTimeGrain = Some("day")),
    Dimension("carrier", t => t("carrier")),
  )
  .withMeasures(
    Measure("total_passengers", t => sum(t("passengers"))),
    Measure("flight_count",    t => count(lit(1))),
  )

// ---------------------------------------------------------------------------
// Step 2 — Group by month (truncate day → month)
// ---------------------------------------------------------------------------

val byMonth = model
  .atTimeGrain("flight_date", "month")   // ← truncate day → month
  .groupBy("flight_date", "carrier")
  .aggregate("total_passengers", "flight_count")

println("=== Flights by month and carrier ===")
byMonth.execute(spark).show(truncate = false)

// ---------------------------------------------------------------------------
// Step 3 — Filter by time range (pre-truncation), then group by quarter
// ---------------------------------------------------------------------------

val byQuarter = model
  .where(Predicate.Compare("ge", "flight_date", "2024-01-01")
    and Predicate.Compare("le", "flight_date", "2024-02-29"))  // Jan–Feb only
  .atTimeGrain("flight_date", "quarter")
  .groupBy("flight_date", "carrier")
  .aggregate("total_passengers")

println("=== Jan–Feb flights by quarter (should show only 1 quarter) ===")
byQuarter.execute(spark).show(truncate = false)

// ---------------------------------------------------------------------------
// Step 4 — Query API: one-shot bundle
// ---------------------------------------------------------------------------

val result = model.query(
  measures    = Seq("total_passengers"),
  dimensions  = Seq("flight_date", "carrier"),
  timeGrain   = Some("month"),
  orderBy     = Seq("flight_date", "carrier"),
  limit       = Some(10),
)

println("=== query() one-shot: by month, top 10 rows ===")
result.execute(spark).show(truncate = false)

// ---------------------------------------------------------------------------
// What happened:
//   flight_date column has day-level timestamps:
//     2024-01-15 → truncated to 2024-01 → month bucket 1
//     2024-01-28 → truncated to 2024-01 → month bucket 1
//     2024-02-03 → truncated to 2024-02 → month bucket 2
//     2024-02-20 → truncated to 2024-02 → month bucket 2
//     2024-03-10 → truncated to 2024-03 → month bucket 3
//     2024-03-25 → truncated to 2024-03 → month bucket 3
//
//   Group by flight_date (now month): 3 rows (one per month)
//
// Time range filter (Jan–Feb):
//   Applied to RAW column: only rows with flight_date in Jan–Feb remain
//   BEFORE truncation: rows are filtered first, then remaining rows truncated
//   Result: only month buckets 1 and 2 appear in the output
//
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Valid grain values: "year", "quarter", "month", "week", "day", "hour", "minute"
// ---------------------------------------------------------------------------
