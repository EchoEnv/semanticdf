package io.semantica

import org.apache.spark.sql.functions.{count, lit, sum}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import Predicate._  // brings "field" === value, "field" > value, etc. into scope

/** Phase 0 (round-trip regression) + Phase 1a (golden group-by) + Phase 1b (calc proof).
  */
class SemanticaSpec
    extends AnyFunSuite
    with Matchers
    with SparkSessionFixture
    with FlightsFixture {

  // ---- Phase 0: round-trip regression (leaf node unchanged) -----------------

  test("Phase 0: toSemanticTable(df).toDataFrame(spark) round-trips the source") {
    val source = flightsDf
    val st = toSemanticTable(source, name = Some("flights"))

    val out = st.toDataFrame(spark)

    out.count() shouldBe source.count()
    out.columns.toSet shouldBe source.columns.toSet
  }

  // ---- Phase 1a: golden group-by (test_query.py::TestBasicQuery::test_simple_query) ----

  test("Phase 1a: groupBy(carrier).aggregate(total_passengers) matches BSL golden rows") {
    val st = toSemanticTable(flightsDf, name = Some("flights"))
      .withDimensions(
        Dimension("carrier", t => t("carrier")),
      )
      .withMeasures(
        Measure("total_passengers", t => sum(t("passengers"))),
      )

    val rows = st.groupBy("carrier").aggregate("total_passengers")
      .execute(spark)
      .collect()
      .map(r => r.getAs[String]("carrier") -> r.getAs[Long]("total_passengers"))
      .toMap

    rows shouldBe Map("AA" -> 550L, "UA" -> 775L, "DL" -> 1050L)
  }

  test("Phase 1a: aggregate with no dimensions yields a single grand-total row") {
    val st = toSemanticTable(flightsDf, name = Some("flights"))
      .withMeasures(Measure("total_passengers", t => sum(t("passengers"))))

    val rows = st.groupBy().aggregate("total_passengers").execute(spark).collect()
    rows.length shouldBe 1
    rows.head.getAs[Long]("total_passengers") shouldBe 2375L
  }

  // ---- Phase 1b: calc-measure proof (name-based compilation, DESIGN §6.1) ----

  test("Phase 1b: calc measure resolves other measures BY NAME against the aggregated df") {
    // total_distance / flight_count, where flight_count is itself a base measure.
    // AA: 1250/10=125, UA: 2250/10=225, DL: 3250/10=325  (distance sums ×5 each pair).
    val st = toSemanticTable(flightsDf, name = Some("flights"))
      .withDimensions(Dimension("carrier", t => t("carrier")))
      .withMeasures(
        Measure("flight_count", t => count(lit(1))),
        Measure("total_distance", t => sum(t("distance"))),
        Measure("avg_distance_per_flight", t => t("total_distance") / t("flight_count")),
      )

    val rows = st.groupBy("carrier")
      .aggregate("flight_count", "total_distance", "avg_distance_per_flight")
      .execute(spark)
      .collect()
      .map { r =>
        r.getAs[String]("carrier") ->
          r.getAs[Double]("avg_distance_per_flight")
      }
      .toMap

    rows shouldBe Map("AA" -> 125.0, "UA" -> 225.0, "DL" -> 325.0)
  }

  test("Phase 1b: typo in a calc measure name gives a 'did you mean?' error, not a crash") {
    val st = toSemanticTable(flightsDf, name = Some("flights"))
      .withMeasures(
        Measure("flight_count", t => count(lit(1))),
        Measure("bad", t => t("flight_cont") / t("flight_count")), // typo: flight_cont
      )

    val ex = intercept[IllegalArgumentException] {
      // Request the base measure too — Phase 1b does not auto-pull calc deps (Phase 2),
      // so the typo surfaces as a real 'did you mean?' error instead of the
      // empty-base-measure guard.
      st.groupBy().aggregate("flight_count", "bad").execute(spark).collect()
    }
    ex.getMessage should include("flight_cont")
    ex.getMessage should include("Did you mean")
  }

  // ---- Phase 2a: calc-of-calc + dependency auto-pull (invariant A1 revised) ----

  test("Phase 2a: calc-of-calc chain resolves by name across topological layers") {
    // distance_index = avg_distance_per_flight / 100, where avg_distance_per_flight is
    // itself a calc (total_distance / flight_count). So distance_index is layer 2.
    val st = toSemanticTable(flightsDf, name = Some("flights"))
      .withDimensions(Dimension("carrier", t => t("carrier")))
      .withMeasures(
        Measure("flight_count", t => count(lit(1))),
        Measure("total_distance", t => sum(t("distance"))),
        Measure("avg_distance_per_flight", t => t("total_distance") / t("flight_count")),
        Measure("distance_index", t => t("avg_distance_per_flight") / lit(100.0)),
      )

    val rows = st.groupBy("carrier")
      .aggregate("flight_count", "total_distance", "avg_distance_per_flight", "distance_index")
      .execute(spark)
      .collect()
      .map(r => r.getAs[String]("carrier") -> r.getAs[Double]("distance_index"))
      .toMap

    rows shouldBe Map("AA" -> 1.25, "UA" -> 2.25, "DL" -> 3.25)
  }

  test("Phase 2a: requesting a leaf calc auto-pulls its transitive measure deps") {
    // Request ONLY the leaf calc — its deps (avg_distance_per_flight → total_distance,
    // flight_count) must be auto-pulled so the calc resolves to real aggregated columns.
    val st = toSemanticTable(flightsDf, name = Some("flights"))
      .withDimensions(Dimension("carrier", t => t("carrier")))
      .withMeasures(
        Measure("flight_count", t => count(lit(1))),
        Measure("total_distance", t => sum(t("distance"))),
        Measure("avg_distance_per_flight", t => t("total_distance") / t("flight_count")),
      )

    val rows = st.groupBy("carrier")
      .aggregate("avg_distance_per_flight")   // deps auto-pulled
      .execute(spark)
      .collect()
      .map(r => r.getAs[String]("carrier") -> r.getAs[Double]("avg_distance_per_flight"))
      .toMap

    rows shouldBe Map("AA" -> 125.0, "UA" -> 225.0, "DL" -> 325.0)
  }

  test("Phase 2a: a calc dependency cycle raises a clear error, not a hang") {
    val st = toSemanticTable(flightsDf, name = Some("flights"))
      .withMeasures(
        Measure("a", t => t("b")),
        Measure("b", t => t("a")),
      )

    val ex = intercept[IllegalArgumentException] {
      st.groupBy().aggregate("a").execute(spark).collect()
    }
    ex.getMessage should include("cycle")
  }

  // ---- Phase 4: join_one (fact-to-dim, no fan-out) ----------------------------

  test("Phase 4 join_one: orders join customers by customer_id — basic structure") {
    val orders    = toSemanticTable(ordersDf,    name = Some("orders"))
    val customers = toSemanticTable(customersDf, name = Some("customers"))

    val joined = orders
      .join_one(customers, (l, r) => l("customer_id") === r("customer_id"))
      .withDimensions(
        Dimension("customer_id", t => t("customer_id")),
      )
      .withMeasures(
        Measure("order_count", t => count(lit(1))),
      )

    val rows = joined.groupBy("customer_id").aggregate("order_count").execute(spark).collect()
      .map(r => r.getAs[String]("customer_id") -> r.getAs[Long]("order_count")).toMap

    // Each customer's order count: cust_A has 2 orders, cust_B has 1
    rows shouldBe Map("cust_A" -> 2L, "cust_B" -> 1L)
  }

  test("Phase 4 join_one: calc on joined model — orders.total_qty / orders.order_count") {
    // Both sides need join-key dimensions for the model to be coherent.
    val orders = toSemanticTable(ordersDf, name = Some("orders"))
      .withDimensions(Dimension("customer_id", t => t("customer_id")))
      .withDimensions(Dimension("carrier",     t => t("carrier")))
    val customers = toSemanticTable(customersDf, name = Some("customers"))
      .withDimensions(Dimension("customer_id", t => t("customer_id")))

    val joined = orders
      .join_one(customers, (l, r) => l("customer_id") === r("customer_id"))
      .withDimensions(Dimension("customer_id", t => t("customer_id")))
      .withMeasures(
        Measure("order_count",       t => count(lit(1))),
        Measure("total_qty",         t => sum(t("qty"))),
        Measure("avg_qty_per_order", t => t("total_qty") / t("order_count")),
      )

    val rows = joined
      .groupBy("customer_id")
      .aggregate("avg_qty_per_order")
      .execute(spark)
      .collect()
      .map(r => r.getAs[String]("customer_id") -> r.getAs[Double]("avg_qty_per_order"))
      .toMap

    // Each order has qty=1 in the fixture → avg_qty_per_order = 1
    rows("cust_A") shouldBe 1.0 +- 0.01
    rows("cust_B") shouldBe 1.0 +- 0.01
  }

  // ---- Phase 4: join_many (fan-out prevention via pre-aggregation) -----------

  test("Phase 4 join_many: orders join line_items — pre-agg prevents fact inflation") {
    // order_1 has 2 line_items (300+200=500), order_2 has 1 (300), order_3 has 1 (200).
    // Group by carrier: AA → {order_1} → 500, UA → {order_2} → 300, DL → {order_3} → 200.
    // Without pre-agg: order_1's revenue (500) × 2 items = inflated.
    // With pre-agg (fan-out prevention): correct totals.
    // Join keys must be declared as dimensions on both sides so pre-agg works
    // (SemanticJoinOp resolves grain cols against side dimensions to find the grain).
    val orders = toSemanticTable(ordersDf, name = Some("orders"))
      .withDimensions(Dimension("order_id",     t => t("order_id")))
      .withDimensions(Dimension("customer_id",  t => t("customer_id")))
      .withDimensions(Dimension("carrier",      t => t("carrier")))

    val lineItems = toSemanticTable(lineItemsDf, name = Some("line_items"))
      .withDimensions(Dimension("order_id", t => t("order_id")))

    val joined = orders
      .join_many(lineItems, (l, r) => l("order_id") === r("order_id"))
      .withDimensions(Dimension("carrier", t => t("carrier")))
      .withMeasures(
        Measure("revenue_cents", t => sum(t("qty") * t("price_cents"))),
      )

    val rows = joined.groupBy("carrier").aggregate("revenue_cents").execute(spark).collect()
      .map(r => r.getAs[String]("carrier") -> r.getAs[Long]("revenue_cents")).toMap

    rows shouldBe Map("AA" -> 500L, "UA" -> 300L, "DL" -> 200L)
  }

  // ---- Phase 4: join_cross (Cartesian product) --------------------------------

  test("Phase 4 join_cross: produces a DataFrame with all combinations") {
    val session = spark
    import session.implicits._
    val smallA = toSemanticTable(Seq((1, "x"), (2, "y")).toDF("id", "label"), name = Some("a"))
    val smallB = toSemanticTable(Seq(("x", true), ("z", false)).toDF("tag", "flag"), name = Some("b"))

    val joined = smallA
      .join_cross(smallB)
      .withDimensions(
        Dimension("id",   t => t("id")),
        Dimension("tag", t => t("tag")),
      )
      .withMeasures(
        Measure("row_count", t => count(lit(1))),
      )

    val rows = joined.groupBy("id", "tag").aggregate("row_count").execute(spark).collect()

    // 2 rows in a × 2 rows in b = 4 rows in cross join
    rows.length shouldBe 4
    val ids   = rows.map(r => r.getAs[Int]("id")).distinct.sorted.toSeq
    val tags  = rows.map(r => r.getAs[String]("tag")).distinct.sorted.toSeq
    ids   shouldBe Seq(1, 2)
    tags  shouldBe Seq("x", "z")
  }

  // ---- Phase 4: calc-of-calc on joined model ---------------------------------

  test("Phase 4: calc-of-calc chain on joined orders+line_items model") {
    // Same fixture as fan-out test; add a calc on top.
    // Join keys must be declared as dimensions on both sides so pre-agg works
    val orders = toSemanticTable(ordersDf, name = Some("orders"))
      .withDimensions(Dimension("order_id",     t => t("order_id")))
      .withDimensions(Dimension("customer_id",  t => t("customer_id")))
      .withDimensions(Dimension("carrier",      t => t("carrier")))

    val lineItems = toSemanticTable(lineItemsDf, name = Some("line_items"))
      .withDimensions(Dimension("order_id", t => t("order_id")))

    val joined = orders
      .join_many(lineItems, (l, r) => l("order_id") === r("order_id"))
      .withDimensions(Dimension("carrier", t => t("carrier")))
      .withMeasures(
        Measure("revenue_cents", t => sum(t("qty") * t("price_cents"))),
        Measure("revenue_dollars", t => t("revenue_cents") / lit(100.0)),
        Measure("revenue_rank",    t => t("revenue_dollars") / lit(100.0)),
      )

    val rows = joined.groupBy("carrier").aggregate("revenue_rank").execute(spark).collect()
      .map(r => r.getAs[String]("carrier") -> r.getAs[Double]("revenue_rank"))
      .toMap

    // revenue_dollars = revenue_cents / 100
    // revenue_rank = revenue_dollars / 100 = revenue_cents / 10000
    rows shouldBe Map("AA" -> 0.05, "UA" -> 0.03, "DL" -> 0.02)
  }

  // ---- Phase 5: Filtering — WHERE (dimension, pre-agg) ----------------------

  test("Phase 5 where: dimension filter routes to WHERE (pre-agg)") {
    val st = toSemanticTable(flightsDf, name = Some("flights"))
      .withDimensions(Dimension("carrier", t => t("carrier")))
      .withMeasures(Measure("total_passengers", t => sum(t("passengers"))))

    // carrier is a dimension → pre-agg filter. Only AA rows survive.
    val rows = st.where("carrier" === "AA")
      .groupBy("carrier").aggregate("total_passengers")
      .execute(spark).collect()
      .map(r => r.getAs[String]("carrier") -> r.getAs[Long]("total_passengers"))
      .toMap

    rows shouldBe Map("AA" -> 550L)
  }

  test("Phase 5 where: `in` operator on dimension routes to WHERE") {
    val st = toSemanticTable(flightsDf, name = Some("flights"))
      .withDimensions(Dimension("carrier", t => t("carrier")))
      .withMeasures(Measure("total_passengers", t => sum(t("passengers"))))

    val rows = st.where("carrier" in ("AA", "UA"))
      .groupBy("carrier").aggregate("total_passengers")
      .execute(spark).collect()
      .map(r => r.getAs[String]("carrier") -> r.getAs[Long]("total_passengers"))
      .toMap

    rows shouldBe Map("AA" -> 550L, "UA" -> 775L)
  }

  // ---- Phase 5: Filtering — HAVING (measure, post-agg) -----------------------

  test("Phase 5 where: measure filter routes to HAVING (post-agg)") {
    val st = toSemanticTable(flightsDf, name = Some("flights"))
      .withDimensions(Dimension("carrier", t => t("carrier")))
      .withMeasures(Measure("total_passengers", t => sum(t("passengers"))))

    // total_passengers is a measure → post-agg filter (HAVING).
    // AA=550 (filtered), UA=775 (survives), DL=1050 (survives).
    val rows = st.where("total_passengers" > 600)
      .groupBy("carrier").aggregate("total_passengers")
      .execute(spark).collect()
      .map(r => r.getAs[String]("carrier") -> r.getAs[Long]("total_passengers"))
      .toMap

    rows shouldBe Map("UA" -> 775L, "DL" -> 1050L)
  }

  // ---- Phase 5: AND-split (dimension → WHERE, measure → HAVING) --------------

  test("Phase 5 where: AND compound splits — dimension to WHERE, measure to HAVING") {
    val st = toSemanticTable(flightsDf, name = Some("flights"))
      .withDimensions(Dimension("carrier", t => t("carrier")))
      .withMeasures(
        Measure("total_passengers", t => sum(t("passengers"))),
        Measure("flight_count", t => count(lit(1))),
      )

    // AND: carrier=AA routes pre-agg (WHERE); total_passengers>100 routes post-agg (HAVING).
    // AA total=550>100 → survives.
    val rows = st.where(("carrier" === "AA") and ("total_passengers" > 100))
      .groupBy("carrier").aggregate("total_passengers", "flight_count")
      .execute(spark).collect()
      .map(r => r.getAs[String]("carrier") -> r.getAs[Long]("total_passengers"))
      .toMap

    rows shouldBe Map("AA" -> 550L)
  }

  // ---- Phase 5: OR-whole (mixing dim+measure stays whole, post-agg) -----------

  test("Phase 5 where: OR mixing dim+measure stays whole (post-agg)") {
    val st = toSemanticTable(flightsDf, name = Some("flights"))
      .withDimensions(Dimension("carrier", t => t("carrier")))
      .withMeasures(Measure("total_passengers", t => sum(t("passengers"))))

    // OR references a measure → cannot split → whole predicate post-agg.
    // AA: carrier=AA → true. UA: 775<800, carrier≠AA → false. DL: 1050>800 → true.
    val rows = st.where(("carrier" === "AA") or ("total_passengers" > 800))
      .groupBy("carrier").aggregate("total_passengers")
      .execute(spark).collect()
      .map(r => r.getAs[String]("carrier")).toSet

    rows shouldBe Set("AA", "DL")
  }

  test("Phase 5 having: explicit post-agg filter on a calc measure") {
    val st = toSemanticTable(flightsDf, name = Some("flights"))
      .withDimensions(Dimension("carrier", t => t("carrier")))
      .withMeasures(
        Measure("flight_count", t => count(lit(1))),
        Measure("total_distance", t => sum(t("distance"))),
        Measure("avg_distance_per_flight", t => t("total_distance") / t("flight_count")),
      )

    // Calc measure filter: avg > 200 → UA (225), DL (325) survive; AA (125) filtered.
    val rows = st.having("avg_distance_per_flight" > 200)
      .groupBy("carrier").aggregate("avg_distance_per_flight")
      .execute(spark).collect()
      .map(r => r.getAs[String]("carrier")).toSet

    rows shouldBe Set("UA", "DL")
  }

  test("Phase 5 where: NOT / =!= / <= shorthand operators") {
    val st = toSemanticTable(flightsDf, name = Some("flights"))
      .withDimensions(Dimension("carrier", t => t("carrier")))
      .withMeasures(Measure("total_passengers", t => sum(t("passengers"))))

    // Three forms that all work: =!=, .not, and a combined AND.
    // (carrier === "AA").not AND (total_passengers <= 775)
    // → UA (775) survives; AA excluded by NOT; DL (1050) excluded by <=.
    val rows = st.where(("carrier" === "AA").not and ("total_passengers" <= 775))
      .groupBy("carrier").aggregate("total_passengers")
      .execute(spark).collect()
      .map(r => r.getAs[String]("carrier")).toSet

    rows shouldBe Set("UA")
  }

  test("Phase 5 where: =!= inequality on dimension") {
    val st = toSemanticTable(flightsDf, name = Some("flights"))
      .withDimensions(Dimension("carrier", t => t("carrier")))
      .withMeasures(Measure("total_passengers", t => sum(t("passengers"))))

    val rows = st.where("carrier" =!= "DL")
      .groupBy("carrier").aggregate("total_passengers")
      .execute(spark).collect()
      .map(r => r.getAs[String]("carrier")).toSet

    rows shouldBe Set("AA", "UA")
  }

  // ---- Phase 3: Percent-of-total (t.all, grand-total cross-join) --------------

  test("Phase 3 percent-of-total: base measure / its grand total") {
    val st = toSemanticTable(flightsDf, name = Some("flights"))
      .withDimensions(Dimension("carrier", t => t("carrier")))
      .withMeasures(
        Measure("total_passengers", t => sum(t("passengers"))),
        // Grand total = 2375 (AA 550 + UA 775 + DL 1050).
        Measure("pct_of_total", t => t("total_passengers") / t.all("total_passengers")),
      )

    val rows = st.groupBy("carrier").aggregate("pct_of_total")
      .execute(spark).collect()
      .map(r => r.getAs[String]("carrier") -> r.getAs[Double]("pct_of_total"))
      .toMap

    // Percentages sum to 1.0 (the defining property of percent-of-total).
    rows.values.sum shouldBe 1.0 +- 1e-9
    rows("AA") shouldBe (550.0 / 2375.0) +- 1e-9
    rows("UA") shouldBe (775.0 / 2375.0) +- 1e-9
    rows("DL") shouldBe (1050.0 / 2375.0) +- 1e-9
  }

  test("Phase 3 percent-of-total: all() on a CALC measure recomputes the formula at zero grain") {
    // The crucial correctness test. avg_distance_per_flight is a calc (total_distance/flight_count).
    // t.all("avg...") MUST be the grand-total calc value (6750/30 = 225),
    // NOT the sum of per-group calcs (125+225+325 = 675). Summing per-group avgs would
    // be the classic BI error; recomputing the formula at zero grain is the fix.
    val st = toSemanticTable(flightsDf, name = Some("flights"))
      .withDimensions(Dimension("carrier", t => t("carrier")))
      .withMeasures(
        Measure("flight_count",      t => count(lit(1))),
        Measure("total_distance",     t => sum(t("distance"))),
        Measure("avg_distance_per_flight", t => t("total_distance") / t("flight_count")),
        Measure("avg_vs_grand_avg", t => t("avg_distance_per_flight") / t.all("avg_distance_per_flight")),
      )

    val rows = st.groupBy("carrier").aggregate("avg_vs_grand_avg")
      .execute(spark).collect()
      .map(r => r.getAs[String]("carrier") -> r.getAs[Double]("avg_vs_grand_avg"))
      .toMap

    // Grand avg = 225. AA=125/225, UA=225/225=1.0, DL=325/225.
    rows("AA") shouldBe (125.0 / 225.0) +- 1e-9
    rows("UA") shouldBe 1.0 +- 1e-9
    rows("DL") shouldBe (325.0 / 225.0) +- 1e-9
  }

  test("Phase 3 percent-of-total: pre-agg filter propagates to the totals table") {
    // Classic BI trap: when a WHERE filter narrows rows, percent-of-total must be
    // computed against the FILTERED grand total, not the unfiltered one. Because the
    // totals table is built from the same (already-filtered) base, this works for free.
    val st = toSemanticTable(flightsDf, name = Some("flights"))
      .withDimensions(Dimension("carrier", t => t("carrier")))
      .withMeasures(
        Measure("total_passengers", t => sum(t("passengers"))),
        Measure("pct_of_total", t => t("total_passengers") / t.all("total_passengers")),
      )

    val rows = st.where("carrier" in ("AA", "UA"))
      .groupBy("carrier").aggregate("pct_of_total")
      .execute(spark).collect()
      .map(r => r.getAs[String]("carrier") -> r.getAs[Double]("pct_of_total"))
      .toMap

    // Filtered grand total = 550 + 775 = 1325 (NOT 2375).
    rows.keySet shouldBe Set("AA", "UA")
    rows.values.sum shouldBe 1.0 +- 1e-9
    rows("AA") shouldBe (550.0 / 1325.0) +- 1e-9
    rows("UA") shouldBe (775.0 / 1325.0) +- 1e-9
  }

  // ---- Phase 5 completion: orderBy / limit / query ---------------------------

  test("Phase 5 orderBy + limit: top-N after aggregate") {
    val st = toSemanticTable(flightsDf, name = Some("flights"))
      .withDimensions(Dimension("carrier", t => t("carrier")))
      .withMeasures(Measure("total_passengers", t => sum(t("passengers"))))

    // Descending by total_passengers, take top 2 → DL (1050), UA (775).
    val rows = st.groupBy("carrier").aggregate("total_passengers")
      .orderBy(SortKey.desc("total_passengers")).limit(2)
      .execute(spark).collect()
      .map(r => r.getAs[String]("carrier"))

    rows shouldBe Seq("DL", "UA")
  }

  test("Phase 5 orderBy: bare string defaults to ascending") {
    val st = toSemanticTable(flightsDf, name = Some("flights"))
      .withDimensions(Dimension("carrier", t => t("carrier")))
      .withMeasures(Measure("total_passengers", t => sum(t("passengers"))))

    val rows = st.groupBy("carrier").aggregate("total_passengers")
      .orderBy("carrier")  // bare string → ascending
      .execute(spark).collect()
      .map(r => r.getAs[String]("carrier"))

    rows shouldBe Seq("AA", "DL", "UA")
  }

  test("Phase 5 query: one-shot bundle (where + groupBy + having + orderBy + limit)") {
    val st = toSemanticTable(flightsDf, name = Some("flights"))
      .withDimensions(Dimension("carrier", t => t("carrier")))
      .withMeasures(Measure("total_passengers", t => sum(t("passengers"))))

    // HAVING total > 600 excludes AA (550). orderBy desc → DL, UA.
    val rows = st.query(
      measures   = Seq("total_passengers"),
      dimensions = Seq("carrier"),
      having     = Some("total_passengers" > 600),
      orderBy    = Seq(SortKey.desc("total_passengers")),
      limit      = Some(5),
    ).execute(spark).collect()
      .map(r => r.getAs[String]("carrier"))

    rows shouldBe Seq("DL", "UA")
  }

  test("Phase 5 query: no-group grand total (single row) + limit 1") {
    val st = toSemanticTable(flightsDf, name = Some("flights"))
      .withMeasures(Measure("total_passengers", t => sum(t("passengers"))))

    val rows = st.query(measures = Seq("total_passengers"), limit = Some(1))
      .execute(spark).collect()

    rows should have length 1
    rows.head.getAs[Long]("total_passengers") shouldBe 2375L
  }

  // ---- Phase 6: Time semantics (grain truncation + time_range + validation) ----

  test("Phase 6 atTimeGrain: groups by truncated month") {
    val st = toSemanticTable(flightsWithTimeDf, name = Some("flights"))
      .withDimensions(Dimension.time("ts", t => t("ts")))
      .withMeasures(Measure("total_passengers", t => sum(t("passengers"))))

    // atTimeGrain overrides the dimension's expr (a model transform), applied BEFORE
    // groupBy — faithful to BSL's with_dimensions-before-group_by ordering.
    val rows = st.atTimeGrain("ts", "month").groupBy("ts")
      .aggregate("total_passengers").execute(spark).collect()
      .map(r => r.getAs[java.sql.Timestamp]("ts").toLocalDateTime.toLocalDate.toString ->
              r.getAs[Long]("total_passengers"))
      .toMap

    // 3 months, each summing 475 (50+75+100+60+80+110).
    rows shouldBe Map("2024-01-01" -> 475L, "2024-02-01" -> 475L, "2024-03-01" -> 475L)
  }

  test("Phase 6 atTimeGrain: TIME_GRAIN_ canonical name works too") {
    val st = toSemanticTable(flightsWithTimeDf, name = Some("flights"))
      .withDimensions(Dimension.time("ts", t => t("ts")))
      .withMeasures(Measure("total_passengers", t => sum(t("passengers"))))

    val rows = st.atTimeGrain("ts", "TIME_GRAIN_MONTH").groupBy("ts")
      .aggregate("total_passengers").execute(spark).collect()
    rows should have length 3
  }

  test("Phase 6 atTimeGrain: grain finer than smallestTimeGrain raises") {
    val st = toSemanticTable(flightsWithTimeDf, name = Some("flights"))
      .withDimensions(Dimension.time("ts", t => t("ts"), smallestTimeGrain = Some("day")))
      .withMeasures(Measure("total_passengers", t => sum(t("passengers"))))

    // 'hour' is finer than 'day' → must raise.
    val ex = intercept[IllegalArgumentException] {
      st.atTimeGrain("ts", "hour")
    }
    ex.getMessage should include ("finer than")
  }

  test("Phase 6 atTimeGrain: non-time dimension raises a clear error") {
    val st = toSemanticTable(flightsDf, name = Some("flights"))
      .withDimensions(Dimension("carrier", t => t("carrier")))  // not a time dim
      .withMeasures(Measure("total_passengers", t => sum(t("passengers"))))

    val ex = intercept[IllegalArgumentException] {
      st.atTimeGrain("carrier", "month")
    }
    ex.getMessage should include ("not a time dimension")
  }

  test("Phase 6 query: timeGrain truncates the time dimension") {
    val st = toSemanticTable(flightsWithTimeDf, name = Some("flights"))
      .withDimensions(Dimension.time("ts", t => t("ts")))
      .withMeasures(Measure("total_passengers", t => sum(t("passengers"))))

    val rows = st.query(
      measures   = Seq("total_passengers"),
      dimensions = Seq("ts"),
      timeGrain  = Some("month"),
      orderBy    = Seq("ts"),
    ).execute(spark).collect().map(r => r.getAs[Long]("total_passengers"))

    rows shouldBe Seq(475L, 475L, 475L)
  }

  test("Phase 6 query: timeRange filters rows by raw timestamp (pre-truncation)") {
    val st = toSemanticTable(flightsWithTimeDf, name = Some("flights"))
      .withDimensions(Dimension.time("ts", t => t("ts")))
      .withMeasures(Measure("total_passengers", t => sum(t("passengers"))))

    // Range Jan–Feb inclusive: excludes the March flights. Group by month → 2 buckets.
    val rows = st.query(
      measures   = Seq("total_passengers"),
      dimensions = Seq("ts"),
      timeRange  = Some(("2024-01-01", "2024-02-28")),
      timeGrain  = Some("month"),
      orderBy    = Seq("ts"),
    ).execute(spark).collect().map(r => r.getAs[Long]("total_passengers"))

    rows shouldBe Seq(475L, 475L)
  }

  // ---- Phase B: observability (explain / SemanticLogger) --------------------

  test("Phase B: explain() returns a non-trivial op-tree summary") {
    val st = toSemanticTable(flightsDf, name = Some("flights"))
      .withDimensions(Dimension("carrier", t => t("carrier")))
      .withMeasures(
        Measure("total_passengers", t => sum(t("passengers"))),
        Measure("flight_count",     t => count(lit(1))),
      )
      .where("carrier" === "AA")
      .groupBy("carrier")
      .aggregate("total_passengers", "flight_count")

    val plan = st.explain()
    plan should include("table:")
    plan should include("filter(carrier = AA)")   // Predicate.describe output
    plan should include("aggregate")
    plan should include("total_passengers")
    plan should include("flight_count")
  }

  test("Phase B: explain(spark) returns Spark plan output (non-empty)") {
    val st = toSemanticTable(flightsDf, name = Some("flights"))
      .withDimensions(Dimension("carrier", t => t("carrier")))
      .withMeasures(Measure("total_passengers", t => sum(t("passengers"))))
      .groupBy("carrier")
      .aggregate("total_passengers")

    val sparkPlan = st.explain(spark)
    sparkPlan should not be empty
    sparkPlan should include("=")  // basic explain has === separators
  }

  test("Phase B: explainExtended returns the extended/cost-mode plan") {
    // The extended plan is a strict superset of the simple one — it includes
    // Parsed / Analyzed / Optimized logical-plan sections in addition to the
    // physical plan. Those logical-plan sections are unique to extended mode;
    // if `explainExtended` is ever silently downgraded to simple, this test
    // catches it.
    val st = toSemanticTable(flightsDf, name = Some("flights"))
      .withDimensions(Dimension("carrier", t => t("carrier")))
      .withMeasures(Measure("total_passengers", t => sum(t("passengers"))))
      .groupBy("carrier")
      .aggregate("total_passengers")

    val simple   = st.explain(spark)
    val extended = st.explainExtended(spark)

    extended should not be empty
    extended should include("== Physical Plan ==")
    // Only extended mode emits logical-plan sections. This is the discriminator.
    extended should include("== Parsed Logical Plan ==")
    extended should include("== Optimized Logical Plan ==")
    // Sanity: extended is strictly larger than simple (logical plans add bytes).
    extended.length should be > simple.length
  }

  test("Phase B: SemanticLogger emits DEBUG classification for a calc measure") {
    // This test verifies the logger is wired correctly: no crash, non-empty debug output.
    // The DEBUG output goes to the test log (captured by ScalaTest's reporter).
    val st = toSemanticTable(flightsDf, name = Some("flights"))
      .withDimensions(Dimension("carrier", t => t("carrier")))
      .withMeasures(
        Measure("total_passengers", t => sum(t("passengers"))),
        Measure("flight_count",     t => count(lit(1))),
        Measure("avg_passengers",   t => t("total_passengers") / t("flight_count")),
      )
      .groupBy("carrier")
      .aggregate("avg_passengers")

    // If SemanticLogger is wired, this compiles and runs without crash.
    // The DEBUG messages (base/calc classification, layers) go to the test log.
    st.execute(spark).collect() should have size 3
  }

  // -------------------------------------------------------------------------
  // Phase D: Metastore + Catalog + Schema
  // -------------------------------------------------------------------------

  test("catalog accessors — dimensions and measures") {
    val st = toSemanticTable(flightsDf, name = Some("flights"))
      .withDimensions(
        Dimension("carrier", t => t("carrier")),
        Dimension("origin", t => t("origin")),
      )
      .withMeasures(
        Measure("total_passengers", t => sum(t("passengers"))),
        Measure("flight_count", t => count(lit(1))),
        Measure("avg_passengers", t => t("total_passengers") / t("flight_count")),
      )

    st.dimensions.keys should contain theSameElementsAs Set("carrier", "origin")
    st.measures.keySet should contain theSameElementsAs Set(
      "total_passengers", "flight_count", "avg_passengers",
    )
    st.findDimension("carrier") shouldBe defined
    st.findDimension("nonexistent") shouldBe empty
    st.findMeasure("total_passengers") shouldBe defined
    st.findMeasure("nonexistent") shouldBe empty
  }

  test("catalog accessors — joined table") {
    val leftDf  = spark.createDataFrame(Seq((1, "Alice"), (2, "Bob"))).toDF("id", "name")
    val rightDf = spark.createDataFrame(Seq((1, 100), (2, 200))).toDF("id", "score")

    val joined = toSemanticTable(leftDf)
      .withDimensions(
        Dimension("id",   t => t("id")),
        Dimension("name", t => t("name")),
      )
      .join_one(
        toSemanticTable(rightDf)
          .withDimensions(Dimension("id",    t => t("id")))
          .withMeasures(Measure("score_sum", t => sum(t("score")))),
        (l, r) => l("id") === r("id"),
      )

    joined.dimensions.keys.toSet should contain theSameElementsAs Set("id", "name")
    joined.measures.keySet should contain ("score_sum")
  }

  test("createOrReplaceTempView — query via Spark SQL") {
    val st = toSemanticTable(flightsDf, name = Some("flights"))
      .withDimensions(Dimension("carrier", t => t("carrier")))
      .withMeasures(Measure("total_passengers", t => sum(t("passengers"))))
      .groupBy("carrier")
      .aggregate("total_passengers")

    implicit val sparkSession = spark
    st.createOrReplaceTempView("flights_view")

    val result = spark.sql("SELECT carrier, total_passengers FROM flights_view ORDER BY carrier")
    val rows = result.collect()
    rows should have size 3
    rows.map(_.getString(0)).sorted shouldEqual Seq("AA", "DL", "UA")
  }

  test("previewSchema — returns correct types without executing rows") {
    val st = toSemanticTable(flightsDf, name = Some("flights"))
      .withDimensions(Dimension("carrier", t => t("carrier")))
      .withMeasures(
        Measure("total_passengers", t => sum(t("passengers"))),
        Measure("flight_count",     t => count(lit(1))),
        Measure("avg_passengers", t => t("total_passengers") / t("flight_count")),
      )
      .groupBy("carrier")
      .aggregate("total_passengers", "avg_passengers")

    val schema = st.previewSchema(spark)
    // allMeasuresClosed pulls in flight_count transitively (referenced by avg_passengers),
    // so the output includes it even though only total_passengers and avg_passengers
    // were explicitly requested.
    schema.fieldNames should contain theSameElementsAs
      Seq("carrier", "total_passengers", "flight_count", "avg_passengers")
    // Dimension columns keep their source type; measure columns are the result of aggregation
    val carrierField = schema.apply("carrier")
    (carrierField.dataType.typeName) should include("string")
    val passengersField = schema.apply("total_passengers")
    (passengersField.dataType.typeName) should include("long")
  }
}
