package io.semantica

import org.apache.spark.sql.functions.{count, lit, sum}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import Predicate._

/** Regression tests for SemanticTable.explainSemantic (Tier 1.5). */
class ExplainSemanticSpec
    extends AnyFunSuite
    with Matchers
    with SparkSessionFixture
    with FlightsFixture {

  // ---- Section presence ----------------------------------------------------

  test("explainSemantic includes all standard section headings") {
    val st = toSemanticTable(flightsDf, name = Some("flights"))
      .withDimensions(Dimension("carrier", t => t("carrier")))
      .withMeasures(Measure("flight_count", t => count(lit(1))))
      .groupBy("carrier")
      .aggregate("flight_count")

    val plan = st.explainSemantic(spark)

    plan should include("PLAN SUMMARY")
    plan should include("SEMANTIC ROUTING")
    plan should include("TRANSITIVE DEPENDENCIES")
    plan should include("DIMENSIONS")
    plan should include("MEASURES")
    plan should include("JOINS")
    plan should include("SPARK PLAN")
  }

  test("explainSemantic demo: full query with filter, calc measure, and join") {
    val customers = toSemanticTable(customersDf, name = Some("customers"))
      .withDimensions(Dimension("customer_id", t => t("customer_id")))
      .withMeasures(Measure("customer_count", t => count(lit(1))))

    val orders = toSemanticTable(ordersDf, name = Some("orders"))
      .withDimensions(
        Dimension("order_id",    t => t("order_id")),
        Dimension("customer_id", t => t("customer_id")),
      )
      .withMeasures(
        Measure("order_count",   t => count(lit(1))),
        Measure("orders_per_customer", t => t("order_count") / t("customer_count")),
      )
      .join_one(customers,
        on = (l, r) => l("customer_id") === r("customer_id"))
      .where("customer_id" === "C001")
      .groupBy("customer_id")
      .aggregate("orders_per_customer")

    val plan = orders.explainSemantic(spark)

    // Joined tables both shown in PLAN SUMMARY
    plan should include("orders")
    plan should include("customers")
    plan should include("WHERE")
    // calc measure classification
    plan should include("orders_per_customer")
    plan should include("[calc]")
  }

  test("explainSemantic works without spark (skip SPARK PLAN section)") {
    val st = toSemanticTable(flightsDf, name = Some("flights"))
      .withMeasures(Measure("flight_count", t => count(lit(1))))
      .groupBy()
      .aggregate("flight_count")

    // Pass `None` to skip Spark compilation — the rest of the sections still render.
    val plan = st.explainSemantic(None)
    plan should include("PLAN SUMMARY")
    plan should not include "SPARK PLAN"
  }

  test("explainSemantic SparkSession overload still works for backwards compat") {
    val st = toSemanticTable(flightsDf, name = Some("flights"))
      .withMeasures(Measure("flight_count", t => count(lit(1))))
      .groupBy("carrier")
      .aggregate("flight_count")

    // Pass an actual SparkSession — the non-null overload should compile the plan.
    val plan = st.explainSemantic(spark)
    plan should include("SPARK PLAN")
  }

  // ---- Routing decisions ----------------------------------------------------

  test("explainSemantic routes a dimension filter to WHERE") {
    val st = toSemanticTable(flightsDf, name = Some("flights"))
      .withDimensions(Dimension("carrier", t => t("carrier")))
      .withMeasures(Measure("flight_count", t => count(lit(1))))
      .where("carrier" === "AA")
      .groupBy("carrier")
      .aggregate("flight_count")

    val plan = st.explainSemantic(spark)
    plan should include("WHERE")
    plan should include("carrier = AA")
    plan should not include "HAVING"
  }

  test("explainSemantic routes a measure filter to HAVING") {
    val st = toSemanticTable(flightsDf, name = Some("flights"))
      .withDimensions(Dimension("carrier", t => t("carrier")))
      .withMeasures(
        Measure("total_passengers", t => sum(t("passengers"))),
        Measure("flight_count",     t => count(lit(1))),
      )
      .having("total_passengers" > 100)
      .groupBy("carrier")
      .aggregate("total_passengers", "flight_count")

    val plan = st.explainSemantic(spark)
    plan should include("HAVING")
    plan should include("total_passengers > 100")
  }

  // ---- Transitive deps ------------------------------------------------------

  test("explainSemantic lists requested measures and split into REQUESTED + AUTO-PULLED") {
    val st = toSemanticTable(flightsDf, name = Some("flights"))
      .withDimensions(Dimension("carrier", t => t("carrier")))
      .withMeasures(
        Measure("total_passengers", t => sum(t("passengers"))),
        Measure("flight_count",     t => count(lit(1))),
        Measure("avg_passengers",   t => t("total_passengers") / t("flight_count")),
      )
      .groupBy("carrier")
      .aggregate("avg_passengers")

    val plan = st.explainSemantic(spark)
    // All three are computed: avg_passengers (requested) + flight_count + total_passengers (auto-pulled).
    plan should include("avg_passengers")
    plan should include("flight_count")
    plan should include("total_passengers")
    // The split into REQUESTED vs AUTO-PULLED sub-blocks.
    plan should include("REQUESTED")
    plan should include("AUTO-PULLED")
    // Aggregate label still present.
    plan should include("Will compute:")
  }

  test("explainSemantic lists declared-but-not-needed measures under Skipped") {
    // extra_distance is declared but never referenced by any requested measure —
    // it should appear under "Skipped (not needed)".
    val st = toSemanticTable(flightsDf, name = Some("flights"))
      .withDimensions(Dimension("carrier", t => t("carrier")))
      .withMeasures(
        Measure("total_passengers", t => sum(t("passengers"))),
        Measure("flight_count",     t => count(lit(1))),
        Measure("extra_distance",   t => sum(t("distance"))),
      )
      .groupBy("carrier")
      .aggregate("total_passengers", "flight_count")

    val plan = st.explainSemantic(spark)
    plan should include("Skipped (not needed)")
    plan should include("extra_distance")
  }

  // ---- Scope filtering -----------------------------------------------------

  test("explainSemantic Scope.Used collapses dimensions and measures not referenced by the query") {
    // carrier and avg_passengers are referenced by this query. origin, distance,
    // total_passengers, flight_count, extra_distance are declared but NOT used.
    val st = toSemanticTable(flightsDf, name = Some("flights"))
      .withDimensions(
        Dimension("carrier", t => t("carrier")),
        Dimension("origin",  t => t("origin")),
        Dimension("distance", t => t("distance")),
      )
      .withMeasures(
        Measure("total_passengers", t => sum(t("passengers"))),
        Measure("flight_count",     t => count(lit(1))),
        Measure("avg_passengers",   t => t("total_passengers") / t("flight_count")),
        Measure("extra_distance",   t => sum(t("distance"))),
      )
      .groupBy("carrier")
      .aggregate("avg_passengers")

    val usedPlan  = st.explainSemantic(spark, Scope.Used)
    val allPlan   = st.explainSemantic(spark)              // default = All

    // Scope.Used: only the query-touched fields appear in DIMENSIONS / MEASURES
    // (origin is a declared dimension not referenced by groupBy/aggregate/filter/orderBy)
    usedPlan should include("carrier")
    usedPlan should not include "origin"
    usedPlan should include("more declared")
    usedPlan should include("use Scope.All")

    // Default Scope.All preserves the legacy full inventory
    allPlan should include("origin")
    allPlan should include("extra_distance")
    allPlan should not include "more declared"
  }

  // ---- t.all() surface ------------------------------------------------------

  test("explainSemantic surfaces t.all() usage under calc measures") {
    // A calc using t.all(...) should appear with a 'uses grand totals' line so
    // a user can see that a 1-row cross-join is built for percent-of-total.
    val st = toSemanticTable(flightsDf, name = Some("flights"))
      .withDimensions(Dimension("carrier", t => t("carrier")))
      .withMeasures(
        Measure("total_passengers", t => sum(t("passengers"))),
        Measure("flight_count",     t => count(lit(1))),
        Measure("avg_passengers",   t => t("total_passengers") / t("flight_count")),
        Measure("pct_of_passengers",
          t => t("total_passengers") / t.all("total_passengers")),
      )
      .groupBy("carrier")
      .aggregate("avg_passengers", "pct_of_passengers")

    val plan = st.explainSemantic(spark)
    // pct_of_passengers uses grand totals:
    plan should include("uses grand totals")
    plan should include("total_passengers")
    // The label appears once per cent-of-total calc — count it to ensure
    // avg_passengers (a plain calc) does NOT get a 'uses grand totals' line.
    val grandTotalMarkers = "uses grand totals".r.findAllMatchIn(plan).length
    grandTotalMarkers shouldBe 1
  }

  // ---- Joins ----------------------------------------------------------------

  test("explainSemantic describes join cardinality and strategy") {
    val customers = toSemanticTable(customersDf, name = Some("customers"))
      .withDimensions(
        Dimension("customer_id", t => t("customer_id")),
        Dimension("name",        t => t("name")),
      )
      .withMeasures(Measure("customer_count", t => count(lit(1))))

    val orders = toSemanticTable(ordersDf, name = Some("orders"))
      .withDimensions(
        Dimension("order_id",    t => t("order_id")),
        Dimension("customer_id", t => t("customer_id")),
      )
      .withMeasures(Measure("order_count", t => count(lit(1))))
      .join_one(customers,
        on = (l, r) => l("customer_id") === r("customer_id"))

    val plan = orders.explainSemantic(spark)
    plan should include("JOINS")
    plan should include("ONE")
    plan should include("LEFT JOIN")
  }

  // ---- Compound predicate split --------------------------------------------
  test("explainSemantic shows AND(pred=dim, pred=measure) split into WHERE + HAVING") {
    val st = toSemanticTable(flightsDf, name = Some("flights"))
      .withDimensions(Dimension("carrier", t => t("carrier")))
      .withMeasures(
        Measure("total_passengers", t => sum(t("passengers"))),
      )
      .where(("carrier" === "AA") and ("total_passengers" > 100))
      .groupBy("carrier")
      .aggregate("total_passengers")

    val plan = st.explainSemantic(spark)

    // The AND has been split by .where() at compile time:
    //   WHERE  carrier = AA        (pre-agg, dim ref)
    //   HAVING total_passengers > 100  (post-agg, measure ref)
    plan should include("WHERE")
    plan should include("HAVING")
    plan should include("carrier = AA")
    plan should include("total_passengers > 100")
    // Aggregate info still surfaced even though root is wrapped by HAVING
    plan should include("group by:")
    plan should include("total_passengers")
  }


}