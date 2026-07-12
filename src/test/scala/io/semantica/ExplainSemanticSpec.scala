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

    // Pass `null` to skip Spark compilation — the rest of the sections still render.
    val plan = st.explainSemantic(null)
    plan should include("PLAN SUMMARY")
    plan should not include "SPARK PLAN"
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

  test("explainSemantic lists requested measures and skipped ones") {
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
    plan should include("avg_passengers")
    plan should include("flight_count")
    plan should include("total_passengers")
    // Only avg_passengers is requested; others are skipped
    plan should include("Will compute:")
    plan should include("avg_passengers")
    plan should include("Skipped (not needed)")
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