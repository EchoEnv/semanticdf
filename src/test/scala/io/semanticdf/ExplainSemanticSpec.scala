package io.semanticdf

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

  test("explainSemantic splits measures into DIRECT + PULLED IN") {
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
    // All three are computed: avg_passengers (DIRECT) + flight_count + total_passengers (PULLED IN).
    plan should include("avg_passengers")
    plan should include("flight_count")
    plan should include("total_passengers")
    // The split into DIRECT vs PULLED IN sub-blocks.
    plan should include("DIRECT")
    plan should include("PULLED IN")
    // Old labels are gone — they were either redundant or jargon.
    plan should not include "REQUESTED"
    plan should not include "AUTO-PULLED"
    plan should not include "Will compute:"
  }

  test("explainSemantic omits PULLED IN block when no transitive work") {
    // Single base measure, aggregate it. No calc deps at all, no sibling
    // measures to skip — so neither PULLED IN nor SKIPPED should appear.
    val st = toSemanticTable(flightsDf, name = Some("flights"))
      .withDimensions(Dimension("carrier", t => t("carrier")))
      .withMeasures(Measure("total_passengers", t => sum(t("passengers"))))
      .groupBy("carrier")
      .aggregate("total_passengers")

    val plan = st.explainSemantic(spark)
    plan should include("DIRECT")
    plan should include("total_passengers")
    plan should not include "PULLED IN"
    plan should not include "SKIPPED"
  }

  test("explainSemantic falls back to hint when no .aggregate called") {
    // A table with measures but no aggregation — the user hasn't asked yet.
    val st = toSemanticTable(flightsDf, name = Some("flights"))
      .withMeasures(Measure("total_passengers", t => sum(t("passengers"))))

    val plan = st.explainSemantic(spark)
    plan should include("declared measures available")
    plan should include(".aggregate / .orderBy selected none")
    plan should not include "DIRECT"
  }

  test("explainSemantic lists declared-but-not-needed measures under SKIPPED") {
    // extra_distance is declared but never referenced by any requested measure.
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
    plan should include("SKIPPED (declared but unused)")
    plan should include("extra_distance")
    plan should not include "Skipped (not needed)"  // old label
  }

  // ---- Section polish -------------------------------------------------------

  test("explainSemantic PLAN SUMMARY has no redundant 'filters:'/'joins:' counters") {
    // Multiple filters and joins — summary should NOT enumerate them; their own
    // sections cover them.
    val customers = toSemanticTable(customersDf, name = Some("customers"))
      .withDimensions(Dimension("customer_id", t => t("customer_id")))
      .withMeasures(Measure("customer_count", t => count(lit(1))))
    val orders = toSemanticTable(ordersDf, name = Some("orders"))
      .withDimensions(
        Dimension("order_id",    t => t("order_id")),
        Dimension("customer_id", t => t("customer_id")),
      )
      .withMeasures(Measure("order_count", t => count(lit(1))))
      .join_one(customers, on = (l, r) => l("customer_id") === r("customer_id"))
      .where("customer_id" === "C001")
      .having("order_count" > 0)
      .groupBy("customer_id")
      .aggregate("order_count")

    val plan = orders.explainSemantic(spark)
    val summary = plan.linesIterator.takeWhile(_.trim.nonEmpty || true)
      .takeWhile(!_.startsWith("─")).mkString("\n")
    summary should not include "filters:"
    summary should not include "joins:"
  }

  test("explainSemantic DIMENSIONS/MEASURES headers compact when not collapsed") {
    // When nothing is collapsed away, header is just "(N)".
    val st = toSemanticTable(flightsDf, name = Some("flights"))
      .withDimensions(Dimension("carrier", t => t("carrier")))
      .withMeasures(Measure("flight_count", t => count(lit(1))))
      .groupBy("carrier")
      .aggregate("flight_count")

    val plan = st.explainSemantic(spark)  // default Scope.All, nothing collapsed
    plan should include("DIMENSIONS (1)")
    plan should include("MEASURES (1)")
    plan should not include "used /"
    plan should not include "of 1 declared"
  }

  test("explainSemantic DIMENSIONS/MEASURES headers show 'N of M' when collapsed") {
    val st = toSemanticTable(flightsDf, name = Some("flights"))
      .withDimensions(
        Dimension("carrier", t => t("carrier")),
        Dimension("origin",  t => t("origin")),
        Dimension("dest",    t => t("dest")),
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

    val plan = st.explainSemantic(spark, Scope.Used)
    plan should include("DIMENSIONS (1 of 4)")
    plan should include("MEASURES (3 of 4)")
  }

  test("explainSemantic JOINS section surfaces the join key inline") {
    val customers = toSemanticTable(customersDf, name = Some("customers"))
      .withDimensions(Dimension("customer_id", t => t("customer_id")))
      .withMeasures(Measure("customer_count", t => count(lit(1))))
    val orders = toSemanticTable(ordersDf, name = Some("orders"))
      .withDimensions(
        Dimension("order_id",    t => t("order_id")),
        Dimension("customer_id", t => t("customer_id")),
      )
      .withMeasures(Measure("order_count", t => count(lit(1))))
      .join_one(customers, on = (l, r) => l("customer_id") === r("customer_id"))

    // Force a compile() so the per-thread grain-key dynamic is populated.
    orders.execute(spark)
    val plan = orders.explainSemantic(spark)
    plan should include("LEFT JOIN on [customer_id]")
    plan should include("ONE")
    // The old "join keys visible in SPARK PLAN below" filler must be gone.
    plan should not include "join keys visible in SPARK PLAN below"
  }

  test("explainSemantic JOINS keys are isolated per SemanticJoinOp instance, not shared") {
    // Replaces a `var _grainCols` field on the case class (which violated the
    // immutability invariant in DESIGN §4.4) with a `scala.util.DynamicVariable`.
    // Each SemanticJoinOp instance gets its own DynamicVariable, so two distinct
    // join ops don't share state. Compiling one must NOT leak into the other.
    val customers = toSemanticTable(customersDf, name = Some("customers"))
      .withDimensions(Dimension("customer_id", t => t("customer_id")))
      .withMeasures(Measure("customer_count", t => count(lit(1))))

    val orders1 = toSemanticTable(ordersDf, name = Some("orders"))
      .withDimensions(
        Dimension("order_id",    t => t("order_id")),
        Dimension("customer_id", t => t("customer_id")),
      )
      .withMeasures(Measure("order_count", t => count(lit(1))))
      .join_one(customers, on = (l, r) => l("customer_id") === r("customer_id"))

    // Second independent join: re-use the same orders one but with a fresh
    // join_one call. Produces a brand new SemanticJoinOp instance with its own
    // DynamicVariable slot.
    val orders2 = toSemanticTable(ordersDf, name = Some("orders"))
      .withDimensions(
        Dimension("order_id",    t => t("order_id")),
        Dimension("customer_id", t => t("customer_id")),
      )
      .withMeasures(Measure("order_count", t => count(lit(1))))
      .join_one(customers, on = (l, r) => l("customer_id") === r("customer_id"))

    orders1.execute(spark)  // populates orders1's DynamicVariable

    // orders2 has NOT been compiled. If the case class's grain state were
    // shared (the old `var _grainCols` design), orders2 would falsely surface
    // customer_id too. With DynamicVariable, orders2's instance has its own
    // empty slot, and the renderer falls back to "(uncompiled)".
    val plan2 = orders2.explainSemantic(None)
    plan2 should include("(uncompiled)")
    plan2 should not include "on [customer_id]"

    // Sanity: orders1 (compiled on this thread) DOES show its keys.
    orders1.explainSemantic(None) should include("LEFT JOIN on [customer_id]")
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
    // pct_of_passengers uses grand totals. Because total_passengers is already
    // a dep (it appears in the calc formula), the renderer annotates it inline
    //   pulls in: total_passengers (as grand total)
    // plus a legend line "(grand totals via 1-row cross-join)".
    plan should include("(as grand total)")
    plan should include("total_passengers")
    plan should include("grand totals via 1-row cross-join")
    // avg_passengers is a plain calc (no t.all) — no grand-total annotation.
    val grandTotalMarkers = "as grand total".r.findAllMatchIn(plan).length
    grandTotalMarkers shouldBe 1
  }

  test("explainSemantic surfaces standalone grand totals (not in deps)") {
    // A calc that uses t.all(X) but doesn't reference X as a column — the
    // standalone totals line should appear.
    val st = toSemanticTable(flightsDf, name = Some("flights"))
      .withDimensions(Dimension("carrier", t => t("carrier")))
      .withMeasures(
        Measure("total_passengers", t => sum(t("passengers"))),
        // pct_no_dep uses total_passengers ONLY as a grand total, not as a column.
        Measure("pct_no_dep", t => lit(100.0) / t.all("total_passengers")),
      )
      .groupBy("carrier")
      .aggregate("pct_no_dep")

    val plan = st.explainSemantic(spark)
    plan should include("uses grand totals")
    plan should include("total_passengers")
    plan should include("(1-row cross-join)")
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
