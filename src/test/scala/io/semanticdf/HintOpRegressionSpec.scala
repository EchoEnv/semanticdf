package io.semanticdf

import org.apache.spark.sql.functions.{count, lit, sum}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import Predicate._

/** Regression tests for `SemanticHintOp` tree-walk coverage.
  *
  * Background: `withHint(...)` wraps the model in a `SemanticHintOp`. The
  * op tree is supposed to be a closed algebra — every catalog/explain/validate
  * method walks it. Before the fix, HintOp was missing from 8+ walkers, so any
  * `.dimensions`, `.explain()`, `.explainSemantic()`, `.validate()`, `.schema()`,
  * etc. called on a hint-rooted table threw `scala.MatchError`.
  *
  * These tests pin the fix: HintOp must be a transparent pass-through for
  * non-compile concerns (catalog access, plan introspection, validation), the
  * same way `Filter`/`OrderBy`/`Limit` already are.
  */
class HintOpRegressionSpec extends AnyFunSuite with Matchers with SparkSessionFixture with FlightsFixture {

  private def hinted: SemanticTable =
    toSemanticTable(flightsDf, name = Some("flights"))
      .withDimensions(
        Dimension("carrier", t => t("carrier")),
        Dimension("origin",  t => t("origin")),
      )
      .withMeasures(
        Measure("total_passengers", t => sum(t("passengers"))),
        Measure("flight_count",     t => count(lit(1))),
        Measure("avg_passengers",   t => t("total_passengers") / t("flight_count")),
      )
      .groupBy("carrier")
      .aggregate("total_passengers", "flight_count")
      .withHint("broadcast")

  // ---- Catalog accessors (resolveRootModel path) ------------------------------

  test("REGRESSION: withHint(...).dimensions returns the model dimensions") {
    hinted.dimensions.keys should contain theSameElementsAs Set("carrier", "origin")
  }

  test("REGRESSION: withHint(...).measures returns the model measures") {
    hinted.measures.keySet should contain theSameElementsAs
      Set("total_passengers", "flight_count", "avg_passengers")
  }

  test("REGRESSION: withHint(...).findDimension resolves the dimension") {
    hinted.findDimension("carrier") shouldBe defined
    hinted.findDimension("nonexistent") shouldBe empty
  }

  test("REGRESSION: withHint(...).findMeasure resolves the measure") {
    hinted.findMeasure("total_passengers") shouldBe defined
    hinted.findMeasure("nonexistent") shouldBe empty
  }

  // ---- Plan introspection ----------------------------------------------------

  test("REGRESSION: withHint(...).explain() returns the op-tree summary") {
    val plan = hinted.explain()
    plan should include("aggregate")
    plan should include("carrier")
    plan should include("total_passengers")
  }

  test("REGRESSION: withHint(...).explainSemantic(spark) renders all sections") {
    val plan = hinted.explainSemantic(spark)
    plan should include("PLAN SUMMARY")
    plan should include("DIMENSIONS")
    plan should include("MEASURES")
    plan should include("SPARK PLAN")
  }

  // ---- Compile-free validation -----------------------------------------------

  test("REGRESSION: withHint(...).validate() does not crash") {
    val result = hinted.validate()
    result.isValid shouldBe true
    result.errors shouldBe empty
  }

  // ---- schema(spark) — analogue of df.schema ---------------------------------

  test("REGRESSION: withHint(...).schema(spark) returns a one-row-per-field DataFrame") {
    val schema = hinted.schema(spark)
    schema.columns should contain ("field_name")
    val rows = schema.collect()
    rows.map(_.getAs[String]("field_name")).toSet should contain allOf (
      "carrier", "origin", "total_passengers", "flight_count", "avg_passengers",
    )
  }

  // ---- withDimensions passthrough: hint stays outermost ---------------------

  // ---- Filter routing (where() walks via resolveAllMeasureNames) ------------

  test("REGRESSION: withHint(...).where(measure_ref) routes to HAVING correctly") {
    // Before the fix: resolveAllMeasureNames fell through to rootModel which
    // didn't handle HintOp, returned empty set, so splitFilter silently routed
    // the predicate to pre-agg WHERE. Spark then threw at runtime because the
    // measure column doesn't exist pre-agg.
    // After the fix: resolveAllMeasureNames returns the correct set; splitFilter
    // correctly routes the predicate to HAVING (postAggPredicates), which is
    // wrapped at aggregate time and applied to the result.
    val st = toSemanticTable(flightsDf, name = Some("flights"))
      .withDimensions(Dimension("carrier", t => t("carrier")))
      .withMeasures(Measure("total_passengers", t => sum(t("passengers"))))
      .where("total_passengers" > 600)                  // post-agg
      .groupBy("carrier")
      .aggregate("total_passengers")                    // wraps postAggPredicates as HAVING
      .withHint("broadcast")                            // hint at outermost

    val rows = st.execute(spark).collect()
      .map(r => r.getAs[String]("carrier"))
      .toSet
    // FlightsFixture golden: AA=550 (excluded by >600), UA=775, DL=1050.
    rows shouldBe Set("UA", "DL")
  }

  test("REGRESSION: chain withDimensions -> aggregate -> withHint works end-to-end") {
    // The natural chain order: model with dims/measures, aggregate, then hint.
    // Before the fix, calling .dimensions() on the result threw MatchError
    // because resolveRootModel didn't handle SemanticHintOp.
    val st = toSemanticTable(flightsDf, name = Some("flights"))
      .withDimensions(
        Dimension("carrier", t => t("carrier")),
        Dimension("origin",  t => t("origin")),
      )
      .withMeasures(Measure("total_passengers", t => sum(t("passengers"))))
      .groupBy("carrier")
      .aggregate("total_passengers")
      .withHint("broadcast")

    // Catalog access works through the hint.
    st.dimensions.keys should contain ("carrier")
    st.dimensions.keys should contain ("origin")
    // Execution still works with the hint applied.
    noException should be thrownBy st.execute(spark).collect()
  }

  // ---- Execution still works (the original behavior) -------------------------

  test("REGRESSION: withHint(...) still produces a working DataFrame") {
    // The original Phase B test only checked the analyzed plan; this regression
    // pins that hint at the outermost level doesn't break the execution path.
    val rows = hinted.execute(spark).collect()
    rows.length shouldBe 3
  }
}
