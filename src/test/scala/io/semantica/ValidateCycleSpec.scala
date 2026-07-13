package io.semantica

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Regression test for the `validate()` cycle-detection fix.
  *
  * Background: `validate()` advertised "compile-free structural check" including
  * calc-dependency cycle detection. The detection logic compared `Measure.expr.toString`
  * (a JVM lambda's class + identity hash) against the user-written source — the
  * user source never appears in `Function1.toString`, so the check silently missed
  * every cycle. The runtime check in `SemanticAggregateOp.topologicalLayers` is the
  * real defense and is covered by an existing Phase 2a test. Fix: delete the broken
  * detection so the contract is honest, and update the doc comment to point to the
  * runtime check.
  */
class ValidateCycleSpec extends AnyFunSuite with Matchers with SparkSessionFixture with FlightsFixture {

  test("REGRESSION: validate() does not falsely report a cycle (compile-free contract is honest)") {
    // A genuine cycle a→b→a. Old validate() would (incorrectly) return no errors
    // because expr.toString never contained the user source. After the fix,
    // validate() no longer claims to detect cycles; the contract is honest.
    val st = toSemanticTable(flightsDf, name = Some("flights"))
      .withMeasures(
        Measure("a", t => t("b")),
        Measure("b", t => t("a")),
      )
    val result = st.validate()
    // No crash, no false claim of cycle detection.
    result.isValid shouldBe true
    result.errors shouldBe empty
  }

  test("REGRESSION: runtime still catches the cycle (defense-in-depth preserved)") {
    // Confirms the runtime check (topologicalLayers) still fires, so removing
    // the broken compile-time check doesn't leave users unprotected.
    val st = toSemanticTable(flightsDf, name = Some("flights"))
      .withMeasures(
        Measure("a", t => t("b")),
        Measure("b", t => t("a")),
      )
    val ex = intercept[Exception] {
      st.groupBy().aggregate("a").execute(spark).collect()
    }
    ex.getMessage.toLowerCase should include ("cycle")
  }
}