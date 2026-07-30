package io.semanticdf

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Regression tests for the data-design audit fix to [[Dimension]]:
 * the `derived` field is only legal on time dimensions. PR #290 added
 * a `require(derived.isEmpty || isTimeDimension, ...)` in the primary
 * constructor body. This file pins the invariant.
 *
 * Without these tests, a future regression (someone deleting the
 * `require`, or weakening the predicate) would not be caught.
 *
 * Pure unit tests — no SparkSession needed. The constructor `require`
 * fires before any DataFrame would be touched, so a session fixture
 * would only add CI startup cost.
 */
class DimensionDataDesignSpec extends AnyFunSuite with Matchers {

  test("Dimension.time(..., derived = Seq('year')) is accepted") {
    val d = Dimension.time(
      "flight_date",
      t => t("flight_date"),
      derived = Seq("year"),
    )
    d.derived shouldBe Seq("year")
    d.isTimeDimension shouldBe true
  }

  test("Dimension with derived on a plain dim is rejected at construction") {
    // `Dimension.apply` doesn't expose `derived` (only `Dimension.time` does),
    // so we build via the primary constructor to exercise the require.
    val ex = intercept[IllegalArgumentException] {
      new Dimension(
        name = "carrier",
        expr = (t: SemanticScope) => t("carrier"),
        isTimeDimension = false,
        derived = Seq("year"),
      )
    }
    ex.getMessage should include ("not a time dimension")
  }

  test("Dimension.copy(derived = Seq('year'), isTimeDimension = false) is rejected") {
    // This is the exact regression scenario the audit cited: a caller
    // builds a time dim, then .copy()'s it to a non-time dim while
    // leaving `derived` set. The `require` must fire.
    val time = Dimension.time("flight_date", t => t("flight_date"), derived = Seq("year"))
    val ex = intercept[IllegalArgumentException] {
      time.copy(isEntity = true, isTimeDimension = false)
    }
    ex.getMessage should include ("not a time dimension")
  }

  test("Dimension.copy can clear derived while preserving time-dimension status") {
    val time = Dimension.time("flight_date", t => t("flight_date"), derived = Seq("year"))
    val cleared = time.copy(derived = Seq.empty)
    cleared.derived shouldBe Seq.empty
    cleared.isTimeDimension shouldBe true
  }
}
