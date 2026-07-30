package io.semanticdf

import org.scalatest.funsuite.AnyFunSuite

/**
 * Regression tests for the data-design audit fix to [[Dimension]]:
 * the `derived` field is only legal on time dimensions. PR #290 added
 * a `require(derived.isEmpty || isTimeDimension, ...)` in the primary
 * constructor body. This file pins the invariant.
 *
 * Without these tests, a future regression (someone deleting the
 * `require`, or weakening the predicate) would not be caught.
 */
class DimensionDataDesignSpec extends AnyFunSuite with SparkSessionFixture {

  test("Dimension.time(..., derived = Seq('year')) is accepted") {
    val d = Dimension.time(
      "flight_date",
      t => t("flight_date"),
      derived = Seq("year"),
    )
    assert(d.derived == Seq("year"))
    assert(d.isTimeDimension)
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
    assert(
      ex.getMessage.contains("not a time dimension"),
      s"expected error to mention 'not a time dimension', got: ${ex.getMessage}"
    )
  }

  test("Dimension.copy(derived = Seq('year'), isTimeDimension = false) is rejected") {
    // This is the exact regression scenario the audit cited: a caller
    // builds a time dim, then .copy()'s it to a non-time dim while
    // leaving `derived` set. The `require` must fire.
    val time = Dimension.time("flight_date", t => t("flight_date"), derived = Seq("year"))
    val ex = intercept[IllegalArgumentException] {
      time.copy(isEntity = true, isTimeDimension = false)
    }
    assert(
      ex.getMessage.contains("not a time dimension"),
      s"expected error to mention 'not a time dimension', got: ${ex.getMessage}"
    )
  }

  test("Dimension.time(...).copy() with empty derived is accepted") {
    // Empty `derived` is fine on a time dim (it's the default).
    val time = Dimension.time("flight_date", t => t("flight_date"), derived = Seq("year"))
    val cleared = time.copy(derived = Seq.empty)
    assert(cleared.derived.isEmpty)
    assert(cleared.isTimeDimension)
  }
}
