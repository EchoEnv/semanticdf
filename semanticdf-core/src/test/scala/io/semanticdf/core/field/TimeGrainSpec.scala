package io.semanticdf.core.field

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Phase 1 consolidation mirror: `io.semanticdf.core.field.TimeGrain`.
  *
  * Verifies the engine-portable mirror of `TimeGrain`'s data parts:
  *  - `Order` is the canonical grain list, finest→coarsest
  *  - `normalize` accepts short, canonical, lowercase, and "TIME_GRAIN_"
  *    prefix forms; throws on unknown grains
  *  - `isCoarserOrEqual` correctly compares grain fineness
  *
  * No Spark imports; verifiable by inspection. */
class TimeGrainSpec extends AnyFunSuite with Matchers {

  test("Order is the canonical 8-grain list, finest→coarsest") {
    TimeGrain.Order shouldBe Seq(
      "SECOND", "MINUTE", "HOUR", "DAY",
      "WEEK", "MONTH", "QUARTER", "YEAR",
    )
    TimeGrain.Order.size shouldBe 8
  }

  test("Grain type alias is String") {
    val g: TimeGrain.Grain = "MONTH"
    g shouldBe "MONTH"
  }

  test("normalize accepts canonical uppercase") {
    TimeGrain.normalize("SECOND") shouldBe "SECOND"
    TimeGrain.normalize("YEAR") shouldBe "YEAR"
  }

  test("normalize accepts lowercase") {
    TimeGrain.normalize("second") shouldBe "SECOND"
    TimeGrain.normalize("month") shouldBe "MONTH"
  }

  test("normalize accepts TIME_GRAIN_<NAME> prefix") {
    TimeGrain.normalize("TIME_GRAIN_MONTH") shouldBe "MONTH"
    TimeGrain.normalize("TIME_GRAIN_year") shouldBe "YEAR"
  }

  test("normalize throws IllegalArgumentException on unknown grain") {
    val ex = intercept[IllegalArgumentException] {
      TimeGrain.normalize("FORTNIGHT")
    }
    ex.getMessage should include ("Invalid time grain 'FORTNIGHT'")
  }

  test("isCoarserOrEqual: same grain -> true") {
    TimeGrain.isCoarserOrEqual("MONTH", "MONTH") shouldBe true
  }

  test("isCoarserOrEqual: coarser requested -> true") {
    TimeGrain.isCoarserOrEqual("YEAR", "MONTH") shouldBe true
    TimeGrain.isCoarserOrEqual("MONTH", "DAY") shouldBe true
  }

  test("isCoarserOrEqual: finer requested -> false") {
    TimeGrain.isCoarserOrEqual("DAY", "MONTH") shouldBe false
    TimeGrain.isCoarserOrEqual("SECOND", "HOUR") shouldBe false
  }

  test("isCoarserOrEqual: accepts mixed-case input") {
    TimeGrain.isCoarserOrEqual("year", "month") shouldBe true
    TimeGrain.isCoarserOrEqual("TIME_GRAIN_DAY", "TIME_GRAIN_MONTH") shouldBe false
  }

  test("isCoarserOrEqual: throws on unknown grain (via normalize)") {
    // isCoarserOrEqual calls normalize internally, so the first error
    // comes from normalize with the canonical "Invalid time grain" message.
    val ex = intercept[IllegalArgumentException] {
      TimeGrain.isCoarserOrEqual("FORTNIGHT", "MONTH")
    }
    ex.getMessage should include ("Invalid time grain")
  }

  test("Data-driven: pure function — same input always produces same output") {
    val r1 = TimeGrain.normalize("month")
    val r2 = TimeGrain.normalize("month")
    r1 shouldBe r2
  }
}