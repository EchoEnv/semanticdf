package io.semanticdf.rollup

import org.scalatest.funsuite.AnyFunSuite

/** Targeted tests for H4-α and H4-β.
  *
  * H4-α: RollupFreshness.NoTracking is unreachable from the public API
  *        because Rollup.apply is 5-arg and hardcodes RollupFreshness.Track.
  *
  * H4-β: source loading NPE when structural checks pass but
  *        sourceProvider returns null.
  */
class H4FreshnessSpec extends AnyFunSuite {

  test("H4-α: user can construct a Rollup with NoTracking freshness") {
    val rollup = Rollup(
      name = "r1",
      baseModel = "orders",
      rollupDimensions = Seq("region"),
      rollupMeasures = Seq(RollupMeasure("total", "sum", "total_storage")),
      sourceProvider = () => null,  // H4-β test below; freshness is independent
      freshness = RollupFreshness.NoTracking,
    )
    assert(rollup.freshness == RollupFreshness.NoTracking,
      s"H4-α REAL: freshness should be NoTracking, got ${rollup.freshness}")
  }

  test("H4-β: source = null throws helpful error, not NPE") {
    val ex = intercept[IllegalArgumentException] {
      Rollup(
        name = "r1",
        baseModel = "orders",
        rollupDimensions = Seq("region"),
        rollupMeasures = Seq(RollupMeasure("total", "sum", "total_storage")),
        sourceProvider = () => null,
      )
    }
    assert(
      ex.getMessage.contains("source") || ex.getMessage.contains("null"),
      s"H4-β: expected helpful null-source error, got: ${ex.getMessage}"
    )
  }
}
