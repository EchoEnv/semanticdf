package io.semanticdf

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Regression test for the `closestMatch` threshold fix.
  *
  * Background: `closestMatch` used a fixed edit-distance threshold of 3. For
  * long namespaced measure names (e.g. `total_passengers_pre_discount_v2`,
  * 32 chars), a 4-edit typo was silently ignored — the user got
  * "Unknown measure '...'" with no "did you mean" hint.
  * Fix: scale the threshold to `max(3, name.length / 4)`, so longer names get
  * proportionally more tolerance. Standard pattern (git, ripgrep).
  */
class ClosestMatchThresholdSpec extends AnyFunSuite with Matchers {

  test("REGRESSION: closestMatch finds suggestions for typos in long namespaced measure names") {
    // A 32-char name with a 4-edit typo (four character drops).
    // Old threshold (3) silently missed this; new threshold (28/4 = 7) catches it.
    // Verified empirically — typo="total_pssngers_pre_dscount_v" has edit distance 4.
    val candidates = Set("total_passengers_pre_discount_v2", "flight_count_with_baggage")
    val result = closestMatch("total_pssngers_pre_dscount_v", candidates)
    result shouldBe Some("total_passengers_pre_discount_v2")
  }

  test("REGRESSION: closestMatch still rejects typos in short names (no false positives)") {
    // For short names (≤12 chars), the threshold stays at 3, so an unrelated
    // typo is still rejected.
    val candidates = Set("carrier", "origin", "dest")
    val result = closestMatch("xyz_measure", candidates)
    result shouldBe None
  }

  test("REGRESSION: closestMatch keeps single-edit typos in short names") {
    val candidates = Set("carrier", "origin", "dest")
    val result = closestMatch("carier", candidates)  // edit distance 1
    result shouldBe Some("carrier")
  }
}
