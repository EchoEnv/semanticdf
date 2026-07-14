package io.semanticdf

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Regression test for the `requireRoot` error message fix.
  *
  * Background: when a user chained `a.join_one(b).join_one(c)` (which is not
  * supported), the error message recommended `table.join_one(first, on).join_one(second, on2)`
  * — the exact failing pattern. Users who followed the suggestion hit the same
  * error. The fix removes the contradiction and points to docs/known-limitations.md.
  */
class RequireRootMessageSpec extends AnyFunSuite with Matchers with SparkSessionFixture {

  test("REGRESSION: requireRoot error no longer suggests the failing operation") {
    val a = toSemanticTable(spark.emptyDataFrame, name = Some("a"))
    val b = toSemanticTable(spark.emptyDataFrame, name = Some("b"))
    val c = toSemanticTable(spark.emptyDataFrame, name = Some("c"))

    val ab = a.join_one(b, (l, r) => l("carrier") === r("carrier"))
    val ex = intercept[IllegalArgumentException] {
      ab.join_one(c, (l, r) => l("carrier") === r("carrier"))
    }
    info(s"chained join error: ${ex.getMessage}")
    // The new message must NOT contain the old self-contradicting suggestion.
    ex.getMessage should not include ("table.join_one(first, on).join_one(second, on2)")
    // The new message should point to the docs.
    ex.getMessage should include ("known-limitations.md")
  }
}
