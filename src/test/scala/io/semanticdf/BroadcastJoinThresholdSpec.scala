package io.semanticdf

import io.semanticdf.predicate._
import Predicate._

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{count, lit}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType, StringType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Tests for the opt-in auto-broadcast threshold on `join_one` /
  * `join_many`.
  *
  * The library emits `broadcast(rightSide)` automatically when
  * `rightSide.queryExecution.optimizedPlan.stats.sizeInBytes < threshold`.
  * The threshold is opt-in (None default → Spark's
  * `autoBroadcastJoinThreshold` applies).
  *
  * IMPORTANT: the threshold must be set BEFORE the join (same pattern
  * as `maxRows` from PR #294). Setting it after `join_one(...)` has no
  * effect on the already-constructed SemanticJoinOp. Example:
  *   `factModel.withBroadcastJoinThreshold(n).join_one(dimModel, ...)`  ✓
  *   `factModel.join_one(dimModel, ...).withBroadcastJoinThreshold(n)`  ✗
  *
  * NOTE: Spark stats are unavailable for in-memory `LocalRelation`
  * DataFrames (`sizeInBytes` returns `Long.MaxValue`). These tests use
  * `spark.range()` which is a Spark-managed source with proper stats.
  * The size-based broadcast only fires when stats are available —
  * falling through to Spark's default decision otherwise.
  */
class BroadcastJoinThresholdSpec extends AnyFunSuite with Matchers with SparkSessionFixture {

  /** Small "dimension" table — 10 Int rows from spark.range() so
    * `sizeInBytes` returns a real value (not Long.MaxValue). */
  private def smallDim(spark: SparkSession) = spark.range(10).toDF("k")

  /** Smaller "fact" table — 10 Int rows (same key range as dim) so the
    * join produces 10 rows. The fact vs dim distinction doesn't matter
    * for broadcast testing — what matters is that stats are available. */
  private def largeFact(spark: SparkSession) = spark.range(10).toDF("k")

  // ----------------------------------------------------------------
  // Default behavior: no library override
  // ----------------------------------------------------------------

  test("default broadcastJoinThreshold = None (no override, let Spark decide)") {
    val dim = toSemanticTable(smallDim(spark), name = Some("dim"))
    dim.broadcastJoinThreshold shouldBe None
  }

  // ----------------------------------------------------------------
  // Validation
  // ----------------------------------------------------------------

  test("withBroadcastJoinThreshold(-1) throws IllegalArgumentException") {
    val dim = toSemanticTable(smallDim(spark), name = Some("dim"))
    val ex = intercept[IllegalArgumentException] {
      dim.withBroadcastJoinThreshold(-1L)
    }
    ex.getMessage should include("withBroadcastJoinThreshold")
    ex.getMessage should include("-1")
  }

  // ----------------------------------------------------------------
  // Chain preservation: threshold set BEFORE join_one propagates to op
  // ----------------------------------------------------------------

  test("withBroadcastJoinThreshold set BEFORE join_one propagates to SemanticJoinOp") {
    val fact = toSemanticTable(largeFact(spark), name = Some("fact"))
      .withDimensions(Dimension("k", t => t("k")))
    val dim = toSemanticTable(smallDim(spark), name = Some("dim"))
      .withDimensions(Dimension("k", t => t("k")))

    // Set threshold BEFORE join_one so the join op captures it
    val joined = fact.withBroadcastJoinThreshold(1024L * 1024L)
      .join_one(dim, (l, r) => l("k") === r("k"))

    val op = joined.root.asInstanceOf[SemanticJoinOp]
    op.broadcastJoinThreshold shouldBe Some(1024L * 1024L)
  }

  test("withBroadcastJoinThreshold survives where / groupBy / aggregate") {
    val dim = toSemanticTable(smallDim(spark), name = Some("dim"))
      .withBroadcastJoinThreshold(1024L)
    val chained = dim
      .where("k" === 1L)
      .groupBy("k")
      .aggregate()
    chained.broadcastJoinThreshold shouldBe Some(1024L)
  }

  // ----------------------------------------------------------------
  // Small side + threshold: broadcast fires (via logical plan check)
  // ----------------------------------------------------------------

  test("small side below threshold adds BroadcastExchange to the optimized plan") {
    val fact = toSemanticTable(largeFact(spark), name = Some("fact"))
      .withDimensions(Dimension("k", t => t("k")))
      .withMeasures(Measure("n", t => count(lit(1))))
    val dim = toSemanticTable(smallDim(spark), name = Some("dim"))
      .withDimensions(Dimension("k", t => t("k")))

    // Threshold set BEFORE join_one so the join op captures it
    val joined = fact.withBroadcastJoinThreshold(1024L * 1024L)  // 1 MiB
      .join_one(dim, (l, r) => l("k") === r("k"))

    val logicalPlan = joined.execute(spark).queryExecution.optimizedPlan.toString
    // The hint appears as `rightHint=(strategy=broadcast)` (lowercase).
    // Spark then converts to a real BroadcastExchange at execution time
    // if the size estimate still satisfies the threshold.
    assert(logicalPlan.toLowerCase.contains("broadcast"),
      s"expected broadcast hint in optimized plan; got:\n$logicalPlan")
  }

  // ----------------------------------------------------------------
  // Small side + threshold = 0: broadcast NOT applied (escape hatch)
  // ----------------------------------------------------------------

  test("threshold = 0 disables the broadcast (escape hatch)") {
    val fact = toSemanticTable(largeFact(spark), name = Some("fact"))
      .withDimensions(Dimension("k", t => t("k")))
      .withMeasures(Measure("n", t => count(lit(1))))
    val dim = toSemanticTable(smallDim(spark), name = Some("dim"))
      .withDimensions(Dimension("k", t => t("k")))

    val joined = fact.withBroadcastJoinThreshold(0L)
      .join_one(dim, (l, r) => l("k") === r("k"))

    // threshold=0 should be persisted as None in the field (escape hatch).
    joined.broadcastJoinThreshold shouldBe None
    val op = joined.root.asInstanceOf[SemanticJoinOp]
    op.broadcastJoinThreshold shouldBe None

    // Join must still execute correctly.
    val rows = joined.execute(spark).collect()
    rows.length shouldBe 10  // 10 fact rows match a dim row (range(0..9))
  }

  // ----------------------------------------------------------------
  // End-to-end correctness: broadcast doesn't change the join result
  // ----------------------------------------------------------------

  test("broadcast hint does not change the join result (correctness invariant)") {
    val fact = toSemanticTable(largeFact(spark), name = Some("fact"))
      .withDimensions(Dimension("k", t => t("k")))
      .withMeasures(Measure("n", t => count(lit(1))))
    val dim = toSemanticTable(smallDim(spark), name = Some("dim"))
      .withDimensions(Dimension("k", t => t("k")))

    val withHint    = fact.withBroadcastJoinThreshold(1024L * 1024L)
      .join_one(dim, (l, r) => l("k") === r("k"))
    val withoutHint = fact
      .join_one(dim, (l, r) => l("k") === r("k"))

    val withRows    = withHint.execute(spark).collect().length
    val withoutRows = withoutHint.execute(spark).collect().length
    withRows shouldBe withoutRows
    withRows shouldBe 10  // 10 fact rows match a dim row (range(0..9))
  }
}
