package io.semanticdf

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Regression tests for the post-join wrapper propagation rule
  * (M3 audit finding).
  *
  * The post-join wrapper (join_one / join_many / join_cross) used to
  * take the LEFT side's runtime fields verbatim. A user who set
  * `right.withMaxRows(n)` / `right.withResultCache(c)` /
  * `right.withAuditSink(s)` etc. on the right side would silently
  * lose the override after the join. The fix introduces 5 helper
  * methods (`joinMaxRows`, `joinAuditSink`, `joinAuditRequest`,
  * `joinResultCache`, `joinBroadcastThreshold`) that define the
  * propagation rule for each field.
  *
  * Rules:
  *   - `maxRows`: `min(this.maxRows, other.maxRows)` (tighter cap
  *     wins; the `0 = no-cap` sentinel propagates correctly because
  *     `min(100_000, 0) = 0`).
  *   - `auditSink` / `auditRequest` / `resultCache` /
  *     `broadcastJoinThreshold`: `orElse(this.X, other.X)` — LEFT
  *     wins when both sides set, RIGHT is the fallback so a user
  *     who set the field on the right side gets the value.
  */
class JoinWrapperPropagationSpec
    extends AnyFunSuite
    with Matchers
    with SparkSessionFixture {

  private def makeSpark(): SparkSession = spark
  private def smallDf(s: SparkSession) = s.createDataFrame(
    s.sparkContext.parallelize(Seq(Row(1), Row(2), Row(3), Row(4), Row(5), Row(6), Row(7), Row(8), Row(9), Row(10))),
    StructType(Seq(StructField("id", IntegerType)))
  )
  private def otherDf(s: SparkSession) = s.createDataFrame(
    s.sparkContext.parallelize(Seq(Row(1), Row(2), Row(3))),
    StructType(Seq(StructField("id", IntegerType)))
  )

  // ----------------------------------------------------------------
  // maxRows: MIN propagation with escape-hatch handling
  // ----------------------------------------------------------------

  test("RIGHT-side withMaxRows propagates to the joined outer table (MIN wins)") {
    val s = makeSpark()
    val lT = toSemanticTable(smallDf(s), name = Some("L"))
      .withDimensions(Dimension("id", t => t("id")))
    val rT = toSemanticTable(otherDf(s), name = Some("R"))
      .withDimensions(Dimension("id", t => t("id")))
      .withMaxRows(20_000)

    val joined = lT.join_one(rT, (l, r) => l("id") === r("id"))
    joined.maxRows shouldBe 20_000
  }

  test("LEFT-side withMaxRows propagates (MIN wins, default cap on right)") {
    val s = makeSpark()
    val lT = toSemanticTable(smallDf(s), name = Some("L"))
      .withDimensions(Dimension("id", t => t("id")))
      .withMaxRows(50_000)
    val rT = toSemanticTable(otherDf(s), name = Some("R"))
      .withDimensions(Dimension("id", t => t("id")))

    val joined = lT.join_one(rT, (l, r) => l("id") === r("id"))
    joined.maxRows shouldBe 50_000
  }

  test("BOTH sides set with different values: MIN wins") {
    val s = makeSpark()
    val lT = toSemanticTable(smallDf(s), name = Some("L"))
      .withDimensions(Dimension("id", t => t("id")))
      .withMaxRows(80_000)
    val rT = toSemanticTable(otherDf(s), name = Some("R"))
      .withDimensions(Dimension("id", t => t("id")))
      .withMaxRows(20_000)

    val joined = lT.join_one(rT, (l, r) => l("id") === r("id"))
    joined.maxRows shouldBe 20_000
  }

  test("maxRows = 0 (no-cap) on RIGHT propagates: outer = 0") {
    val s = makeSpark()
    val lT = toSemanticTable(smallDf(s), name = Some("L"))
      .withDimensions(Dimension("id", t => t("id")))
    val rT = toSemanticTable(otherDf(s), name = Some("R"))
      .withDimensions(Dimension("id", t => t("id")))
      .withMaxRows(0)

    val joined = lT.join_one(rT, (l, r) => l("id") === r("id"))
    joined.maxRows shouldBe 0
  }

  test("maxRows = 0 (no-cap) on LEFT propagates: outer = 0") {
    val s = makeSpark()
    val lT = toSemanticTable(smallDf(s), name = Some("L"))
      .withDimensions(Dimension("id", t => t("id")))
      .withMaxRows(0)
    val rT = toSemanticTable(otherDf(s), name = Some("R"))
      .withDimensions(Dimension("id", t => t("id")))

    val joined = lT.join_one(rT, (l, r) => l("id") === r("id"))
    joined.maxRows shouldBe 0
  }

  // ----------------------------------------------------------------
  // resultCache: orElse propagation
  // ----------------------------------------------------------------

  test("RIGHT-side withResultCache propagates (LEFT=None, RIGHT=Some)") {
    val s = makeSpark()
    val lT = toSemanticTable(smallDf(s), name = Some("L"))
      .withDimensions(Dimension("id", t => t("id")))
    val rT = toSemanticTable(otherDf(s), name = Some("R"))
      .withDimensions(Dimension("id", t => t("id")))
      .withResultCache(io.semanticdf.cache.ResultCache.inMemory(64))

    val joined = lT.join_one(rT, (l, r) => l("id") === r("id"))
    joined.resultCache shouldBe defined
  }

  test("BOTH sides set: LEFT wins") {
    val s = makeSpark()
    val lCache = io.semanticdf.cache.ResultCache.inMemory(64)
    val rCache = io.semanticdf.cache.ResultCache.inMemory(128)
    val lT = toSemanticTable(smallDf(s), name = Some("L"))
      .withDimensions(Dimension("id", t => t("id")))
      .withResultCache(lCache)
    val rT = toSemanticTable(otherDf(s), name = Some("R"))
      .withDimensions(Dimension("id", t => t("id")))
      .withResultCache(rCache)

    val joined = lT.join_one(rT, (l, r) => l("id") === r("id"))
    joined.resultCache shouldBe Some(lCache)
  }

  // ----------------------------------------------------------------
  // auditSink: orElse propagation
  // ----------------------------------------------------------------

  test("RIGHT-side withAuditSink propagates (LEFT=None, RIGHT=Some)") {
    val s = makeSpark()
    val lT = toSemanticTable(smallDf(s), name = Some("L"))
      .withDimensions(Dimension("id", t => t("id")))
    val sink = io.semanticdf.audit.AuditSink.inMemory(16)
    val rT = toSemanticTable(otherDf(s), name = Some("R"))
      .withDimensions(Dimension("id", t => t("id")))
      .withAuditSink(sink)

    val joined = lT.join_one(rT, (l, r) => l("id") === r("id"))
    joined.auditSink shouldBe Some(sink)
  }

  test("BOTH sides set with different sinks: LEFT wins") {
    val s = makeSpark()
    val lSink = io.semanticdf.audit.AuditSink.inMemory(16)
    val rSink = io.semanticdf.audit.AuditSink.inMemory(32)
    val lT = toSemanticTable(smallDf(s), name = Some("L"))
      .withDimensions(Dimension("id", t => t("id")))
      .withAuditSink(lSink)
    val rT = toSemanticTable(otherDf(s), name = Some("R"))
      .withDimensions(Dimension("id", t => t("id")))
      .withAuditSink(rSink)

    val joined = lT.join_one(rT, (l, r) => l("id") === r("id"))
    joined.auditSink shouldBe Some(lSink)
  }

  // ----------------------------------------------------------------
  // Same rule for join_many and join_cross
  // ----------------------------------------------------------------

  test("RIGHT-side withMaxRows propagates through join_many") {
    val s = makeSpark()
    val lT = toSemanticTable(smallDf(s), name = Some("L"))
      .withDimensions(Dimension("id", t => t("id")))
    val rT = toSemanticTable(otherDf(s), name = Some("R"))
      .withDimensions(Dimension("id", t => t("id")))
      .withMaxRows(15_000)

    val joined = lT.join_many(rT, (l, r) => l("id") === r("id"))
    joined.maxRows shouldBe 15_000
  }

  test("RIGHT-side withMaxRows propagates through join_cross") {
    val s = makeSpark()
    val lT = toSemanticTable(smallDf(s), name = Some("L"))
      .withDimensions(Dimension("id", t => t("id")))
    val rT = toSemanticTable(otherDf(s), name = Some("R"))
      .withDimensions(Dimension("id", t => t("id")))
      .withMaxRows(10_000)

    val joined = lT.join_cross(rT)
    joined.maxRows shouldBe 10_000
  }
}
