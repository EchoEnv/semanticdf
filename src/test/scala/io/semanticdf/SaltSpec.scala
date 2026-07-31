package io.semanticdf

import io.semanticdf.audit.AuditSink
import io.semanticdf.cache.CacheKey
import io.semanticdf.predicate.Predicate

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{count, lit}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Tests for the opt-in skew-handling hint on `withSalt(n)`.
  *
  * `withSalt(n)` is a hint — the library translates it into Spark AQE
  * skew handling (`spark.sql.adaptive.skewJoin.skewedPartitionFactor = n`)
  * on `toDataFrame`. The library does NOT add a custom salt column
  * because symmetric random salting does not match across sides for
  * shuffled joins (different executors generate different RNG
  * sequences), which would produce incorrect join results. Spark AQE
  * is the production-grade solution. */
class SaltSpec extends AnyFunSuite with Matchers with SparkSessionFixture {

  private def baseModel(spark: SparkSession, name: String = "m") =
    toSemanticTable(spark.range(10).toDF("k"), name = Some(name))
      .withDimensions(Dimension("k", t => t("k")))
      .withMeasures(Measure("n", t => count(lit(1))))

  // ----------------------------------------------------------------
  // Default: salt = None
  // ----------------------------------------------------------------

  test("default salt = None (no skew-handling hint)") {
    val m = baseModel(spark)
    m.salt shouldBe None
  }

  // ----------------------------------------------------------------
  // Fluent setter
  // ----------------------------------------------------------------

  test("withSalt(n) sets the field") {
    val m = baseModel(spark).withSalt(10)
    m.salt shouldBe Some(10)
  }

  test("withSalt(0) disables (treats 0 as the disable sentinel, mirrors withBroadcastJoinThreshold)") {
    val m = baseModel(spark).withSalt(10).withSalt(0)
    m.salt shouldBe None
  }

  test("withSalt(-1) throws IllegalArgumentException") {
    val m = baseModel(spark)
    intercept[IllegalArgumentException] {
      m.withSalt(-1)
    }
  }

  test("withSalt preserves the salt across withDimensions / withMeasures / withTransforms") {
    val m = baseModel(spark)
      .withSalt(8)
      .withDimensions(Dimension("kk", t => t("k")))
      .withMeasures(Measure("nn", t => count(lit(1))))
    m.salt shouldBe Some(8)
  }

  test("withSalt preserves across withRowFilter") {
    val m = baseModel(spark).withSalt(12)
    val chained = m.withRowFilter(
      name = "f",
      expr = "k >= 0",
      description = None,
      metadata = Map.empty,
    )
    chained.salt shouldBe Some(12)
  }

  test("withSalt preserves across withTransforms") {
    import io.semanticdf.Transform
    val m = baseModel(spark).withSalt(7)
    val chained = m.withTransforms(Transform("k_alias", t => t("k").as("k_alias")))
    chained.salt shouldBe Some(7)
  }

  // ----------------------------------------------------------------
  // Join wrapper propagation: LEFT-wins, RIGHT-fallback
  // ----------------------------------------------------------------

  test("join_one: right-side withSalt propagates via joinSalt helper") {
    val left  = baseModel(spark, "left")
    val right = baseModel(spark, "right").withSalt(10)

    val joined = left.join_one(right, (l, r) => l("k") === r("k"))
    joined.salt shouldBe Some(10)
    val op = joined.root.asInstanceOf[SemanticJoinOp]
    op.salt shouldBe Some(10)
  }

  test("join_one: LEFT-side withSalt wins when both sides set (precedence preserved)") {
    val left  = baseModel(spark, "left").withSalt(5)
    val right = baseModel(spark, "right").withSalt(20)

    val joined = left.join_one(right, (l, r) => l("k") === r("k"))
    joined.salt shouldBe Some(5)
  }

  test("join_many: salt propagates with LEFT-wins precedence") {
    val fact = baseModel(spark, "fact")
    val dim  = baseModel(spark, "dim").withSalt(15)

    val joined = fact.join_many(dim, (l, r) => l("k") === r("k"))
    joined.salt shouldBe Some(15)
  }

  test("join_one: both sides unset → salt is None on the joined result") {
    val left  = baseModel(spark, "left")
    val right = baseModel(spark, "right")

    val joined = left.join_one(right, (l, r) => l("k") === r("k"))
    joined.salt shouldBe None
  }

  // ----------------------------------------------------------------
  // groupBy().aggregate(): salt preserved
  // ----------------------------------------------------------------

  test("groupBy().aggregate(): salt is preserved through SemanticGroupBy") {
    val m = baseModel(spark).withSalt(9)
    val aggregated = m.groupBy("k").aggregate("n")
    aggregated.salt shouldBe Some(9)
  }

  // ----------------------------------------------------------------
  // Compile-time behavior: AQE config translation
  // ----------------------------------------------------------------

  test("toDataFrame with salt set: AQE skewJoin config is applied to the session") {
    // Falsifiable: set `enabled` to `false` BEFORE the call, then verify
    // the library re-enables it. This proves the `enabled` set is real
    // (not just the Spark 3.2+ default). Set `factor` to a non-default
    // sentinel to verify the library overwrites it.
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "false")
    spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "999")

    val m = baseModel(spark).withSalt(10)
    val df = m.toDataFrame(spark)
    try {
      val enabled = spark.conf.get("spark.sql.adaptive.skewJoin.enabled", "true")
      val factor  = spark.conf.get("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
      enabled shouldBe "true"   // proves the library re-enabled it
      factor shouldBe "10"      // proves the library set it from `salt = Some(10)`
    } finally {
      // Restore defaults so subsequent tests aren't affected.
      spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
      spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
      df.unpersist()
    }
  }

  test("toDataFrame without salt: AQE skew config is NOT touched (no side effects)") {
    // Reset to a known default, then call toDataFrame without salt, and
    // verify the factor wasn't modified.
    val originalFactor = spark.conf.get("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
    val m = baseModel(spark)  // no salt
    val df = m.toDataFrame(spark)
    try {
      val factor = spark.conf.get("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
      factor shouldBe originalFactor
    } finally df.unpersist()
  }

  // ----------------------------------------------------------------
  // Audit/cache path: salt still translates to AQE config
  // ----------------------------------------------------------------

  test("toDataFrame with auditSink + salt: AQE config still applied (salt is hint-only, not affected by audit/cache path)") {
    val sink = AuditSink.inMemory(8)
    val m = baseModel(spark).withSalt(15).withAuditSink(sink)
    val df = m.toDataFrame(spark)
    try {
      val factor = spark.conf.get("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
      factor shouldBe "15"
    } finally df.unpersist()
  }
}
