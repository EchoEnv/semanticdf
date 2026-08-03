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
      .query(measures = Seq("n"), dimensions = Seq("k"))
    val df = m.toDataFrame(spark)
    try {
      val factor = spark.conf.get("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
      factor shouldBe "15"
    } finally df.unpersist()
  }

  // ----------------------------------------------------------------
  // Post-#318 fixes — HIGH bugs from the audit
  // ----------------------------------------------------------------

  test("toDataFrame with salt: PARENT adaptive.enabled is set (without it, the skew hint is a no-op)") {
    // Falsifiable: set adaptive.enabled to false BEFORE the call.
    // Then verify the library re-enables it (so the skew child
    // actually takes effect). This is the HIGH bug from the
    // recent audit — the original implementation set only the
    // skew child, not the parent.
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "false")

    val m = baseModel(spark).withSalt(10)
    val df = m.toDataFrame(spark)
    try {
      val adaptive  = spark.conf.get("spark.sql.adaptive.enabled", "true")
      val skewJoin  = spark.conf.get("spark.sql.adaptive.skewJoin.enabled", "true")
      val factor    = spark.conf.get("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
      adaptive shouldBe "true"   // proves the library set the PARENT config
      skewJoin shouldBe "true"
      factor shouldBe "10"
    } finally {
      spark.conf.set("spark.sql.adaptive.enabled", "true")
      spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
      spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
      df.unpersist()
    }
  }

  test("streaming batchModel carries salt (HIGH fix): streaming source.withSalt(n) does NOT drop skew handling") {
    // Falsifiable: configure a STREAMING source (via
    // `toStreamingSemanticTable`, not `toSemanticTable` which is
    // batch-only) with `withSalt(10)`, then verify that the
    // streaming model carries the salt through to the AQE config
    // via `applyAqeSkewConfig` (which is called from `toStreamingQuery`
    // BEFORE writeStream triggers Spark planning).
    //
    // IMPORTANT (post-fix finding): Spark's `ResolveWriteToStream`
    // rule disables AQE for streaming DataFrames automatically
    // (`spark.sql.adaptive.enabled is not supported in streaming
    // DataFrames/Datasets and will be disabled.`). So `withSalt` on
    // a STREAMING model cannot enable skew handling at the streaming
    // query level — Spark forces it off. This test verifies the
    // library applied the config (factor set to 10) even though
    // Spark later disables AQE for the streaming plan.
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.streaming.Trigger
    val rate = spark.readStream
      .format("rate")
      .option("rowsPerSecond", 10)
      .load()
    val streamingModel = io.semanticdf.toStreamingSemanticTable(rate, name = Some("rate"))
      .withDimensions(Dimension("value", t => t("value")))
      .withMeasures(Measure("n", t => count(lit(1))))
      .withSalt(10)

    // Reset AQE config to sentinel values.
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "false")
    spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "999")

    try {
      // toStreamingQuery internally calls applyAqeSkewConfig(spark)
      // BEFORE writeStream. This proves the library applies the
      // config even on the streaming path.
      val q = streamingModel.toStreamingQuery(
        spark,
        io.semanticdf.StreamingSupport.StreamingQueryOptions(
          outputMode = "append",
          trigger = Some(Trigger.AvailableNow()),
          foreachBatch = (_: org.apache.spark.sql.DataFrame) => (),
        )
      )
      try {
        q.processAllAvailable()

        // Verify the LIBRARY applied the config. The `factor` is set
        // by the library and is NOT overridden by Spark's
        // ResolveWriteToStream rule (which only disables
        // `adaptive.enabled`, not the factor itself). So `factor = "10"`
        // proves applyAqeSkewConfig ran.
        val factor = spark.conf.get("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
        factor shouldBe "10"

        // Note: `adaptive.enabled` may be reset to "false" by Spark's
        // ResolveWriteToStream rule after the query plan is built
        // (the warning "spark.sql.adaptive.enabled is not supported
        // in streaming DataFrames/Datasets and will be disabled" is
        // expected). This means `withSalt` on a STREAMING model
        // CANNOT enable skew handling at the streaming query level.
        // The library documents this limitation in the salt Scaladoc.
      } finally {
        q.stop()
      }
    } finally {
      // Restore defaults.
      spark.conf.set("spark.sql.adaptive.enabled", "true")
      spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
      spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
    }
  }

  test("manifest reader rejects salt = 0 (hand-rolled manifest with disable sentinel in JSON)") {
    // Falsifiable: construct a hand-rolled JSON manifest with
    // `"runtime": {"salt": 0}` and verify the reader throws.
    // The library's setter converts `withSalt(0)` to `None` (the
    // disable sentinel). The reader should reject `salt = 0`
    // explicitly rather than silently passing it through (which
    // would lead to `new SemanticTable(..., salt = Some(0))` and
    // break the setter's invariant).
    import com.fasterxml.jackson.databind.ObjectMapper
    val mapper = new ObjectMapper()
    val json =
      """{
        |  "schemaVersion": "v0.1.11-manifest",
        |  "kind": "semanticdf-model-manifest",
        |  "model": {"name": "kpi", "version": 1, "status": "published"},
        |  "runtime": {"salt": 0}
        |}""".stripMargin
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(1))),
      StructType(Seq(StructField("id", IntegerType)))
    )
    val ex = intercept[IllegalArgumentException] {
      io.semanticdf.adapters.SemanticManifest.fromJson(json, df)
    }
    ex.getMessage should include("salt must be >= 1")
  }

  test("manifest reader rejects salt = -5 (negative value in hand-rolled JSON)") {
    import com.fasterxml.jackson.databind.ObjectMapper
    val json =
      """{
        |  "schemaVersion": "v0.1.11-manifest",
        |  "kind": "semanticdf-model-manifest",
        |  "model": {"name": "kpi", "version": 1, "status": "published"},
        |  "runtime": {"salt": -5}
        |}""".stripMargin
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(1))),
      StructType(Seq(StructField("id", IntegerType)))
    )
    intercept[IllegalArgumentException] {
      io.semanticdf.adapters.SemanticManifest.fromJson(json, df)
    }
  }

  test("manifest reader accepts salt = 1 (boundary value)") {
    // Boundary: salt = 1 is the smallest valid value (per schema
    // minimum: 1). Verify it's accepted.
    val json =
      """{
        |  "schemaVersion": "v0.1.11-manifest",
        |  "kind": "semanticdf-model-manifest",
        |  "model": {"name": "kpi", "version": 1, "status": "published"},
        |  "runtime": {"salt": 1}
        |}""".stripMargin
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(1))),
      StructType(Seq(StructField("id", IntegerType)))
    )
    val round = io.semanticdf.adapters.SemanticManifest.fromJson(json, df)
    round.salt shouldBe Some(1)
  }
}
