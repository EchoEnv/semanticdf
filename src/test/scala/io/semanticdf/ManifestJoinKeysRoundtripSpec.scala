package io.semanticdf
import io.semanticdf.adapters._

import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Tests for `toJoinedJson` / `fromJoinedJson` with the joined-key wire
  * shape (PR #154). Validates the round-trip end-to-end. */
class ManifestJoinKeysRoundtripSpec extends AnyFunSuite with Matchers {

  private def makeSpark(): SparkSession = {
    val s = SparkSession.builder().master("local[1]").appName("jkr").getOrCreate()
    s.sparkContext.setLogLevel("WARN")
    s
  }

  private def leftDf(spark: SparkSession) =
    spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1, 100), Row(2, 200), Row(3, 300),
      )),
      StructType(Seq(
        StructField("id", IntegerType),
        StructField("amount", IntegerType),
      ))
    )

  private def rightDf(spark: SparkSession) =
    spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1, "alice"), Row(2, "bob"), Row(3, "carol"),
      )),
      StructType(Seq(
        StructField("id", IntegerType),
        StructField("name", StringType),
      ))
    )

  test("typed single-key join_on round-trips functionally") {
    val spark = makeSpark()
    try {
      val lSrc = leftDf(spark)
      val rSrc = rightDf(spark)
      val lT = toSemanticTable(lSrc, name = Some("L"))
        .withDimensions(
          Dimension("id", _ => org.apache.spark.sql.functions.col("id")),
          Dimension("amount", _ => org.apache.spark.sql.functions.col("amount")),
        )
      val rT = toSemanticTable(rSrc, name = Some("R"))
        .withDimensions(
          Dimension("id", _ => org.apache.spark.sql.functions.col("id")),
          Dimension("name", _ => org.apache.spark.sql.functions.col("name")),
        )
      val joined = lT.join_on(rT, "id" -> "id")
      val json = SemanticManifest.toJoinedJson(joined, prettyPrint = true)
      val restored = SemanticManifest.fromJoinedJson(json, lSrc, rSrc)
      val j = restored.root.asInstanceOf[SemanticJoinOp]
      assert(j.leftKeys == Seq("id"))
      assert(j.rightKeys == Seq("id"))
      val probeL = JoinSide.recording("L",
        scala.collection.mutable.LinkedHashMap.empty[String, Boolean])
      val probeR = JoinSide.recording("R",
        scala.collection.mutable.LinkedHashMap.empty[String, Boolean])
      val col = j.on(probeL, probeR)
      assert(col != null)
      assert(probeL.captured.contains("__L__id"))
      assert(probeR.captured.contains("__R__id"))
    } finally spark.stop()
  }

  test("typed multi-key join_on round-trips with AND over equi pairs") {
    val spark = makeSpark()
    try {
      val lSrc = leftDf(spark)
      val rSrc = rightDf(spark)
      val lT = toSemanticTable(lSrc, name = Some("L"))
        .withDimensions(
          Dimension("id", _ => org.apache.spark.sql.functions.col("id")),
          Dimension("amount", _ => org.apache.spark.sql.functions.col("amount")),
        )
      val rT = toSemanticTable(rSrc, name = Some("R"))
      val joined = lT.join_on(rT, Seq("id", "amount"), Seq("id", "amount"))
      val json = SemanticManifest.toJoinedJson(joined, prettyPrint = true)
      val restored = SemanticManifest.fromJoinedJson(json, lSrc, rSrc)
      val j = restored.root.asInstanceOf[SemanticJoinOp]
      assert(j.leftKeys == Seq("id", "amount"))
      assert(j.rightKeys == Seq("id", "amount"))
    } finally spark.stop()
  }

  test("parseJoinedMeta reflects the wire keys + multiColumn + onExprString") {
    val spark = makeSpark()
    try {
      val lT = toSemanticTable(leftDf(spark), name = Some("L"))
        .withDimensions(Dimension("id", _ => org.apache.spark.sql.functions.col("id")))
      val rT = toSemanticTable(rightDf(spark), name = Some("R"))
        .withDimensions(Dimension("id", _ => org.apache.spark.sql.functions.col("id")))
      val joined = lT.join_on(rT, "id" -> "id")
      val json = SemanticManifest.toJoinedJson(joined, prettyPrint = true)
      val meta = SemanticManifest.parseJoinedMeta(json)
      assert(meta.kind == "semanticdf-joined-manifest")
      assert(meta.leftKeys == Seq("id"))
      assert(meta.rightKeys == Seq("id"))
      assert(meta.multiColumn == false)
      assert(meta.cardinality == "one")
    } finally spark.stop()
  }

  // -- joined-manifest runtime preservation (PR #303 follow-up) -----------
  //
  // The outer `new SemanticTable(...)` constructed by `fromJoinedJson`
  // previously did not pass `maxRows` or `broadcastJoinThreshold`,
  // silently dropping any runtime tuning the caller set on the joined
  // model. Each side preserves its own runtime via the single-table
  // round-trip (PR #303), but the OUTER envelope was lossy. These tests
  // pin the outer-envelope preservation.

  test("joined envelope preserves outer maxRows (regression: post-#303 DE H2)") {
    val spark = makeSpark()
    try {
      val lSrc = leftDf(spark)
      val rSrc = rightDf(spark)
      val lT = toSemanticTable(lSrc, name = Some("L"))
        .withDimensions(Dimension("id", _ => org.apache.spark.sql.functions.col("id")))
      val rT = toSemanticTable(rSrc, name = Some("R"))
        .withDimensions(Dimension("id", _ => org.apache.spark.sql.functions.col("id")))
      val joined = lT.join_on(rT, "id" -> "id").withMaxRows(50_000)
      assert(joined.maxRows == 50_000)

      val json = SemanticManifest.toJoinedJson(joined, prettyPrint = true)
      val restored = SemanticManifest.fromJoinedJson(json, lSrc, rSrc)
      restored.maxRows shouldBe 50_000
    } finally spark.stop()
  }

  test("joined envelope preserves outer maxRows = 0 (escape hatch round-trips)") {
    val spark = makeSpark()
    try {
      val lSrc = leftDf(spark)
      val rSrc = rightDf(spark)
      val lT = toSemanticTable(lSrc, name = Some("L"))
        .withDimensions(Dimension("id", _ => org.apache.spark.sql.functions.col("id")))
      val rT = toSemanticTable(rSrc, name = Some("R"))
        .withDimensions(Dimension("id", _ => org.apache.spark.sql.functions.col("id")))
      val joined = lT.join_on(rT, "id" -> "id").withMaxRows(0)
      assert(joined.maxRows == 0)

      val json = SemanticManifest.toJoinedJson(joined, prettyPrint = true)
      val restored = SemanticManifest.fromJoinedJson(json, lSrc, rSrc)
      restored.maxRows shouldBe 0
    } finally spark.stop()
  }

  test("joined envelope preserves outer broadcastJoinThreshold") {
    val spark = makeSpark()
    try {
      val lSrc = leftDf(spark)
      val rSrc = rightDf(spark)
      val lT = toSemanticTable(lSrc, name = Some("L"))
        .withDimensions(Dimension("id", _ => org.apache.spark.sql.functions.col("id")))
      val rT = toSemanticTable(rSrc, name = Some("R"))
        .withDimensions(Dimension("id", _ => org.apache.spark.sql.functions.col("id")))
      val joined =
        lT.join_on(rT, "id" -> "id").withBroadcastJoinThreshold(2L * 1024 * 1024)

      val json = SemanticManifest.toJoinedJson(joined, prettyPrint = true)
      val restored = SemanticManifest.fromJoinedJson(json, lSrc, rSrc)
      restored.broadcastJoinThreshold shouldBe Some(2L * 1024 * 1024)
    } finally spark.stop()
  }

  test("joined envelope: RIGHT-side withBroadcastJoinThreshold survives the round-trip (regression: post-#306 audit)") {
    // PR #304 added the LEFT-side round-trip for the op's
    // broadcastJoinThreshold but used `leftT.broadcastJoinThreshold`
    // (LEFT-only). PR #306 widened the in-memory join path to
    // `orElse(this, other)`, but the joined-manifest reader was
    // not updated. This test pins the right-side survival.
    val spark = makeSpark()
    try {
      val lSrc = leftDf(spark)
      val rSrc = rightDf(spark)
      val lT = toSemanticTable(lSrc, name = Some("L"))
        .withDimensions(Dimension("id", _ => org.apache.spark.sql.functions.col("id")))
      val rT = toSemanticTable(rSrc, name = Some("R"))
        .withDimensions(Dimension("id", _ => org.apache.spark.sql.functions.col("id")))
        .withBroadcastJoinThreshold(2L * 1024 * 1024)

      val joined = lT.join_on(rT, "id" -> "id")
      val json = SemanticManifest.toJoinedJson(joined, prettyPrint = true)
      val restored = SemanticManifest.fromJoinedJson(json, lSrc, rSrc)
      val op = restored.root.asInstanceOf[SemanticJoinOp]
      op.broadcastJoinThreshold shouldBe Some(2L * 1024 * 1024)
    } finally spark.stop()
  }

  test("SemanticJoinOp's broadcastJoinThreshold survives the round-trip (regression: op construction)") {
    // The op's `broadcastJoinThreshold` is set at join construction
    // time from the LEFT side's `withBroadcastJoinThreshold(n)` call.
    // Before PR #304, `fromJoinedJson` reconstructed the
    // `SemanticJoinOp` with `broadcastJoinThreshold = None` (default),
    // silently dropping the override even though the LEFT side's
    // runtime was preserved. This test pins the op-level propagation.
    val spark = makeSpark()
    try {
      val lSrc = leftDf(spark)
      val rSrc = rightDf(spark)
      val lT = toSemanticTable(lSrc, name = Some("L"))
        .withDimensions(Dimension("id", _ => org.apache.spark.sql.functions.col("id")))
      val rT = toSemanticTable(rSrc, name = Some("R"))
        .withDimensions(Dimension("id", _ => org.apache.spark.sql.functions.col("id")))
      // Set BEFORE the join — the LEFT side's value flows into the op
      val joined =
        lT.withBroadcastJoinThreshold(2L * 1024 * 1024).join_on(rT, "id" -> "id")

      val json = SemanticManifest.toJoinedJson(joined, prettyPrint = true)
      val restored = SemanticManifest.fromJoinedJson(json, lSrc, rSrc)
      val op = restored.root.asInstanceOf[SemanticJoinOp]
      op.broadcastJoinThreshold shouldBe Some(2L * 1024 * 1024)
    } finally spark.stop()
  }

  test("joined envelope: per-side and outer runtime preserved independently") {
    // The LEFT side's runtime is preserved by the per-side single-table
    // round-trip (PR #303). The OUTER envelope's runtime is preserved
    // by PR #304. Both must survive the round-trip independently.
    val spark = makeSpark()
    try {
      val lSrc = leftDf(spark)
      val rSrc = rightDf(spark)
      val lT = toSemanticTable(lSrc, name = Some("L"))
        .withDimensions(Dimension("id", _ => org.apache.spark.sql.functions.col("id")))
        .withMaxRows(50_000)  // LEFT side's runtime
      val rT = toSemanticTable(rSrc, name = Some("R"))
        .withDimensions(Dimension("id", _ => org.apache.spark.sql.functions.col("id")))
      val joined = lT.join_on(rT, "id" -> "id").withMaxRows(75_000)  // OUTER's runtime

      val json = SemanticManifest.toJoinedJson(joined, prettyPrint = true)
      val restored = SemanticManifest.fromJoinedJson(json, lSrc, rSrc)

      restored.maxRows shouldBe 75_000  // OUTER
      val op = restored.root.asInstanceOf[SemanticJoinOp]
      op.leftSide.get.maxRows shouldBe 50_000  // LEFT preserved
      op.rightSide.get.maxRows shouldBe io.semanticdf.cache.CacheKey.DefaultMaxRows  // RIGHT untouched
    } finally spark.stop()
  }

  test("joined envelope: join_many cardinality preserves outer runtime") {
    val spark = makeSpark()
    try {
      val lSrc = leftDf(spark)
      val rSrc = rightDf(spark)
      val lT = toSemanticTable(lSrc, name = Some("L"))
        .withDimensions(Dimension("id", _ => org.apache.spark.sql.functions.col("id")))
      val rT = toSemanticTable(rSrc, name = Some("R"))
        .withDimensions(Dimension("id", _ => org.apache.spark.sql.functions.col("id")))
      val joined = lT.join_many_on(rT, Seq("id"), Seq("id")).withMaxRows(20_000)

      val json = SemanticManifest.toJoinedJson(joined, prettyPrint = true)
      val restored = SemanticManifest.fromJoinedJson(json, lSrc, rSrc)
      restored.maxRows shouldBe 20_000
      val op = restored.root.asInstanceOf[SemanticJoinOp]
      op.cardinality shouldBe JoinCardinality.Many
    } finally spark.stop()
  }

  test("joined envelope: join_cross cardinality preserves outer runtime") {
    val spark = makeSpark()
    try {
      val lSrc = leftDf(spark)
      val rSrc = rightDf(spark)
      val lT = toSemanticTable(lSrc, name = Some("L"))
        .withDimensions(Dimension("id", _ => org.apache.spark.sql.functions.col("id")))
      val rT = toSemanticTable(rSrc, name = Some("R"))
        .withDimensions(Dimension("id", _ => org.apache.spark.sql.functions.col("id")))
      val joined = lT.join_cross(rT).withMaxRows(15_000)

      val json = SemanticManifest.toJoinedJson(joined, prettyPrint = true)
      val restored = SemanticManifest.fromJoinedJson(json, lSrc, rSrc)
      restored.maxRows shouldBe 15_000
      val op = restored.root.asInstanceOf[SemanticJoinOp]
      op.cardinality shouldBe JoinCardinality.Cross
    } finally spark.stop()
  }

  test("joined envelope OMITS runtime block when both outer fields are at default") {
    val spark = makeSpark()
    try {
      val lSrc = leftDf(spark)
      val rSrc = rightDf(spark)
      val lT = toSemanticTable(lSrc, name = Some("L"))
        .withDimensions(Dimension("id", _ => org.apache.spark.sql.functions.col("id")))
      val rT = toSemanticTable(rSrc, name = Some("R"))
        .withDimensions(Dimension("id", _ => org.apache.spark.sql.functions.col("id")))
      val joined = lT.join_on(rT, "id" -> "id")
      assert(joined.broadcastJoinThreshold.isEmpty)
      assert(joined.maxRows == io.semanticdf.cache.CacheKey.DefaultMaxRows)

      val json = SemanticManifest.toJoinedJson(joined, prettyPrint = true)
      // The outer envelope's `runtime` block is omitted because both
      // outer fields are at default. Each side may still carry its own
      // `runtime` block if it had non-default values.
      val outerTree = new com.fasterxml.jackson.databind.ObjectMapper().readTree(json)
      outerTree.has("runtime") shouldBe false
    } finally spark.stop()
  }

  // -- materializeLevel round-trip (joined envelope) ----------------------

  test("joined envelope round-trips materializeLevel on the OUTER (LEFT-wins precedence)") {
    val spark = makeSpark()
    try {
      val lSrc = leftDf(spark)
      val rSrc = rightDf(spark)
      val lT = toSemanticTable(lSrc, name = Some("L"))
        .withDimensions(Dimension("id", _ => org.apache.spark.sql.functions.col("id")))
      val rT = toSemanticTable(rSrc, name = Some("R"))
        .withDimensions(Dimension("id", _ => org.apache.spark.sql.functions.col("id")))
      val joined = lT.join_on(rT, "id" -> "id")
        .withMaterialize(org.apache.spark.storage.StorageLevel.MEMORY_ONLY)

      val json = SemanticManifest.toJoinedJson(joined, prettyPrint = true)
      val restored = SemanticManifest.fromJoinedJson(json, lSrc, rSrc)
      restored.materializeLevel shouldBe Some(org.apache.spark.storage.StorageLevel.MEMORY_ONLY)
    } finally spark.stop()
  }

  test("joined envelope round-trips materializeLevel = None (default, no override)") {
    val spark = makeSpark()
    try {
      val lSrc = leftDf(spark)
      val rSrc = rightDf(spark)
      val lT = toSemanticTable(lSrc, name = Some("L"))
        .withDimensions(Dimension("id", _ => org.apache.spark.sql.functions.col("id")))
      val rT = toSemanticTable(rSrc, name = Some("R"))
        .withDimensions(Dimension("id", _ => org.apache.spark.sql.functions.col("id")))
      val joined = lT.join_on(rT, "id" -> "id")

      val json = SemanticManifest.toJoinedJson(joined, prettyPrint = true)
      val restored = SemanticManifest.fromJoinedJson(json, lSrc, rSrc)
      restored.materializeLevel shouldBe None
    } finally spark.stop()
  }

  // -- salt round-trip (joined envelope) --------------------------------

  test("joined envelope round-trips salt on the OUTER (skew-handling hint)") {
    // Salt is a session-global AQE config — the joined-manifest reader
    // restores it on the OUTER SemanticTable. The op's salt is not
    // restored (it's a derived field; AQE handles actual skew
    // handling from the OUTER table's salt).
    val spark = makeSpark()
    try {
      val lSrc = leftDf(spark)
      val rSrc = rightDf(spark)
      val lT = toSemanticTable(lSrc, name = Some("L"))
        .withDimensions(Dimension("id", _ => org.apache.spark.sql.functions.col("id")))
      val rT = toSemanticTable(rSrc, name = Some("R"))
        .withDimensions(Dimension("id", _ => org.apache.spark.sql.functions.col("id")))
      val joined = lT.join_on(rT, "id" -> "id")
        .withSalt(10)

      val json = SemanticManifest.toJoinedJson(joined, prettyPrint = true)
      val restored = SemanticManifest.fromJoinedJson(json, lSrc, rSrc)
      restored.salt shouldBe Some(10)
    } finally spark.stop()
  }

  test("joined envelope round-trips salt = None (default, no skew-handling)") {
    val spark = makeSpark()
    try {
      val lSrc = leftDf(spark)
      val rSrc = rightDf(spark)
      val lT = toSemanticTable(lSrc, name = Some("L"))
        .withDimensions(Dimension("id", _ => org.apache.spark.sql.functions.col("id")))
      val rT = toSemanticTable(rSrc, name = Some("R"))
        .withDimensions(Dimension("id", _ => org.apache.spark.sql.functions.col("id")))
      val joined = lT.join_on(rT, "id" -> "id")

      val json = SemanticManifest.toJoinedJson(joined, prettyPrint = true)
      val restored = SemanticManifest.fromJoinedJson(json, lSrc, rSrc)
      restored.salt shouldBe None
    } finally spark.stop()
  }
}
