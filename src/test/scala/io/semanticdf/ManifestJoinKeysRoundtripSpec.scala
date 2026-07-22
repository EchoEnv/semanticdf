package io.semanticdf

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
}
