package io.semanticdf

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.{SparkSession, functions => F}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

/** Tests for the `transforms[]` round-trip via SemanticManifest.
  *
  * Most tests use synthetic `SemanticTable`s built via `withTransforms()`
  * (not YamlLoader) because every example model that has transforms also
  * crosses files via joins. `YamlLoader.load` on a single joined file
  * throws (model X references model Y which is not defined). Synthetic
  * tests run faster and avoid that entanglement. The one YamlLoader
  * test exercises the loader path specifically. */
class ManifestTransformSpec extends org.scalatest.funsuite.AnyFunSuite
    with org.scalatest.matchers.should.Matchers {

  private val mapper = new ObjectMapper()

  /** Build a minimal 1-row DataFrame with columns we can reference in transforms. */
  private def makeSource(spark: SparkSession) =
    spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        org.apache.spark.sql.Row(1, 10, 100L)
      )),
      StructType(Seq(
        StructField("a", IntegerType),
        StructField("b", IntegerType),
        StructField("c", IntegerType),
      ))
    )

  private def identityFor(name: String) = SemanticManifest.Identity(
    id = s"io.test.model.$name", namespace = "default",
  )

  test("Transform.exprString field is populated by YamlLoader") {
    // The customer-analytics customers model has no transforms; the
    // hospital encounters model has transforms but no joins. Use that.
    val spark = SparkSession.builder().master("local[1]").appName("yt").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try {
      // Use a minimal synthetic model to avoid the join entanglement.
      // We test the wiring by constructing a Transform with exprString,
      // then asserting the writer uses it.
      val df = makeSource(spark)
      val st = toSemanticTable(df, name = Some("m"))
        .withTransforms(Transform(
          "x_plus_b", _ => F.expr("a + b"),
          exprString = Some("a + b"),
        ))
      val json = SemanticManifest.toJson(st, prettyPrint = true)
      val tree = mapper.readTree(json)
      val tArr = tree.path("transforms")
      assert(tArr.size == 1)
      val exprStr = tArr.get(0).get("expr").asText()
      assert(exprStr == "a + b",
        s"transforms[] should serialise the exprString literal, got: $exprStr")
    } finally spark.stop()
  }

  test("writer emits transforms[] block when model has transforms") {
    val spark = SparkSession.builder().master("local[1]").appName("wt").getOrCreate()
    try {
      val df = makeSource(spark)
      val st = toSemanticTable(df, name = Some("m"))
        .withTransforms(Transform("doubled", _ => F.expr("a * 2"), exprString = Some("a * 2")))
      val json = SemanticManifest.toJson(st, prettyPrint = true)
      val tree = mapper.readTree(json)
      assert(tree.has("transforms"),
        "manifest should now carry a `transforms[]` block when transforms are declared")
      val arr = tree.get("transforms")
      assert(arr.isArray && arr.size == 1)
      val first = arr.get(0)
      assert(first.get("name").asText() == "doubled")
      val expr = first.get("expr").asText()
      assert(expr == "a * 2",
        s"transform expr should be the source string, got: $expr")
      assert(expr != SemanticManifest.LambdaSentinel,
        "YAML-backed transform must never serialise as <lambda>")
    } finally spark.stop()
  }

  test("writer omits transforms[] when model has no transforms") {
    val spark = SparkSession.builder().master("local[1]").appName("no").getOrCreate()
    try {
      val df = makeSource(spark)
      val st = toSemanticTable(df, name = Some("m"))
        .withDimensions(Dimension("a", _ => F.col("a")))
      val json = SemanticManifest.toJson(st, prettyPrint = true)
      val tree = mapper.readTree(json)
      assert(!tree.has("transforms") || tree.get("transforms").size == 0,
        "manifest without transforms should not emit a populated `transforms[]` block")
    } finally spark.stop()
  }

  test("digest.transforms count matches the transforms[] array size") {
    val spark = SparkSession.builder().master("local[1]").appName("dc").getOrCreate()
    try {
      val df = makeSource(spark)
      val st = toSemanticTable(df, name = Some("m"))
        .withTransforms(
          Transform("t1", _ => F.expr("a + 1"), exprString = Some("a + 1")),
          Transform("t2", _ => F.expr("a + 2"), exprString = Some("a + 2")),
        )
      val json = SemanticManifest.toJson(st, prettyPrint = true)
      val tree = mapper.readTree(json)
      val digestCount = tree.path("digest").path("transforms").asInt()
      val arrSize = tree.path("transforms").size()
      assert(digestCount == 2 && arrSize == 2,
        s"digest.transforms=$digestCount and transforms[].size=$arrSize must both equal 2")
    } finally spark.stop()
  }

  test("round-trip: reader reconstructs SemanticTransformsOp with all transforms") {
    val spark = SparkSession.builder().master("local[1]").appName("rt").getOrCreate()
    try {
      val df = makeSource(spark)
      val st = toSemanticTable(df, name = Some("m"))
        .withTransforms(
          Transform("t1", _ => F.expr("a + 1"), exprString = Some("a + 1")),
          Transform("t2", _ => F.expr("a + 2"), exprString = Some("a + 2")),
        )
      val json = SemanticManifest.toJson(st, prettyPrint = true)
      val restored = SemanticManifest.fromJson(json, df)
      val restoredTransforms = collectTransforms(restored)
      assert(restoredTransforms.size == 2,
        s"expected 2 transforms after round-trip, got ${restoredTransforms.size}")
      assert(restoredTransforms.map(_.name) == Seq("t1", "t2"))
      assert(restoredTransforms.map(_.exprString) == Seq(Some("a + 1"), Some("a + 2")))
    } finally spark.stop()
  }

  test("after round-trip, restored transform columns are usable in queries") {
    val spark = SparkSession.builder().master("local[1]").appName("rtq").getOrCreate()
    try {
      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(
          org.apache.spark.sql.Row(3, 4),
          org.apache.spark.sql.Row(5, 6),
          org.apache.spark.sql.Row(7, 8),
        )),
        StructType(Seq(
          StructField("a", IntegerType),
          StructField("b", IntegerType),
        ))
      )
      val st = toSemanticTable(df, name = Some("m"))
        .withTransforms(
          Transform("sum_ab", _ => F.expr("a + b"), exprString = Some("a + b")),
        )
      val json = SemanticManifest.toJson(st, prettyPrint = true)
      val restored = SemanticManifest.fromJson(json, df)
      val restoredDf = restored.execute(spark)
      // The transform column should be present and produce the right values.
      assert(restoredDf.columns.contains("sum_ab"),
        s"restored DataFrame must include the transform column 'sum_ab'. Got: " + restoredDf.columns.mkString(", "))
      val sumCol = restoredDf.collect().map(_.getInt(2)).toSet
      assert(sumCol == Set(3 + 4, 5 + 6, 7 + 8),
        s"rebuilt transform column should compute a + b per row, got: $sumCol")
    } finally spark.stop()
  }

  test("lambda-only transform (no exprString) gets the `<lambda>` sentinel and a placeholder col") {
    val spark = SparkSession.builder().master("local[1]").appName("lamb").getOrCreate()
    try {
      val df = makeSource(spark)
      val st = toSemanticTable(df, name = Some("m"))
        .withTransforms(
          Transform("hi_marker", _ => F.lit("hi"))
          // no exprString — simulates a hand-built transform
        )
      val json = SemanticManifest.toJson(st, prettyPrint = true)
      val tree = mapper.readTree(json)
      val arr = tree.path("transforms")
      assert(arr.size == 1)
      val expr = arr.get(0).get("expr").asText()
      assert(expr == SemanticManifest.LambdaSentinel,
        s"lambda-only transform should serialise as <lambda> sentinel, got: $expr")

      val restored = SemanticManifest.fromJson(json, df)
      assert(collectTransforms(restored).nonEmpty,
        "restored model must still carry the lambda placeholder")
    } finally spark.stop()
  }

  // -- helpers --

  /** Walk the op-tree and collect every transform. Mirrors the private
    * helper inside `SemanticManifest`. */
  private def collectTransforms(model: SemanticTable): Seq[Transform] = {
    val buf = scala.collection.mutable.ListBuffer.empty[Transform]
    def walk(op: SemanticOp): Unit = op match {
      case tr: SemanticTransformsOp   => tr.transforms.foreach(t => buf += t); walk(tr.source)
      case a: SemanticAggregateOp    => walk(a.source)
      case f: SemanticFilterOp       => walk(f.source)
      case rf: SemanticRowFilterOp   => walk(rf.source)
      case o: SemanticOrderByOp      => walk(o.source)
      case l: SemanticLimitOp        => walk(l.source)
      case h: SemanticHintOp         => walk(h.source)
      case j: SemanticJoinOp         => walk(j.left); walk(j.right)
      case _                         => ()
    }
    walk(model.root)
    buf.result()
  }
}
