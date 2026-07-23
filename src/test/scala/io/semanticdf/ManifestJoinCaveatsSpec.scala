package io.semanticdf

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Tests for the joined-manifest Path C (caveats §1.2 + §1.3 from
  * `docs/design/joined-models-manifest.md`).
  *
  * Caveat §1.2: alias-prefixed dims/measures (e.g. `carriers.name`)
  *   now flow through the joined wire shape via `model.extra_dimensions[]`
  *   / `model.extra_measures[]`. The reader reconstructs them as a
  *   `SemanticTransformsOp` wrapper.
  *
  * Caveat §1.3: `leftPrefix` / `rightPrefix` on the `join` block now
  *   round-trip end-to-end. The reconstructed `on` lambda applies the
  *   prefixes so joined DataFrames with overlapping column names
  *   resolve correctly. */
class ManifestJoinCaveatsSpec extends AnyFunSuite with Matchers {

  private val mapper = new ObjectMapper()

  private def setupSpark(): SparkSession = {
    val spark = SparkSession.builder().master("local[1]").appName("jcc").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark
  }

  // -- §1.2: alias-prefixed dims/measures round-trip --

  test("§1.2: extra_dimensions[] round-trip preserves alias-prefixed dims") {
    val spark = setupSpark()
    try {
      val ordersDf = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(
          Row(1, 100, "2024-01-01"),
          Row(2, 200, "2024-01-02"),
        )),
        StructType(Seq(
          StructField("id", IntegerType),
          StructField("amount", IntegerType),
          StructField("order_date", StringType),
        ))
      )
      val customersDf = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(
          Row(100, "alice"),
          Row(200, "bob"),
        )),
        StructType(Seq(
          StructField("customer_id", IntegerType),
          StructField("name",        StringType),
        ))
      )
      val ordersT    = toSemanticTable(ordersDf,    name = Some("orders"))
      val customersT = toSemanticTable(customersDf, name = Some("customers"))

      // Mimic YamlLoader's runtime wiring: join, then add the
      // alias-prefixed dimension on the right side via `withDimensions`.
      val joinedBare = ordersT.join_one(
        customersT,
        (l, r) => l("customer_id") === r("customer_id")
      )
      val joined = joinedBare.withDimensions(
        new Dimension(
          name = "customers.name",
          expr = _ => col("name"),
          description = Some("Customer name (aliased)"),
          exprString   = Some("customers.name"))
      )
      val j = joined.root.asInstanceOf[SemanticJoinOp]
      assert(j.extraDimensions.contains("customers.name"),
        s"expected extraDimensions to contain customers.name, got: ${j.extraDimensions.keys}")

      val identity = SemanticManifest.Identity(
        id = "io.example.test.caveats12", namespace = "demo",
        metadata = Map("audit" -> "caveat-1.2"))
      val json = SemanticManifest.toJoinedJson(joined, identity, prettyPrint = true)
      val tree = mapper.readTree(json)
      val model = tree.path("model")

      val extraDims = model.path("extra_dimensions")
      assert(extraDims.isArray, s"writer should emit extra_dimensions[], got: $model")
      assert(extraDims.size == 1, s"expected 1 extra_dim, got ${extraDims.size}")
      assert(extraDims.get(0).get("name").asText() == "customers.name",
        s"expected customers.name, got: ${extraDims.get(0)}")
      assert(extraDims.get(0).get("kind").asText() == "categorical",
        s"expected kind=categorical, got: ${extraDims.get(0).get("kind")}")

      val restored = SemanticManifest.fromJoinedJson(json, ordersDf, customersDf)
      assert(restored.root.isInstanceOf[SemanticTransformsOp],
        s"restored root should be SemanticTransformsOp (wraps the base join with the extras), " +
        s"got: ${restored.root.getClass.getName}")
      val transformed = restored.root.asInstanceOf[SemanticTransformsOp]
      assert(transformed.source.isInstanceOf[SemanticJoinOp],
        s"transforms.source should be a SemanticJoinOp, got: ${transformed.source.getClass.getName}")
    } finally spark.stop()
  }

  test("§1.2: extra_measures[] round-trip preserves alias-prefixed measures") {
    val spark = setupSpark()
    try {
      val leftDf = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1, 100), Row(2, 200))),
        StructType(Seq(StructField("id", IntegerType), StructField("qty", IntegerType)))
      )
      val rightDf = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(100, "alice"))),
        StructType(Seq(StructField("id", IntegerType), StructField("alias_v", StringType)))
      )
      val leftT  = toSemanticTable(leftDf, name = Some("L"))
        .withDimensions(Dimension("id", _ => col("id")))
      val rightT = toSemanticTable(rightDf, name = Some("R"))
        .withDimensions(Dimension("id", _ => col("id")))
      val joinedBare2 = leftT.join_one(rightT, (l, r) => l("id") === r("id"))
      val joined = joinedBare2.withMeasures(
        new Measure(
          name = "orders.count_alias",
          expr = _ => count(lit(1)),
          description = Some("Count per join alias"),
          exprString   = Some("count(1)"))
      )
      val j = joined.root.asInstanceOf[SemanticJoinOp]
      assert(j.extraMeasures.contains("orders.count_alias"),
        s"expected orders.count_alias in extraMeasures, got: ${j.extraMeasures.keys}")

      val json = SemanticManifest.toJoinedJson(joined, prettyPrint = true)
      val tree = mapper.readTree(json)
      val extraM = tree.path("model").path("extra_measures")
      assert(extraM.isArray && extraM.size == 1,
        s"writer should emit extra_measures[] with 1 entry, got: $extraM")
      assert(extraM.get(0).get("name").asText() == "orders.count_alias")
    } finally spark.stop()
  }

  test("§1.2: joined manifest without extras still round-trips (back-compat for legacy)") {
    val spark = setupSpark()
    try {
      val lDf = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1, "x"), Row(2, "y"))),
        StructType(Seq(StructField("id", IntegerType), StructField("name", StringType)))
      )
      val rDf = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1, 100))),
        StructType(Seq(StructField("id", IntegerType), StructField("qty", IntegerType)))
      )
      val leftT  = toSemanticTable(lDf, name = Some("L"))
        .withDimensions(Dimension("id", _ => col("id")))
      val rightT = toSemanticTable(rDf, name = Some("R"))
      val joined = leftT.join_one(rightT, (l, r) => l("id") === r("id"))
      val json = SemanticManifest.toJoinedJson(joined, prettyPrint = true)
      val tree = mapper.readTree(json)
      val model = tree.path("model")
      assert(!model.has("extra_dimensions") || model.path("extra_dimensions").size() == 0,
        s"writer should not emit empty extra_dimensions[] for legacy models, got: $model")
      val restored = SemanticManifest.fromJoinedJson(json, lDf, rDf)
      assert(restored.root.isInstanceOf[SemanticJoinOp],
        s"restored root should be a plain SemanticJoinOp (no extras), got: ${restored.root.getClass.getName}")
    } finally spark.stop()
  }

  // -- §1.3: leftPrefix / rightPrefix round-trip --

  test("§1.3: leftPrefix / rightPrefix on the join block round-trip via the joined manifest") {
    val spark = setupSpark()
    try {
      val lDf = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1, "x"))),
        StructType(Seq(StructField("id", IntegerType), StructField("name", StringType)))
      )
      val rDf = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1, 100))),
        StructType(Seq(StructField("id", IntegerType), StructField("qty", IntegerType)))
      )
      val leftT  = toSemanticTable(lDf, name = Some("L"))
        .withDimensions(Dimension("id", _ => col("id")))
      val rightT = toSemanticTable(rDf, name = Some("R"))
      val joined = leftT.join_one(rightT, (l, r) => l("id") === r("id"))

      // Build a join with prefixes via .copy() and round-trip via the manifest.
      val withPrefixes = joined.root.asInstanceOf[SemanticJoinOp]
        .copy(leftPrefix = "left_", rightPrefix = "right_")
      val withT = new SemanticTable(withPrefixes)
      val json = SemanticManifest.toJoinedJson(withT, prettyPrint = true)
      val tree = mapper.readTree(json)
      val jb = tree.path("model").path("join")
      assert(jb.path("leftPrefix").asText() == "left_",
        s"expected leftPrefix=left_, got: ${jb.path("leftPrefix")}")
      assert(jb.path("rightPrefix").asText() == "right_",
        s"expected rightPrefix=right_, got: ${jb.path("rightPrefix")}")
      assert(jb.path("leftKeys").get(0).asText() == "id")
      assert(jb.path("rightKeys").get(0).asText() == "id")

      val meta = SemanticManifest.parseJoinedMeta(json)
      assert(meta.leftPrefix == "left_")
      assert(meta.rightPrefix == "right_")
    } finally spark.stop()
  }

  test("§1.3: prefixes apply at the reconstructed `on` lambda") {
    val spark = setupSpark()
    try {
      val lDf = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1, "x"))),
        StructType(Seq(StructField("id", IntegerType), StructField("name", StringType)))
      )
      val rDf = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1, 100))),
        StructType(Seq(StructField("id", IntegerType), StructField("qty", IntegerType)))
      )
      val leftT  = toSemanticTable(lDf, name = Some("L"))
        .withDimensions(Dimension("id", _ => col("id")))
      val rightT = toSemanticTable(rDf, name = Some("R"))
      val joined = leftT.join_one(rightT, (l, r) => l("id") === r("id"))
      val withPrefixes = joined.root.asInstanceOf[SemanticJoinOp]
        .copy(leftPrefix = "L_", rightPrefix = "R_")
      val withT = new SemanticTable(withPrefixes)
      val json = SemanticManifest.toJoinedJson(withT, prettyPrint = true)
      val restored = SemanticManifest.fromJoinedJson(json, lDf, rDf)
      val rj = restored.root.asInstanceOf[SemanticJoinOp]
      val probeL = JoinSide.recording("L", scala.collection.mutable.LinkedHashMap.empty[String, Boolean])
      val probeR = JoinSide.recording("R", scala.collection.mutable.LinkedHashMap.empty[String, Boolean])
      val _ = rj.on(probeL, probeR)
      assert(probeL.captured.keys.exists(_.contains("L_id")),
        s"reconstructed on should reference L_id, got: ${probeL.captured.toMap}")
      assert(probeR.captured.keys.exists(_.contains("R_id")),
        s"reconstructed on should reference R_id, got: ${probeR.captured.toMap}")
    } finally spark.stop()
  }

  test("§1.3: no prefixes — reconstructed `on` uses bare column names (canonical case)") {
    val spark = setupSpark()
    try {
      val lDf = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1, "x"))),
        StructType(Seq(StructField("id", IntegerType), StructField("name", StringType)))
      )
      val rDf = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1, 100))),
        StructType(Seq(StructField("id", IntegerType), StructField("qty", IntegerType)))
      )
      val leftT  = toSemanticTable(lDf, name = Some("L"))
        .withDimensions(Dimension("id", _ => col("id")))
      val rightT = toSemanticTable(rDf, name = Some("R"))
      val joined = leftT.join_one(rightT, (l, r) => l("id") === r("id"))
      val json = SemanticManifest.toJoinedJson(joined, prettyPrint = true)
      val jb = mapper.readTree(json).path("model").path("join")
      assert(!jb.has("leftPrefix") || jb.path("leftPrefix").isNull,
        s"writer should omit empty leftPrefix, got: ${jb.path("leftPrefix")}")
      assert(!jb.has("rightPrefix") || jb.path("rightPrefix").isNull,
        s"writer should omit empty rightPrefix, got: ${jb.path("rightPrefix")}")
      val meta = SemanticManifest.parseJoinedMeta(json)
      assert(meta.leftPrefix == "")
      assert(meta.rightPrefix == "")
    } finally spark.stop()
  }
}
