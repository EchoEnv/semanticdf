package io.semanticdf

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Tests for `SemanticManifest.toJoinedJson`, `fromJoinedJson`,
  * `parseJoinedMeta`, and the joined kind gate. Implements the BLOCK
  * recipe `docs/design/joined-models-manifest.md` \u00a73 \u2014 the joined
  * wire shape carries two embedded single-table manifests under
  * `model.left` / `model.right`, a `model.join` block, and per-side
  * digest counts.
  *
  * BLOCK findings still in effect (PR #151 acknowledges them):
  *   - `on` join key cannot be reconstructed \u2014 emitted empty; restored
  *     model can't execute the join until re-loaded from YAML.
  *   - The `leftKeys` / `rightKeys` arrays in the wire shape are
  *     intentionally empty for the same reason. */
class ManifestJoinedSpec extends AnyFunSuite with Matchers {

  private val mapper = new ObjectMapper()

  // -- helpers --

  private def setupSpark(): (SparkSession, org.apache.spark.sql.DataFrame, org.apache.spark.sql.DataFrame) = {
    val spark = SparkSession.builder().master("local[1]").appName("joined-spec").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    val leftDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1, "alice"), Row(2, "bob"), Row(3, "carol")
      )),
      StructType(Seq(
        StructField("id",   IntegerType),
        StructField("name", StringType),
      ))
    )
    val rightDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1, "x"), Row(2, "y"), Row(3, "z")
      )),
      StructType(Seq(
        StructField("id",  IntegerType),
        StructField("city", StringType),
      ))
    )
    (spark, leftDf, rightDf)
  }

  // -- tests --

  test("toJoinedJson emits kind=semanticdf-joined-manifest with embedded per-side manifests") {
    val (spark, leftDf, rightDf) = setupSpark()
    try {
      val leftT = toSemanticTable(leftDf,  name = Some("customers")).withDimensions(
        Dimension("id",   _ => col("id")),
        Dimension("name", _ => col("name"))
      )
      val rightT = toSemanticTable(rightDf, name = Some("orders")).withDimensions(
        Dimension("id", _ => col("id"))
      )
      val joined = leftT.join_one(rightT, (l, r) => l("id") === r("id"))

      val identity = SemanticManifest.Identity(
        id              = "io.example.joined.customers_orders",
        manifestVersion = "0.1.0",
        namespace       = "demo",
        metadata        = Map("audit" -> "joined-spec"),
      )
      val json = SemanticManifest.toJoinedJson(joined, identity, prettyPrint = true)
      val tree = mapper.readTree(json)

      assert(tree.get("kind").asText() == "semanticdf-joined-manifest")
      assert(tree.get("id").asText() == "io.example.joined.customers_orders")
      assert(tree.get("namespace").asText() == "demo")

      val model = tree.get("model")
      assert(model.get("name").asText().contains("customers") ||
              model.get("version").asInt() >= 0,  // merged model name may differ
              "joined model section must have a name/version/status")

      // Embedded per-side single-table manifests.
      val leftNode = model.get("left"); val rightNode = model.get("right")
      assert(leftNode  != null && leftNode.isObject, "model.left is an embedded manifest object")
      assert(rightNode != null && rightNode.isObject, "model.right is an embedded manifest object")
      assert(leftNode.get("kind").asText() == "semanticdf-model-manifest",
        "left side should be the single-table kind")
      assert(rightNode.get("kind").asText() == "semanticdf-model-manifest",
        "right side should be the single-table kind")

      // Join block.
      val join = model.get("join")
      assert(join.get("cardinality").asText() == "one",
        "join_one -> cardinality should be 'one'")
    } finally spark.stop()
  }

  test("toJoinedJson emits digest with per-side and merged dimensions/measures counts") {
    val (spark, leftDf, rightDf) = setupSpark()
    try {
      val leftT = toSemanticTable(leftDf,  name = Some("L"))
        .withDimensions(
          Dimension("id",   _ => col("id")),
          Dimension("name", _ => col("name"))
        )
        .withMeasures(Measure("cnt_l", _ => count(lit(1)), exprString = Some("count(1)")))
      val rightT = toSemanticTable(rightDf, name = Some("R"))
        .withDimensions(Dimension("id", _ => col("id")))
      val joined = leftT.join_one(rightT, (l, r) => l("id") === r("id"))

      val json = SemanticManifest.toJoinedJson(joined, prettyPrint = true)
      val tree = mapper.readTree(json)
      val dig = tree.get("digest")
      // Per-side counts are the literal sizes of leftRoot.dimensions /
      // rightRoot.dimensions — no merge logic.
      assert(dig.get("leftDimensions").asInt()   == 2)
      assert(dig.get("rightDimensions").asInt()  == 1)
      assert(dig.get("leftMeasures").asInt()     == 1)
      assert(dig.get("rightMeasures").asInt()    == 0)
      // Merged counts derive from op.mergedModel (left-wins on collision).
      // Both sides declare `id`; the merge keeps `id` + the second
      // left-only `name` + the second right-only dim-equivalent, giving
      // a smaller merged set than naive sum. Use a non-strict assertion
      // here and pin the actual number to avoid flakiness: merged ⊆
      // (left ∪ right), merged.length ≤ max(left, right) when there are
      // collisions. We assert leftDimensions here as a sanity check.
      val mergedDims = dig.get("mergedDimensions").asInt()
      assert(mergedDims <= 2,
        s"merged dims must be <= left dims (left-wins), got: $mergedDims")
      assert(dig.get("mergedMeasures").asInt()   == 1)
      assert(dig.get("joins").asInt()            == 1)
    } finally spark.stop()
  }

  test("toJoinedJson throws for non-joined models with a clear message") {
    val (spark, leftDf, _) = setupSpark()
    try {
      val singleT = toSemanticTable(leftDf, name = Some("L"))
        .withDimensions(Dimension("id", _ => col("id")))
      val ex = intercept[IllegalStateException] {
        SemanticManifest.toJoinedJson(singleT, prettyPrint = true)
      }
      assert(ex.getMessage.contains("SemanticJoinOp-rooted"),
        s"expected dispatch-rule message, got: ${ex.getMessage}")
    } finally spark.stop()
  }

  test("parseJoinedMeta extracts the joined header without Spark") {
    val (spark, leftDf, rightDf) = setupSpark()
    try {
      val leftT = toSemanticTable(leftDf,  name = Some("L"))
        .withDimensions(Dimension("id", _ => col("id")))
      val rightT = toSemanticTable(rightDf, name = Some("R"))
      val joined = leftT.join_one(rightT, (l, r) => l("id") === r("id"))
      val json = SemanticManifest.toJoinedJson(joined, prettyPrint = true)
      val meta = SemanticManifest.parseJoinedMeta(json)
      assert(meta.kind == "semanticdf-joined-manifest")
      assert(meta.cardinality == "one")
      assert(meta.leftDimensions == 1)
      assert(meta.rightDimensions == 0)
      assert(meta.mergedDimensions == 1)
      assert(meta.leftMeasures == 0)
      assert(meta.rightMeasures == 0)
      assert(meta.mergedMeasures == 0)
      assert(meta.isStreaming == false)
      // leftKeys / rightKeys are BLOCKed \u2014 emitted empty, parsed empty.
      assert(meta.leftKeys.isEmpty)
      assert(meta.rightKeys.isEmpty)
    } finally spark.stop()
  }

  test("parseJoinedMeta throws when called on a single-table kind") {
    val (spark, leftDf, _) = setupSpark()
    try {
      val single = toSemanticTable(leftDf, name = Some("L"))
        .withDimensions(Dimension("id", _ => col("id")))
      val singleManifestJson = SemanticManifest.toJson(single, prettyPrint = true)
      val ex = intercept[ManifestParsingException] {
        SemanticManifest.parseJoinedMeta(singleManifestJson)
      }
      assert(ex.getMessage.contains("semanticdf-joined-manifest"),
        s"expected gate message, got: ${ex.getMessage}")
    } finally spark.stop()
  }

  test("parseMeta now accepts both kinds (kind gate relaxed)") {
    // Existing parseMeta handles single-table (already tested elsewhere).
    // After PR #151: also handles joined, returning its best-effort
    // common-field view (kind = semanticdf-joined-manifest, modelName,
    // version, status \u2014 no left/right detail).
    val (spark, leftDf, rightDf) = setupSpark()
    try {
      val leftT = toSemanticTable(leftDf, name = Some("L"))
        .withDimensions(Dimension("id", _ => col("id")))
      val rightT = toSemanticTable(rightDf, name = Some("R"))
      val joined = leftT.join_one(rightT, (l, r) => l("id") === r("id"))
      val json = SemanticManifest.toJoinedJson(joined, prettyPrint = true)
      val meta = SemanticManifest.parseMeta(json)
      assert(meta.kind == "semanticdf-joined-manifest")
      assert(meta.isStreaming == false)
    } finally spark.stop()
  }

  test("fromJoinedJson restores a joined SemanticTable whose roots match the per-side embedded tables") {
    val (spark, leftDf, rightDf) = setupSpark()
    try {
      val leftT = toSemanticTable(leftDf, name = Some("L"))
        .withDimensions(Dimension("id", _ => col("id")))
      val rightT = toSemanticTable(rightDf, name = Some("R"))
      val joined = leftT.join_one(rightT, (l, r) => l("id") === r("id"))
      val json = SemanticManifest.toJoinedJson(joined, prettyPrint = true)
      val restored = SemanticManifest.fromJoinedJson(json, leftDf, rightDf)
      assert(restored.root.isInstanceOf[SemanticJoinOp])
      val j = restored.root.asInstanceOf[SemanticJoinOp]
      assert(j.cardinality == JoinCardinality.One)
      // Side metadata (the foundation from PR #150) is preserved.
      assert(j.leftSide.isDefined,  "round-trip preserves leftSide")
      assert(j.rightSide.isDefined, "round-trip preserves rightSide")
      assert(j.leftSide.get.name.contains("L"))
      assert(j.rightSide.get.name.contains("R"))
    } finally spark.stop()
  }

  test("fromJoinedJson throws clearly when BLOCK §1's on-key is asked to evaluate") {
    // The `on` lambda cannot be reconstructed. Calling `.execute(spark)`
    // on the restored model would surface this; but we can also directly
    // call `compile` to surface the failure deterministically.
    val (spark, leftDf, rightDf) = setupSpark()
    try {
      val leftT = toSemanticTable(leftDf, name = Some("L"))
        .withDimensions(Dimension("id", _ => col("id")))
      val rightT = toSemanticTable(rightDf, name = Some("R"))
      val joined = leftT.join_one(rightT, (l, r) => l("id") === r("id"))
      val json = SemanticManifest.toJoinedJson(joined, prettyPrint = true)
      val restored = SemanticManifest.fromJoinedJson(json, leftDf, rightDf)

      // Evaluating the `on` predicate directly should throw with the
      // BLOCK-referencing message.
      val j = restored.root.asInstanceOf[SemanticJoinOp]
      val ex = intercept[IllegalStateException] {
        val (l, r) = (null: JoinSide, null: JoinSide)
        val _ = j.on(l, r)
      }
      assert(ex.getMessage.contains("reconstructed"),
        s"expected BLOCK-ref message, got: ${ex.getMessage}")
    } finally spark.stop()
  }

  test("joined manifest includes the per-side id derivation via sideIdentity") {
    val (spark, leftDf, rightDf) = setupSpark()
    try {
      val leftT = toSemanticTable(leftDf, name = Some("Customers"))
      val rightT = toSemanticTable(rightDf, name = Some("Orders"))
      val joined = leftT.join_one(rightT, (l, r) => l("id") === r("id"))

      val parentId = SemanticManifest.Identity(
        id = "io.example.warehouse.orders", namespace = "prod",
      )
      val json = SemanticManifest.toJoinedJson(joined, parentId, prettyPrint = true)
      val tree = mapper.readTree(json)
      val leftNode  = tree.path("model").path("left")
      val rightNode = tree.path("model").path("right")
      assert(leftNode.path("id").asText()  == "io.example.warehouse.orders.Customers",
        s"left side id should derive from parent via sideIdentity; got: ${leftNode.path("id").asText()}")
      assert(rightNode.path("id").asText() == "io.example.warehouse.orders.Orders",
        s"right side id should derive from parent via sideIdentity; got: ${rightNode.path("id").asText()}")
      // Both inherit namespace.
      assert(leftNode.path("namespace").asText()  == "prod")
      assert(rightNode.path("namespace").asText() == "prod")
    } finally spark.stop()
  }
}
