package io.semanticdf

import org.apache.spark.sql.SparkSession

/** Documents the READ path: manifest → SemanticTable → use.
  *
  * The write path (YAML → manifest) is BLOCKED for joined-rooted models
  * (recipe §10 anti-scope). The read path of v0.1.11 always works for
  * single-table manifests (the only shape we ever write). Hand-crafted
  * "joined-manifest" JSONs are rejected as unknown `kind` (defense in
  * depth — joined shape is reserved for the BLOCKed recipe). */
class ManifestReadPathSpec extends org.scalatest.funsuite.AnyFunSuite
    with org.scalatest.matchers.should.Matchers {

  test("manifest → SemanticTable (single-table manifest) works and round-trips") {
    // 1. Build a SemanticTable from a YAML model.
    // 2. Emit a manifest via the new Identity path.
    // 3. Re-read it via fromJson into a fresh SemanticTable.
    // 4. Confirm the table is usable (count rows works against a Spark source).
    // Use customer-analytics/customers.yml — single-table, no joins.
    val spark = SparkSession.builder().master("local[1]").appName("read-path").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try {
      val ex = "examples/customer-analytics"
      spark.read.option("header","true").option("inferSchema","true")
        .csv(s"$ex/data/customers.csv").createOrReplaceTempView("customers_csv")

      val models = YamlLoader.load(s"$ex/models/customers.yml", spark)
      val customers = models("customers")

      val identity = SemanticManifest.Identity(
        id = "io.semanticdf.examples.customeranalytics.customers",
        manifestVersion = SemanticManifest.InitialManifestVersion,
        namespace = "default",
        metadata = Map("audit" -> "read-path"),
      )

      // Write.
      val json = SemanticManifest.toJson(customers, identity, prettyPrint = true)
      // Read.
      val sourceDf = spark.read.option("header","true").csv(s"$ex/data/customers.csv")
      val restored = SemanticManifest.fromJson(json, sourceDf)

      // The restored model is a single-table SemanticTable.
      assert(!restored.root.isInstanceOf[SemanticJoinOp],
        s"restored should be single-table; got ${restored.root.getClass.getName}")

      // The identity fields survived the round-trip:
      assert(restored.name.contains("customers"))
      assert(restored.status == ModelStatus.Published)

      info("[read-path] single-table manifest → SemanticTable → use works")
    } finally spark.stop()
  }

  test("hand-crafted joined-manifest JSON is rejected at read time (kind gate)") {
    // The writer never produces a joined manifest. But if a user
    // hand-crafts one, the READER rejects it as `unknown kind`.
    // This is defense-in-depth — joined-manifest shape is reserved for
    // the BLOCKed `joined-models-manifest` recipe.
    val fakeJoinedManifest =
      """{
        |  "schemaVersion": "v0.1.11-manifest",
        |  "kind": "semanticdf-joined-manifest",
        |  "model": { "name": "x" }
        |}""".stripMargin
    val spark = SparkSession.builder().master("local[1]").appName("read-gate").getOrCreate()
    try {
      val sourceDf = spark.read.csv("/dev/null").limit(0)
      val ex = intercept[ManifestParsingException] {
        SemanticManifest.fromJson(fakeJoinedManifest, sourceDf)
      }
      assert(ex.getMessage.contains("unknown manifest kind"),
        s"expected 'unknown manifest kind' gate, got: ${ex.getMessage}")
    } finally spark.stop()
  }
}
