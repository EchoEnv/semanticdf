package io.semanticdf

/** Documents the current CLI behavior when a YAML defines a joined model.
  *
  * The CLI's `tools.Main manifest` calls `SemanticManifest.toJson` for
  * each model produced by `YamlLoader.load` (or `loadDir`). If a model's
  * `root` op is `SemanticJoinOp`, `toJson` throws
  * `IllegalStateException("SemanticManifest.toJson: joined models
  * (SemanticJoinOp root) are not supported. See
  * docs/design/manifest-artifact.md §10.")`. The exception propagates
  * out of `Main.main`, the JVM dies, no manifest is written, the
  * operator sees a stacktrace.
  *
  * This is intentional and documented as anti-scope in
  * `docs/design/manifest-artifact.md` §10 — manifests are a
  * single-table artifact. Workaround for joined-rooted YAMLs: split
  * into their constituent single-table YAMLs and emit per-table
  * manifests; or compose a "joined catalog" later via the
  * `joined-models-manifest` recipe.
  *
  * These tests pin the contract via the direct library call (the same
  * path the CLI uses) so the behavior is documented and can be
  * intentionally evolved in future. */
class ManifestJoinedCliBehaviorSpec extends org.scalatest.funsuite.AnyFunSuite
    with org.scalatest.matchers.should.Matchers {

  test("toJson on a joined-rooted SemanticTable throws IllegalStateException with recipe §10 message") {
    val spark = org.apache.spark.sql.SparkSession.builder()
      .master("local[1]").appName("joined-behavior").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try {
      val ex = "examples/customer-analytics"
      spark.read.option("header","true").option("inferSchema","true")
        .csv(s"$ex/data/customers.csv").createOrReplaceTempView("customers_csv")
      spark.read.option("header","true").option("inferSchema","true")
        .csv(s"$ex/data/orders.csv").createOrReplaceTempView("orders_csv")

      val models = YamlLoader.loadDir(s"$ex/models", spark)
      val orders = models.getOrElse("orders",
        fail(s"`orders` model not loaded. Got: ${models.keys.mkString(", ")}"))

      assert(orders.root.isInstanceOf[SemanticJoinOp],
        s"orders.root should be SemanticJoinOp, was ${orders.root.getClass.getName}")

      val identity = SemanticManifest.Identity(
        id              = "io.semanticdf.examples.customeranalytics.orders",
        manifestVersion = SemanticManifest.InitialManifestVersion,
        namespace       = "default",
        metadata        = Map.empty,
      )

      val thrown = intercept[IllegalStateException] {
        SemanticManifest.toJson(orders, identity, prettyPrint = true)
      }
      assert(thrown.getMessage.contains("joined"),
        s"expected message to mention 'joined', got: ${thrown.getMessage}")
      assert(thrown.getMessage.contains("docs/design/manifest-artifact.md"),
        s"expected message to point at the design doc, got: ${thrown.getMessage}")
    } finally spark.stop()
  }

  test("CLI flow: a joined model aborts the multi-model emit loop with IllegalStateException") {
    // Replicates the runManifest loop byte-for-byte. Documents that
    // when YAML produces multiple models and one of them is joined,
    // the loop aborts on the joined one with `IllegalStateException`
    // (recipe §10 anti-scope). The CLI's exception aborts the run
    // entirely — whatever bytes were accumulated in the in-memory
    // buffer are dropped (the `--out` writer is never reached), so
    // partial output never reaches disk.
    //
    // This test pins the THROW contract. It does NOT assert on the
    // buffer's content because HashMap iteration order is
    // implementation-defined: depending on which model is iterated
    // first, the buffer may be empty (joined-first) or hold one
    // single-table model's bytes (single-table-first). Both are
    // acceptable; the CLI still aborts and drops the buffer.
    // Replicates the runManifest loop byte-for-byte. Documents that
    // when YAML produces multiple models and one of them is joined,
    // the loop aborts on the joined one and any earlier models'
    // manifest bytes (already accumulated in the buffer) are lost —
    // no output file is written.
    val spark = org.apache.spark.sql.SparkSession.builder()
      .master("local[1]").appName("joined-flow").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try {
      spark.read.option("header","true").option("inferSchema","true")
        .csv("examples/customer-analytics/data/customers.csv").createOrReplaceTempView("customers_csv")
      spark.read.option("header","true").option("inferSchema","true")
        .csv("examples/customer-analytics/data/orders.csv").createOrReplaceTempView("orders_csv")

      val models = YamlLoader.loadDir("examples/customer-analytics/models", spark)
      val identity = SemanticManifest.Identity(
        id              = "io.semanticdf.examples.customeranalytics.orders",
        manifestVersion = SemanticManifest.InitialManifestVersion,
        namespace       = "default",
        metadata        = Map.empty,
      )

      val buf = new StringBuilder
      val ex = intercept[IllegalStateException] {
        models.values.foreach { m =>
          buf.append(SemanticManifest.toJson(m, identity, prettyPrint = true))
          buf.append('\n')
        }
      }
      assert(ex.getMessage.contains("joined"),
        s"the thrown exception should mention 'joined', got: ${ex.getMessage}")
      info("[joined-flow] buffer length after exception: " + buf.length)
      // See test scaladoc above for why we don't pin the buffer's content.
    } finally spark.stop()
  }
}
