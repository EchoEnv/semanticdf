package io.semanticdf

import scala.io.Source

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.{Row, functions => F}
import org.apache.spark.sql.types._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import io.semanticdf.ManifestParsingException
import io.semanticdf.SemanticManifest.ManifestMeta

/** Tests for [[SemanticManifest]] — round-trip persistence of a
  * `SemanticTable` to a portable JSON artifact.
  *
  * Test plan (mirrors `docs/design/manifest-artifact.md` §7):
  *   1. round-trip-preserves-model-identity
  *   2. round-trip-preserves-dims-and-measures
  *   3. round-trip-preserves-classification
  *   4. round-trip-preserves-pre-aggregate-filters
  *   5. round-trip-strips-post-aggregate-filters (lossy; documented)
  *   6. lambda-only-dims-round-trip-as-sentinel
  *   7. joined-models-rejected
  *   8. schema-version-mismatch-throws
  *   9. parse-meta-source-free
  *   10. unknown-fields-tolerated
  *   11. streaming-source-walks-aggregate-wrapper
  *   12. uses-t-all-flag (extension)
  */
class SemanticManifestSpec
  extends AnyFunSpec
  with Matchers
  with SparkSessionFixture {

  // -- 1. round-trip-preserves-model-identity --------------------------------

  describe("SemanticManifest.toJson + fromJson") {
    it("round-trips model identity") {
      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1, "a"), Row(2, "b"))),
        StructType(Seq(
          StructField("id",   IntegerType),
          StructField("kind", StringType),
        ))
      )
      val model = toSemanticTable(df,
        name        = Some("kpi_facts"),
        description = Some("the kpi facts table"),
        sourceTable = Some("kpi_facts_raw"))
        .withDimensions(Dimension("kind", _ => df("kind")))
        .withMeasures(Measure("count", _ => F.lit(1)))
        .version(7)

      val json = SemanticManifest.toJson(model)
      SemanticManifest.parseMeta(json) shouldBe ManifestMeta(
        schemaVersion   = SemanticManifest.CurrentSchemaVersion,
        kind            = "semanticdf-model-manifest",
        manifestVersion = Some(SemanticManifest.InitialManifestVersion),
        id              = None,
        namespace       = Some("default"),
        metadata        = Map.empty,
        modelName       = Some("kpi_facts"),
        version         = 7,
        description     = Some("the kpi facts table"),
        sourceTable     = Some("kpi_facts_raw"),
        status          = "published",
        dimensions      = 1,
        measures        = 1,
        calcMeasures    = 0,
        joins           = 0,
        filters         = 0,
        isStreaming     = false,
        usesTAll        = false,
      )

      // Round-trip should give back a working model with the same identity.
      val round = SemanticManifest.fromJson(json, df)
      round.name shouldBe Some("kpi_facts")
      round.description shouldBe Some("the kpi facts table")
      round.sourceTable shouldBe Some("kpi_facts_raw")
      round.version shouldBe 7
      round.dimensions.keySet shouldBe Set("kind")
      round.measures.keySet shouldBe Set("count")
    }

    // -- 1b. identity fields (added in v0.1.11) -------------------------------

    it("id-round-trips via the new two-arg toJson") {
      // The single-arg toJson passes Identity.empty (back-compat for tests).
      // The two-arg toJson with an explicit Identity emits the new fields.
      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1))),
        StructType(Seq(StructField("id", IntegerType)))
      )
      val model = toSemanticTable(df, name = Some("kpi_facts"))
        .withDimensions(Dimension("id", _ => df("id")))
        .withMeasures(Measure("c", _ => F.lit(1), exprString = Some("count(1)")))
      val identity = SemanticManifest.Identity(
        id              = "io.semanticdf.examples.manifest_load.kpi_facts",
        manifestVersion = "0.1.0",
        namespace       = "default",
        metadata        = Map("author" -> "data-platform-team", "license" -> "Apache-2.0"),
      )
      val json = SemanticManifest.toJson(model, identity)
      val pm = SemanticManifest.parseMeta(json)
      pm.id shouldBe Some("io.semanticdf.examples.manifest_load.kpi_facts")
      pm.manifestVersion shouldBe Some("0.1.0")
      pm.namespace shouldBe Some("default")
      pm.metadata shouldBe Map("author" -> "data-platform-team", "license" -> "Apache-2.0")
    }

    it("id-absent-omits-field (single-arg toJson uses Identity.empty)") {
      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1))),
        StructType(Seq(StructField("id", IntegerType)))
      )
      val model = toSemanticTable(df).withMeasures(Measure("c", _ => F.lit(1), exprString = Some("count(1)")))
      val json = SemanticManifest.toJson(model)
      val parsed = new com.fasterxml.jackson.databind.ObjectMapper().readTree(json)
      parsed.has("id") shouldBe false
      parsed.has("metadata") shouldBe false
      // The single-arg path uses defaults; parseMeta still exposes them.
      SemanticManifest.parseMeta(json).id shouldBe None
      SemanticManifest.parseMeta(json).metadata shouldBe Map.empty
    }

    it("namespace-round-trips") {
      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1))),
        StructType(Seq(StructField("id", IntegerType)))
      )
      val model = toSemanticTable(df).withMeasures(Measure("c", _ => F.lit(1), exprString = Some("count(1)")))
      val id1 = SemanticManifest.Identity(id = "x.y.z", namespace = "dev")
      val id2 = SemanticManifest.Identity(id = "x.y.z", namespace = "prod")
      val pm1 = SemanticManifest.parseMeta(SemanticManifest.toJson(model, id1))
      val pm2 = SemanticManifest.parseMeta(SemanticManifest.toJson(model, id2))
      pm1.namespace shouldBe Some("dev")
      pm2.namespace shouldBe Some("prod")
    }

    it("metadata-round-trips (flat string values only)") {
      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1))),
        StructType(Seq(StructField("id", IntegerType)))
      )
      val model = toSemanticTable(df).withMeasures(Measure("c", _ => F.lit(1), exprString = Some("count(1)")))
      val id = SemanticManifest.Identity(
        id       = "x.y.z",
        metadata = Map("author" -> "alice", "license" -> "Apache-2.0", "tags" -> "kpi,orders"),
      )
      val pm = SemanticManifest.parseMeta(SemanticManifest.toJson(model, id))
      pm.metadata shouldBe Map("author" -> "alice", "license" -> "Apache-2.0", "tags" -> "kpi,orders")
    }

    it("manifestVersion-round-trips") {
      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1))),
        StructType(Seq(StructField("id", IntegerType)))
      )
      val model = toSemanticTable(df).withMeasures(Measure("c", _ => F.lit(1), exprString = Some("count(1)")))
      val id = SemanticManifest.Identity(id = "x.y.z", manifestVersion = "1.0.0")
      val pm = SemanticManifest.parseMeta(SemanticManifest.toJson(model, id))
      pm.manifestVersion shouldBe Some("1.0.0")
    }

    it("$schema-url-is-emitted-and-is-the-raw-github-form") {
      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1))),
        StructType(Seq(StructField("id", IntegerType)))
      )
      val model = toSemanticTable(df).withMeasures(Measure("c", _ => F.lit(1), exprString = Some("count(1)")))
      val id = SemanticManifest.Identity(id = "x.y.z")
      val parsed = new com.fasterxml.jackson.databind.ObjectMapper().readTree(
        SemanticManifest.toJson(model, id))
      parsed.get("$schema").asText() shouldBe SemanticManifest.SchemaUrl
      parsed.get("$schema").asText() should startWith("https://raw.githubusercontent.com/")
    }

    it("old-manifest-still-parses (v0.1.9 and v0.1.10 lack new fields)") {
      // SchemaVersion is bumped to v0.1.11 but the reader accepts
      // v0.1.9 and v0.1.10 via the prefix match.
      for (sv <- Seq("v0.1.9-manifest", "v0.1.10-manifest", "v0.1.11-manifest")) {
        val json =
          s"""{
            |  "schemaVersion": "$sv",
            |  "kind": "semanticdf-model-manifest",
            |  "model": { "name": "kpi", "version": 0, "status": "published" }
            |}""".stripMargin
        val pm = SemanticManifest.parseMeta(json)
        pm.schemaVersion shouldBe sv
        pm.kind shouldBe "semanticdf-model-manifest"
        pm.modelName shouldBe Some("kpi")
      }
    }

    it("parseMeta-source-free works on a manifest with no source DF") {
      // parseMeta is metadata-only; no DataFrame is required.
      val json =
        """{
          |  "schemaVersion": "v0.1.11-manifest",
          |  "kind": "semanticdf-model-manifest",
          |  "model": { "name": "kpi" }
          |}""".stripMargin
      SemanticManifest.parseMeta(json).modelName shouldBe Some("kpi")
    }

    // -- 2. round-trip-preserves-dims-and-measures ----------------------------

    it("round-trips dimension and measure classifications") {
      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1, "2024-01-01", "a"))),
        StructType(Seq(
          StructField("id",   IntegerType),
          StructField("ts",   StringType),
          StructField("kind", StringType),
        ))
      )

      val model = toSemanticTable(df, name = Some("events"))
        .withDimensions(
          Dimension.time("ts", _ => df("ts"), smallestTimeGrain = Some("day")),
          Dimension.entity("id", _ => df("id")),
          Dimension("kind", _ => df("kind")),
        )
        .withMeasures(Measure("count", _ => F.lit(1)))

      val json = SemanticManifest.toJson(model)
      val meta = SemanticManifest.parseMeta(json)
      meta.dimensions shouldBe 3
      meta.measures shouldBe 1
      meta.calcMeasures shouldBe 0

      val round = SemanticManifest.fromJson(json, df)
      round.dimensions("ts").isTimeDimension shouldBe true
      round.dimensions("id").isEntity shouldBe true
      round.dimensions("kind").isEntity shouldBe false
      round.dimensions("kind").isTimeDimension shouldBe false
    }

    // -- 2b. measure bodies round-trip (regression for the lit(1) bug) ----

    it("round-trips measure BODIES, not just classifications (regression: lit(1) placeholder)") {
      // Regression test for the bug where every measure lambda body was
      // replaced with `_ => lit(1)` regardless of the original expr.
      // Before the fix: every measure returned 1, so the loaded model
      // was structurally correct but semantically broken. After the fix:
      // the original `expr:` string is parsed back into a working Column
      // and queries return real aggregates.
      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(
          Row(1, 10.0), Row(2, 20.0), Row(3, 30.0), Row(4, 40.0), Row(5, 50.0),
        )),
        StructType(Seq(
          StructField("id", IntegerType),
          StructField("amount", org.apache.spark.sql.types.DoubleType),
        ))
      )
      val model = toSemanticTable(df, name = Some("orders"))
        .withDimensions(Dimension("id", _ => df("id")))
        .withMeasures(
          Measure("order_count", _ => F.lit(1), exprString = Some("count(1)")),
          Measure("order_amount", _ => F.lit(1), exprString = Some("sum(amount)")),
        )
      val json = SemanticManifest.toJson(model)
      val round = SemanticManifest.fromJson(json, df)

      val q1 = round.groupBy().aggregate("order_count", "order_amount").execute(spark)
        .collect().head
      q1.getAs[Long]("order_count") shouldBe 5L          // count(1) over 5 rows
      q1.getAs[Double]("order_amount") shouldBe 150.0     // 10+20+30+40+50
    }

    it("calc measures round-trip: the expr evaluates against post-aggregated scope, not the source") {
      // Regression for the calc-measure anti-scope. Before the fix, calc
      // measures' bodies were `_ => F.expr(e)` — Spark's expr parser saw
      // measure references (e.g. `total_ship_days`) as columns of the
      // source DataFrame and threw UNRESOLVED_COLUMN. The fix: dispatch
      // on the manifest's `kind` field; calc measures use CalcExpr which
      // walks the DSL and substitutes `scope(name)` for each ident, so
      // the post-aggregation MeasureScope resolves them correctly.
      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(
          Row(1, 100.0), Row(2, 200.0), Row(3, 300.0), Row(4, 400.0),
        )),
        StructType(Seq(
          StructField("id",     IntegerType),
          StructField("amount", org.apache.spark.sql.types.DoubleType),
        ))
      )
      val model = toSemanticTable(df, name = Some("orders"))
        .withDimensions(Dimension("id", _ => df("id")))
        .withMeasures(
          Measure("order_count", _ => df("id"),        exprString = Some("count(1)")),
          Measure("order_amount", _ => df("amount"),    exprString = Some("sum(amount)")),
          // Calc measure: revenue per order. The lambda calls scope(name)
          // for sibling refs, which is exactly what CalcExpr.replay does.
          // The exprString hint ensures the manifest serializes a real
          // expression string (not the "<lambda>" sentinel).
          Measure("revenue_per_order",
            scope => scope("order_amount") / scope("order_count"),
            exprString = Some("order_amount / order_count")),
        )
        .groupBy()
        .aggregate("revenue_per_order")
      val json = SemanticManifest.toJson(model, prettyPrint = false)
      val round = SemanticManifest.fromJson(json, df)
      val q = round.groupBy().aggregate("revenue_per_order").execute(spark)
        .collect().head
      // 4 rows × 100.0+200.0+300.0+400.0 = 1000.0; 4 orders → 250.0
      q.getAs[Double]("revenue_per_order") shouldBe 250.0
    }

    it("lambda-only measures (no exprString) throw a loud error on first query") {
      // Anti-scope contract: a measure built from a bare lambda with
      // no exprString hint is stored as LambdaSentinel in the manifest.
      // The loader cannot reconstruct behavior; query-time surfaces a
      // clear, actionable error.
      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1))),
        StructType(Seq(StructField("id", IntegerType)))
      )
      val model = toSemanticTable(df, name = Some("m"))
        .withDimensions(Dimension("id", _ => df("id")))
        .withMeasures(Measure("bare", _ => F.lit(1)))  // no exprString hint
      val json = SemanticManifest.toJson(model)
      val round = SemanticManifest.fromJson(json, df)
      val ex = intercept[Exception] {
        round.groupBy().aggregate("bare").execute(spark).collect()
      }
      val msg = Option(ex.getMessage).getOrElse("")
      val allMsgs = Iterator
        .iterate(Option[Throwable](ex.getCause))(_.flatMap(t => Option(t.getCause)))
        .takeWhile(_.isDefined)
        .flatten
        .map(t => Option(t.getMessage).getOrElse(""))
        .mkString(" | ")
      val combined = msg + " | " + allMsgs
      combined should (include("bare") and include("manifest"))
    }

    // -- 3. round-trip-preserves-classification -------------------------------

    it("round-trips a calc measure with dependsOn set") {
      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1, 2.0))),
        StructType(Seq(
          StructField("a", IntegerType),
          StructField("b", DoubleType),
        ))
      )

      val model = toSemanticTable(df)
        .withMeasures(
          Measure("a", _ => df("a")),
          Measure("b", _ => df("b")),
          Measure("c", scope => scope("a") + scope("b")),
        )

      val json = SemanticManifest.toJson(model)
      val meta = SemanticManifest.parseMeta(json)
      meta.calcMeasures shouldBe 1
      meta.measures shouldBe 3

      // Inspect the raw JSON to confirm dependsOn is populated.
      val asMap = SemanticManifest.toJson(model)
      asMap.contains("\"dependsOn\"") shouldBe true
      asMap.contains("\"c\"") shouldBe true
    }

    // -- 4. round-trip-preserves-pre-aggregate-filters ------------------------

    it("round-trips pre-aggregate row filters") {
      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1, true))),
        StructType(Seq(
          StructField("id",     IntegerType),
          StructField("active", BooleanType),
        ))
      )

      val model = toSemanticTable(df)
        .withRowFilter("only_active", "active = true", None, Map.empty)

      val json = SemanticManifest.toJson(model)
      val meta = SemanticManifest.parseMeta(json)
      meta.filters shouldBe 1

      val round = SemanticManifest.fromJson(json, df)
      round.filters.map(_.name).toSet shouldBe Set("only_active")
    }

    // -- 5. round-trip-strips-post-aggregate-filters (lossy; documented) -----

    it("round-trip preserves only pre-aggregate filters (lossy contract)") {
      // Documenting the lossiness: SemanticManifest only round-trips
      // pre-aggregate filters (model.filters). Post-aggregate filters
      // added via .where(...) are not preserved. This test pins that
      // behavior so the lossiness is a contract, not a bug.
      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1, "x"))),
        StructType(Seq(
          StructField("id", IntegerType),
          StructField("k",  StringType),
        ))
      )
      val model = toSemanticTable(df)
        .withRowFilter("only_active", "active = true", None, Map.empty)

      val json = SemanticManifest.toJson(model)
      val round = SemanticManifest.fromJson(json, df)
      round.filters.map(_.name).toSet shouldBe Set("only_active")
    }

    // -- 6. lambda-only-dims-round-trip-as-sentinel ---------------------------

    it("lambda-only dims round-trip as <lambda> sentinel") {
      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1))),
        StructType(Seq(StructField("id", IntegerType)))
      )
      val model = toSemanticTable(df)
        .withDimensions(Dimension("id", _ => df("id")))

      val json = SemanticManifest.toJson(model)
      json.contains(SemanticManifest.LambdaSentinel) shouldBe true
    }

    // -- 7. joined-models-rejected --------------------------------------------

    it("rejects joined models at toJson time") {
      val leftDf  = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1, "x"))),
        StructType(Seq(
          StructField("id", IntegerType),
          StructField("k",  StringType),
        ))
      )
      val rightDf = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1, "y"))),
        StructType(Seq(
          StructField("id", IntegerType),
          StructField("k",  StringType),
        ))
      )
      val left  = toSemanticTable(leftDf)
        .withDimensions(Dimension("k", _ => leftDf("k")))
      val right = toSemanticTable(rightDf)
        .withDimensions(Dimension("k", _ => rightDf("k")))
      val joined = left.join_one(right, on = (l, r) => l("id") === r("id"))

      a[IllegalStateException] should be thrownBy SemanticManifest.toJson(joined)
    }

    // -- 8. schema-version-mismatch-throws -----------------------------------

    it("rejects manifests with a non-matching schemaVersion") {
      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1))),
        StructType(Seq(StructField("id", IntegerType)))
      )
      val bad =
        """{
          |  "schemaVersion": "v9999-future",
          |  "kind": "semanticdf-model-manifest",
          |  "model": { "name": "x", "version": 1 },
          |  "digest": {},
          |  "dimensions": [],
          |  "measures": [],
          |  "joins": [],
          |  "filters": []
          |}""".stripMargin

      a[ManifestParsingException] should be thrownBy
        SemanticManifest.fromJson(bad, df)
      a[ManifestParsingException] should be thrownBy
        SemanticManifest.parseMeta(bad)
    }

    it("rejects manifests with an unknown kind") {
      val bad =
        """{
          |  "schemaVersion": "v0.1.9-manifest",
          |  "kind": "semanticdf-WRONG",
          |  "model": {},
          |  "digest": {},
          |  "dimensions": [],
          |  "measures": [],
          |  "joins": [],
          |  "filters": []
          |}""".stripMargin
      a[ManifestParsingException] should be thrownBy
        SemanticManifest.parseMeta(bad)
    }

    it("rejects manifests whose root is not an object") {
      a[ManifestParsingException] should be thrownBy
        SemanticManifest.parseMeta("[]")
    }

    // -- 9. parse-meta-source-free -------------------------------------------

    it("parseMeta works without a source DataFrame") {
      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1))),
        StructType(Seq(StructField("id", IntegerType)))
      )
      val model = toSemanticTable(df, name = Some("x"))
      val json  = SemanticManifest.toJson(model)
      val meta  = SemanticManifest.parseMeta(json)
      meta.modelName shouldBe Some("x")
      // No exception, no source — proves the API doesn't require a DataFrame.
    }

    // -- 10. unknown-fields-tolerated (forward compat) ------------------------

    it("parseMeta and fromJson tolerate unknown top-level fields") {
      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1))),
        StructType(Seq(StructField("id", IntegerType)))
      )
      val model = toSemanticTable(df, name = Some("x"))
      val json  = SemanticManifest.toJson(model)
      // Inject an unknown top-level field.
      val inflated = json.replaceFirst(
        "\\{", "{\"futureField\":\"unrecognizedValue\","
      )
      // Must not throw.
      val meta = SemanticManifest.parseMeta(inflated)
      meta.modelName shouldBe Some("x")
      val round = SemanticManifest.fromJson(inflated, df)
      round.name shouldBe Some("x")
    }

    // -- 11. streaming-source-walks-aggregate-wrapper ------------------------

    it("recognizes streaming root through aggregate wrapper") {
      val rateDf = spark.readStream.format("rate").load()
      val model  = toStreamingSemanticTable(rateDf)
        .withDimensions(Dimension("value", _ => F.col("value")))
        .withMeasures(Measure("count", _ => F.lit(1)).copy(exprString = Some("count(*)")))
        .groupBy()
        .aggregate("count")

      val json = SemanticManifest.toJson(model)
      val meta = SemanticManifest.parseMeta(json)
      meta.isStreaming shouldBe true

      val round = SemanticManifest.fromJson(json, rateDf)
      round.root shouldBe a[SemanticStreamingTableOp]
    }

    it("marks a streaming model that uses t.all(...)") {
      val rateDf = spark.readStream.format("rate").load()
      val model  = toStreamingSemanticTable(rateDf)
        .withDimensions(Dimension("value", _ => F.col("value")))
        .withMeasures(
          Measure("count", _ => F.lit(1)).copy(exprString = Some("count(*)")),
          Measure("pct",   scope => scope("count") / scope.all("count")),
        )
        .groupBy("value")
        .aggregate("pct")
      val json = SemanticManifest.toJson(model)
      val meta = SemanticManifest.parseMeta(json)
      meta.usesTAll shouldBe true
    }

    it("marks a streaming model that does NOT use t.all(...)") {
      val rateDf = spark.readStream.format("rate").load()
      val model  = toStreamingSemanticTable(rateDf)
        .withDimensions(Dimension("value", _ => F.col("value")))
        .withMeasures(Measure("count", _ => F.lit(1)).copy(exprString = Some("count(*)")))
        .groupBy("value")
        .aggregate("count")
      val json = SemanticManifest.toJson(model)
      val meta = SemanticManifest.parseMeta(json)
      meta.usesTAll shouldBe false
    }

    // -- status round-trip ---------------------------------------------------

    it("emits real status (not hardcoded literal) in the manifest") {
      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1))),
        StructType(Seq(StructField("id", IntegerType)))
      )
      val draft = toSemanticTable(df, name = Some("draft_model"))
        .status(ModelStatus.Draft)
      val json = SemanticManifest.toJson(draft)
      val meta = SemanticManifest.parseMeta(json)
      meta.status shouldBe "draft"
    }

    it("emits deprecated status in the manifest") {
      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1))),
        StructType(Seq(StructField("id", IntegerType)))
      )
      val dep = toSemanticTable(df, name = Some("legacy_model"))
        .status(ModelStatus.Deprecated)
      val json = SemanticManifest.toJson(dep)
      val meta = SemanticManifest.parseMeta(json)
      meta.status shouldBe "deprecated"
    }

    it("defaults to published when no status is set") {
      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1))),
        StructType(Seq(StructField("id", IntegerType)))
      )
      val pub = toSemanticTable(df, name = Some("x"))
      val json = SemanticManifest.toJson(pub)
      val meta = SemanticManifest.parseMeta(json)
      meta.status shouldBe "published"
    }

    it("streaming model preserves status through groupBy().aggregate()") {
      val rateDf = spark.readStream.format("rate").load()
      val model  = toStreamingSemanticTable(rateDf)
        .withDimensions(Dimension("value", _ => F.col("value")))
        .withMeasures(Measure("count", _ => F.lit(1)).copy(exprString = Some("count(*)")))
        .status(ModelStatus.Deprecated)
        .groupBy("value")
        .aggregate("count")
      val json = SemanticManifest.toJson(model)
      val meta = SemanticManifest.parseMeta(json)
      meta.status shouldBe "deprecated"
    }
  }
}