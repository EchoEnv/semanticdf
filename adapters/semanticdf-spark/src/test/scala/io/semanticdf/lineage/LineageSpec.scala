package io.semanticdf.lineage

import io.semanticdf.SparkSessionFixture
import io.semanticdf.{Dimension, Measure, ModelStatus, SemanticTable}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

/** Tests for [[Lineage.of]] and [[Lineage.workspaceOf]] — the static-
  * analysis entry points. */
class LineageSpec extends AnyFunSuite with SparkSessionFixture {

  override def beforeAll(): Unit = {
    super.beforeAll()
    val s = spark
    import s.implicits._
    // A small in-memory DataFrame the tests can build models from.
  }

  test("of: single-table model — source table is the DataFrame's first column batch") {
    val st = toModel("flights",
      dimensions = Seq(
        new Dimension("carrier", t => t("carrier"), exprString = Some("carrier")),
      ),
      measures = Seq(
        Measure("pax_sum", t => org.apache.spark.sql.functions.sum(t("pax")),
               exprString = Some("sum(pax)")),
      ),
    )
    val ml = Lineage.of(st)
    assert(ml.modelId == "flights", s"expected modelId=flights, got ${ml.modelId}")
    assert(ml.sourceKind == SourceKind.Batch)
    assert(ml.joins.isEmpty, "single-table model should have no joins")
    assert(ml.dimensions.length == 1)
    assert(ml.dimensions.head.name == "carrier")
    assert(ml.dimensions.head.baseColumns == Seq("carrier"))
    assert(ml.dimensions.head.status == LineageStatus.Complete)
    assert(ml.measures.length == 1)
    assert(ml.measures.head.name == "pax_sum")
    assert(ml.measures.head.baseColumns == Seq("pax"))
  }

  test("of: dimension with no exprString is Opaque") {
    val st = toModel("flights",
      dimensions = Seq(
        new Dimension("carrier", t => t("carrier"), exprString = None),
      ),
    )
    val ml = Lineage.of(st)
    assert(ml.dimensions.head.status == LineageStatus.Opaque)
    assert(ml.dimensions.head.baseColumns.isEmpty)
  }

  test("of: calc measure detects measure-name dependencies in exprString") {
    val st = toModel("orders",
      measures = Seq(
        Measure("total",  t => org.apache.spark.sql.functions.sum(t("amount")),
               exprString = Some("sum(amount)")),
        Measure("pct",    t => t("amount") / org.apache.spark.sql.functions.lit(100),
               exprString = Some("total / 100")),  // references the `total` measure
      ),
    )
    val ml = Lineage.of(st)
    val pct = ml.measures.find(_.name == "pct").get
    // `total` is in st.measures, so it should appear in dependsOn
    assert(pct.dependsOn.contains("total"), s"expected pct.dependsOn to contain 'total', got ${pct.dependsOn}")
  }

  test("of: streaming source sets sourceKind=Streaming") {
    // A streaming-rooted model — use the streaming factory, not the
    // batch one (which would wrap in SemanticTableOp, not
    // SemanticStreamingTableOp).
    val streamDf = spark.readStream.format("rate").load()
    val st = io.semanticdf.toStreamingSemanticTable(streamDf, name = Some("events"))
      .withDimensions(new Dimension("value", t => t("value"), exprString = Some("value")))
    val ml = Lineage.of(st)
    assert(ml.sourceKind == SourceKind.Streaming)
  }

  test("of: transform is captured from the op tree") {
    val st = io.semanticdf.toSemanticTable(makeDf(), name = Some("flights"))
      .withTransforms(
        io.semanticdf.Transform("pax_per_mile", t => t("pax") / t("distance"),
                                exprString = Some("pax / distance")),
      )
    val ml = Lineage.of(st)
    assert(ml.transforms.length == 1)
    assert(ml.transforms.head.name == "pax_per_mile")
    assert(ml.transforms.head.baseColumns == Seq("pax", "distance"))
    assert(ml.transforms.head.status == LineageStatus.Complete)
  }

  test("toJson / fromJson: round-trip preserves the data shape") {
    val st = toModel("flights",
      dimensions = Seq(new Dimension("carrier", t => t("carrier"), exprString = Some("carrier"))),
      measures   = Seq(Measure("pax_sum", t => org.apache.spark.sql.functions.sum(t("pax")),
                              exprString = Some("sum(pax)"))),
    )
    val original  = Lineage.workspaceOf(Map("flights" -> st))
    val json      = Lineage.toJson(original)
    val parsed    = Lineage.fromJson(json)
    assert(parsed.models.keySet == original.models.keySet)
    val origModel = original.models("flights")
    val parModel  = parsed.models("flights")
    assert(parModel.modelId == origModel.modelId)
    assert(parModel.dimensions.length == origModel.dimensions.length)
    assert(parModel.measures.length == origModel.measures.length)
    assert(parModel.dimensions.head.name == origModel.dimensions.head.name)
    assert(parModel.measures.head.baseColumns == origModel.measures.head.baseColumns)
  }

  test("toJson: emits the schema-version envelope") {
    val st = toModel("flights")
    val wl = Lineage.workspaceOf(Map("flights" -> st))
    val json = Lineage.toJson(wl)
    assert(json.contains("\"schema\""), s"expected 'schema' field in JSON, got: $json")
    assert(json.contains("semanticdf-lineage-v1"), s"expected schema version, got: $json")
  }

  // ------- helpers -------

  private def toModel(
      name: String,
      dimensions: Seq[Dimension] = Seq.empty,
      measures:   Seq[Measure]   = Seq.empty,
  ): SemanticTable = {
    io.semanticdf.toSemanticTable(makeDf(), name = Some(name))
      .withDimensions(dimensions: _*)
      .withMeasures(measures: _*)
  }

  private def makeDf() = {
    val s = spark
    import s.implicits._
    Seq(
      ("AA", 100, 50),
      ("UA", 200, 75),
      ("DL", 300, 100),
    ).toDF("carrier", "distance", "pax")
  }
}
