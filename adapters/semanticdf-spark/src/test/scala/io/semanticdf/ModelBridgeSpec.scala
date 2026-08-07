package io.semanticdf

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.semanticdf.core.expr.Expr
import io.semanticdf.core.rel.{AggregateFn, JoinKind}
import io.semanticdf.core.model.{Model, ModelPolicyDefaults, SourceRef}

/** Tests for [ModelBridge] \u2014 the partial bridge from the legacy
  * [SemanticTable] (spark-flavored) to the engine-portable
  * [core.model.Model].
  *
  * Per PR 8a (Option A of the v0.3.0 deferred work): the
  * converter covers the easy fields only. Filters, calc
  * measures, rollups, and the measure's aggregate function
  * detection are deferred to v0.5.0. */
class ModelBridgeSpec extends AnyFunSuite with Matchers {

  // -- Helper: build a minimal SemanticTable for conversion --

  private def buildSimpleTable(
      name:        String = "orders",
      sourceTable: Option[String] = Some("orders"),
      description: Option[String] = None,
      status: ModelStatus = io.semanticdf.ModelStatus.Published,
      version: Int = 1,
  ): SemanticTable = {
    val spark = SemanticTableFixture.spark
    import spark.implicits._
    import org.apache.spark.sql.functions.{col, count, lit}
    val df = Seq(
      (1, "2024-01-01", 100.0),
      (2, "2024-01-02", 200.0),
    ).toDF("id", "date", "amount")

    val base = toSemanticTable(df, name = Some(name), description = description,
      sourceTable = sourceTable)
      .withDimensions(
        Dimension("id",     _ => col("id")),
        Dimension("date",   _ => col("date")),
        Dimension("amount", _ => col("amount")),
      )
      .withMeasures(
        Measure("row_count", _ => count(lit(1))),
        Measure("total",     _ => col("amount")),
      )
      .status(status)
      .version(version)
    base
  }

  // -- Successful conversion: easy fields --

  test("toModel converts name") {
    val st = buildSimpleTable(name = "orders")
    val m = ModelBridge.toModel(st).toOption.get
    m.name shouldBe "orders"
  }

  test("toModel converts sourceTable to SourceRef.ByName") {
    val st = buildSimpleTable(sourceTable = Some("orders_v2"))
    val m = ModelBridge.toModel(st).toOption.get
    m.source shouldBe SourceRef.ByName(
      catalog = None, namespace = None, table = "orders_v2",
    )
  }

  test("toModel falls back to name when sourceTable is None") {
    val st = buildSimpleTable(name = "orders", sourceTable = None)
    val m = ModelBridge.toModel(st).toOption.get
    m.source shouldBe SourceRef.ByName(
      catalog = None, namespace = None, table = "orders",
    )
  }

  test("toModel converts all dimensions (each as Expr.FieldRef)") {
    val st = buildSimpleTable()
    val m = ModelBridge.toModel(st).toOption.get
    m.dimensions.size shouldBe 3
    m.dimensions.map(_.name).toSet shouldBe Set("id", "date", "amount")
    m.dimensions.foreach { d =>
      d.expr shouldBe Expr.FieldRef(d.name)
    }
  }

  test("toModel converts all measures (each as AggregateCall placeholder)") {
    val st = buildSimpleTable()
    val m = ModelBridge.toModel(st).toOption.get
    m.measures.size shouldBe 2
    m.measures.map(_.name).toSet shouldBe Set("row_count", "total")
    m.measures.foreach { ms =>
      ms.expr.fn shouldBe AggregateFn.Sum  // v1 placeholder
      ms.expr.input shouldBe Some(Expr.FieldRef(ms.name))
      ms.expr.alias shouldBe ms.name
    }
  }

  test("toModel converts ModelStatus (legacy -> core)") {
    val stDraft = buildSimpleTable(status = io.semanticdf.ModelStatus.Draft)
    ModelBridge.toModel(stDraft).toOption.get.status shouldBe io.semanticdf.core.model.ModelStatus.Draft

    val stPublished = buildSimpleTable(status = io.semanticdf.ModelStatus.Published)
    ModelBridge.toModel(stPublished).toOption.get.status shouldBe io.semanticdf.core.model.ModelStatus.Published

    val stDeprecated = buildSimpleTable(status = io.semanticdf.ModelStatus.Deprecated)
    ModelBridge.toModel(stDeprecated).toOption.get.status shouldBe io.semanticdf.core.model.ModelStatus.Deprecated
  }

  test("toModel converts version") {
    val st = buildSimpleTable(version = 5)
    val m = ModelBridge.toModel(st).toOption.get
    m.version shouldBe 5
  }

  test("toModel converts description") {
    val st = buildSimpleTable(description = Some("the orders model"))
    val m = ModelBridge.toModel(st).toOption.get
    m.description shouldBe Some("the orders model")
  }

  // -- Joins conversion --

  test("toModel converts joins with cardinality='one' to JoinKind.Inner") {
    // Build a tiny 2-table join
    val spark = SemanticTableFixture.spark
    import spark.implicits._
    import org.apache.spark.sql.functions.col

    val leftDf = Seq((1, "a")).toDF("id", "name")
    val rightDf = Seq((1, "x")).toDF("id", "tag")

    val left  = toSemanticTable(leftDf,  name = Some("left"),  sourceTable = Some("left"))
    val right = toSemanticTable(rightDf, name = Some("right"), sourceTable = Some("right"))

    val joined = left.join_on(right, "id" -> "id")
    val converted = ModelBridge.toModel(joined)
    converted match {
      case Right(_) =>  // ok
      case Left(err) => fail(s"conversion failed: $err")
    }
    val m = converted.toOption.get
    m.joins.size shouldBe 1
    val j = m.joins.head
    j.kind shouldBe JoinKind.Inner
    j.rightModel shouldBe "right"
    j.keys shouldBe List("id" -> "id")
  }

  test("toModel converts joins with cardinality='cross' to JoinKind.Cross") {
    val spark = SemanticTableFixture.spark
    import spark.implicits._
    import org.apache.spark.sql.functions.col

    val leftDf  = Seq((1, "a")).toDF("id", "name")
    val rightDf = Seq((1, "x")).toDF("id", "tag")

    val left  = toSemanticTable(leftDf,  name = Some("left"))
    val right = toSemanticTable(rightDf, name = Some("right"))
    val joined = left.join_cross(right)
    val converted = ModelBridge.toModel(joined)
    converted match {
      case Right(_) =>  // ok
      case Left(err) => fail(s"conversion failed: $err")
    }
    val m = converted.toOption.get
    m.joins.size shouldBe 1
    m.joins.head.kind shouldBe JoinKind.Cross
  }

  // -- Empty cases --

  test("toModel of a SemanticTable with no dimensions returns empty dimensions") {
    val spark = SemanticTableFixture.spark
    import spark.implicits._
    import org.apache.spark.sql.functions.{count, lit}
    val df = Seq((1, "a")).toDF("id", "name")
    val st = toSemanticTable(df, name = Some("no_dims"), sourceTable = Some("t"))
      .withMeasures(Measure("n", _ => count(lit(1))))
    val m = ModelBridge.toModel(st).toOption.get
    m.dimensions shouldBe Nil
    m.measures.size shouldBe 1
  }

  test("toModel of a SemanticTable with no joins returns empty joins") {
    val st = buildSimpleTable()
    val m = ModelBridge.toModel(st).toOption.get
    m.joins shouldBe Nil
  }

  // -- v1 defaults: things that are NOT converted --

  test("toModel sets filters to Nil (Predicate conversion deferred)") {
    val st = buildSimpleTable()
    val m = ModelBridge.toModel(st).toOption.get
    m.filters shouldBe Nil
  }

  test("toModel sets calculatedMeasures to Nil (CalcGraph conversion deferred)") {
    val st = buildSimpleTable()
    val m = ModelBridge.toModel(st).toOption.get
    m.calculatedMeasures shouldBe Nil
  }

  test("toModel sets rollups to Nil (rollup definitions deferred)") {
    val st = buildSimpleTable()
    val m = ModelBridge.toModel(st).toOption.get
    m.rollups shouldBe Nil
  }

  test("toModel sets defaultPolicies to ModelPolicyDefaults.none") {
    val st = buildSimpleTable()
    val m = ModelBridge.toModel(st).toOption.get
    m.defaultPolicies shouldBe ModelPolicyDefaults.none
  }

  test("toModel sets extensions to empty Map") {
    val st = buildSimpleTable()
    val m = ModelBridge.toModel(st).toOption.get
    m.extensions shouldBe Map.empty
  }

  // -- Determinism (data-driven) --

  test("toModel is deterministic: same input => same Model fields") {
    // NOTE: Model is a `final class` (NOT `case class`) per the
    // design's "no auto-derived Serializable" boundary. Two Model
    // instances with the same fields are NOT reference-equal, but
    // their fields ARE equal. Verify field-by-field equality.
    val st = buildSimpleTable()
    val m1 = ModelBridge.toModel(st).toOption.get
    val m2 = ModelBridge.toModel(st).toOption.get
    m1.name shouldBe m2.name
    m1.source shouldBe m2.source
    m1.dimensions shouldBe m2.dimensions
    m1.measures shouldBe m2.measures
    m1.joins shouldBe m2.joins
    m1.description shouldBe m2.description
    m1.version shouldBe m2.version
    m1.status shouldBe m2.status
  }

  // -- Validation failure --

  test("toModel returns Left(ModelValidationError) when name is empty") {
    val st = buildSimpleTable(name = "")
    val result = ModelBridge.toModel(st)
    result.isLeft shouldBe true
  }
}