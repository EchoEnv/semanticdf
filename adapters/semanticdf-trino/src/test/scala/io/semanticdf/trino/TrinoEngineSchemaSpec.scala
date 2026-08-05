package io.semanticdf.trino

import io.semanticdf.core.engine.EngineContext
import io.semanticdf.core.expr.Expr
import io.semanticdf.core.model.{CalculatedMeasure, Dimension, JoinSpec, Measure, Model, ModelPolicyDefaults, ModelStatus, SourceRef}
import io.semanticdf.core.rel.{AggregateCall, AggregateFn, JoinKind}
import io.semanticdf.core.schema.{SchemaFieldKind, SchemaSummary, SealedDataType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Tests for [[TrinoEngine.schema]] — the engine-portable mirror
  * of the original Spark library's `SemanticTableCore.schema`.
  *
  * Per scala-data-driven-refacer §1: the SCHEMA CONTRACT
  * ([[SchemaSummary]]) lives in core; the SCHEMA BEHAVIOR (walking
  * the model) lives in the engine adapter. These tests pin the
  * adapter-side behavior.
  *
  * ==Why no Spark mock / no cluster==
  *
  * `schema()` is a pure projection of the [[Model]] — no Trino
  * cluster round-trip needed. Tests build a `Model` via `Model.of`,
  * call `TrinoEngine.schema(model, ctx)`, and assert on the
  * returned `SchemaSummary`. The connection factory is irrelevant
  * (schema never touches it). */
class TrinoEngineSchemaSpec extends AnyFunSuite with Matchers {

  // -- fixture builders --

  /** Build a minimal valid `Model` for testing `schema`. Uses
    * `Model.of` so validation passes. */
  private def sampleModel(
      dimensions:        List[Dimension]         = Nil,
      measures:          List[Measure]           = Nil,
      calculatedMeasures: List[CalculatedMeasure] = Nil,
      joins:             List[JoinSpec]          = Nil,
  ): Model = Model.of(
    name     = "orders",
    source   = SourceRef.ByName(catalog = None, namespace = Some("public"), table = "orders"),
    dimensions         = dimensions,
    measures           = measures,
    calculatedMeasures = calculatedMeasures,
    joins              = joins,
    defaultPolicies    = ModelPolicyDefaults.none,
    status             = ModelStatus.Draft,
  ).fold(
    err => fail(s"sampleModel failed validation: $err"),
    identity,
  )

  private val ctx: EngineContext = EngineContext.defaultContext

  // -- engine-portable return shape --

  test("schema returns Right(SchemaSummary) for a valid model") {
    val m = sampleModel()
    val result = TrinoEngine.instance.schema(m, ctx)
    result.isRight shouldBe true
    result.toOption.get shouldBe a [SchemaSummary]
  }

  // -- empty model --

  test("schema for an empty model returns SchemaSummary with no fields and rowCount = 0") {
    val m = sampleModel()
    val summary = TrinoEngine.instance.schema(m, ctx).toOption.get

    summary.modelName shouldBe "orders"
    summary.modelDescription shouldBe None
    summary.rowCount shouldBe 0
    summary.isEmpty shouldBe true
    summary.fields shouldBe Nil
  }

  // -- dimensions --

  test("schema projects each dimension as a SchemaField with kind = Dimension") {
    val m = sampleModel(dimensions = List(
      Dimension("region",  Expr.FieldRef("region"),  Some(SealedDataType.Varchar)),
      Dimension("country", Expr.FieldRef("country"), Some(SealedDataType.Varchar)),
    ))
    val summary = TrinoEngine.instance.schema(m, ctx).toOption.get

    summary.ofKind(SchemaFieldKind.Dimension).map(_.fieldName) shouldBe List("region", "country")
    summary.ofKind(SchemaFieldKind.Dimension).map(_.dataType) shouldBe
      List(Some(SealedDataType.Varchar), Some(SealedDataType.Varchar))
    summary.rowCount shouldBe 2
  }

  // -- measures --

  test("schema projects each measure as a SchemaField with kind = Measure") {
    val m = sampleModel(measures = List(
      Measure("amount", AggregateCall(AggregateFn.Sum, Some(Expr.FieldRef("amount")), "amount")),
      Measure("count",  AggregateCall(AggregateFn.Count, None, "count")),
    ))
    val summary = TrinoEngine.instance.schema(m, ctx).toOption.get

    summary.ofKind(SchemaFieldKind.Measure).map(_.fieldName) shouldBe List("amount", "count")
    summary.rowCount shouldBe 2
  }

  // -- calculated measures --

  test("schema projects each calculated measure as a SchemaField with kind = CalculatedMeasure") {
    val m = sampleModel(calculatedMeasures = List(
      CalculatedMeasure("margin", Expr.FieldRef("profit")),
      CalculatedMeasure("ratio",  Expr.FieldRef("profit")),
    ))
    val summary = TrinoEngine.instance.schema(m, ctx).toOption.get

    summary.ofKind(SchemaFieldKind.CalculatedMeasure).map(_.fieldName) shouldBe
      List("margin", "ratio")
    summary.rowCount shouldBe 2
  }

  // -- joins --

  test("schema projects each join as a SchemaField with kind = JoinKey") {
    val m = sampleModel(joins = List(
      JoinSpec(
        name        = "orders_to_customers",
        rightModel  = "customers",
        kind        = JoinKind.Inner,
        keys        = Nil,
      ),
    ))
    val summary = TrinoEngine.instance.schema(m, ctx).toOption.get

    summary.ofKind(SchemaFieldKind.JoinKey).map(_.fieldName) shouldBe
      List("orders_to_customers")
    summary.rowCount shouldBe 1
  }

  // -- full model: all field kinds together --

  test("schema projects dimensions, measures, calculated measures, and joins together (preserving order)") {
    val m = sampleModel(
      dimensions = List(Dimension("region", Expr.FieldRef("region"))),
      measures   = List(Measure("amount", AggregateCall(AggregateFn.Sum, Some(Expr.FieldRef("amount")), "amount"))),
      calculatedMeasures = List(CalculatedMeasure("margin", Expr.FieldRef("profit"))),
      joins      = List(JoinSpec(
        name        = "orders_to_customers",
        rightModel  = "customers",
        kind        = JoinKind.Inner,
        keys        = Nil,
      )),
    )
    val summary = TrinoEngine.instance.schema(m, ctx).toOption.get

    summary.modelName shouldBe "orders"
    summary.rowCount shouldBe 4
    summary.fields.map(_.fieldKind) shouldBe List(
      SchemaFieldKind.Dimension,
      SchemaFieldKind.Measure,
      SchemaFieldKind.CalculatedMeasure,
      SchemaFieldKind.JoinKey,
    )
    summary.fields.map(_.fieldName) shouldBe List(
      "region", "amount", "margin", "orders_to_customers",
    )
  }

  // -- determinism --

  test("schema is deterministic (same model → same SchemaSummary)") {
    val m = sampleModel(dimensions = List(Dimension("region", Expr.FieldRef("region"))))
    val a = TrinoEngine.instance.schema(m, ctx).toOption.get
    val b = TrinoEngine.instance.schema(m, ctx).toOption.get
    a shouldBe b
  }

  // -- no Spark / no cluster roundtrip --

  test("schema does NOT call connectionFactory (pure projection)") {
    // schema() should be deterministic and not require a Trino
    // cluster. We verify by calling it on a fresh engine with NO
    // connection factory — it must succeed (Right), unlike
    // execute/explainPlan which need a factory.
    val engineWithoutFactory = new TrinoEngine()
    val m = sampleModel()
    val result = engineWithoutFactory.schema(m, ctx)
    result.isRight shouldBe true
  }
}