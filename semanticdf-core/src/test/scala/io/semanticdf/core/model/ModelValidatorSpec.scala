package io.semanticdf.core.model

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.semanticdf.core.expr.{Expr, LiteralValue}
import io.semanticdf.core.rel.{AggregateCall, AggregateFn}
import io.semanticdf.core.schema.SealedDataType

/** Phase 2 contract: prove `ModelValidator.validate` runs all 6
  * checks in the correct order. Per scala-data-driven-refactor,
  * validity is enforced exactly once, at the boundary. The
  * validator is the SINGLE boundary between untrusted input and
  * the portable Model type.
  */
class ModelValidatorSpec extends AnyFunSuite with Matchers {

  // -- helpers --

  private val sampleSource = SourceRef.ByName(
    catalog   = None,
    namespace = Some("public"),
    table     = "orders",
  )

  private val amountDim = Dimension.field("amount", SealedDataType.BigInt)
  private val regionDim = Dimension.field("region", SealedDataType.Varchar)

  private val totalMeasure = Measure.aggregate(
    name = "total",
    fn   = AggregateFn.Sum,
    expr = Expr.FieldRef("amount"),
  )

  /** Build a basic valid validate(...) call. */
  private def validateAll(
      name:        String              = "orders",
      dimensions:  List[Dimension]     = List(amountDim, regionDim),
      measures:    List[Measure]       = List(totalMeasure),
      calc:        List[CalculatedMeasure] = Nil,
      extensions:  Map[String, ExtensionValue] = Map.empty,
      policies:    ModelPolicyDefaults = ModelPolicyDefaults.none,
  ) = ModelValidator.validate(
    name = name, source = sampleSource,
    dimensions = dimensions, measures = measures,
    calculatedMeasures = calc,
    joins = Nil, filters = Nil, rollups = Nil,
    extensions = extensions, defaultPolicies = policies,
  )

  // -- check (1): name is non-blank --

  test("check (1): empty name returns InvalidName") {
    val r = validateAll(name = "")
    r shouldBe Left(ModelValidationError.InvalidName("name is blank"))
  }

  test("check (1): blank name (whitespace only) returns InvalidName") {
    val r = validateAll(name = "   ")
    r shouldBe Left(ModelValidationError.InvalidName("name is blank"))
  }

  test("check (1): valid name returns Right(())") {
    validateAll().isRight shouldBe true
  }

  // -- check (2): no duplicate names --

  test("check (2): dimension name colliding with measure name returns DuplicateMember") {
    val dupMeasure = Measure.aggregate("region", AggregateFn.Count, Expr.FieldRef("id"))
    val r = ModelValidator.validate(
      name = "orders", source = sampleSource,
      dimensions = List(regionDim),  // "region"
      measures = List(dupMeasure),  // also "region"!
      calculatedMeasures = Nil,
      joins = Nil, filters = Nil, rollups = Nil,
      extensions = Map.empty, defaultPolicies = ModelPolicyDefaults.none,
    )
    r shouldBe Left(ModelValidationError.DuplicateMember("dimension/measure", "region"))
  }

  test("check (2): duplicate calc-measure names returns DuplicateMember") {
    val c1 = CalculatedMeasure("c", Expr.MeasureRef("total"))
    val c2 = CalculatedMeasure("c", Expr.MeasureRef("total"))
    val r = ModelValidator.validate(
      name = "orders", source = sampleSource,
      dimensions = List(amountDim), measures = List(totalMeasure),
      calculatedMeasures = List(c1, c2),  // duplicate "c"
      joins = Nil, filters = Nil, rollups = Nil,
      extensions = Map.empty, defaultPolicies = ModelPolicyDefaults.none,
    )
    r shouldBe Left(ModelValidationError.DuplicateMember("calculatedMeasure", "c"))
  }

  // -- check (3): every calc-measure refers to a declared measure --

  test("check (3): calc-measure references undeclared measure returns UnknownReference") {
    val badCalc = CalculatedMeasure(
      name = "bad",
      expr = Expr.MeasureRef("ghost_measure"),  // not declared
    )
    val r = ModelValidator.validate(
      name = "orders", source = sampleSource,
      dimensions = List(amountDim), measures = List(totalMeasure),
      calculatedMeasures = List(badCalc),
      joins = Nil, filters = Nil, rollups = Nil,
      extensions = Map.empty, defaultPolicies = ModelPolicyDefaults.none,
    )
    r shouldBe Left(ModelValidationError.UnknownReference(
      "calculatedMeasures", "ghost_measure",
    ))
  }

  test("check (3): valid calc-measure reference returns Right(())") {
    val goodCalc = CalculatedMeasure(
      name = "double_total",
      expr = Expr.Multiply(Expr.MeasureRef("total"), Expr.Literal(LiteralValue.IntValue(2), SealedDataType.Int)),
    )
    val r = ModelValidator.validate(
      name = "orders", source = sampleSource,
      dimensions = List(amountDim), measures = List(totalMeasure),
      calculatedMeasures = List(goodCalc),
      joins = Nil, filters = Nil, rollups = Nil,
      extensions = Map.empty, defaultPolicies = ModelPolicyDefaults.none,
    )
    r shouldBe Right(())
  }

  // -- check (4): calc DAG is acyclic + depth --

  test("check (4): cycle in calc DAG returns CalcDepthExceeded") {
    // c1 -> c2 -> c1 (cycle)
    val c1 = CalculatedMeasure(
      name = "c1",
      expr = Expr.Add(Expr.MeasureRef("c2"), Expr.Literal(LiteralValue.IntValue(1), SealedDataType.Int)),
    )
    val c2 = CalculatedMeasure(
      name = "c2",
      expr = Expr.Add(Expr.MeasureRef("c1"), Expr.Literal(LiteralValue.IntValue(1), SealedDataType.Int)),
    )
    val r = ModelValidator.validate(
      name = "orders", source = sampleSource,
      dimensions = List(amountDim), measures = List(totalMeasure),
      calculatedMeasures = List(c1, c2),
      joins = Nil, filters = Nil, rollups = Nil,
      extensions = Map.empty, defaultPolicies = ModelPolicyDefaults.none,
    )
    r.isLeft shouldBe true
    r.left.get shouldBe a [ModelValidationError.CalcDepthExceeded]
  }

  // -- check (5): extension envelope fits limits --

  test("check (5): too-large extension returns ExtensionEnvelopeExceeded") {
    val bigStr = "x" * (ExtensionLimits.MaxInlineBytes + 1)
    val r = ModelValidator.validate(
      name = "orders", source = sampleSource,
      dimensions = List(amountDim), measures = List(totalMeasure),
      calculatedMeasures = Nil,
      joins = Nil, filters = Nil, rollups = Nil,
      extensions = Map("big" -> ExtensionValue.String(bigStr)),
      defaultPolicies = ModelPolicyDefaults.none,
    )
    r.isLeft shouldBe true
    r.left.get shouldBe a [ModelValidationError.ExtensionEnvelopeExceeded]
  }

  // -- realistic happy path --

  test("realistic happy path: simple model returns Right(())") {
    val r = validateAll()
    r shouldBe Right(())
  }

  test("realistic happy path: model with calc measure + extensions returns Right(())") {
    val calc = CalculatedMeasure(
      name = "double_total",
      expr = Expr.Multiply(Expr.MeasureRef("total"), Expr.Literal(LiteralValue.IntValue(2), SealedDataType.Int)),
    )
    val r = ModelValidator.validate(
      name = "orders", source = sampleSource,
      dimensions = List(amountDim), measures = List(totalMeasure),
      calculatedMeasures = List(calc),
      joins = Nil, filters = Nil, rollups = Nil,
      extensions = Map("description" -> ExtensionValue.String("orders model")),
      defaultPolicies = ModelPolicyDefaults.none,
    )
    r shouldBe Right(())
  }
}