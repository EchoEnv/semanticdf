package io.semanticdf.core.model

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.semanticdf.core.expr.{Expr, LiteralValue}
import io.semanticdf.core.rel.{AggregateCall, AggregateFn, JoinKind}
import io.semanticdf.core.schema.SealedDataType

/** Phase 2 contract: prove `Model.of` is the canonical smart
  * constructor (boundary validator) and `Model.unsafe` is the
  * trusted-internal-caller bypass. Per scala-data-driven-refactor
  * §2, validity is enforced exactly once, at the boundary.
  */
class ModelSpec extends AnyFunSuite with Matchers {

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

  private val countMeasure = Measure(
    name = "flight_count",
    expr = AggregateCall(fn = AggregateFn.Count, alias = "*"),
  )

  /** Build a basic valid Model.of call. */
  private def buildModel(
      name:               String                                = "orders",
      dimensions:         List[Dimension]                       = List(amountDim, regionDim),
      measures:           List[Measure]                         = List(totalMeasure, countMeasure),
      calculatedMeasures: List[CalculatedMeasure]               = Nil,
      joins:              List[JoinSpec]                        = Nil,
      filters:            List[FilterSpec]                      = Nil,
      rollups:            List[RollupSpec]                      = Nil,
      defaultPolicies:    ModelPolicyDefaults                   = ModelPolicyDefaults.none,
      extensions:         Map[String, ExtensionValue]          = Map.empty,
      description:        Option[String]                        = None,
      version:            Int                                   = 1,
      status:             ModelStatus                           = ModelStatus.Draft,
  ): Either[ModelValidationError, Model] =
    Model.of(
      name = name, source = sampleSource,
      dimensions = dimensions, measures = measures,
      calculatedMeasures = calculatedMeasures,
      joins = joins, filters = filters, rollups = rollups,
      defaultPolicies = defaultPolicies,
      extensions = extensions,
      description = description, version = version, status = status,
    )

  // -- Model.of success path --

  test("Model.of happy path returns Right(Model)") {
    val r = buildModel()
    r.isRight shouldBe true
    val Right(m) = r
    m.name shouldBe "orders"
    m.source shouldBe sampleSource
    m.dimensions.size shouldBe 2
    m.measures.size shouldBe 2
  }

  test("Model.of with all defaults uses ModelPolicyDefaults.none") {
    val Right(m) = buildModel()
    m.defaultPolicies shouldBe ModelPolicyDefaults.none
  }

  test("Model.of with description + version + status") {
    val Right(m) = buildModel(description = Some("orders model"), version = 7, status = ModelStatus.Published)
    m.description shouldBe Some("orders model")
    m.version shouldBe 7
    m.status shouldBe ModelStatus.Published
  }

  test("Model.of with calculated measure") {
    val calc = CalculatedMeasure(
      name = "avg_total",
      expr = Expr.Divide(Expr.MeasureRef("total"), Expr.MeasureRef("flight_count")),
    )
    val Right(m) = buildModel(calculatedMeasures = List(calc))
    m.calculatedMeasures.size shouldBe 1
  }

  test("Model.of with joins + filters + rollups") {
    val join = JoinSpec(
      name       = "orders_to_customers",
      rightModel = "customers",
      kind       = JoinKind.Left,
      keys       = List(("customer_id", "id")),
    )
    val filter = FilterSpec(
      name      = "active_only",
      predicate = Expr.GreaterThan(
        Expr.FieldRef("status"),
        Expr.Literal(LiteralValue.IntValue(0), SealedDataType.Int),
      ),
    )
    val rollup = RollupSpec(
      name       = "orders_by_region",
      baseModel  = "orders",
      dimensions = List("region"),
      measures   = List(RollupMeasureSpec("total", AggregateFn.Sum, "sum_amount")),
      freshness  = RollupFreshnessSpec.NoTracking,
    )
    val Right(m) = buildModel(joins = List(join), filters = List(filter), rollups = List(rollup))
    m.joins.size shouldBe 1
    m.filters.size shouldBe 1
    m.rollups.size shouldBe 1
  }

  test("Model.of with extensions") {
    val exts = Map("description" -> ExtensionValue.String("orders model"))
    val Right(m) = buildModel(extensions = exts)
    m.extensions shouldBe exts
  }

  // -- Model.of failure path --

  test("Model.of with empty name returns Left(InvalidName)") {
    val r = buildModel(name = "")
    r shouldBe Left(ModelValidationError.InvalidName("name is blank"))
  }

  test("Model.of with dimension/measure name collision returns Left(DuplicateMember)") {
    val dupMeasure = Measure.aggregate("amount", AggregateFn.Count, Expr.FieldRef("id"))
    val r = buildModel(measures = List(dupMeasure))  // collides with amountDim
    r shouldBe Left(ModelValidationError.DuplicateMember("dimension/measure", "amount"))
  }

  test("Model.of with calc-measure referencing unknown measure returns Left(UnknownReference)") {
    val badCalc = CalculatedMeasure(
      name = "bad",
      expr = Expr.MeasureRef("ghost_measure"),
    )
    val r = buildModel(calculatedMeasures = List(badCalc))
    r shouldBe Left(ModelValidationError.UnknownReference("calculatedMeasures", "ghost_measure"))
  }

  test("Model.of with too-large extension envelope returns Left(ExtensionEnvelopeExceeded)") {
    val bigStr = "x" * (ExtensionLimits.MaxInlineBytes + 1)
    val r = buildModel(extensions = Map("big" -> ExtensionValue.String(bigStr)))
    r.isLeft shouldBe true
    r.left.get shouldBe a [ModelValidationError.ExtensionEnvelopeExceeded]
  }

  // -- Model.unsafe bypass --

  test("Model.unsafe builds a Model without validation (trusted callers)") {
    val m = Model.unsafe(
      name              = "unsafe_model",
      description       = None,
      source            = sampleSource,
      dimensions        = Nil,
      measures          = Nil,
      calculatedMeasures = Nil,
      joins             = Nil,
      filters           = Nil,
      version           = 1,
      status            = ModelStatus.Draft,
      rollups           = Nil,
      defaultPolicies   = ModelPolicyDefaults.none,
      extensions        = Map.empty,
    )
    m.name shouldBe "unsafe_model"
  }

  test("Model.unsafe does NOT validate (can construct with empty name)") {
    // The whole point of unsafe is to skip validation. The caller
    // has already validated via another path.
    val m = Model.unsafe(
      name              = "",
      description       = None,
      source            = sampleSource,
      dimensions        = Nil,
      measures          = Nil,
      calculatedMeasures = Nil,
      joins             = Nil,
      filters           = Nil,
      version           = 1,
      status            = ModelStatus.Draft,
      rollups           = Nil,
      defaultPolicies   = ModelPolicyDefaults.none,
      extensions        = Map.empty,
    )
    m.name shouldBe ""
  }

  // -- Serializable --

  test("Model round-trips through Java serialization") {
    val Right(m) = buildModel(description = Some("orders model"), version = 7)
    val bos = new java.io.ByteArrayOutputStream()
    val oos = new java.io.ObjectOutputStream(bos)
    oos.writeObject(m)
    oos.close()
    val bis = new java.io.ByteArrayInputStream(bos.toByteArray)
    val ois = new java.io.ObjectInputStream(bis)
    val restored = ois.readObject().asInstanceOf[Model]
    restored.name shouldBe m.name
    restored.description shouldBe m.description
    restored.dimensions shouldBe m.dimensions
    restored.measures shouldBe m.measures
  }
}