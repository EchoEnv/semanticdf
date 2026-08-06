package io.semanticdf.duckdb

import io.semanticdf.core.engine.{Capability, EngineContext, EngineError, EngineIdentity, ExecutionPlan, ParameterizedSql, ResolvedSource}
import io.semanticdf.core.expr.{Expr, LiteralValue}
import io.semanticdf.core.model.{CalculatedMeasure, Dimension, Measure, Model, ModelPolicyDefaults, ModelStatus, SourceRef}
import io.semanticdf.core.rel.{AggregateCall, AggregateFn}
import io.semanticdf.core.schema.{SchemaFieldKind, SealedDataType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Unit tests for [[DuckDBEngine]] — mirrors the structure of
  * `TrinoEngineSpec`. Per the user's standing constraint, the
  * API surface must match `TrinoEngine` exactly (additive
  * engine, no breaking changes).
  *
  * ==Why no real DuckDB here (vs. in-memory integration test)==
  *
  * These are pure unit tests. The DuckDB-specific behavior
  * (JDBC, ResultSet → DuckDBResult mapping) is covered by the
  * integration test in [[DuckDBEngineIntegrationSpec]], which
  * uses in-memory DuckDB (`jdbc:duckdb:`) — no Docker required,
  * sub-second startup. */
class DuckDBEngineSpec extends AnyFunSuite with Matchers {

  private def sampleModel: Model = Model.of(
    name      = "orders",
    source    = SourceRef.ByName(catalog = None, namespace = Some("main"), table = "orders"),
    dimensions         = List(Dimension("region", Expr.FieldRef("region"), Some(SealedDataType.Varchar))),
    measures           = List(Measure("amount", AggregateCall(AggregateFn.Sum, Some(Expr.FieldRef("amount")), "amount"))),
    calculatedMeasures = Nil,
    joins              = Nil,
    defaultPolicies    = ModelPolicyDefaults.none,
    status             = ModelStatus.Draft,
  ).fold(err => fail(s"sampleModel failed validation: $err"), identity)

  // -- engine-portable contract conformance --

  test("DuckDBEngine.instance is a DuckDBEngine") {
    DuckDBEngine.instance shouldBe a [DuckDBEngine]
  }

  test("identity is \"duckdb\"") {
    DuckDBEngine.instance.identity shouldBe "duckdb"
  }

  test("capabilities is a non-empty closed Set") {
    val caps = DuckDBEngine.instance.capabilities
    caps should not be empty
    caps.size shouldBe caps.toList.distinct.size
  }

  test("capabilities includes NestedStructTypes, BroadcastJoin, SkewJoin, LateBinding, Materialize") {
    val caps = DuckDBEngine.instance.capabilities
    caps should contain (Capability.NestedStructTypes)
    caps should contain (Capability.BroadcastJoin)
    caps should contain (Capability.SkewJoin)
    caps should contain (Capability.LateBinding)
    caps should contain (Capability.Materialize)
  }

  test("describeCapabilities has an entry for every capability in `capabilities`") {
    val caps = DuckDBEngine.instance.capabilities
    val desc = DuckDBEngine.instance.describeCapabilities
    caps.foreach { c =>
      desc.get(c) shouldBe defined
      desc(c) should not be empty
    }
  }

  // -- compile() --

  test("compile returns Right(ExecutionPlan) with a ParameterizedSql") {
    val result = DuckDBEngine.instance.compile(sampleModel, EngineContext.defaultContext)
    result.isRight shouldBe true
    result.toOption.get shouldBe a [ExecutionPlan[_]]
    result.toOption.get.native shouldBe a [ParameterizedSql]
  }

  test("compile result includes the source table in the FROM clause") {
    val psql = DuckDBEngine.instance.compile(sampleModel, EngineContext.defaultContext)
      .toOption.get.native.asInstanceOf[ParameterizedSql]
    psql.sql should include ("""FROM "memory"."main"."orders"""")
  }

  test("compile returns Left(EngineError.FeatureDeferred) when the resolver rejects with NotFound") {
    val resolver = new io.semanticdf.core.engine.SourceResolver {
      override def resolve(s: SourceRef, i: EngineIdentity): ResolvedSource =
        ResolvedSource.NotFound(source = s, reason = "test")
    }
    val engine = new DuckDBEngine().withSourceResolver(resolver)
    engine.compile(sampleModel, EngineContext.defaultContext).isLeft shouldBe true
  }

  // -- execute() --

  test("execute returns Left(EngineError.ConnectionFailed) when no factory is configured") {
    val plan = DuckDBEngine.instance.compile(sampleModel, EngineContext.defaultContext).toOption.get
    val result = DuckDBEngine.instance.execute(plan, EngineContext.defaultContext)
    result.isLeft shouldBe true
    result.left.toOption.get shouldBe a [EngineError.ConnectionFailed]
  }

  test("execute returns Right(DuckDBResult) when factory is configured and SQL matches a fake response") {
    val fake = FakeDuckDBConnection(
      ("""SELECT "region", SUM("amount") AS "amount" FROM "memory"."main"."orders" GROUP BY "region"""" ->
        DuckDBResult(
          columns = List("region", "amount"),
          rows    = List(
            List(LiteralValue.StringValue("us"), LiteralValue.LongValue(100)),
          ),
        )),
    )
    val engine = new DuckDBEngine().withConnectionFactory(() => fake)
    val plan   = engine.compile(sampleModel, EngineContext.defaultContext).toOption.get
    val result = engine.execute(plan, EngineContext.defaultContext)
    result.isRight shouldBe true
    val duckResult = result.toOption.get.asInstanceOf[DuckDBResult]
    duckResult.columns shouldBe List("region", "amount")
    duckResult.rowCount shouldBe 1
  }

  // -- preview() --

  test("preview(n) appends LIMIT n to the compiled SQL") {
    val fake = FakeDuckDBConnection.withCatchAll(DuckDBResult(
      columns = List("region", "amount"),
      rows    = List(List(LiteralValue.StringValue("us"), LiteralValue.LongValue(100))),
    ))
    val engine = new DuckDBEngine().withConnectionFactory(() => fake)
    val baseSql = DuckDBEngine.instance.compile(sampleModel, EngineContext.defaultContext)
      .toOption.get.native.asInstanceOf[ParameterizedSql].sql
    val expectedSql = baseSql + " LIMIT 3"
    val result = engine.preview(sampleModel, 3, EngineContext.defaultContext)
    result.isRight shouldBe true
    fake.recordedCalls.exists(_._1 == expectedSql) shouldBe true
  }

  test("preview with n < 0 returns Left(EngineError.ConnectionFailed)") {
    val engine = new DuckDBEngine()
    val result = engine.preview(sampleModel, -1, EngineContext.defaultContext)
    result.isLeft shouldBe true
    result.left.toOption.get.toString should include ("n must be >= 0")
  }

  // -- schema() --

  test("schema returns a SchemaSummary with dimensions + measures") {
    val m = sampleModel
    val summary = DuckDBEngine.instance.schema(m, EngineContext.defaultContext).toOption.get
    summary.modelName shouldBe "orders"
    summary.rowCount shouldBe 2  // 1 dim + 1 measure
    summary.ofKind(SchemaFieldKind.Dimension).map(_.fieldName) shouldBe List("region")
    summary.ofKind(SchemaFieldKind.Measure).map(_.fieldName) shouldBe List("amount")
  }

  test("schema projects dimensions, calculated measures, and joins together") {
    val m = Model.of(
      name      = "complex",
      source    = SourceRef.ByName(None, Some("main"), "t"),
      dimensions         = List(Dimension("d", Expr.FieldRef("d"))),
      measures           = List(Measure("m", AggregateCall(AggregateFn.Sum, Some(Expr.FieldRef("m")), "m"))),
      calculatedMeasures = List(CalculatedMeasure("c", Expr.FieldRef("m"))),
      joins              = Nil,
      defaultPolicies    = ModelPolicyDefaults.none,
      status             = ModelStatus.Draft,
    ).fold(err => fail(s"sampleModel failed: $err"), identity)
    val summary = DuckDBEngine.instance.schema(m, EngineContext.defaultContext).toOption.get
    summary.fields.map(_.fieldKind) shouldBe List(
      SchemaFieldKind.Dimension,
      SchemaFieldKind.Measure,
      SchemaFieldKind.CalculatedMeasure,
    )
  }

  // -- withSourceResolver wiring (mirrors PR #395) --

  test("withSourceResolver sets the resolver and returns the same engine (fluent)") {
    val engine = new DuckDBEngine()
    val resolver = new io.semanticdf.core.engine.SourceResolver {
      override def resolve(s: SourceRef, i: EngineIdentity): ResolvedSource =
        ResolvedSource.Scan(source = s, schema = io.semanticdf.core.engine.ResolvedSchema())
    }
    engine.withSourceResolver(resolver) should be theSameInstanceAs engine
    engine.sourceResolver shouldBe Some(resolver)
  }
}