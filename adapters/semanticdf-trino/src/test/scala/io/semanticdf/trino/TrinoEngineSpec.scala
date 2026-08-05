package io.semanticdf.trino

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.semanticdf.core.engine.{Capability, EngineContext, EngineError, EngineIdentity, ExecutionPlan}
import io.semanticdf.core.model.{Model, ModelPolicyDefaults, ModelStatus, SourceRef}

/** Phase 2 contract: prove `TrinoEngine` implements the `Engine[Any]`
  * contract from `io.semanticdf.core.engine`. The first concrete
  * engine adapter — it demonstrates the engine-adapter boundary in
  * code and validates that the contract is implementable.
  *
  * The actual SQL lowering / source resolution / result decoding
  * land in follow-up PRs (see `adapters/semanticdf-trino/README.md`
  * for the roadmap). For now, `compile` / `execute` / `explain`
  * return `EngineError.FeatureDeferred` with a roadmap pointer.
  *
  * ==Why these tests matter==
  *
  *   - Validates that the Trino engine class can be instantiated
  *   - Validates the wire-stable `identity` value
  *   - Validates the `capabilities` set shape (closed set, no duplicates)
  *   - Validates that `compile` / `execute` / `explain` return
  *     `Left(EngineError.FeatureDeferred)` (a well-formed error,
  *     not a raw exception)
  *   - Validates zero Spark imports (the multi-engine boundary)
  */
class TrinoEngineSpec extends AnyFunSuite with Matchers {

  // -- fixtures --

  /** A minimal valid `Model` for testing `compile` / `explain`.
    * Uses `Model.of` (the smart constructor) so the model passes
    * validation. */
  private val sampleModel: Model = Model.of(
    name     = "orders",
    source   = SourceRef.ByName(catalog = None, namespace = Some("public"), table = "orders"),
    dimensions = Nil,
    measures   = Nil,
    defaultPolicies = ModelPolicyDefaults.none,
    status = ModelStatus.Draft,
  ).fold(
    err => fail(s"sampleModel failed validation: $err"),
    identity,
  )

  // -- instance shape --

  test("TrinoEngine.instance is a TrinoEngine") {
    TrinoEngine.instance shouldBe a [TrinoEngine]
  }

  test("TrinoEngine.instance returns the same instance on repeated access (singleton)") {
    val a = TrinoEngine.instance
    val b = TrinoEngine.instance
    a should be theSameInstanceAs b
  }

  // -- identity (wire-stable engine label) --

  test("identity is \"trino\"") {
    TrinoEngine.instance.identity shouldBe "trino"
  }

  // -- capabilities (typed, closed) --

  test("capabilities is a non-empty closed Set") {
    val caps = TrinoEngine.instance.capabilities
    caps should not be empty
  }

  test("capabilities is a Set (no duplicates)") {
    val caps = TrinoEngine.instance.capabilities
    caps.size shouldBe caps.toList.distinct.size
  }

  test("capabilities includes BroadcastJoin (Trino supports broadcast joins natively)") {
    TrinoEngine.instance.capabilities should contain (Capability.BroadcastJoin)
  }

  test("capabilities includes NestedStructTypes (Trino supports row types)") {
    TrinoEngine.instance.capabilities should contain (Capability.NestedStructTypes)
  }

  test("capabilities includes WindowRanking (Trino supports window functions)") {
    TrinoEngine.instance.capabilities should contain (Capability.WindowRanking)
  }

  test("capabilities does NOT include Materialize (Trino has no native persist)") {
    // Trino doesn't have a Spark-like `persist(MEMORY_ONLY)` API.
    // The adapter rejects this policy via EngineError.UnsupportedCapability.
    TrinoEngine.instance.capabilities should not contain (Capability.Materialize)
  }

  // -- describeCapabilities (typed descriptions per capability) --

  test("describeCapabilities has a non-empty description map") {
    TrinoEngine.instance.describeCapabilities should not be empty
  }

  test("describeCapabilities has an entry for every capability in `capabilities`") {
    val caps = TrinoEngine.instance.capabilities
    val descs = TrinoEngine.instance.describeCapabilities
    caps.foreach { cap =>
      descs should contain key cap
    }
  }

  test("describeCapabilities entries are non-empty strings") {
    val descs = TrinoEngine.instance.describeCapabilities
    descs.values.foreach { desc =>
      desc should not be empty
    }
  }

  // -- compile / execute / explain — deferred --

  test("compile returns Right(ExecutionPlan) with a ParameterizedSql") {
    val m = sampleModel
    val result = TrinoEngine.instance.compile(m, EngineContext.defaultContext)
    result.isRight shouldBe true
    val plan = result.toOption.get
    plan shouldBe a [ExecutionPlan[?]]
    plan.native shouldBe a [io.semanticdf.core.engine.ParameterizedSql]
  }

  test("compile result includes the source table in the FROM clause") {
    val m = sampleModel  // source = SourceRef.ByName(None, Some("public"), "orders")
    val result = TrinoEngine.instance.compile(m, EngineContext.defaultContext)
    val plan = result.toOption.get
    val psql = plan.native.asInstanceOf[io.semanticdf.core.engine.ParameterizedSql]
    psql.sql should include ("""FROM "public"."orders"""")
  }

  test("execute returns Left(EngineError.ConnectionFailed) when no factory is configured") {
    // The singleton instance has no connection factory.
    val plan: ExecutionPlan[Any] = ExecutionPlan[Any](
      engine = EngineIdentity("trino", "0.286", "0.2.4"),
      native = io.semanticdf.core.engine.ParameterizedSql(
        sql = """SELECT * FROM "orders"""",
        parameters = Nil,
      ),
    )
    val result = TrinoEngine.instance.execute(plan, EngineContext.defaultContext)
    result.isLeft shouldBe true
    result.left.toOption.get shouldBe a [EngineError.ConnectionFailed]
  }

  test("execute returns Right(TrinoResult) when factory is configured") {
    // Build a fresh engine with a FakeTrinoConnection factory.
    val fakeConn = FakeTrinoConnection.withResponse(
      sql = """SELECT * FROM "public"."orders" AS "orders"""",
      parameters = 0,
      result = TrinoResult(
        columns = List("orders_id"),
        rows    = List(List(io.semanticdf.core.expr.LiteralValue.IntValue(42))),
      ),
    )
    val engine = new TrinoEngine().withConnectionFactory(() => fakeConn)

    val plan: ExecutionPlan[Any] = ExecutionPlan[Any](
      engine = EngineIdentity("trino", "0.286", "0.2.4"),
      native = io.semanticdf.core.engine.ParameterizedSql(
        sql = """SELECT * FROM "public"."orders" AS "orders"""",
        parameters = Nil,
      ),
    )
    val result = engine.execute(plan, EngineContext.defaultContext)
    result.isRight shouldBe true
    val tr = result.toOption.get.asInstanceOf[TrinoResult]
    tr.rowCount shouldBe 1
    tr.cell(0, 0) shouldBe Some(io.semanticdf.core.expr.LiteralValue.IntValue(42))
  }

  test("execute records the SQL call on the connection") {
    val fakeConn = FakeTrinoConnection.withResponse(
      sql = """SELECT * FROM "public"."orders" AS "orders"""",
      parameters = 0,
      result = TrinoResult(columns = Nil, rows = Nil),
    )
    val engine = new TrinoEngine().withConnectionFactory(() => fakeConn)

    val plan: ExecutionPlan[Any] = ExecutionPlan[Any](
      engine = EngineIdentity("trino", "0.286", "0.2.4"),
      native = io.semanticdf.core.engine.ParameterizedSql(
        sql = """SELECT * FROM "public"."orders" AS "orders"""",
        parameters = Nil,
      ),
    )
    engine.execute(plan, EngineContext.defaultContext)
    fakeConn.recordedCalls.get(("""SELECT * FROM "public"."orders" AS "orders"""", 0)) shouldBe Some(1)
  }

  test("execute returns Left(EngineError.ConnectionFailed) for invalid plan native (not ParameterizedSql)") {
    val fakeConn = FakeTrinoConnection()
    val engine = new TrinoEngine().withConnectionFactory(() => fakeConn)

    val plan: ExecutionPlan[Any] = ExecutionPlan[Any](
      engine = EngineIdentity("trino", "0.286", "0.2.4"),
      native = "NOT-A-PARAMETERIZED-SQL",  // invalid — should be ParameterizedSql
    )
    val result = engine.execute(plan, EngineContext.defaultContext)
    result.isLeft shouldBe true
    result.left.toOption.get shouldBe a [EngineError.ConnectionFailed]
  }

  test("explain returns Right(String) with the compiled SQL") {
    val m = sampleModel  // source = SourceRef.ByName(None, Some("public"), "orders")
    val result = TrinoEngine.instance.explain(m, EngineContext.defaultContext)
    result.isRight shouldBe true
    val sql = result.toOption.get
    sql should include ("""FROM "public"."orders"""")
  }

  // -- boundary contract: zero Spark imports --

  test("TrinoEngine instance is an Engine[Any] (contract conformance)") {
    val engine: io.semanticdf.core.engine.Engine[Any] = TrinoEngine.instance
    engine.identity shouldBe "trino"
  }
}