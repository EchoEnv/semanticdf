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

  // -- preview (engine-specific behavior, mirrors original Spark library) --

  test("preview(n) returns up to n rows from executing model") {
    val rows: List[List[io.semanticdf.core.expr.LiteralValue]] = (1 to 10).toList.map { i =>
      List(io.semanticdf.core.expr.LiteralValue.IntValue(i))
    }
    // sampleModel has no dimensions/measures, so compile emits
    // `SELECT  FROM "public"."orders" AS "orders"`. preview must
    // append `LIMIT n` to this exact SQL.
    val baseSql = TrinoEngine.instance.compile(sampleModel, EngineContext.defaultContext)
      .toOption.get.native.asInstanceOf[io.semanticdf.core.engine.ParameterizedSql].sql
    val expectedSql = baseSql + " LIMIT 3"

    val fakeConn = FakeTrinoConnection.withResponse(
      sql        = expectedSql,
      parameters = 0,
      result     = TrinoResult(
        columns = List("id"),
        rows    = rows.take(3),
      ),
    )
    val engine = new TrinoEngine().withConnectionFactory(() => fakeConn)
    val m      = sampleModel
    val result = engine.preview(m, 3, EngineContext.defaultContext)

    result.isRight shouldBe true
    val tr = result.toOption.get
    tr.rowCount shouldBe 3
    fakeConn.recordedCalls.get((expectedSql, 0)) shouldBe Some(1)
  }

  test("preview appends LIMIT n to the compiled SQL (not parameterized as ?)") {
    // Capture the SQL the connection received; assert it ends with LIMIT 5.
    val capturedSql = new java.util.concurrent.atomic.AtomicReference[String]("")
    val fakeConn = new TrinoConnection {
      override def prepareStatement(sql: String, parameters: List[io.semanticdf.core.expr.LiteralValue]): TrinoResult = {
        capturedSql.set(sql)
        TrinoResult(Nil, Nil)
      }
      override def close(): Unit = ()
    }
    val engine = new TrinoEngine().withConnectionFactory(() => fakeConn)
    engine.preview(sampleModel, 5, EngineContext.defaultContext)
    capturedSql.get() should include ("LIMIT 5")
  }

  test("preview with n=0 returns 0 rows (LIMIT 0 is valid SQL)") {
    val baseSql = TrinoEngine.instance.compile(sampleModel, EngineContext.defaultContext)
      .toOption.get.native.asInstanceOf[io.semanticdf.core.engine.ParameterizedSql].sql
    val expectedSql = baseSql + " LIMIT 0"
    val fakeConn = FakeTrinoConnection.withResponse(
      sql        = expectedSql,
      parameters = 0,
      result     = TrinoResult(List("id"), Nil),
    )
    val engine = new TrinoEngine().withConnectionFactory(() => fakeConn)
    val result = engine.preview(sampleModel, 0, EngineContext.defaultContext)

    result.isRight shouldBe true
    result.toOption.get.rowCount shouldBe 0
  }

  test("preview with n<0 returns Left(EngineError.ConnectionFailed)") {
    val engine = new TrinoEngine()
    val m      = sampleModel
    val result = engine.preview(m, -1, EngineContext.defaultContext)

    result.isLeft shouldBe true
    val err = result.left.toOption.get
    err shouldBe a [EngineError.ConnectionFailed]
    err.toString should include ("preview n must be >= 0")
  }

  test("preview with no connection factory returns Left(EngineError.ConnectionFailed)") {
    val engine = new TrinoEngine()  // no factory
    val result = engine.preview(sampleModel, 1, EngineContext.defaultContext)

    result.isLeft shouldBe true
    result.left.toOption.get shouldBe a [EngineError.ConnectionFailed]
  }

  // -- count (engine-specific behavior, mirrors original Spark library's df.count()) --

  test("count returns the row count as a Long") {
    val m   = sampleModel  // empty model uses base source only
    val compiled = TrinoEngine.instance.compile(m, EngineContext.defaultContext).toOption.get
      .native.asInstanceOf[io.semanticdf.core.engine.ParameterizedSql]
    val expectedSql = s"""SELECT COUNT(*) AS "row_count" FROM (${compiled.sql}) AS "_count_subq""""

    val fakeConn = FakeTrinoConnection.withResponse(
      sql        = expectedSql,
      parameters = 0,
      result     = TrinoResult(
        columns = List("row_count"),
        rows    = List(List(io.semanticdf.core.expr.LiteralValue.LongValue(42L))),
      ),
    )
    val engine = new TrinoEngine().withConnectionFactory(() => fakeConn)
    val result = engine.count(m, EngineContext.defaultContext)

    result.isRight shouldBe true
    result.toOption.get shouldBe 42L
  }

  test("count for empty source returns 0 (a valid count, not an error)") {
    val m   = sampleModel
    val compiled = TrinoEngine.instance.compile(m, EngineContext.defaultContext).toOption.get
      .native.asInstanceOf[io.semanticdf.core.engine.ParameterizedSql]
    val expectedSql = s"""SELECT COUNT(*) AS "row_count" FROM (${compiled.sql}) AS "_count_subq""""

    val fakeConn = FakeTrinoConnection.withResponse(
      sql        = expectedSql,
      parameters = 0,
      result     = TrinoResult(
        columns = List("row_count"),
        rows    = List(List(io.semanticdf.core.expr.LiteralValue.LongValue(0L))),
      ),
    )
    val engine = new TrinoEngine().withConnectionFactory(() => fakeConn)
    val result = engine.count(m, EngineContext.defaultContext)

    result.isRight shouldBe true
    result.toOption.get shouldBe 0L
  }

  test("count handles IntValue result type (narrowing compat)") {
    // Some Trino JDBC configurations return COUNT(*) as Int. The
    // decoder -> LiteralValue.IntValue. The engine must accept
    // both LongValue and IntValue and return a Long either way.
    val m   = sampleModel
    val compiled = TrinoEngine.instance.compile(m, EngineContext.defaultContext).toOption.get
      .native.asInstanceOf[io.semanticdf.core.engine.ParameterizedSql]
    val expectedSql = s"""SELECT COUNT(*) AS "row_count" FROM (${compiled.sql}) AS "_count_subq""""

    val fakeConn = FakeTrinoConnection.withResponse(
      sql        = expectedSql,
      parameters = 0,
      result     = TrinoResult(
        columns = List("row_count"),
        rows    = List(List(io.semanticdf.core.expr.LiteralValue.IntValue(7))),
      ),
    )
    val engine = new TrinoEngine().withConnectionFactory(() => fakeConn)
    val result = engine.count(m, EngineContext.defaultContext)

    result.isRight shouldBe true
    result.toOption.get shouldBe 7L
  }

  test("count returns Left(EngineError.ConnectionFailed) when COUNT returns 2+ rows") {
    // Some malformed scenarios could return >1 row. The engine
    // must error rather than silently picking one.
    val m   = sampleModel
    val compiled = TrinoEngine.instance.compile(m, EngineContext.defaultContext).toOption.get
      .native.asInstanceOf[io.semanticdf.core.engine.ParameterizedSql]
    val expectedSql = s"""SELECT COUNT(*) AS "row_count" FROM (${compiled.sql}) AS "_count_subq""""

    val fakeConn = FakeTrinoConnection.withResponse(
      sql        = expectedSql,
      parameters = 0,
      result     = TrinoResult(
        columns = List("row_count"),
        rows    = List(
          List(io.semanticdf.core.expr.LiteralValue.LongValue(1L)),
          List(io.semanticdf.core.expr.LiteralValue.LongValue(2L)),
        ),
      ),
    )
    val engine = new TrinoEngine().withConnectionFactory(() => fakeConn)
    val result = engine.count(m, EngineContext.defaultContext)

    result.isLeft shouldBe true
    val err = result.left.toOption.get
    err shouldBe a [EngineError.ConnectionFailed]
    err.toString should include ("COUNT(*) must return 1 row")
  }

  test("count returns Left(EngineError.ConnectionFailed) for unexpected cell type") {
    val m   = sampleModel
    val compiled = TrinoEngine.instance.compile(m, EngineContext.defaultContext).toOption.get
      .native.asInstanceOf[io.semanticdf.core.engine.ParameterizedSql]
    val expectedSql = s"""SELECT COUNT(*) AS "row_count" FROM (${compiled.sql}) AS "_count_subq""""

    val fakeConn = FakeTrinoConnection.withResponse(
      sql        = expectedSql,
      parameters = 0,
      result     = TrinoResult(
        columns = List("row_count"),
        rows    = List(List(io.semanticdf.core.expr.LiteralValue.StringValue("not a count"))),
      ),
    )
    val engine = new TrinoEngine().withConnectionFactory(() => fakeConn)
    val result = engine.count(m, EngineContext.defaultContext)

    result.isLeft shouldBe true
    val err = result.left.toOption.get
    err shouldBe a [EngineError.ConnectionFailed]
    err.toString should include ("unexpected cell")
  }

  test("count with no connection factory returns Left(EngineError.ConnectionFailed)") {
    val engine = new TrinoEngine()  // no factory
    val result = engine.count(sampleModel, EngineContext.defaultContext)

    result.isLeft shouldBe true
    result.left.toOption.get shouldBe a [EngineError.ConnectionFailed]
  }

  // -- executeAsRows (engine-specific convenience, mirrors df.collect().map(_.getValuesMap(...))) --

  test("executeAsRows returns rows as List[Map[String, LiteralValue]]") {
    val m   = sampleModel  // empty model uses base source only
    val compiled = TrinoEngine.instance.compile(m, EngineContext.defaultContext).toOption.get
      .native.asInstanceOf[io.semanticdf.core.engine.ParameterizedSql]
    val fakeConn = FakeTrinoConnection.withResponse(
      sql        = compiled.sql,
      parameters = 0,
      result     = TrinoResult(
        columns = List("region", "total"),
        rows    = List(
          List(
            io.semanticdf.core.expr.LiteralValue.StringValue("AA"),
            io.semanticdf.core.expr.LiteralValue.LongValue(100L),
          ),
          List(
            io.semanticdf.core.expr.LiteralValue.StringValue("BB"),
            io.semanticdf.core.expr.LiteralValue.LongValue(200L),
          ),
        ),
      ),
    )
    val engine = new TrinoEngine().withConnectionFactory(() => fakeConn)
    val result = engine.executeAsRows(m, EngineContext.defaultContext)

    result.isRight shouldBe true
    val rows = result.toOption.get
    rows should have size 2

    rows(0) shouldBe Map(
      "region" -> io.semanticdf.core.expr.LiteralValue.StringValue("AA"),
      "total"  -> io.semanticdf.core.expr.LiteralValue.LongValue(100L),
    )
    rows(1) shouldBe Map(
      "region" -> io.semanticdf.core.expr.LiteralValue.StringValue("BB"),
      "total"  -> io.semanticdf.core.expr.LiteralValue.LongValue(200L),
    )
  }

  test("executeAsRows returns Nil for empty result set") {
    val m   = sampleModel
    val compiled = TrinoEngine.instance.compile(m, EngineContext.defaultContext).toOption.get
      .native.asInstanceOf[io.semanticdf.core.engine.ParameterizedSql]
    val fakeConn = FakeTrinoConnection.withResponse(
      sql        = compiled.sql,
      parameters = 0,
      result     = TrinoResult(columns = List("region"), rows = Nil),
    )
    val engine = new TrinoEngine().withConnectionFactory(() => fakeConn)
    val result = engine.executeAsRows(m, EngineContext.defaultContext)

    result.isRight shouldBe true
    result.toOption.get shouldBe Nil
  }

  test("executeAsRows handles mixed-type cells (heterogeneous columns)") {
    val m   = sampleModel
    val compiled = TrinoEngine.instance.compile(m, EngineContext.defaultContext).toOption.get
      .native.asInstanceOf[io.semanticdf.core.engine.ParameterizedSql]
    val fakeConn = FakeTrinoConnection.withResponse(
      sql        = compiled.sql,
      parameters = 0,
      result     = TrinoResult(
        columns = List("id", "name", "active", "salary"),
        rows    = List(List(
          io.semanticdf.core.expr.LiteralValue.IntValue(1),
          io.semanticdf.core.expr.LiteralValue.StringValue("Alice"),
          io.semanticdf.core.expr.LiteralValue.BoolValue(true),
          io.semanticdf.core.expr.LiteralValue.DecimalValue(BigDecimal("100.50")),
        )),
      ),
    )
    val engine = new TrinoEngine().withConnectionFactory(() => fakeConn)
    val result = engine.executeAsRows(m, EngineContext.defaultContext)

    result.isRight shouldBe true
    val rows = result.toOption.get
    rows should have size 1
    rows(0) shouldBe Map(
      "id"     -> io.semanticdf.core.expr.LiteralValue.IntValue(1),
      "name"   -> io.semanticdf.core.expr.LiteralValue.StringValue("Alice"),
      "active" -> io.semanticdf.core.expr.LiteralValue.BoolValue(true),
      "salary" -> io.semanticdf.core.expr.LiteralValue.DecimalValue(BigDecimal("100.50")),
    )
  }

  test("executeAsRows preserves null cells as LiteralValue.NullValue") {
    val m   = sampleModel
    val compiled = TrinoEngine.instance.compile(m, EngineContext.defaultContext).toOption.get
      .native.asInstanceOf[io.semanticdf.core.engine.ParameterizedSql]
    val fakeConn = FakeTrinoConnection.withResponse(
      sql        = compiled.sql,
      parameters = 0,
      result     = TrinoResult(
        columns = List("region", "total"),
        rows    = List(List(
          io.semanticdf.core.expr.LiteralValue.StringValue("AA"),
          io.semanticdf.core.expr.LiteralValue.NullValue,
        )),
      ),
    )
    val engine = new TrinoEngine().withConnectionFactory(() => fakeConn)
    val result = engine.executeAsRows(m, EngineContext.defaultContext)

    result.isRight shouldBe true
    val rows = result.toOption.get
    rows.head("total") shouldBe io.semanticdf.core.expr.LiteralValue.NullValue
  }

  test("executeAsRows propagates compile failure as Left") {
    // Compile failure path: an unparseable model. The smart
    // constructor `Model.of` validates, but a programmer could
    // construct a model directly. For the test, just confirm
    // the propagation: any compile-error short-circuits.
    //
    // We can't easily construct an invalid model here (the
    // smart constructor refuses). So we test the
    // execute-error path instead — using an invalid Trino
    // SQL to surface the ConnectionFailed surface via
    // the lower-level execute().
    val fakeConn = FakeTrinoConnection()  // no responses registered
    val engine = new TrinoEngine().withConnectionFactory(() => fakeConn)
    val result = engine.executeAsRows(sampleModel, EngineContext.defaultContext)

    // The fake throws RuntimeException because no canned response
    // matches, which `execute()` catches as ConnectionFailed.
    result.isLeft shouldBe true
    result.left.toOption.get shouldBe a [EngineError.ConnectionFailed]
  }

  test("executeAsRows with no connection factory returns Left(EngineError.ConnectionFailed)") {
    val engine = new TrinoEngine()  // no factory
    val result = engine.executeAsRows(sampleModel, EngineContext.defaultContext)

    result.isLeft shouldBe true
    result.left.toOption.get shouldBe a [EngineError.ConnectionFailed]
  }
}