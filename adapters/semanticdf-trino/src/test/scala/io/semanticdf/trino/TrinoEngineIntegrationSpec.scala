package io.semanticdf.trino

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.semanticdf.core.engine.{EngineContext, ParameterizedSql}
import io.semanticdf.core.expr.{Expr, LiteralValue}
import io.semanticdf.core.model.{Dimension, FilterSpec, JoinSpec, Measure, Model, SourceRef}
import io.semanticdf.core.rel.{AggregateCall, AggregateFn, JoinKind}
import io.semanticdf.core.schema.SealedDataType

/** End-to-end integration test for `TrinoEngine`:
  *   compile(model) → ExecutionPlan → execute(plan) → TrinoResult
  *
  * Phase 2 follow-up to PR #372. The Trino adapter's contract is
  * complete — these tests prove the FULL FLOW works, not just the
  * individual pieces.
  *
  * ==Why this matters==
  *
  * Unit tests on `compile` (TrinoQueryCompilerSpec) prove the
  * compiler emits the right SQL. Unit tests on `execute`
  * (TrinoEngineSpec) prove the engine calls the connection. The
  * integration test proves that compile's OUTPUT (a
  * ParameterizedSql) is what execute CONSUMES (the connection's
  * prepareStatement).
  *
  * This is the last gate before a real Trino cluster integration
  * test (the Phase 1 decision gate).
  *
  * ==Why per scala-data-driven-refactor §1==
  *
  * Pure behavior tests — given a Model + fake connection, the
  * engine produces a deterministic TrinoResult. Same input →
  * same output. No state, no IO.
  */
class TrinoEngineIntegrationSpec extends AnyFunSuite with Matchers {

  // -- helpers --

  /** Build a minimal Model with a single dimension + measure + filter. */
  private def buildModel(): Model = {
    val attempt = Model.of(
      name       = "orders_by_region",
      source     = SourceRef.ByName(
        catalog   = Some("hive"),
        namespace = Some("silver"),
        table     = "orders",
      ),
      dimensions = List(Dimension.field("region", SealedDataType.Varchar)),
      measures   = List(
        Measure.aggregate("total", AggregateFn.Sum, Expr.FieldRef("amount")),
      ),
      filters    = List(FilterSpec(
        name      = "active",
        predicate = Expr.GreaterThan(
          Expr.FieldRef("amount"),
          Expr.Literal(LiteralValue.IntValue(0), SealedDataType.Int),
        ),
      )),
    )
    attempt.fold(err => fail(s"Model.of failed: $err"), identity)
  }

  /** Build a TrinoEngine with a FakeTrinoConnection factory pre-configured
    * to return `result` for any `prepareStatement` call. */
  private def buildEngine(
      result:    TrinoResult,
      keySql:    String,
      keyParams: Int,
  ): (TrinoEngine, FakeTrinoConnection) = {
    val fakeConn = FakeTrinoConnection.withResponse(
      sql        = keySql,
      parameters = keyParams,
      result     = result,
    )
    val engine = new TrinoEngine().withConnectionFactory(() => fakeConn)
    (engine, fakeConn)
  }

  // -- single-row integration --

  test("compile → execute integration returns the expected TrinoResult") {
    val m   = buildModel()
    val expectedResult = TrinoResult(
      columns = List("region", "total"),
      rows    = List(List(
        LiteralValue.StringValue("AA"),
        LiteralValue.LongValue(12345L),
      )),
    )
    val (engine, fakeConn) = buildEngine(
      result    = expectedResult,
      keySql    = """SELECT "region" AS "region", SUM("amount") AS "total" FROM "hive"."silver"."orders" AS "orders" WHERE (("amount" > ?)) GROUP BY "region"""",
      keyParams = 1,
    )

    val compiled = engine.compile(m, EngineContext.defaultContext)
    compiled.isRight shouldBe true
    val plan = compiled.toOption.get

    val executed = engine.execute(plan, EngineContext.defaultContext)
    executed.isRight shouldBe true
    val result = executed.toOption.get.asInstanceOf[TrinoResult]
    result shouldBe expectedResult

    // Sanity check: the fake connection saw the call
    fakeConn.recordedCalls.size shouldBe 1
    fakeConn.recordedCalls.head shouldBe (
      ("""SELECT "region" AS "region", SUM("amount") AS "total" FROM "hive"."silver"."orders" AS "orders" WHERE (("amount" > ?)) GROUP BY "region"""", 1),
      1,
    )
  }

  // -- multi-row integration --

  test("compile → execute integration returns multiple rows in order") {
    val m   = buildModel()
    val expectedResult = TrinoResult(
      columns = List("region", "total"),
      rows    = List(
        List(LiteralValue.StringValue("AA"), LiteralValue.LongValue(100L)),
        List(LiteralValue.StringValue("BB"), LiteralValue.LongValue(200L)),
        List(LiteralValue.StringValue("CC"), LiteralValue.LongValue(300L)),
      ),
    )
    val (engine, _) = buildEngine(
      result    = expectedResult,
      keySql    = """SELECT "region" AS "region", SUM("amount") AS "total" FROM "hive"."silver"."orders" AS "orders" WHERE (("amount" > ?)) GROUP BY "region"""",
      keyParams = 1,
    )

    val compiled = engine.compile(m, EngineContext.defaultContext)
    val plan     = compiled.toOption.get
    val result   = engine.execute(plan, EngineContext.defaultContext).toOption.get.asInstanceOf[TrinoResult]

    result.rowCount shouldBe 3
    result.cell(0, 0) shouldBe Some(LiteralValue.StringValue("AA"))
    result.cell(0, 1) shouldBe Some(LiteralValue.LongValue(100L))
    result.cell(1, 0) shouldBe Some(LiteralValue.StringValue("BB"))
    result.cell(1, 1) shouldBe Some(LiteralValue.LongValue(200L))
    result.cell(2, 0) shouldBe Some(LiteralValue.StringValue("CC"))
    result.cell(2, 1) shouldBe Some(LiteralValue.LongValue(300L))
  }

  // -- empty result set --

  test("compile → execute integration returns empty rows") {
    val m   = buildModel()
    val expectedResult = TrinoResult(columns = List("region", "total"), rows = Nil)
    val (engine, _) = buildEngine(
      result    = expectedResult,
      keySql    = """SELECT "region" AS "region", SUM("amount") AS "total" FROM "hive"."silver"."orders" AS "orders" WHERE (("amount" > ?)) GROUP BY "region"""",
      keyParams = 1,
    )

    val compiled = engine.compile(m, EngineContext.defaultContext)
    val plan     = compiled.toOption.get
    val result   = engine.execute(plan, EngineContext.defaultContext).toOption.get.asInstanceOf[TrinoResult]

    result.rowCount shouldBe 0
    result.rows shouldBe Nil
  }

  // -- multiple compile/execute cycles --

  test("multiple compile → execute cycles work independently") {
    val m   = buildModel()
    val expectedResult = TrinoResult(
      columns = List("region", "total"),
      rows    = List(List(LiteralValue.StringValue("AA"), LiteralValue.LongValue(100L))),
    )
    val (engine, fakeConn) = buildEngine(
      result    = expectedResult,
      keySql    = """SELECT "region" AS "region", SUM("amount") AS "total" FROM "hive"."silver"."orders" AS "orders" WHERE (("amount" > ?)) GROUP BY "region"""",
      keyParams = 1,
    )

    // Cycle 1
    val plan1 = engine.compile(m, EngineContext.defaultContext).toOption.get
    engine.execute(plan1, EngineContext.defaultContext).isRight shouldBe true

    // Cycle 2 (independent)
    val plan2 = engine.compile(m, EngineContext.defaultContext).toOption.get
    engine.execute(plan2, EngineContext.defaultContext).isRight shouldBe true

    // Each cycle produces a fresh connection (per-request)
    fakeConn.recordedCalls.size shouldBe 1  // same SQL + param count → same key
    fakeConn.recordedCalls.values.sum shouldBe 2  // but called twice
  }

  // -- explain → compile consistency --

  test("explain returns SQL that matches compile's SQL") {
    val m   = buildModel()
    val engine = new TrinoEngine()  // no factory needed for explain

    val compiledSql = engine.compile(m, EngineContext.defaultContext).toOption.get
      .native.asInstanceOf[ParameterizedSql].sql
    val explainedSql = engine.explain(m, EngineContext.defaultContext).toOption.get

    explainedSql shouldBe compiledSql
  }

  // -- boundary contract --

  test("TrinoEngine implements Engine[Any] (contract conformance)") {
    val engine = new TrinoEngine()
    engine shouldBe a [io.semanticdf.core.engine.Engine[?]]
  }
}