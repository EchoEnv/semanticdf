package io.semanticdf.hera

import io.semanticdf.core.engine.{
  EngineContext,
  EngineError,
  EngineIdentity,
  ExecutionPlan,
  PortableQueryResult,
  ResultSchema,
}
import io.semanticdf.core.expr.Expr
import io.semanticdf.core.model.{Dimension, Measure, Model, SourceRef}
import io.semanticdf.core.rel.AggregateFn
import io.semanticdf.core.schema.SealedDataType

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** v0.3.1: tests for [[HeraEngine]] — proves the engine-portable
  * `Engine[Any]` contract is satisfied via Hera's
  * `POST /private/explore/query`.
  *
  * Per `docs/design/error-handling-style.md`:
  *   - All public methods return `Either[EngineError, X]`
  *   - Map [[HeraClientError]] → [[EngineError]] SPECIFICALLY
  *     (no catch-all `ServerError`)
  *   - Programmer errors at boundary throw `IllegalArgumentException`
  *
  * Per scala-spark-batch-bugs §1: assert actual SQL output and
  * actual engine behavior, not just compile success. */
class HeraEngineSpec extends AnyFunSuite with Matchers {

  private val realmId = 1L
  private val zeusId  = 1L

  private def engine(fake: FakeHeraClient): HeraEngine =
    HeraEngine(fake, realmId, zeusId)

  // -- identity / capabilities --

  test("identity is 'hera:<realmId>:<zeusId>' (per user: 'realm is top layer; zeus is engine')") {
    val e = engine(FakeHeraClient.empty)
    e.identity shouldBe "hera:1:1"
  }

  test("identity encodes realm + zeus (different realm/zeus → different identity)") {
    val e1 = HeraEngine(FakeHeraClient.empty, realmId = 1L, zeusId = 1L)
    val e2 = HeraEngine(FakeHeraClient.empty, realmId = 1L, zeusId = 2L)
    val e3 = HeraEngine(FakeHeraClient.empty, realmId = 2L, zeusId = 1L)
    e1.identity shouldBe "hera:1:1"
    e2.identity shouldBe "hera:1:2"
    e3.identity shouldBe "hera:2:1"
    e1.identity should not be e2.identity
    e1.identity should not be e3.identity
  }

  test("capabilities advertises NestedStructTypes only (v1)") {
    val e = engine(FakeHeraClient.empty)
    e.capabilities should contain (io.semanticdf.core.engine.Capability.NestedStructTypes)
  }

  // -- compile(model) --

  test("compile(model) returns Right with the SQL in the plan's native field") {
    val m = sampleModel()
    val e = engine(FakeHeraClient.empty)
    val result = e.compile(m, EngineContext.defaultContext)
    result match {
      case Right(plan) =>
        plan.native shouldBe a [String]
        plan.engine.name shouldBe "hera:1:1"
      case Left(err) => fail(s"expected Right, got Left($err)")
    }
  }

  // -- compile(RelOp) returns Left(UnsupportedCapability) --

  test("compile(RelOp) returns Left(UnsupportedCapability), not throws (per standard)") {
    val e = engine(FakeHeraClient.empty)
    val plan = io.semanticdf.core.rel.RelOp.Scan(
      io.semanticdf.core.engine.ResolvedSource.Scan(
        SourceRef.ByName(catalog = Some("realm_1"), namespace = Some("public"), table = "orders"),
        io.semanticdf.core.engine.ResolvedSchema(Map.empty),
      ),
      Nil,
      Nil,
    )
    val result = e.compile(plan, EngineContext.defaultContext)
    result match {
      case Left(EngineError.UnsupportedCapability(name, reason)) =>
        name shouldBe "RelOp.compile"
        reason should include ("Hera")
      case other => fail(s"expected Left(UnsupportedCapability), got $other")
    }
  }

  // -- execute --

  test("execute passes the SQL string from compile to the client") {
    val fake = FakeHeraClient.empty
    val e = engine(fake)
    val m = sampleModel()
    val plan = e.compile(m, EngineContext.defaultContext).fold(err => fail(s"compile failed: $err"), identity)
    val result = e.execute(plan, EngineContext.defaultContext)
    result shouldBe a [Right[_, _]]
    fake.executedQueries.size shouldBe 1
    val (sql, qRealmId, _, _, qZeusId) = fake.executedQueries.head
    qRealmId shouldBe realmId
    qZeusId shouldBe Some(zeusId)
    sql should include ("SELECT")
  }

  test("execute forwards the zeusId to the client (per user: 'zeus is hera engine for execution')") {
    val fake = FakeHeraClient.empty
    val e = HeraEngine(fake, realmId = 1L, zeusId = 99L)
    val plan = e.compile(sampleModel(), EngineContext.defaultContext).fold(err => fail(s"compile failed: $err"), identity)
    e.execute(plan, EngineContext.defaultContext)
    val (_, _, _, _, qZeusId) = fake.executedQueries.head
    qZeusId shouldBe Some(99L)
  }

  // -- executePortable --

  test("executePortable returns Right(PortableQueryResult) with the rows + columns") {
    // Use a SQL fragment that's guaranteed to be in the compile
    // output (the model has a `total` measure, so "total" appears
    // in the SQL).
    val fake = FakeHeraClient.withQueryResult(
      "total",
      HeraQueryResult(
        fields = List(HeraField("id", "bigint", nullable = false)),
        rows   = List(Map("id" -> 1L), Map("id" -> 2L), Map("id" -> 3L)),
        queryTime = java.time.Duration.ofMillis(10),
      ),
    )
    val e = engine(fake)
    val plan = e.compile(sampleModel(), EngineContext.defaultContext).fold(err => fail(s"compile failed: $err"), identity)
    val result = e.executePortable(plan, EngineContext.defaultContext)
    result match {
      case Right(pqr) =>
        pqr.rowCount shouldBe 3
        pqr.schema.fields.size shouldBe 1
        pqr.schema.fields.head.name shouldBe "id"
      case Left(err) => fail(s"expected Right, got Left($err)")
    }
  }

  // -- explain --

  test("explain returns Right with the SQL + realm + zeus context") {
    val e = engine(FakeHeraClient.empty)
    val result = e.explain(sampleModel(), EngineContext.defaultContext)
    result match {
      case Right(text) =>
        text should include ("Hera SQL")
        text should include ("Realm: 1")
        text should include ("Zeus: 1")
        text should include ("SELECT")
      case Left(err) => fail(s"expected Right, got Left($err)")
    }
  }

  // -- builders --

  private def sampleModel(): Model = Model.of(
    name      = "orders_test",
    source    = SourceRef.ByName(
      catalog = Some("realm_1"), namespace = Some("public"), table = "orders",
    ),
    dimensions = List(Dimension.field("region", SealedDataType.Varchar)),
    measures   = List(Measure.aggregate("total", AggregateFn.Sum, Expr.FieldRef("amount"))),
  ).fold(err => fail(s"Model.of failed: $err"), identity)
}