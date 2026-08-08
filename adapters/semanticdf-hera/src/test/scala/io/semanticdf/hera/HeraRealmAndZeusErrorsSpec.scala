package io.semanticdf.hera

import io.semanticdf.core.engine.{EngineContext, EngineError, ExecutionPlan, PortableQueryResult}
import io.semanticdf.core.model.{Dimension, Measure, Model}
import io.semanticdf.core.rel.AggregateFn
import io.semanticdf.core.schema.SealedDataType

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** v0.3.1: tests for the typed RealmNotFound / ZeusNotFound errors
  * surfaced by [[HeraEngine]].
  *
  * Per user domain knowledge: a query with an invalid `realmId` or
  * `zeusId` should fail with a SPECIFIC error case (not a generic
  * "QueryFailed" that hides the routing failure). Per
  * `docs/design/error-handling-style.md` "Hard bans": no generic
  * catch-all error case — caller needs to distinguish a config
  * error (don't retry) from a transient query error (may retry)
  * from a routing error (try a different Zeus).
  *
  * Per scala-spark-batch-bugs §1: assert actual error cases, not
  * just compile success. */
class HeraRealmAndZeusErrorsSpec extends AnyFunSuite with Matchers {

  // -- RealmNotFound --

  test("executePortable returns Left(EngineError.SourceSchemaChanged) when client returns RealmNotFound") {
    val fake = new FakeHeraClientWithError(HeraClientError.RealmNotFound(reason = "no realm 99"))
    val engine = HeraEngine(fake, realmId = 99L, zeusId = 1L)
    val plan = samplePlan()
    val result = engine.executePortable(plan, EngineContext.defaultContext)
    result match {
      case Left(EngineError.SourceSchemaChanged(reason)) =>
        reason should include ("RealmNotFound")
      case other => fail(s"expected SourceSchemaChanged for RealmNotFound, got $other")
    }
  }

  test("execute returns Left(SourceSchemaChanged) for RealmNotFound too") {
    val fake = new FakeHeraClientWithError(HeraClientError.RealmNotFound(reason = "no realm 99"))
    val engine = HeraEngine(fake, realmId = 99L, zeusId = 1L)
    val plan = samplePlan()
    val result = engine.execute(plan, EngineContext.defaultContext)
    result match {
      case Left(EngineError.SourceSchemaChanged(reason)) =>
        reason should include ("RealmNotFound")
      case other => fail(s"expected SourceSchemaChanged, got $other")
    }
  }

  // -- ZeusNotFound --

  test("executePortable returns Left(EngineError.EngineUnavailable) when client returns ZeusNotFound") {
    val fake = new FakeHeraClientWithError(HeraClientError.ZeusNotFound(reason = "no zeus 42 in realm 1"))
    val engine = HeraEngine(fake, realmId = 1L, zeusId = 42L)
    val plan = samplePlan()
    val result = engine.executePortable(plan, EngineContext.defaultContext)
    result match {
      case Left(EngineError.EngineUnavailable(name, available, wasDefault)) =>
        name should include ("ZeusNotFound")
      case other => fail(s"expected EngineUnavailable for ZeusNotFound, got $other")
    }
  }

  // -- QueryFailed stays QueryFailed (regression guard) --

  test("executePortable returns Left(EngineError.QueryRuntimeFailed) for generic QueryFailed") {
    val fake = new FakeHeraClientWithError(HeraClientError.QueryFailed(reason = "syntax error"))
    val engine = HeraEngine(fake, realmId = 1L, zeusId = 1L)
    val plan = samplePlan()
    val result = engine.executePortable(plan, EngineContext.defaultContext)
    result match {
      case Left(EngineError.QueryRuntimeFailed(reason)) =>
        reason should include ("QueryFailed")
      case other => fail(s"expected QueryRuntimeFailed, got $other")
    }
  }

  // -- Build a sample plan --

  private def samplePlan(): ExecutionPlan[Any] = {
    val model = Model.of(
      name      = "test",
      source    = io.semanticdf.core.model.SourceRef.ByName(
        catalog = Some("realm_1"), namespace = Some("public"), table = "orders",
      ),
      dimensions = List(Dimension.field("region", SealedDataType.Varchar)),
      measures   = List(Measure.aggregate("total", AggregateFn.Sum, io.semanticdf.core.expr.Expr.FieldRef("amount"))),
    ).fold(err => fail(s"Model.of failed: $err"), identity)
    ExecutionPlan(
      engine               = io.semanticdf.core.engine.EngineIdentity(
        name                 = "hera:1:1",
        nativeVersion        = "1.0",
        engineAdapterVersion = "0.3.0",
      ),
      native               = "SELECT * FROM orders",
      warnings             = Nil,
      requiredCapabilities = Set.empty,
      normalizedSchema     = io.semanticdf.core.engine.ResultSchema(Nil),
    )
  }
}

/** Test-only FakeHeraClient that returns a SINGLE pre-canned error
  * for `executeQuery`. Per scala-data-driven-refacer §1: this is
  * a test-only data table — it answers one question with one
  * answer. Mirrors the FakeHeraClient pattern but narrower in scope
  * for clarity. */
private final class FakeHeraClientWithError(err: HeraClientError) extends HeraClient {
  override def executeQuery(
      sql: String, realmId: Long, limit: Int = 100,
      jobGroupId: Option[String] = None, zeusId: Option[Long] = None,
  ): Either[HeraClientError, HeraQueryResult] = Left(err)
  // Other methods unused in these tests; throw to surface unexpected calls.
  override def describeTable(tableName: String, realmId: Long) = ???
  override def registerSparkJob(action: String, realmId: Long) = ???
  override def listTables(realmId: Long, prefix: String) = ???
  override def tableExists(tableName: String, realmId: Long) = ???
  override def getTableMeta(tableName: String, realmId: Long) = ???
  override def createTableFromSql(tableName: String, dataType: String, sql: String, realmId: Long) = ???
  override def updateTableSource(tableName: String, path: String, expectedOptLock: Long, realmId: Long) = ???
  override def refreshTable(tableName: String, realmId: Long) = ???
  override def listRealms() = ???
  override def getRealm(realmId: Long) = ???
}