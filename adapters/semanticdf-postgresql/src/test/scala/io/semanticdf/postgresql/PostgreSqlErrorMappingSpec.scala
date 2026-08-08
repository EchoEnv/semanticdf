package io.semanticdf.postgresql

import io.semanticdf.core.catalog.{CatalogError, CatalogIdentity, PublishMode, PublishResult}
import io.semanticdf.core.catalog.CatalogEntity

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** v0.4.0: tests for the typed error mapping in
  * [[PostgreSqlCatalogAdapter]] — proves every [[PostgreSqlError]] case
  * surfaces via the typed `Either[CatalogError, X]` contract.
  *
  * Per `docs/design/error-handling-style.md` "Hard bans": NO generic
  * `ServerError(String)` — every failure mode deserves its own case. */
class PostgreSqlErrorMappingSpec extends AnyFunSuite with Matchers {

  private val identity = CatalogIdentity(
    catalog   = "postgresql_realm",
    namespace = "silver",
    name      = "orders",
  )

  /** A FakePostgreSqlClient that returns a pre-canned error from
    * `getTableVersion`. */
  private final class FakeClientWithError(err: PostgreSqlError) extends FakePostgreSqlClient() {
    override def getTableVersion(schema: String, table: String): Either[PostgreSqlError, Long] = Left(err)
  }

  test("ConnectionFailed maps to CatalogError.Network") {
    val a = PostgreSqlCatalogAdapter(
      new FakeClientWithError(PostgreSqlError.ConnectionFailed("connection refused")),
      "postgresql_realm",
    )
    val result = a.publish(identity, "doc", CatalogEntity.Model, PublishMode.CreateOnly)
    result shouldBe Left(CatalogError.Network(reason = "connection refused"))
  }

  test("AuthenticationFailed maps to CatalogError.Unauthorized") {
    val a = PostgreSqlCatalogAdapter(
      new FakeClientWithError(PostgreSqlError.AuthenticationFailed("bad password")),
      "postgresql_realm",
    )
    val result = a.publish(identity, "doc", CatalogEntity.Model, PublishMode.CreateOnly)
    result shouldBe Left(CatalogError.Unauthorized(reason = "bad password"))
  }

  test("SyntaxError maps to CatalogError.MalformedManifest") {
    val a = PostgreSqlCatalogAdapter(
      new FakeClientWithError(PostgreSqlError.SyntaxError("syntax error at or near 'SELECT'")),
      "postgresql_realm",
    )
    val result = a.publish(identity, "doc", CatalogEntity.Model, PublishMode.CreateOnly)
    result shouldBe Left(CatalogError.MalformedManifest(reason = "syntax error at or near 'SELECT'"))
  }

  test("CasConflict maps to CatalogError.Conflict (CAS failures surface as Conflict)") {
    val a = PostgreSqlCatalogAdapter(
      new FakeClientWithError(PostgreSqlError.CasConflict("xmin mismatch")),
      "postgresql_realm",
    )
    val result = a.publish(identity, "doc", CatalogEntity.Model, PublishMode.CreateOnly)
    result match {
      case Left(CatalogError.Conflict(reason)) => reason should include ("xmin")
      case other => fail(s"expected Conflict, got $other")
    }
  }

  test("TableNotFound from getTableVersion maps to Right(None), NOT Left (the 'absent' semantic)") {
    val a = PostgreSqlCatalogAdapter(
      new FakeClientWithError(PostgreSqlError.TableNotFound("not here")),
      "postgresql_realm",
    )
    val result = a.publish(identity, "doc", CatalogEntity.Model, PublishMode.CreateOnly)
    result match {
      case Right(PublishResult.Inserted(_)) => // expected — not-found feeds into create path
      case other => fail(s"expected Right(Inserted), got $other")
    }
  }
}
