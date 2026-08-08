package io.semanticdf.postgresql

import io.semanticdf.core.catalog.{
  CatalogAdapter,
  CatalogEntity,
  CatalogError,
  CatalogIdentity,
  CatalogRef,
  PublishMode,
  PublishResult,
}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** v0.4.0: tests for [[PostgreSqlCatalogAdapter]] — proves the
  * engine-portable [[CatalogAdapter]] contract is satisfied via
  * PostgreSQL DDL, with CAS via the `xmin` system column.
  *
  * Per `docs/design/error-handling-style.md`:
  *   - Typed `Either[CatalogError, X]` for all failure paths
  *   - CAS failures surface as `PublishResult.Conflict` (NOT throw)
  *   - Programmer errors (empty catalog) at the boundary throw
  *     `IllegalArgumentException`
  *
  * Per `scala-spark-batch-bugs §1`: assert actual state (the
  * stored xmin value) after publish, not just compile success. */
class PostgreSqlCatalogAdapterSpec extends AnyFunSuite with Matchers {

  private val catalogName = "postgresql_realm"
  private val identity = CatalogIdentity(
    catalog   = "postgresql_realm",
    namespace = "silver",
    name      = "orders",
  )

  private def adapter(fake: FakePostgreSqlClient): PostgreSqlCatalogAdapter =
    PostgreSqlCatalogAdapter(fake, catalogName)

  // -- instance shape --

  test("PostgreSqlCatalogAdapter is a CatalogAdapter (contract conformance)") {
    val a = adapter(FakePostgreSqlClient.empty)
    a.isInstanceOf[CatalogAdapter] shouldBe true
    a.catalog shouldBe catalogName
  }

  // -- CreateOnly --

  test("publish CreateOnly on empty catalog returns Right(Inserted)") {
    val a = adapter(FakePostgreSqlClient.empty)
    val result = a.publish(identity, "doc-content", CatalogEntity.Model, PublishMode.CreateOnly)
    result match {
      case Right(PublishResult.Inserted(ref)) =>
        ref.version should be > 0
      case other => fail(s"expected Right(Inserted), got $other")
    }
  }

  test("publish CreateOnly on existing entity returns Right(Conflict)") {
    val fake = FakePostgreSqlClient.withManifests(
      List(("silver", "orders", "doc-v1", 5L)),
    )
    val a = adapter(fake)
    val result = a.publish(identity, "doc-v2", CatalogEntity.Model, PublishMode.CreateOnly)
    result match {
      case Right(PublishResult.Conflict(reason, Some(currentRef))) =>
        currentRef.version shouldBe 5
      case other => fail(s"expected Right(Conflict) with current, got $other")
    }
  }

  // -- Upsert --

  test("publish Upsert on existing entity increments xmin") {
    val fake = FakePostgreSqlClient.withManifests(
      List(("silver", "orders", "doc-v1", 1L)),
    )
    val a = adapter(fake)
    val result = a.publish(identity, "doc-v2", CatalogEntity.Model, PublishMode.Upsert)
    result match {
      case Right(PublishResult.Updated(prev, cur)) =>
        prev.version shouldBe 1
        cur.version.toInt should be > 1
      case other => fail(s"expected Right(Updated), got $other")
    }
    // Per scala-spark-batch-bugs §1: assert actual state.
    fake.currentXmin("silver", "orders").get should be > 1L
  }

  // -- CompareAndSet (CAS via xmin) --

  test("publish CompareAndSet with matching xmin returns Right(Updated)") {
    val fake = FakePostgreSqlClient.withManifests(
      List(("silver", "orders", "doc-v1", 1L)),
    )
    val a = adapter(fake)
    val result = a.publish(identity, "doc-v2", CatalogEntity.Model,
      PublishMode.CompareAndSet(expectedDigest = "xmin:1"))
    result match {
      case Right(PublishResult.Updated(prev, cur)) =>
        prev.version shouldBe 1
        cur.version.toInt should be > 1
      case other => fail(s"expected Right(Updated), got $other")
    }
  }

  test("publish CompareAndSet with mismatched xmin returns Right(Conflict)") {
    val fake = FakePostgreSqlClient.withManifests(
      List(("silver", "orders", "doc-v1", 5L)),
    )
    val a = adapter(fake)
    val result = a.publish(identity, "doc-v2", CatalogEntity.Model,
      PublishMode.CompareAndSet(expectedDigest = "xmin:1"))
    result match {
      case Right(PublishResult.Conflict(reason, Some(currentRef))) =>
        currentRef.version shouldBe 5
      case other => fail(s"expected Right(Conflict) with current, got $other")
    }
  }

  // -- discover --

  test("discover returns Right(Some) when ref matches stored xmin") {
    val fake = FakePostgreSqlClient.withManifests(
      List(("silver", "orders", "xmin:1-content", 1L)),
    )
    val a = adapter(fake)
    val ref = CatalogRef(catalogName, "silver", "orders", version = 1, digest = "any")
    val result = a.discover(ref)
    result shouldBe Right(Some("any"))
  }

  test("discover returns Right(None) when stored xmin differs") {
    val fake = FakePostgreSqlClient.withManifests(
      List(("silver", "orders", "xmin:5-content", 5L)),
    )
    val a = adapter(fake)
    val staleRef = CatalogRef(catalogName, "silver", "orders", version = 1, digest = "any")
    a.discover(staleRef) shouldBe Right(None)
  }

  // -- error path: empty catalog throws (programmer error) --

  test("publish with empty catalog throws IllegalArgumentException") {
    val badAdapter = PostgreSqlCatalogAdapter(FakePostgreSqlClient.empty, "")
    val badIdentity = CatalogIdentity(catalog = "", namespace = "silver", name = "orders")
    intercept[IllegalArgumentException] {
      badAdapter.publish(badIdentity, "doc", CatalogEntity.Model, PublishMode.CreateOnly)
    }
  }
}
