package io.semanticdf.myplatform

import io.semanticdf.core.catalog.{
  CatalogAdapter,
  CatalogEntity,
  CatalogError,
  CatalogFilter,
  CatalogIdentity,
  CatalogRef,
  PublishMode,
  PublishResult,
}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** v0.3.1: tests for [[MyPlatformCatalogAdapter]] — proves the
  * engine-portable [[CatalogAdapter]] contract is satisfied via
  * MyPlatform's REST API, with CAS via the table's `version` field.
  *
  * Per `docs/design/error-handling-style.md`:
  *   - Typed `Either[CatalogError, X]` for all failure paths
  *   - No exceptions for "not implemented" / "wrong mode"
  *   - CAS failures surface as `PublishResult.Conflict` (NOT throw)
  *
  * Per `scala-spark-batch-bugs §1`: assert actual state after publish,
  * not just compile success. */
class MyPlatformCatalogAdapterSpec extends AnyFunSuite with Matchers {

  private val catalogName = "myplatform_realm"
  private val identity = CatalogIdentity(
    catalog   = "myplatform_realm",
    namespace = "silver",
    name      = "orders",
  )

  private def adapter(fake: FakeMyPlatformClient): MyPlatformCatalogAdapter =
    MyPlatformCatalogAdapter(fake, catalogName)

  // -- instance shape --

  test("MyPlatformCatalogAdapter is a CatalogAdapter (contract conformance)") {
    val a = adapter(FakeMyPlatformClient.empty)
    a.isInstanceOf[CatalogAdapter] shouldBe true
    a.catalog shouldBe catalogName
  }

  // -- CreateOnly --

  test("publish CreateOnly on empty catalog returns Right(Inserted)") {
    val a = adapter(FakeMyPlatformClient.empty)
    val result = a.publish(identity, "doc-content", CatalogEntity.Model, PublishMode.CreateOnly)
    result match {
      case Right(PublishResult.Inserted(ref)) =>
        ref.version shouldBe 1
      case other => fail(s"expected Right(Inserted), got $other")
    }
  }

  test("publish CreateOnly on existing entity returns Right(Conflict)") {
    val fake = FakeMyPlatformClient.withTables(
      ("orders", catalogName, 1L),
    )
    val a = adapter(fake)
    val result = a.publish(identity, "doc-v2", CatalogEntity.Model, PublishMode.CreateOnly)
    result match {
      case Right(PublishResult.Conflict(reason, Some(currentRef))) =>
        currentRef.version shouldBe 1
      case other => fail(s"expected Right(Conflict) with current, got $other")
    }
  }

  // -- Upsert --

  test("publish Upsert on empty catalog returns Right(Inserted) at version=1") {
    val a = adapter(FakeMyPlatformClient.empty)
    val result = a.publish(identity, "doc-v1", CatalogEntity.Model, PublishMode.Upsert)
    result match {
      case Right(PublishResult.Inserted(ref)) =>
        ref.version shouldBe 1
      case other => fail(s"expected Right(Inserted), got $other")
    }
  }

  test("publish Upsert on existing entity increments version") {
    val fake = FakeMyPlatformClient.withTables(
      ("orders", catalogName, 1L),
    )
    val a = adapter(fake)
    val result = a.publish(identity, "doc-v2", CatalogEntity.Model, PublishMode.Upsert)
    result match {
      case Right(PublishResult.Updated(prev, cur)) =>
        prev.version shouldBe 1
        cur.version shouldBe 2
      case other => fail(s"expected Right(Updated), got $other")
    }
    // Per scala-spark-batch-bugs §1: assert actual state.
    fake.currentVersion("orders", catalogName) shouldBe Some(2L)
  }

  // -- CompareAndSet --

  test("publish CompareAndSet with matching version returns Right(Updated)") {
    val fake = FakeMyPlatformClient.withTables(
      ("orders", catalogName, 1L),
    )
    val a = adapter(fake)
    val result = a.publish(identity, "doc-v2", CatalogEntity.Model,
      PublishMode.CompareAndSet(expectedDigest = "1"))
    result match {
      case Right(PublishResult.Updated(prev, cur)) =>
        prev.version shouldBe 1
        cur.version shouldBe 2
      case other => fail(s"expected Right(Updated), got $other")
    }
  }

  test("publish CompareAndSet with mismatched version returns Right(Conflict)") {
    val fake = FakeMyPlatformClient.withTables(
      ("orders", catalogName, 5L),
    )
    val a = adapter(fake)
    val result = a.publish(identity, "doc-v2", CatalogEntity.Model,
      PublishMode.CompareAndSet(expectedDigest = "1"))
    result match {
      case Right(PublishResult.Conflict(reason, Some(currentRef))) =>
        currentRef.version shouldBe 5
      case other => fail(s"expected Right(Conflict) with current, got $other")
    }
  }

  // -- discover --

  test("discover returns Right(Some) when ref matches stored version") {
    val fake = FakeMyPlatformClient.withTables(
      ("orders", catalogName, 1L),
    )
    val a = adapter(fake)
    val ref = CatalogRef(catalogName, "silver", "orders", version = 1, digest = "version:1")
    val result = a.discover(ref)
    result shouldBe Right(Some("version:1"))
  }

  test("discover returns Right(None) when stored version differs") {
    val fake = FakeMyPlatformClient.withTables(
      ("orders", catalogName, 5L),
    )
    val a = adapter(fake)
    val staleRef = CatalogRef(catalogName, "silver", "orders", version = 1, digest = "version:1")
    a.discover(staleRef) shouldBe Right(None)
  }

  test("discover returns Right(None) when entity doesn't exist") {
    val a = adapter(FakeMyPlatformClient.empty)
    val ref = CatalogRef(catalogName, "silver", "orders", version = 1, digest = "any")
    a.discover(ref) shouldBe Right(None)
  }

  // -- list --

  test("list returns matching entries from the realm") {
    val fake = FakeMyPlatformClient.withTables(
      ("orders", catalogName, 1L),
      ("customers", catalogName, 1L),
    )
    val a = adapter(fake)
    val filter = CatalogFilter(catalog = Some(catalogName), namePrefix = Some("ord"))
    val result = a.list(filter)
    result match {
      case Right(entries) =>
        entries.size shouldBe 1
        entries.head.ref.name shouldBe "orders"
      case Left(err) => fail(s"expected Right, got Left($err)")
    }
  }

  // -- error path: empty catalog throws (programmer error) --

  test("publish with empty catalog throws IllegalArgumentException") {
    val badAdapter = MyPlatformCatalogAdapter(FakeMyPlatformClient.empty, "")
    val badIdentity = CatalogIdentity(
      catalog   = "",
      namespace = "silver",
      name      = "orders",
    )
    intercept[IllegalArgumentException] {
      badAdapter.publish(badIdentity, "doc", CatalogEntity.Model, PublishMode.CreateOnly)
    }
  }
}