package io.semanticdf.unitycatalog

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

/** v0.3.1 (Gap 7 closure): tests for [[UnityCatalogCatalogAdapter]]
  * — proves the engine-portable [[CatalogAdapter]] contract is
  * satisfied via UC REST's POST/PATCH/GET calls, with CAS via
  * the table's `properties` map.
  *
  * Per debug-mantra §1: tests use [[FakeUnityCatalogClient]] for
  * determinism; integration tests against a real UC server are
  * deferred (per the existing `HttpUnityCatalogClient` pattern
  * "no embedded mode in this PR" — see PR #394).
  *
  * Per the v0.3.1 standard (error-handling-style.md): typed
  * `Either[CatalogError, X]` for all failure paths. */
class UnityCatalogCatalogAdapterSpec extends AnyFunSuite with Matchers {

  private val catalogName = "unity"
  private val identity = CatalogIdentity(
    catalog   = "unity",
    namespace = "silver",
    name      = "orders",
  )

  private def adapter(fake: FakeUnityCatalogClient): UnityCatalogCatalogAdapter =
    UnityCatalogCatalogAdapter(fake, catalogName)

  // -- instance shape --

  test("UnityCatalogCatalogAdapter is a CatalogAdapter (contract conformance)") {
    val a = adapter(FakeUnityCatalogClient.empty)
    a.isInstanceOf[CatalogAdapter] shouldBe true
    a.catalog shouldBe catalogName
  }

  // -- CreateOnly --

  test("publish CreateOnly on empty catalog returns Right(Inserted)") {
    val a = adapter(FakeUnityCatalogClient.empty)
    val result = a.publish(identity, "doc-content", CatalogEntity.Model, PublishMode.CreateOnly)
    result match {
      case Right(PublishResult.Inserted(ref)) =>
        ref.version shouldBe 1
        ref.digest shouldBe "doc-content"
      case other => fail(s"expected Right(Inserted), got $other")
    }
  }

  test("publish CreateOnly on existing entity returns Right(Conflict)") {
    val fake = FakeUnityCatalogClient.empty
    val a = adapter(fake)
    a.publish(identity, "doc-v1", CatalogEntity.Model, PublishMode.CreateOnly)
    val result = a.publish(identity, "doc-v2", CatalogEntity.Model, PublishMode.CreateOnly)
    result match {
      case Right(PublishResult.Conflict(reason, Some(currentRef))) =>
        currentRef.version shouldBe 1
        currentRef.digest shouldBe "doc-v1"
      case other => fail(s"expected Right(Conflict) with current, got $other")
    }
  }

  // -- Upsert --

  test("publish Upsert on empty catalog returns Right(Inserted) at version 1") {
    val a = adapter(FakeUnityCatalogClient.empty)
    val result = a.publish(identity, "doc-v1", CatalogEntity.Model, PublishMode.Upsert)
    result match {
      case Right(PublishResult.Inserted(ref)) =>
        ref.version shouldBe 1
      case other => fail(s"expected Right(Inserted), got $other")
    }
  }

  test("publish Upsert on existing entity returns Right(Updated) at version+1") {
    val fake = FakeUnityCatalogClient.empty
    val a = adapter(fake)
    a.publish(identity, "doc-v1", CatalogEntity.Model, PublishMode.Upsert)
    val result = a.publish(identity, "doc-v2", CatalogEntity.Model, PublishMode.Upsert)
    result match {
      case Right(PublishResult.Updated(prev, cur)) =>
        prev.version shouldBe 1
        cur.version shouldBe 2
        cur.digest shouldBe "doc-v2"
      case other => fail(s"expected Right(Updated), got $other")
    }
  }

  // -- CompareAndSet --

  test("publish CompareAndSet with matching digest returns Right(Updated)") {
    val fake = FakeUnityCatalogClient.empty
    val a = adapter(fake)
    a.publish(identity, "doc-v1", CatalogEntity.Model, PublishMode.Upsert)
    val result = a.publish(identity, "doc-v2", CatalogEntity.Model,
      PublishMode.CompareAndSet(expectedDigest = "doc-v1"))
    result match {
      case Right(PublishResult.Updated(prev, cur)) =>
        prev.digest shouldBe "doc-v1"
        cur.digest shouldBe "doc-v2"
        cur.version shouldBe 2
      case other => fail(s"expected Right(Updated), got $other")
    }
  }

  test("publish CompareAndSet with mismatched digest returns Right(Conflict)") {
    val fake = FakeUnityCatalogClient.empty
    val a = adapter(fake)
    a.publish(identity, "doc-v1", CatalogEntity.Model, PublishMode.Upsert)
    val result = a.publish(identity, "doc-v2", CatalogEntity.Model,
      PublishMode.CompareAndSet(expectedDigest = "wrong-digest"))
    result match {
      case Right(PublishResult.Conflict(reason, Some(currentRef))) =>
        currentRef.digest shouldBe "doc-v1"
      case other => fail(s"expected Right(Conflict) with current, got $other")
    }
  }

  // -- discover --

  test("discover returns Right(Some(...)) when ref matches stored values") {
    val fake = FakeUnityCatalogClient.empty
    val a = adapter(fake)
    a.publish(identity, "doc-v1", CatalogEntity.Model, PublishMode.Upsert)
    val ref = CatalogRef(catalogName, "silver", "orders", version = 1, digest = "doc-v1")
    val result = a.discover(ref)
    result shouldBe Right(Some("doc-v1"))
  }

  test("discover returns Right(None) when stored digest differs (stale)") {
    val fake = FakeUnityCatalogClient.empty
    val a = adapter(fake)
    a.publish(identity, "doc-v1", CatalogEntity.Model, PublishMode.Upsert)
    a.publish(identity, "doc-v2", CatalogEntity.Model, PublishMode.Upsert)
    val staleRef = CatalogRef(catalogName, "silver", "orders", version = 1, digest = "doc-v1")
    a.discover(staleRef) shouldBe Right(None)
  }

  // -- list --

  test("list returns published entries with kind info") {
    val fake = FakeUnityCatalogClient.empty
    val a = adapter(fake)
    a.publish(identity, "doc-v1", CatalogEntity.Model, PublishMode.Upsert)
    val filter = CatalogFilter(catalog = Some(catalogName), namespace = Some("silver"))
    val result = a.list(filter)
    result match {
      case Right(entries) =>
        entries.size shouldBe 1
        entries.head.kind shouldBe CatalogEntity.Model
      case Left(err) => fail(s"expected Right, got Left($err)")
    }
  }

  test("list respects name prefix filter") {
    val fake = FakeUnityCatalogClient.empty
    val a = adapter(fake)
    val orders = CatalogIdentity(catalogName, "silver", "orders")
    val customers = CatalogIdentity(catalogName, "silver", "customers")
    a.publish(orders, "doc-orders", CatalogEntity.Model, PublishMode.Upsert)
    a.publish(customers, "doc-customers", CatalogEntity.Model, PublishMode.Upsert)
    val filter = CatalogFilter(
      catalog    = Some(catalogName),
      namespace  = Some("silver"),
      namePrefix = Some("ord"),
    )
    val result = a.list(filter)
    result match {
      case Right(entries) =>
        entries.size shouldBe 1
        entries.head.ref.name shouldBe "orders"
      case Left(err) => fail(s"expected Right, got Left($err)")
    }
  }

  test("list without namespace filter returns Nil (UC scope)") {
    val a = adapter(FakeUnityCatalogClient.empty)
    val filter = CatalogFilter(catalog = Some(catalogName))
    val result = a.list(filter)
    result shouldBe Right(Nil)
  }

  // -- Rollup kind propagation --

  test("publish Rollup stores kind=rollup in properties") {
    val fake = FakeUnityCatalogClient.empty
    val a = adapter(fake)
    a.publish(identity, "rollup-v1", CatalogEntity.Rollup, PublishMode.Upsert)
    fake.currentProperties(catalogName, "silver", "orders") should contain ("semanticdf_kind" -> "rollup")
  }
}