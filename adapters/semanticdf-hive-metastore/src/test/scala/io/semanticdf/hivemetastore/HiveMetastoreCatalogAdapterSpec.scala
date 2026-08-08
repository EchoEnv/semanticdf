package io.semanticdf.hivemetastore

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

/** v0.3.1 (Gap 7 closure): tests for [[HiveMetastoreCatalogAdapter]]
  * — proves the engine-portable [[CatalogAdapter]] contract is
  * satisfied via HMS Thrift's `create_table` / `alter_table` calls,
  * with CAS via the `parameters` map.
  *
  * Per debug-mantra §1: tests use [[FakeHiveMetastoreClient]] for
  * determinism; integration tests against a real HMS server are
  * deferred (per the existing `ThriftHiveMetastoreClient` scaladoc
  * "no embedded mode in this PR" — see PR #398).
  *
  * Per the v0.3.1 standard (error-handling-style.md): typed
  * `Either[CatalogError, X]` for all failure paths; no exceptions
  * for "not implemented" / "wrong mode" — those surface as
  * `PublishResult.Conflict` per the CatalogAdapter CAS contract. */
class HiveMetastoreCatalogAdapterSpec extends AnyFunSuite with Matchers {

  private val catalogName = "hive"
  private val identity = CatalogIdentity(
    catalog   = "hive",
    namespace = "silver",
    name      = "orders",
  )

  private def adapter(fake: FakeHiveMetastoreClient): HiveMetastoreCatalogAdapter =
    HiveMetastoreCatalogAdapter(fake, catalogName)

  // -- instance shape --

  test("HiveMetastoreCatalogAdapter is a CatalogAdapter (contract conformance)") {
    val a = adapter(FakeHiveMetastoreClient.empty)
    a.isInstanceOf[CatalogAdapter] shouldBe true
    a.catalog shouldBe catalogName
  }

  // -- CreateOnly --

  test("publish CreateOnly on empty catalog returns Right(Inserted)") {
    val a = adapter(FakeHiveMetastoreClient.empty)
    val result = a.publish(identity, "doc-content", CatalogEntity.Model, PublishMode.CreateOnly)
    result match {
      case Right(PublishResult.Inserted(ref)) =>
        ref.version shouldBe 1
        ref.digest shouldBe "doc-content"
      case other => fail(s"expected Right(Inserted), got $other")
    }
  }

  test("publish CreateOnly on existing entity returns Right(Conflict)") {
    val fake = FakeHiveMetastoreClient.empty
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
    val a = adapter(FakeHiveMetastoreClient.empty)
    val result = a.publish(identity, "doc-v1", CatalogEntity.Model, PublishMode.Upsert)
    result match {
      case Right(PublishResult.Inserted(ref)) =>
        ref.version shouldBe 1
      case other => fail(s"expected Right(Inserted), got $other")
    }
  }

  test("publish Upsert on existing entity returns Right(Updated) at version+1") {
    val fake = FakeHiveMetastoreClient.empty
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
    val fake = FakeHiveMetastoreClient.empty
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
    val fake = FakeHiveMetastoreClient.empty
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

  test("publish CompareAndSet on empty catalog returns Right(Conflict)") {
    val a = adapter(FakeHiveMetastoreClient.empty)
    val result = a.publish(identity, "doc-v1", CatalogEntity.Model,
      PublishMode.CompareAndSet(expectedDigest = "any"))
    result match {
      case Right(PublishResult.Conflict(reason, None)) =>
        reason should include ("no entity at identity")
      case other => fail(s"expected Right(Conflict) with no current, got $other")
    }
  }

  // -- discover --

  test("discover returns Right(Some(...)) when ref matches stored values") {
    val fake = FakeHiveMetastoreClient.empty
    val a = adapter(fake)
    a.publish(identity, "doc-v1", CatalogEntity.Model, PublishMode.Upsert)
    val ref = CatalogRef(catalogName, "silver", "orders", version = 1, digest = "doc-v1")
    val result = a.discover(ref)
    result shouldBe Right(Some("doc-v1"))
  }

  test("discover returns Right(None) when stored digest differs (stale)") {
    val fake = FakeHiveMetastoreClient.empty
    val a = adapter(fake)
    a.publish(identity, "doc-v1", CatalogEntity.Model, PublishMode.Upsert)
    a.publish(identity, "doc-v2", CatalogEntity.Model, PublishMode.Upsert)
    val staleRef = CatalogRef(catalogName, "silver", "orders", version = 1, digest = "doc-v1")
    a.discover(staleRef) shouldBe Right(None)
  }

  test("discover returns Right(None) when entity doesn't exist") {
    val a = adapter(FakeHiveMetastoreClient.empty)
    val ref = CatalogRef(catalogName, "silver", "orders", version = 1, digest = "any")
    a.discover(ref) shouldBe Right(None)
  }

  // -- list --

  test("list returns published entries with kind info") {
    val fake = FakeHiveMetastoreClient.empty
    val a = adapter(fake)
    a.publish(identity, "doc-v1", CatalogEntity.Model, PublishMode.Upsert)
    val filter = CatalogFilter(catalog = Some(catalogName), namespace = Some("silver"))
    val result = a.list(filter)
    result match {
      case Right(entries) =>
        entries.size shouldBe 1
        entries.head.kind shouldBe CatalogEntity.Model
        entries.head.ref.name shouldBe "orders"
      case Left(err) => fail(s"expected Right, got Left($err)")
    }
  }

  test("list respects name prefix filter") {
    val fake = FakeHiveMetastoreClient.empty
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

  test("list without namespace filter returns Nil (HMS scope)") {
    val a = adapter(FakeHiveMetastoreClient.empty)
    val filter = CatalogFilter(catalog = Some(catalogName))
    val result = a.list(filter)
    result shouldBe Right(Nil)
  }

  // -- Rollup kind propagation --

  test("publish Rollup stores kind=rollup in parameters") {
    val fake = FakeHiveMetastoreClient.empty
    val a = adapter(fake)
    a.publish(identity, "rollup-v1", CatalogEntity.Rollup, PublishMode.Upsert)
    fake.currentParameters(catalogName, "silver", "orders") should contain ("semanticdf_kind" -> "rollup")
  }
}