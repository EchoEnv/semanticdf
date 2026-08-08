package io.semanticdf.hera

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

/** v0.3.1: tests for [[HeraCatalogAdapter]] — proves the
  * engine-portable [[CatalogAdapter]] contract is satisfied via
  * Hera's TableManage API, with CAS via the table's `optLock`
  * version field.
  *
  * Per debug-mantra §1: tests use [[FakeHeraClient]] for
  * determinism (no real Hera — per user constraint: "not yet need
  * to provision"). Integration tests against a real Hera server
  * deferred to v0.4.0 (per the existing UC/HMS "no embedded mode"
  * pattern).
  *
  * Per `docs/design/error-handling-style.md`:
  *   - Typed `Either[CatalogError, X]` for all failure paths
  *   - No exceptions for "not implemented" / "wrong mode"
  *   - CAS failures surface as `PublishResult.Conflict` (NOT throw)
  *
  * Per scala-spark-batch-bugs §1: assert actual state after
  * publish, not just compile success. */
class HeraCatalogAdapterSpec extends AnyFunSuite with Matchers {

  private val catalogName = "realm_1"
  private val identity = CatalogIdentity(
    catalog   = "realm_1",
    namespace = "silver",
    name      = "orders",
  )

  private def adapter(fake: FakeHeraClient): HeraCatalogAdapter =
    HeraCatalogAdapter(fake, catalogName)

  // -- instance shape --

  test("HeraCatalogAdapter is a CatalogAdapter (contract conformance)") {
    val a = adapter(FakeHeraClient.empty)
    a.isInstanceOf[CatalogAdapter] shouldBe true
    a.catalog shouldBe catalogName
  }

  // -- CreateOnly --

  test("publish CreateOnly on empty catalog returns Right(Inserted)") {
    val a = adapter(FakeHeraClient.empty)
    val result = a.publish(identity, "doc-content", CatalogEntity.Model, PublishMode.CreateOnly)
    result match {
      case Right(PublishResult.Inserted(ref)) =>
        ref.version shouldBe 1
      case other => fail(s"expected Right(Inserted), got $other")
    }
  }

  test("publish CreateOnly on existing entity returns Right(Conflict)") {
    val fake = FakeHeraClient.withTables(
      ("orders", 1L, 1L),
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

  test("publish Upsert on empty catalog returns Right(Inserted) at optLock=1") {
    val a = adapter(FakeHeraClient.empty)
    val result = a.publish(identity, "doc-v1", CatalogEntity.Model, PublishMode.Upsert)
    result match {
      case Right(PublishResult.Inserted(ref)) =>
        ref.version shouldBe 1
      case other => fail(s"expected Right(Inserted), got $other")
    }
  }

  test("publish Upsert on existing entity increments optLock") {
    val fake = FakeHeraClient.withTables(
      ("orders", 1L, 1L),
    )
    val a = adapter(fake)
    val result = a.publish(identity, "doc-v2", CatalogEntity.Model, PublishMode.Upsert)
    result match {
      case Right(PublishResult.Updated(prev, cur)) =>
        prev.version shouldBe 1
        cur.version shouldBe 2
      case other => fail(s"expected Right(Updated), got $other")
    }
    // Per scala-spark-batch-bugs §1: assert actual state
    fake.currentOptLock("orders", 1L) shouldBe Some(2L)
  }

  // -- CompareAndSet --

  test("publish CompareAndSet with matching optLock returns Right(Updated)") {
    val fake = FakeHeraClient.withTables(
      ("orders", 1L, 1L),
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

  test("publish CompareAndSet with mismatched optLock returns Right(Conflict)") {
    val fake = FakeHeraClient.withTables(
      ("orders", 1L, 5L),  // current optLock is 5
    )
    val a = adapter(fake)
    val result = a.publish(identity, "doc-v2", CatalogEntity.Model,
      PublishMode.CompareAndSet(expectedDigest = "1"))  // expecting optLock=1
    result match {
      case Right(PublishResult.Conflict(reason, Some(currentRef))) =>
        currentRef.version shouldBe 5
      case other => fail(s"expected Right(Conflict) with current, got $other")
    }
  }

  // -- discover --

  test("discover returns Right(Some) when ref matches stored optLock") {
    val fake = FakeHeraClient.withTables(
      ("orders", 1L, 1L),
    )
    val a = adapter(fake)
    val ref = CatalogRef(catalogName, "silver", "orders", version = 1, digest = "optLock:1")
    val result = a.discover(ref)
    result shouldBe Right(Some("optLock:1"))
  }

  test("discover returns Right(None) when stored optLock differs") {
    val fake = FakeHeraClient.withTables(
      ("orders", 1L, 5L),
    )
    val a = adapter(fake)
    val staleRef = CatalogRef(catalogName, "silver", "orders", version = 1, digest = "optLock:1")
    a.discover(staleRef) shouldBe Right(None)
  }

  test("discover returns Right(None) when entity doesn't exist") {
    val a = adapter(FakeHeraClient.empty)
    val ref = CatalogRef(catalogName, "silver", "orders", version = 1, digest = "any")
    a.discover(ref) shouldBe Right(None)
  }

  // -- list --

  test("list returns matching entries from the realm") {
    val fake = FakeHeraClient.withTables(
      ("orders", 1L, 1L),
      ("customers", 1L, 1L),
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

  // -- error path: invalid catalog format throws (programmer error) --

  test("publish with catalog not in 'realm_<id>' format throws IllegalArgumentException") {
    val badAdapter = HeraCatalogAdapter(FakeHeraClient.empty, "wrongformat")
    val badIdentity = CatalogIdentity(
      catalog   = "wrongformat",
      namespace = "silver",
      name      = "orders",
    )
    intercept[IllegalArgumentException] {
      badAdapter.publish(badIdentity, "doc", CatalogEntity.Model, PublishMode.CreateOnly)
    }
  }
}