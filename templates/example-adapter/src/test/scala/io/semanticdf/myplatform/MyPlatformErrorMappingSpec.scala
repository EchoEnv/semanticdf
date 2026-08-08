package io.semanticdf.myplatform

import io.semanticdf.core.catalog.{CatalogError, CatalogIdentity, CatalogRef, PublishMode, PublishResult}
import io.semanticdf.core.model.CatalogEntity

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** v0.3.1: tests for the typed error mapping in
  * [[MyPlatformCatalogAdapter]] — proves every [[MyPlatformError]] case
  * surfaces via the typed `Either[CatalogError, X]` contract.
  *
  * Per `docs/design/error-handling-style.md` "Hard bans": NO generic
  * `ServerError(String)` — every failure mode deserves its own case,
  * and this test pins that EVERY MyPlatformError case maps to a
  * specific CatalogError case (not a fallback).
  *
  * Per `scala-spark-batch-bugs §1`: assert the actual CatalogError
  * case class (not just "returned Left"). */
class MyPlatformErrorMappingSpec extends AnyFunSuite with Matchers {

  private val identity = CatalogIdentity(
    catalog   = "myplatform_realm",
    namespace = "silver",
    name      = "orders",
  )

  /** A FakeMyPlatformClient that returns a pre-canned error from
    * `getTableMeta`. We use this to drive the catalog adapter's
    * error-mapping paths without writing 8 different test scenarios. */
  private final class FakeClientWithError(err: MyPlatformError) extends FakeMyPlatformClient() {
    override def getTableMeta(
        table:   String,
        realmId: String,
    ): Either[MyPlatformError, MyPlatformTableMeta] = Left(err)
  }

  // -- Error mapping per MyPlatformError case --

  test("Unauthorized maps to CatalogError.Unauthorized") {
    val a = MyPlatformCatalogAdapter(
      new FakeClientWithError(MyPlatformError.Unauthorized("auth bad")),
      "myplatform_realm",
    )
    val result = a.publish(identity, "doc", CatalogEntity.Model, PublishMode.CreateOnly)
    result shouldBe Left(CatalogError.Unauthorized(reason = "auth bad"))
  }

  test("Forbidden maps to CatalogError.Unauthorized (auth-side)") {
    val a = MyPlatformCatalogAdapter(
      new FakeClientWithError(MyPlatformError.Forbidden("no scope")),
      "myplatform_realm",
    )
    val result = a.publish(identity, "doc", CatalogEntity.Model, PublishMode.CreateOnly)
    result shouldBe Left(CatalogError.Unauthorized(reason = "no scope"))
  }

  test("BadRequest maps to CatalogError.MalformedManifest") {
    val a = MyPlatformCatalogAdapter(
      new FakeClientWithError(MyPlatformError.BadRequest("bad sql")),
      "myplatform_realm",
    )
    val result = a.publish(identity, "doc", CatalogEntity.Model, PublishMode.CreateOnly)
    result shouldBe Left(CatalogError.MalformedManifest(reason = "bad sql"))
  }

  test("NetworkError maps to CatalogError.Network") {
    val a = MyPlatformCatalogAdapter(
      new FakeClientWithError(MyPlatformError.NetworkError("timeout")),
      "myplatform_realm",
    )
    val result = a.publish(identity, "doc", CatalogEntity.Model, PublishMode.CreateOnly)
    result shouldBe Left(CatalogError.Network(reason = "timeout"))
  }

  test("MalformedResponse maps to CatalogError.MalformedManifest") {
    val a = MyPlatformCatalogAdapter(
      new FakeClientWithError(MyPlatformError.MalformedResponse("unexpected shape")),
      "myplatform_realm",
    )
    val result = a.publish(identity, "doc", CatalogEntity.Model, PublishMode.CreateOnly)
    result shouldBe Left(CatalogError.MalformedManifest(reason = "unexpected shape"))
  }

  // -- NotFound is the "absent" path: should NOT surface as an error --

  test("NotFound from getTableMeta maps to Right(None), NOT Left (the 'absent' semantic)") {
    val a = MyPlatformCatalogAdapter(
      new FakeClientWithError(MyPlatformError.NotFound("not here")),
      "myplatform_realm",
    )
    // The adapter should detect this and treat the entity as absent
    // (Right(None) — feed into the create-or-update logic).
    val result = a.publish(identity, "doc", CatalogEntity.Model, PublishMode.CreateOnly)
    result match {
      case Right(PublishResult.Inserted(_)) => // expected
      case other => fail(s"expected Right(Inserted) — NotFound must NOT surface as Left, got $other")
    }
  }
}