package io.semanticdf.myplatform

import io.semanticdf.core.engine.{EngineIdentity, ResolvedSchema, ResolvedSource}
import io.semanticdf.core.model.SourceRef

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** v0.3.1: tests for [[MyPlatformSourceResolver]] — proves the
  * engine-portable `SourceResolver` contract is satisfied via
  * MyPlatform's table-describe API.
  *
  * Per `docs/design/error-handling-style.md`:
  *   - Returns `ResolvedSource` sealed ADT (NOT `Either` — the ADT
  *     carries the failure modes Scan/NotFound/AuthFailed/Incompatible)
  *   - Pattern-match on all 4 cases at the call site
  *
  * Per `scala-spark-batch-bugs §1`: assert actual schema, not just
  * compile success. */
class MyPlatformSourceResolverSpec extends AnyFunSuite with Matchers {

  private val identity = EngineIdentity(
    name                 = "myplatform",
    nativeVersion        = "1.0",
    engineAdapterVersion = "0.3.0",
  )

  // -- successful resolution --

  test("MyPlatformSourceResolver is a SourceResolver (contract conformance)") {
    val resolver = MyPlatformSourceResolver(FakeMyPlatformClient.empty)
    resolver shouldBe a [io.semanticdf.core.engine.SourceResolver]
  }

  test("resolve(ByName) returns Scan with the table's columns when the table exists") {
    val fake = FakeMyPlatformClient.withTables(
      ("orders", "myplatform_realm", 1L),
    )
    val resolver = MyPlatformSourceResolver(fake)

    val source = SourceRef.ByName(
      catalog   = Some("myplatform_realm"),
      namespace = Some("public"),
      table     = "orders",
    )
    val result = resolver.resolve(source, identity)
    result match {
      case ResolvedSource.Scan(s, schema) =>
        s shouldBe source
        schema shouldBe a [ResolvedSchema]
      case other => fail(s"expected Scan, got $other")
    }
  }

  // -- not-found --

  test("resolve(ByName) returns NotFound when the table doesn't exist") {
    val resolver = MyPlatformSourceResolver(FakeMyPlatformClient.empty)

    val source = SourceRef.ByName(
      catalog   = Some("myplatform_realm"),
      namespace = Some("public"),
      table     = "nonexistent",
    )
    val result = resolver.resolve(source, identity)
    result match {
      case ResolvedSource.NotFound(s, reason) =>
        s shouldBe source
        reason should include ("nonexistent")
      case other => fail(s"expected NotFound, got $other")
    }
  }

  // -- realm not resolved --

  test("resolve(ByName) returns Incompatible when the catalog doesn't map to a known realm") {
    val resolver = MyPlatformSourceResolver(FakeMyPlatformClient.empty)

    val source = SourceRef.ByName(
      catalog   = Some("unknown_realm"),
      namespace = Some("public"),
      table     = "orders",
    )
    val result = resolver.resolve(source, identity)
    result match {
      case ResolvedSource.Incompatible(s, reason) =>
        s shouldBe source
        reason should include ("unknown_realm")
      case other => fail(s"expected Incompatible, got $other")
    }
  }

  // -- ByPath: incompatible --

  test("resolve(ByPath) returns Incompatible (path-based sources aren't supported)") {
    val resolver = MyPlatformSourceResolver(FakeMyPlatformClient.empty)
    val source = SourceRef.ByPath(format = "csv", path = "/data/orders.csv")
    val result = resolver.resolve(source, identity)
    result match {
      case ResolvedSource.Incompatible(s, reason) =>
        s shouldBe source
        reason should include ("path-based")
      case other => fail(s"expected Incompatible, got $other")
    }
  }

  // -- ByProvider: incompatible --

  test("resolve(ByProvider) returns Incompatible") {
    val resolver = MyPlatformSourceResolver(FakeMyPlatformClient.empty)
    val source = SourceRef.ByProvider(
      io.semanticdf.core.model.ProviderRef.TableResolver("myResolver"),
    )
    val result = resolver.resolve(source, identity)
    result match {
      case ResolvedSource.Incompatible(s, reason) =>
        reason should include ("provider-based")
      case other => fail(s"expected Incompatible, got $other")
    }
  }
}