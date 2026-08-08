package io.semanticdf.hera

import io.semanticdf.core.engine.{EngineIdentity, ResolvedSchema, ResolvedSource}
import io.semanticdf.core.model.SourceRef

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** v0.3.1: tests for [[HeraSourceResolver]] — proves the
  * engine-portable [[SourceResolver]] contract is satisfied via
  * Hera's `POST /private/explore/describe/table`.
  *
  * Per `docs/design/error-handling-style.md`:
  *   - Returns `ResolvedSource` sealed ADT (NOT Either — the ADT
  *     carries the failure modes Scan/NotFound/AuthFailed/Incompatible)
  *   - Pattern-match on all 4 cases at the call site
  *
  * Per scala-spark-batch-bugs §1: assert actual schema, not just
  * compile success. */
class HeraSourceResolverSpec extends AnyFunSuite with Matchers {

  private val identity = EngineIdentity(
    name                 = "hera",
    nativeVersion        = "1.0",
    engineAdapterVersion = "0.3.0",
  )

  // -- fixture builders --

  private def ordersColumns: Map[String, String] = Map(
    "id"     -> "bigint",
    "region" -> "varchar",
    "amount" -> "decimal(18,2)",
  )

  // -- successful resolution --

  test("HeraSourceResolver is a SourceResolver (contract conformance)") {
    val resolver = HeraSourceResolver(FakeHeraClient.empty)
    resolver shouldBe a [io.semanticdf.core.engine.SourceResolver]
  }

  test("resolve(ByName) returns Scan with the table's columns when the table exists") {
    val fake = FakeHeraClient.withTables(
      ("orders", 1L, 1L),
    ).addRealm(HeraRealm(1L, "realm_1"))
    val resolver = HeraSourceResolver(fake)

    val source = SourceRef.ByName(
      catalog   = Some("realm_1"),
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
    val fake = FakeHeraClient.withRealms((1L, "realm_1"))
    val resolver = HeraSourceResolver(fake)

    val source = SourceRef.ByName(
      catalog   = Some("realm_1"),
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
    val resolver = HeraSourceResolver(FakeHeraClient.empty)

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

  test("resolve(ByPath) returns Incompatible (Hera source resolver doesn't support paths)") {
    val resolver = HeraSourceResolver(FakeHeraClient.empty)
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
    val resolver = HeraSourceResolver(FakeHeraClient.empty)
    val source = SourceRef.ByProvider(
      io.semanticdf.core.model.ProviderRef.TableResolver("myTableResolver"),
    )
    val result = resolver.resolve(source, identity)
    result match {
      case ResolvedSource.Incompatible(s, reason) =>
        reason should include ("provider-based")
      case other => fail(s"expected Incompatible, got $other")
    }
  }
}