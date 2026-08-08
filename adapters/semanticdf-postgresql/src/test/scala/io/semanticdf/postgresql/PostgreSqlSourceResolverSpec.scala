package io.semanticdf.postgresql

import io.semanticdf.core.engine.{EngineIdentity, ResolvedSchema, ResolvedSource}
import io.semanticdf.core.model.SourceRef

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** v0.4.0: tests for [[PostgreSqlSourceResolver]] — proves the
  * engine-portable `SourceResolver` contract is satisfied via
  * PostgreSQL's `DatabaseMetaData.getColumns` (called via
  * [[PostgreSqlClient.describeTable]]).
  *
  * Per `docs/design/error-handling-style.md`:
  *   - Returns `ResolvedSource` sealed ADT (NOT `Either` — the ADT
  *     carries the failure modes Scan/NotFound/AuthFailed/Incompatible)
  *   - Pattern-match on all 4 cases at the call site
  *
  * Per `scala-spark-batch-bugs §1`: assert actual schema, not just
  * compile success. */
class PostgreSqlSourceResolverSpec extends AnyFunSuite with Matchers {

  private val identity = EngineIdentity(
    name                 = "postgresql",
    nativeVersion        = "16",
    engineAdapterVersion = "0.4.0",
  )

  test("PostgreSqlSourceResolver is a SourceResolver (contract conformance)") {
    val resolver = PostgreSqlSourceResolver(FakePostgreSqlClient.empty, "mydb")
    resolver shouldBe a [io.semanticdf.core.engine.SourceResolver]
  }

  test("resolve(ByName) returns Scan with the table's columns when the table exists") {
    val fake = new FakePostgreSqlClient(
      initialTables = Map(
        "public.orders" -> Map("id" -> "integer", "region" -> "varchar", "amount" -> "numeric"),
      ),
    )
    val resolver = PostgreSqlSourceResolver(fake, "mydb")

    val source = SourceRef.ByName(
      catalog   = Some("mydb"),
      namespace = Some("public"),
      table     = "orders",
    )
    val result = resolver.resolve(source, identity)
    result match {
      case ResolvedSource.Scan(s, schema) =>
        s shouldBe source
        schema shouldBe a [ResolvedSchema]
        schema.fields.keys.toSet should contain ("id")
      case other => fail(s"expected Scan, got $other")
    }
  }

  test("resolve(ByName) returns NotFound when the table doesn't exist") {
    val resolver = PostgreSqlSourceResolver(FakePostgreSqlClient.empty, "mydb")
    val source = SourceRef.ByName(
      catalog = Some("mydb"), namespace = Some("public"), table = "nonexistent",
    )
    val result = resolver.resolve(source, identity)
    result match {
      case ResolvedSource.NotFound(s, reason) =>
        s shouldBe source
        reason should include ("nonexistent")
      case other => fail(s"expected NotFound, got $other")
    }
  }

  test("resolve(ByPath) returns Incompatible (PG has no path-based sources)") {
    val resolver = PostgreSqlSourceResolver(FakePostgreSqlClient.empty, "mydb")
    val source = SourceRef.ByPath(format = "csv", path = "/data/orders.csv")
    val result = resolver.resolve(source, identity)
    result match {
      case ResolvedSource.Incompatible(s, reason) =>
        reason should include ("path-based")
      case other => fail(s"expected Incompatible, got $other")
    }
  }
}
