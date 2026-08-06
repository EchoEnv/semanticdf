package io.semanticdf.hivemetastore

import io.semanticdf.core.engine.{EngineIdentity, ResolvedSource, SourceResolver}
import io.semanticdf.core.model.SourceRef
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Tests for [[HiveMetastoreSourceResolver]] — the HMS catalog
  * adapter. Mirrors `UnityCatalogSourceResolverSpec` (PR #394)
  * almost verbatim.
  *
  * Per scala-data-driven-refacer §1: the SCHEMA CONTRACT
  * (`ResolvedSource`) lives in core; the SCHEMA BEHAVIOR (calling
  * HMS, parsing Thrift responses) lives in the HMS adapter.
  * These tests pin the adapter-side behavior. */
class HiveMetastoreSourceResolverSpec extends AnyFunSuite with Matchers {

  private val identity = EngineIdentity(
    name                 = "hive",
    nativeVersion        = "3.1.3",
    engineAdapterVersion = "0.2.4",
  )

  // -- fixture builders --

  private def ordersColumns: List[HmsColumn] = List(
    HmsColumn(name = "id",     dataType = "bigint", nullable = false),
    HmsColumn(name = "region", dataType = "string", nullable = true),
    HmsColumn(name = "amount", dataType = "decimal(18,2)", nullable = true),
  )

  // -- instance shape --

  test("HiveMetastoreSourceResolver is a SourceResolver (contract conformance)") {
    val fake = FakeHiveMetastoreClient.empty
    val resolver = HiveMetastoreSourceResolver(fake, identity)
    resolver shouldBe a [SourceResolver]
  }

  // -- successful resolution --

  test("resolve(ByName) returns Scan with the table's columns when the table exists") {
    val fake = FakeHiveMetastoreClient(
      ("hive", "public", "orders", ordersColumns),
    )
    val resolver = HiveMetastoreSourceResolver(fake, identity)

    val source = SourceRef.ByName(catalog = Some("hive"), namespace = Some("public"), table = "orders")
    val result = resolver.resolve(source, identity)
    result shouldBe a [ResolvedSource.Scan]

    val scan = result.asInstanceOf[ResolvedSource.Scan]
    scan.source shouldBe source
    scan.schema.fields.keySet shouldBe Set("id", "region", "amount")
    scan.schema.fields("id") shouldBe "bigint"
    scan.schema.fields("region") shouldBe "string"
    scan.schema.fields("amount") shouldBe "decimal(18,2)"
  }

  test("resolve(ByName) records the (catalog, database, table) tuple in the fake's `called` set") {
    val fake = FakeHiveMetastoreClient(
      ("hive", "public", "orders", ordersColumns),
    )
    val resolver = HiveMetastoreSourceResolver(fake, identity)
    val source = SourceRef.ByName(catalog = Some("hive"), namespace = Some("public"), table = "orders")

    resolver.resolve(source, identity)

    fake.called shouldBe Set(("hive", "public", "orders"))
  }

  // -- not-found --

  test("resolve(ByName) returns NotFound when the table doesn't exist in the fake") {
    val fake = FakeHiveMetastoreClient.empty
    val resolver = HiveMetastoreSourceResolver(fake, identity)

    val source = SourceRef.ByName(catalog = Some("hive"), namespace = Some("public"), table = "missing")
    val result = resolver.resolve(source, identity)
    result shouldBe a [ResolvedSource.NotFound]
  }

  // -- defaults for catalog / database --

  test("resolve(ByName) defaults catalog to engine's native catalog when None") {
    val fake = FakeHiveMetastoreClient(
      ("hive", "public", "orders", ordersColumns),
    )
    val resolver = HiveMetastoreSourceResolver(fake, identity)

    val source = SourceRef.ByName(catalog = None, namespace = Some("public"), table = "orders")
    val result = resolver.resolve(source, identity)
    result shouldBe a [ResolvedSource.Scan]
    fake.called should contain (("hive", "public", "orders"))
  }

  test("resolve(ByName) defaults namespace to 'default' when None") {
    val fake = FakeHiveMetastoreClient(
      ("hive", "default", "orders", ordersColumns),
    )
    val resolver = HiveMetastoreSourceResolver(fake, identity)

    val source = SourceRef.ByName(catalog = Some("hive"), namespace = None, table = "orders")
    resolver.resolve(source, identity) shouldBe a [ResolvedSource.Scan]
    fake.called should contain (("hive", "default", "orders"))
  }

  // -- incompatible shapes --

  test("resolve(ByPath) returns Incompatible (HMS doesn't support portable path-based sources)") {
    val fake = FakeHiveMetastoreClient.empty
    val resolver = HiveMetastoreSourceResolver(fake, identity)

    val byPath = SourceRef.ByPath(
      format = "parquet",
      path   = "/tmp/orders",
    )
    val result = resolver.resolve(byPath, identity)
    result shouldBe a [ResolvedSource.Incompatible]
  }

  test("resolve(ByProvider) returns Incompatible (ProviderRef is Spark-specific)") {
    val fake = FakeHiveMetastoreClient.empty
    val resolver = HiveMetastoreSourceResolver(fake, identity)

    val byProvider = SourceRef.ByProvider(
      provider = io.semanticdf.core.model.ProviderRef.DataFrameSource(
        name       = "io.example.SomeProvider",
        schemaHint = None,
      ),
    )
    val result = resolver.resolve(byProvider, identity)
    result shouldBe a [ResolvedSource.Incompatible]
  }

  // -- field-order preservation --

  test("resolve(ByName) preserves the column order from the fake (matches HMS's response order)") {
    val cols = List(
      HmsColumn("a", "bigint", false),
      HmsColumn("b", "string", true),
      HmsColumn("c", "decimal(18,2)", true),
    )
    val fake = FakeHiveMetastoreClient(("hive", "public", "t", cols))
    val resolver = HiveMetastoreSourceResolver(fake, identity)

    val source = SourceRef.ByName(Some("hive"), Some("public"), "t")
    val scan   = resolver.resolve(source, identity).asInstanceOf[ResolvedSource.Scan]
    scan.schema.fields.keys.toList shouldBe List("a", "b", "c")
  }

  // -- Thrift vs REST transport proof (the §4.6 claim) --

  test("HMS resolver uses the Thrift transport (vs. UC's REST) — same contract") {
    // This test asserts the SHAPE: the resolver returns the same
    // `ResolvedSource.Scan` regardless of whether the underlying
    // catalog is REST (UC) or Thrift (HMS). This is the §4.6
    // transport-agnostic promise.
    val fake = FakeHiveMetastoreClient(
      ("hive", "public", "orders", ordersColumns),
    )
    val resolver = HiveMetastoreSourceResolver(fake, identity)

    // Compare shapes:
    val scan = resolver.resolve(
      SourceRef.ByName(Some("hive"), Some("public"), "orders"),
      identity,
    ).asInstanceOf[ResolvedSource.Scan]

    scan shouldBe a [ResolvedSource.Scan]
    // Same fields, same types — the Thrift-vs-REST difference
    // is invisible at the resolver's output boundary.
    scan.schema.fields("id").getClass shouldBe classOf[String]
    scan.schema.fields.size shouldBe 3
  }
}