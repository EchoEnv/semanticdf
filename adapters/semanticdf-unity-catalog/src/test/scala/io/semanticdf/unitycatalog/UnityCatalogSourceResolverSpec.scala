package io.semanticdf.unitycatalog

import io.semanticdf.core.engine.{EngineIdentity, ResolvedSource, SourceResolver}
import io.semanticdf.core.model.SourceRef
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Tests for [[UnityCatalogSourceResolver]] — the FIRST catalog
  * adapter in the multi-engine design.
  *
  * Per the multi-engine design §4.6, this proves the
  * layer-separation principle: a `SourceResolver` impl can
  * produce portable `ResolvedSource` results from Unity Catalog
  * metadata, regardless of which engine consumes them.
  *
  * ==Why no Spark / no cluster roundtrip==
  *
  * The resolver is pure: it walks the `SourceRef` shape and
  * delegates to a `UnityCatalogClient`. Tests inject a
  * [[FakeUnityCatalogClient]] — no real UC cluster, no JSON
  * parsing, no HTTP. The integration test
  * ([[UnityCatalogIntegrationSpec]]) covers the real cluster
  * path; this spec covers the resolver's contract. */
class UnityCatalogSourceResolverSpec extends AnyFunSuite with Matchers {

  private val identity = EngineIdentity(
    name                 = "trino",
    nativeVersion        = "0.286",
    engineAdapterVersion = "0.3.0",
  )

  // -- fixture builders --

  private def ordersColumns: List[UcColumn] = List(
    UcColumn(name = "id",     dataType = "LONG",    nullable = false),
    UcColumn(name = "region", dataType = "STRING",  nullable = true),
    UcColumn(name = "amount", dataType = "DECIMAL", nullable = true),
  )

  // -- instance shape --

  test("UnityCatalogSourceResolver is a SourceResolver (contract conformance)") {
    val fake = FakeUnityCatalogClient.empty
    val resolver = UnityCatalogSourceResolver(fake, identity)
    resolver shouldBe a [SourceResolver]
  }

  // -- successful resolution --

  test("resolve(ByName) returns Scan with the table's columns when the table exists") {
    val fake = FakeUnityCatalogClient(
      ("unity", "public", "orders", ordersColumns),
    )
    val resolver = UnityCatalogSourceResolver(fake, identity)

    val source = SourceRef.ByName(catalog = Some("unity"), namespace = Some("public"), table = "orders")
    val result = resolver.resolve(source, identity)
    result shouldBe a [ResolvedSource.Scan]

    val scan = result.asInstanceOf[ResolvedSource.Scan]
    scan.source shouldBe source
    scan.schema.fields.keySet shouldBe Set("id", "region", "amount")
    scan.schema.fields("id") shouldBe "LONG"
    scan.schema.fields("region") shouldBe "STRING"
    scan.schema.fields("amount") shouldBe "DECIMAL"
  }

  test("resolve(ByName) records the (catalog, schema, table) tuple in the fake's `called` set") {
    val fake = FakeUnityCatalogClient(
      ("unity", "public", "orders", ordersColumns),
    )
    val resolver = UnityCatalogSourceResolver(fake, identity)
    val source = SourceRef.ByName(catalog = Some("unity"), namespace = Some("public"), table = "orders")

    resolver.resolve(source, identity)

    fake.called shouldBe Set(("unity", "public", "orders"))
  }

  // -- not-found --

  test("resolve(ByName) returns NotFound when the table doesn't exist in the fake") {
    val fake = FakeUnityCatalogClient.empty
    val resolver = UnityCatalogSourceResolver(fake, identity)

    val source = SourceRef.ByName(catalog = Some("unity"), namespace = Some("public"), table = "missing")
    val result = resolver.resolve(source, identity)
    result shouldBe a [ResolvedSource.NotFound]
  }

  // -- defaults for catalog / namespace --

  test("resolve(ByName) defaults catalog to engine's native catalog when None") {
    val fake = FakeUnityCatalogClient(
      ("trino", "public", "orders", ordersColumns),
    )
    val resolver = UnityCatalogSourceResolver(fake, identity)

    val source = SourceRef.ByName(catalog = None, namespace = Some("public"), table = "orders")
    val result = resolver.resolve(source, identity)
    result shouldBe a [ResolvedSource.Scan]
    fake.called should contain (("trino", "public", "orders"))
  }

  test("resolve(ByName) defaults namespace to 'public' when None") {
    val fake = FakeUnityCatalogClient(
      ("unity", "public", "orders", ordersColumns),
    )
    val resolver = UnityCatalogSourceResolver(fake, identity)

    val source = SourceRef.ByName(catalog = Some("unity"), namespace = None, table = "orders")
    resolver.resolve(source, identity) shouldBe a [ResolvedSource.Scan]
    fake.called should contain (("unity", "public", "orders"))
  }

  // -- incompatible shapes --

  test("resolve(ByPath) returns Incompatible (UC doesn't support portable path-based sources)") {
    val fake = FakeUnityCatalogClient.empty
    val resolver = UnityCatalogSourceResolver(fake, identity)

    // Build a ByPath via the constructor — the field shape varies
    // by core version; this test just exercises the path.
    val byPath = SourceRef.ByPath(
      format = "parquet",
      path   = "/tmp/orders",
    )
    val result = resolver.resolve(byPath, identity)
    result shouldBe a [ResolvedSource.Incompatible]
  }

  test("resolve(ByProvider) returns Incompatible (ProviderRef is Spark-specific)") {
    val fake = FakeUnityCatalogClient.empty
    val resolver = UnityCatalogSourceResolver(fake, identity)

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

  test("resolve(ByName) preserves the column order from the fake (matches UC's response order)") {
    val cols = List(
      UcColumn("a", "LONG",   false),
      UcColumn("b", "STRING", true),
      UcColumn("c", "DECIMAL", true),
    )
    val fake = FakeUnityCatalogClient(("unity", "public", "t", cols))
    val resolver = UnityCatalogSourceResolver(fake, identity)

    val source = SourceRef.ByName(Some("unity"), Some("public"), "t")
    val scan   = resolver.resolve(source, identity).asInstanceOf[ResolvedSource.Scan]
    scan.schema.fields.keys.toList shouldBe List("a", "b", "c")
  }
}