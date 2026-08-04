package io.semanticdf.trino

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.semanticdf.core.engine.{EngineIdentity, ResolvedSource}
import io.semanticdf.core.model.SourceRef
import io.semanticdf.core.schema.SealedDataType

/** Phase 2 contract: prove `TrinoSourceResolver` correctly resolves
  * portable `SourceRef`s using a `TrinoClient` (fake in tests, real
  * in production). The first real engine adapter implementation
  * — concrete behavior, not a placeholder.
  *
  * Per scala-data-driven-refactor §1: the BEHAVIOR (calling Trino,
  * mapping responses to portable types) lives here; the CONTRACT
  * (`SourceResolver`) lives in core.
  */
class TrinoSourceResolverSpec extends AnyFunSuite with Matchers {

  private val trinoIdentity = EngineIdentity(
    name                 = "trino",
    nativeVersion        = "0.286",
    engineAdapterVersion = "0.2.4",
  )

  // -- helpers --

  /** Build a `TrinoSourceResolver` with the given fake client. */
  private def resolverWith(fake: FakeTrinoClient): TrinoSourceResolver =
    new TrinoSourceResolver(fake, trinoIdentity)

  // -- ByName: success path --

  test("resolve(ByName with all fields) returns ResolvedSource.Scan") {
    val schema = TrinoTableSchema.of(List(
      "id"    -> SealedDataType.BigInt,
      "name"  -> SealedDataType.Varchar,
      "price" -> SealedDataType.Double,
    ))
    val fake = FakeTrinoClient.withDescribe(
      catalog = "hive",
      schema  = "silver",
      table   = "orders",
      columns = schema,
    )
    val resolver = resolverWith(fake)
    val source = SourceRef.ByName(
      catalog   = Some("hive"),
      namespace = Some("silver"),
      table     = "orders",
    )
    val result = resolver.resolve(source, trinoIdentity)
    result shouldBe a [ResolvedSource.Scan]
  }

  test("resolve(ByName with all fields) carries the source through") {
    val schema = TrinoTableSchema.of(List("id" -> SealedDataType.BigInt))
    val fake = FakeTrinoClient.withDescribe(
      "hive", "silver", "orders", schema,
    )
    val resolver = resolverWith(fake)
    val source = SourceRef.ByName(Some("hive"), Some("silver"), "orders")
    val result = resolver.resolve(source, trinoIdentity).asInstanceOf[ResolvedSource.Scan]
    result.source shouldBe source
  }

  test("resolve(ByName) maps SealedDataType to Trino type names in ResolvedSchema") {
    val schema = TrinoTableSchema.of(List(
      "id"   -> SealedDataType.BigInt,
      "name" -> SealedDataType.Varchar,
    ))
    val fake = FakeTrinoClient.withDescribe("hive", "silver", "orders", schema)
    val resolver = resolverWith(fake)
    val source = SourceRef.ByName(Some("hive"), Some("silver"), "orders")
    val result = resolver.resolve(source, trinoIdentity).asInstanceOf[ResolvedSource.Scan]
    result.schema.fields("id") shouldBe "bigint"
    result.schema.fields("name") shouldBe "varchar"
  }

  test("resolve(ByName) defaults catalog to engine identity name when missing") {
    val schema = TrinoTableSchema.of(List("id" -> SealedDataType.BigInt))
    // No catalog specified — should default to "trino"
    val fake = FakeTrinoClient.withDescribe("trino", "public", "orders", schema)
    val resolver = resolverWith(fake)
    val source = SourceRef.ByName(catalog = None, namespace = Some("public"), table = "orders")
    resolver.resolve(source, trinoIdentity) shouldBe a [ResolvedSource.Scan]
  }

  test("resolve(ByName) defaults namespace to 'public' when missing") {
    val schema = TrinoTableSchema.of(List("id" -> SealedDataType.BigInt))
    val fake = FakeTrinoClient.withDescribe("hive", "public", "orders", schema)
    val resolver = resolverWith(fake)
    val source = SourceRef.ByName(catalog = Some("hive"), namespace = None, table = "orders")
    resolver.resolve(source, trinoIdentity) shouldBe a [ResolvedSource.Scan]
  }

  // -- ByName: failure path --

  test("resolve(ByName with unknown table) returns ResolvedSource.NotFound") {
    val fake = FakeTrinoClient()  // no canned responses
    val resolver = resolverWith(fake)
    val source = SourceRef.ByName(Some("hive"), Some("silver"), "ghost_table")
    val result = resolver.resolve(source, trinoIdentity)
    result shouldBe a [ResolvedSource.NotFound]
    result match {
      case ResolvedSource.NotFound(_, reason) =>
        reason should include ("ghost_table")
      case _ => fail("expected NotFound")
    }
  }

  // -- ByPath: rejected --

  test("resolve(ByPath) returns ResolvedSource.Incompatible (not supported on Trino)") {
    val fake = FakeTrinoClient()
    val resolver = resolverWith(fake)
    val source = SourceRef.ByPath(
      format  = "parquet",
      path    = "/data/orders",
      options = Map.empty,
    )
    val result = resolver.resolve(source, trinoIdentity)
    result shouldBe a [ResolvedSource.Incompatible]
    result match {
      case ResolvedSource.Incompatible(_, reason) =>
        reason should include ("path-based")
      case _ => fail("expected Incompatible")
    }
  }

  // -- ByProvider: rejected --

  test("resolve(ByProvider) returns ResolvedSource.Incompatible (ProviderRef is Spark-specific)") {
    val fake = FakeTrinoClient()
    val resolver = resolverWith(fake)
    val provider = io.semanticdf.core.model.ProviderRef.DataFrameSource(
      name       = "myDataFrame",
      schemaHint = None,
    )
    val source = SourceRef.ByProvider(provider)
    val result = resolver.resolve(source, trinoIdentity)
    result shouldBe a [ResolvedSource.Incompatible]
    result match {
      case ResolvedSource.Incompatible(_, reason) =>
        reason should include ("ProviderRef")
      case _ => fail("expected Incompatible")
    }
  }

  // -- boundary contract --

  test("TrinoSourceResolver is a SourceResolver (contract conformance)") {
    val resolver = new TrinoSourceResolver(FakeTrinoClient(), trinoIdentity)
    resolver shouldBe a [io.semanticdf.core.engine.SourceResolver]
  }
}