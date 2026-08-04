package io.semanticdf.core.engine

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.semanticdf.core.model.{ProviderRef, SourceRef}

/** Phase 2 contract: prove `ResolvedSource` is a usable, Spark-free
  * data record + the closed 4-variant enumeration. Pure data —
  * no behavior, no engine coupling. Per scala-data-driven-refactor,
  * the resolution RESULT lives in core; the resolver that produces
  * it lives in the engine adapter layer.
  */
class ResolvedSourceSpec extends AnyFunSuite with Matchers {

  // -- ResolvedSource.Scan --

  test("Scan carries source and schema") {
    val source = SourceRef.ByName(None, None, "orders")
    val schema = ResolvedSchema(Map("id" -> "bigint", "amount" -> "double"))
    val scan = ResolvedSource.Scan(source, schema)
    scan.source shouldBe source
    scan.schema shouldBe schema
  }

  test("Scan with default empty schema") {
    val scan = ResolvedSource.Scan(SourceRef.ByName(None, None, "orders"), ResolvedSchema())
    scan.schema.fields shouldBe Map.empty
  }

  // -- ResolvedSource.Incompatible --

  test("Incompatible carries source and reason") {
    val source = SourceRef.ByPath("csv", "/data/file.csv")
    val incompat = ResolvedSource.Incompatible(source, "CSV not supported by Trino; use parquet instead")
    incompat.source shouldBe source
    incompat.reason shouldBe "CSV not supported by Trino; use parquet instead"
  }

  // -- ResolvedSource.AuthFailed --

  test("AuthFailed carries source and reason") {
    val source = SourceRef.ByName(Some("hive"), Some("sales"), "orders")
    val auth = ResolvedSource.AuthFailed(source, "Kerberos ticket expired")
    auth.source shouldBe source
    auth.reason shouldBe "Kerberos ticket expired"
  }

  // -- ResolvedSource.NotFound --

  test("NotFound carries source and reason") {
    val source = SourceRef.ByName(Some("hive"), None, "nonexistent_table")
    val nf = ResolvedSource.NotFound(source, "table 'nonexistent_table' not found in catalog 'hive'")
    nf.source shouldBe source
    nf.reason shouldBe "table 'nonexistent_table' not found in catalog 'hive'"
  }

  // -- closed enumeration + sealed exhaustiveness --

  test("ResolvedSource has exactly 4 cases: Scan, Incompatible, AuthFailed, NotFound") {
    val source = SourceRef.ByName(None, None, "orders")
    val all: Set[ResolvedSource] = Set(
      ResolvedSource.Scan(source, ResolvedSchema()),
      ResolvedSource.Incompatible(source, "x"),
      ResolvedSource.AuthFailed(source, "y"),
      ResolvedSource.NotFound(source, "z"),
    )
    all.size shouldBe 4
  }

  test("Sealed exhaustiveness: pattern-match over all 4 cases") {
    val source = SourceRef.ByName(None, None, "orders")
    val examples: Seq[ResolvedSource] = Seq(
      ResolvedSource.Scan(source, ResolvedSchema()),
      ResolvedSource.Incompatible(source, "x"),
      ResolvedSource.AuthFailed(source, "y"),
      ResolvedSource.NotFound(source, "z"),
    )
    examples.foreach {
      case ResolvedSource.Scan(_, _)        => ()
      case ResolvedSource.Incompatible(_, _) => ()
      case ResolvedSource.AuthFailed(_, _)   => ()
      case ResolvedSource.NotFound(_, _)     => ()
    }
  }

  // -- equality + Product with Serializable --

  test("ResolvedSource equality: same data => equal") {
    val source = SourceRef.ByName(None, None, "orders")
    val s1 = ResolvedSource.Scan(source, ResolvedSchema())
    val s2 = ResolvedSource.Scan(source, ResolvedSchema())
    s1 shouldBe s2

    ResolvedSource.Incompatible(source, "x") shouldBe ResolvedSource.Incompatible(source, "x")
    ResolvedSource.AuthFailed(source, "y") shouldBe ResolvedSource.AuthFailed(source, "y")
    ResolvedSource.NotFound(source, "z") shouldBe ResolvedSource.NotFound(source, "z")
  }

  test("ResolvedSource different cases with same data fields are not equal") {
    val source = SourceRef.ByName(None, None, "orders")
    ResolvedSource.Incompatible(source, "x") should not be
      ResolvedSource.AuthFailed(source, "x")
  }

  // -- ByProvider path: closure-bypass wiring --

  test("Scan from a ByProvider source preserves the ByProvider identity") {
    val provider = ProviderRef.DataFrameSource("orders_2024")
    val source = SourceRef.ByProvider(provider)
    val scan = ResolvedSource.Scan(source, ResolvedSchema(Map("id" -> "bigint")))
    scan.source shouldBe source
    scan.source shouldBe a [SourceRef.ByProvider]
  }

  test("ByPath source with format options is preserved in Scan") {
    val source = SourceRef.ByPath(
      format = "parquet",
      path = "s3://bucket/orders/",
      options = Map("compression" -> "snappy"),
    )
    val scan = ResolvedSource.Scan(source, ResolvedSchema())
    scan.source shouldBe source
  }
}