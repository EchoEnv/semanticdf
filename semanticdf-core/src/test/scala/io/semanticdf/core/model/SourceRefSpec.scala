package io.semanticdf.core.model

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Phase 2 contract: prove `SourceRef` is a usable, Spark-free
  * data record + the closed 3-variant enumeration. Pure data —
  * no behavior, no engine coupling. Per scala-data-driven-refactor,
  * the source IDENTITY lives in core; the source RESOLUTION lives
  * in the engine adapter layer.
  */
class SourceRefSpec extends AnyFunSuite with Matchers {

  // -- SourceRef.ByName --

  test("ByName carries catalog, namespace, table") {
    val s = SourceRef.ByName(Some("hive"), Some("sales"), "orders")
    s.catalog shouldBe Some("hive")
    s.namespace shouldBe Some("sales")
    s.table shouldBe "orders"
  }

  test("ByName with None catalog/namespace uses engine defaults") {
    val s = SourceRef.ByName(None, None, "orders")
    s.catalog shouldBe None
    s.namespace shouldBe None
    s.table shouldBe "orders"
  }

  test("ByName with only table is valid (engine-default catalog+namespace)") {
    val s = SourceRef.ByName(None, None, "raw_events")
    s.table shouldBe "raw_events"
  }

  // -- SourceRef.ByPath --

  test("ByPath carries format, path, and options") {
    val s = SourceRef.ByPath(
      format  = "parquet",
      path    = "s3://my-bucket/data/orders/",
      options = Map("compression" -> "snappy"),
    )
    s.format shouldBe "parquet"
    s.path shouldBe "s3://my-bucket/data/orders/"
    s.options shouldBe Map("compression" -> "snappy")
  }

  test("ByPath defaults options to empty Map when not provided") {
    val s = SourceRef.ByPath(format = "csv", path = "/data/events.csv")
    s.options shouldBe Map.empty
  }

  test("ByPath supports various storage schemes") {
    SourceRef.ByPath("csv", "file:///tmp/data.csv").path shouldBe "file:///tmp/data.csv"
    SourceRef.ByPath("parquet", "s3://bucket/key").path shouldBe "s3://bucket/key"
    SourceRef.ByPath("orc", "gs://bucket/key").path shouldBe "gs://bucket/key"
    SourceRef.ByPath("avro", "abfs://container/path").path shouldBe "abfs://container/path"
  }

  // -- SourceRef.ByProvider --

  test("ByProvider carries a ProviderRef") {
    val provider = ProviderRef.DataFrameSource("orders_2024")
    val s = SourceRef.ByProvider(provider)
    s.provider shouldBe provider
  }

  test("ByProvider equality: same provider => equal") {
    val provider = ProviderRef.DataFrameSource("orders")
    SourceRef.ByProvider(provider) shouldBe SourceRef.ByProvider(provider)
  }

  // -- closed enumeration + sealed exhaustiveness --

  test("SourceRef has exactly 3 cases: ByName, ByPath, ByProvider") {
    val all: Set[SourceRef] = Set(
      SourceRef.ByName(None, None, "x"),
      SourceRef.ByPath("csv", "/x"),
      SourceRef.ByProvider(ProviderRef.DataFrameSource("x")),
    )
    all.size shouldBe 3
  }

  test("Sealed exhaustiveness: pattern-match over all 3 cases") {
    val examples: Seq[SourceRef] = Seq(
      SourceRef.ByName(None, None, "t"),
      SourceRef.ByPath("csv", "/p"),
      SourceRef.ByProvider(ProviderRef.DataFrameSource("n")),
    )
    examples.foreach {
      case SourceRef.ByName(_, _, _)      => ()
      case SourceRef.ByPath(_, _, _)      => ()
      case SourceRef.ByProvider(_)        => ()
    }
  }

  // -- equality + Product with Serializable --

  test("SourceRef equality: same data => equal") {
    SourceRef.ByName(None, None, "t") shouldBe SourceRef.ByName(None, None, "t")
    SourceRef.ByPath("c", "p") shouldBe SourceRef.ByPath("c", "p")
    SourceRef.ByProvider(ProviderRef.DataFrameSource("n")) shouldBe
      SourceRef.ByProvider(ProviderRef.DataFrameSource("n"))
  }

  test("SourceRef different variants with same data fields are not equal") {
    val provider = ProviderRef.DataFrameSource("x")
    SourceRef.ByName(None, None, "x") should not be
      SourceRef.ByProvider(provider)
  }
}