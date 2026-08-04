package io.semanticdf.core.schema

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.semanticdf.core.model.SourceRef

/** Phase 2 contract: prove `ResolvedScan` is a usable, Spark-free
  * data record (source + fields + projection). Per scala-data-driven-
  * refactor, this is pure data: the resolved scan is the engine-
  * portable contract; the engine-specific source resolver is the
  * behavior that produces it.
  */
class ResolvedScanSpec extends AnyFunSuite with Matchers {

  // -- constructor --

  test("ResolvedScan carries source, fields, projection") {
    val source = SourceRef.ByName(Some("hive"), Some("sales"), "orders")
    val fields = Seq(
      Field.nonNull("id", SealedDataType.BigInt),
      Field.nullable("amount", SealedDataType.Decimal(10, 2)),
    )
    val scan = ResolvedScan(source, fields, projection = Seq("id"))
    scan.source shouldBe source
    scan.fields shouldBe fields
    scan.projection shouldBe Seq("id")
  }

  test("ResolvedScan defaults projection to empty Seq (no projection = all columns)") {
    val source = SourceRef.ByName(None, None, "orders")
    val scan = ResolvedScan(source, Seq(Field.nonNull("id", SealedDataType.BigInt)))
    scan.projection shouldBe Seq.empty
  }

  // -- sugar factory --

  test("ResolvedScan.full creates a scan with no projection") {
    val source = SourceRef.ByName(None, None, "orders")
    val fields = Seq(Field.nonNull("id", SealedDataType.BigInt))
    val scan = ResolvedScan.full(source, fields)
    scan.projection shouldBe Seq.empty
    scan.fields shouldBe fields
    scan.source shouldBe source
  }

  // -- ByPath and ByProvider source variants --

  test("ResolvedScan with ByPath source carries path and format") {
    val source = SourceRef.ByPath("parquet", "s3://bucket/data/", Map("compression" -> "snappy"))
    val scan = ResolvedScan.full(source, Seq(Field.nonNull("id", SealedDataType.BigInt)))
    scan.source shouldBe source
  }

  test("ResolvedScan with ByProvider source carries the ProviderRef") {
    val provider = io.semanticdf.core.model.ProviderRef.DataFrameSource("orders_2024")
    val source = SourceRef.ByProvider(provider)
    val scan = ResolvedScan.full(source, Seq(Field.nonNull("id", SealedDataType.BigInt)))
    scan.source shouldBe source
  }

  // -- equality --

  test("ResolvedScan equality: same data => equal") {
    val source = SourceRef.ByName(None, None, "orders")
    val fields = Seq(Field.nonNull("id", SealedDataType.BigInt))
    ResolvedScan(source, fields, projection = Seq.empty) shouldBe
      ResolvedScan(source, fields, projection = Seq.empty)
  }

  test("ResolvedScan with different projection => not equal") {
    val source = SourceRef.ByName(None, None, "orders")
    val fields = Seq(Field.nonNull("id", SealedDataType.BigInt))
    ResolvedScan(source, fields, projection = Seq.empty) should not be
      ResolvedScan(source, fields, projection = Seq("id"))
  }

  test("ResolvedScan with different source => not equal") {
    val fields = Seq(Field.nonNull("id", SealedDataType.BigInt))
    ResolvedScan(SourceRef.ByName(None, None, "orders"), fields) should not be
      ResolvedScan(SourceRef.ByName(None, None, "users"), fields)
  }

  // -- field composition (the typical use case) --

  test("ResolvedScan with a Row-typed field") {
    val source = SourceRef.ByName(Some("hive"), Some("sales"), "orders")
    val scan = ResolvedScan(
      source,
      fields = Seq(
        Field.nonNull("id", SealedDataType.BigInt),
        Field.nullable(
          "address",
          SealedDataType.Row(Seq(
            Field.nonNull("street", SealedDataType.Varchar),
            Field.nullable("zip", SealedDataType.Varchar),
          )),
        ),
      ),
      projection = Seq("id", "address"),
    )
    scan.fields.size shouldBe 2
    scan.projection shouldBe Seq("id", "address")
  }

  // -- Serializable round-trip --

  test("ResolvedScan round-trips through Java serialization") {
    val source = SourceRef.ByName(Some("hive"), Some("sales"), "orders")
    val fields = Seq(
      Field.nonNull("id", SealedDataType.BigInt),
      Field.nullable("amount", SealedDataType.Decimal(10, 2)),
    )
    val scan = ResolvedScan(source, fields, projection = Seq("id", "amount"))
    val bos = new java.io.ByteArrayOutputStream()
    val oos = new java.io.ObjectOutputStream(bos)
    oos.writeObject(scan)
    oos.close()
    val bis = new java.io.ByteArrayInputStream(bos.toByteArray)
    val ois = new java.io.ObjectInputStream(bis)
    val restored = ois.readObject().asInstanceOf[ResolvedScan]
    restored shouldBe scan
  }
}