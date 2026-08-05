package io.semanticdf.core.schema

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SchemaSummarySpec extends AnyFunSuite with Matchers {

  // -- SchemaFieldKind --

  test("SchemaFieldKind is a sealed trait (closed set of cases)") {
    // Compile-time guarantee: the cases are Dimension, Measure,
    // CalculatedMeasure, JoinKey. Adding a new case is a
    // deliberate API change.
    SchemaFieldKind.Dimension.toString shouldBe "Dimension"
    SchemaFieldKind.Measure.toString shouldBe "Measure"
    SchemaFieldKind.CalculatedMeasure.toString shouldBe "CalculatedMeasure"
    SchemaFieldKind.JoinKey.toString shouldBe "JoinKey"
  }

  // -- SchemaField --

  test("SchemaField has the four expected fields with the expected defaults") {
    val f = SchemaField(fieldName = "region", fieldKind = SchemaFieldKind.Dimension)
    f.fieldName shouldBe "region"
    f.fieldKind shouldBe SchemaFieldKind.Dimension
    f.description shouldBe None
    f.dataType shouldBe None
  }

  test("SchemaField preserves description and dataType when set") {
    val f = SchemaField(
      fieldName   = "amount",
      fieldKind   = SchemaFieldKind.Measure,
      description = Some("Total amount in USD"),
      dataType    = Some(SealedDataType.Decimal(precision = 18, scale = 2)),
    )
    f.description shouldBe Some("Total amount in USD")
    f.dataType shouldBe Some(SealedDataType.Decimal(18, 2))
  }

  // -- SchemaSummary --

  test("SchemaSummary.rowCount returns the number of fields") {
    val s = SchemaSummary(
      modelName        = "orders",
      modelDescription = None,
      fields           = List(
        SchemaField("region",  SchemaFieldKind.Dimension),
        SchemaField("amount",  SchemaFieldKind.Measure),
        SchemaField("margin",  SchemaFieldKind.CalculatedMeasure),
      ),
    )
    s.rowCount shouldBe 3
  }

  test("SchemaSummary.isEmpty is true when fields is empty") {
    val s = SchemaSummary("empty", None, Nil)
    s.isEmpty shouldBe true
    s.rowCount shouldBe 0
  }

  test("SchemaSummary.isEmpty is false when at least one field is present") {
    val s = SchemaSummary(
      modelName = "non_empty",
      modelDescription = None,
      fields    = List(SchemaField("x", SchemaFieldKind.Dimension)),
    )
    s.isEmpty shouldBe false
  }

  test("SchemaSummary.ofKind filters to fields of the requested kind") {
    val s = SchemaSummary(
      modelName = "orders",
      modelDescription = None,
      fields    = List(
        SchemaField("region", SchemaFieldKind.Dimension),
        SchemaField("amount", SchemaFieldKind.Measure),
        SchemaField("margin", SchemaFieldKind.CalculatedMeasure),
        SchemaField("id",     SchemaFieldKind.Dimension),
      ),
    )
    s.ofKind(SchemaFieldKind.Dimension).map(_.fieldName) shouldBe List("region", "id")
    s.ofKind(SchemaFieldKind.Measure).map(_.fieldName) shouldBe List("amount")
    s.ofKind(SchemaFieldKind.CalculatedMeasure).map(_.fieldName) shouldBe List("margin")
    s.ofKind(SchemaFieldKind.JoinKey) shouldBe Nil
  }

  // -- Serializable (wire-format round-trip) --

  test("SchemaSummary is Serializable (Product + Serializable mixin)") {
    // Compile-time guarantee: `extends Product with Serializable`.
    // Runtime guarantee: round-trip via Java serialization.
    val s = SchemaSummary(
      modelName        = "orders",
      modelDescription = Some("An orders model"),
      fields           = List(
        SchemaField("region", SchemaFieldKind.Dimension, Some("Region name"), Some(SealedDataType.Varchar)),
        SchemaField("amount", SchemaFieldKind.Measure,   None,                Some(SealedDataType.Decimal(18, 2))),
      ),
    )
    val out = new java.io.ByteArrayOutputStream()
    val oos = new java.io.ObjectOutputStream(out)
    oos.writeObject(s)
    oos.close()
    val ois  = new java.io.ObjectInputStream(new java.io.ByteArrayInputStream(out.toByteArray))
    val back = ois.readObject().asInstanceOf[SchemaSummary]
    back shouldBe s
    back.rowCount shouldBe 2
    back.ofKind(SchemaFieldKind.Dimension).map(_.fieldName) shouldBe List("region")
  }

  test("SchemaField is Serializable (Product + Serializable mixin)") {
    val f = SchemaField("amount", SchemaFieldKind.Measure, Some("USD"), Some(SealedDataType.Decimal(18, 2)))
    val out = new java.io.ByteArrayOutputStream()
    val oos = new java.io.ObjectOutputStream(out)
    oos.writeObject(f)
    oos.close()
    val ois  = new java.io.ObjectInputStream(new java.io.ByteArrayInputStream(out.toByteArray))
    val back = ois.readObject().asInstanceOf[SchemaField]
    back shouldBe f
  }
}