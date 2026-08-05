package io.semanticdf.trino

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.semanticdf.core.expr.LiteralValue

/** Phase 2 contract: prove `TrinoResultDecoder` correctly translates
  * raw JDBC values to portable `LiteralValue`s.
  *
  * The decoder is a pure function — given raw rows, produce
  * translated rows. No state, no IO.
  *
  * Per scala-data-driven-refactor §1: pure data flow. Per
  * scala-data-driven-refactor §3: the input types are
  * exhaustively matched.
  */
class TrinoResultDecoderSpec extends AnyFunSuite with Matchers {

  // -- empty result set --

  test("decode(empty rows) returns TrinoResult with empty rows") {
    val result = TrinoResultDecoder.decode(
      columns = List("a", "b"),
      rawRows = Nil,
    )
    result.columns shouldBe List("a", "b")
    result.rows shouldBe Nil
    result.rowCount shouldBe 0
  }

  // -- primitive type mapping --

  test("decode maps null to LiteralValue.NullValue") {
    val result = TrinoResultDecoder.decode(
      columns = List("a"),
      rawRows = List(List(null)),
    )
    result.cell(0, 0) shouldBe Some(LiteralValue.NullValue)
  }

  test("decode maps String to LiteralValue.StringValue") {
    val result = TrinoResultDecoder.decode(
      columns = List("s"),
      rawRows = List(List("hello")),
    )
    result.cell(0, 0) shouldBe Some(LiteralValue.StringValue("hello"))
  }

  test("decode maps Boolean to LiteralValue.BoolValue") {
    val result = TrinoResultDecoder.decode(
      columns = List("b"),
      rawRows = List(List(true), List(false)),
    )
    result.cell(0, 0) shouldBe Some(LiteralValue.BoolValue(true))
    result.cell(1, 0) shouldBe Some(LiteralValue.BoolValue(false))
  }

  // -- numeric type mapping --

  test("decode maps Int to LiteralValue.IntValue") {
    val result = TrinoResultDecoder.decode(
      columns = List("n"),
      rawRows = List(List(42: Int)),
    )
    result.cell(0, 0) shouldBe Some(LiteralValue.IntValue(42))
  }

  test("decode maps Long to LiteralValue.LongValue") {
    val result = TrinoResultDecoder.decode(
      columns = List("n"),
      rawRows = List(List(1234567890123L)),
    )
    result.cell(0, 0) shouldBe Some(LiteralValue.LongValue(1234567890123L))
  }

  test("decode maps Double to LiteralValue.DoubleValue") {
    val result = TrinoResultDecoder.decode(
      columns = List("n"),
      rawRows = List(List(3.14: Double)),
    )
    result.cell(0, 0) shouldBe Some(LiteralValue.DoubleValue(3.14))
  }

  test("decode maps Float to LiteralValue.FloatValue") {
    val result = TrinoResultDecoder.decode(
      columns = List("n"),
      rawRows = List(List(1.5f: Float)),
    )
    result.cell(0, 0) shouldBe Some(LiteralValue.FloatValue(1.5f))
  }

  test("decode maps Short to LiteralValue.ShortValue") {
    val result = TrinoResultDecoder.decode(
      columns = List("n"),
      rawRows = List(List(7: Short)),
    )
    result.cell(0, 0) shouldBe Some(LiteralValue.ShortValue(7))
  }

  test("decode maps Byte to LiteralValue.ByteValue") {
    val result = TrinoResultDecoder.decode(
      columns = List("n"),
      rawRows = List(List(1: Byte)),
    )
    result.cell(0, 0) shouldBe Some(LiteralValue.ByteValue(1))
  }

  test("decode maps BigDecimal to LiteralValue.DecimalValue") {
    val result = TrinoResultDecoder.decode(
      columns = List("n"),
      rawRows = List(List(BigDecimal("123.45"))),
    )
    result.cell(0, 0) shouldBe Some(LiteralValue.DecimalValue(BigDecimal("123.45")))
  }

  // -- temporal type mapping --

  test("decode maps java.sql.Timestamp to LiteralValue.TimestampValue") {
    val ts = java.sql.Timestamp.from(java.time.Instant.parse("2025-01-15T12:00:00Z"))
    val result = TrinoResultDecoder.decode(
      columns = List("ts"),
      rawRows = List(List(ts)),
    )
    result.cell(0, 0) shouldBe Some(LiteralValue.TimestampValue(java.time.Instant.parse("2025-01-15T12:00:00Z")))
  }

  test("decode maps java.time.LocalDateTime to LiteralValue.TimestampValue") {
    val ldt = java.time.LocalDateTime.parse("2025-01-15T12:00:00")
    val result = TrinoResultDecoder.decode(
      columns = List("ts"),
      rawRows = List(List(ldt)),
    )
    val expectedInstant = ldt.toInstant(java.time.ZoneOffset.UTC)
    result.cell(0, 0) shouldBe Some(LiteralValue.TimestampValue(expectedInstant))
  }

  test("decode maps java.time.LocalDate to LiteralValue.DateValue") {
    val ld = java.time.LocalDate.parse("2025-01-15")
    val result = TrinoResultDecoder.decode(
      columns = List("d"),
      rawRows = List(List(ld)),
    )
    result.cell(0, 0) shouldBe Some(LiteralValue.DateValue(ld))
  }

  test("decode maps java.sql.Date to LiteralValue.DateValue") {
    val d = java.sql.Date.valueOf("2025-01-15")
    val result = TrinoResultDecoder.decode(
      columns = List("d"),
      rawRows = List(List(d)),
    )
    result.cell(0, 0) shouldBe Some(LiteralValue.DateValue(java.time.LocalDate.parse("2025-01-15")))
  }

  // -- binary type mapping --

  test("decode maps Array[Byte] to LiteralValue.BinaryValue") {
    val bytes = Array[Byte](1, 2, 3, 4)
    val result = TrinoResultDecoder.decode(
      columns = List("b"),
      rawRows = List(List(bytes)),
    )
    result.cell(0, 0) shouldBe Some(LiteralValue.BinaryValue(Vector[Byte](1, 2, 3, 4)))
  }

  // -- mixed row --

  test("decode handles a mixed-type row") {
    val result = TrinoResultDecoder.decode(
      columns = List("name", "age", "active", "salary", "joined_at"),
      rawRows = List(List(
        "Alice"           : Any,  // String
        30                : Any,  // Int
        true              : Any,  // Boolean
        BigDecimal("75000.50"),  // Decimal
        java.time.LocalDateTime.parse("2020-01-15T12:00:00"),  // Timestamp
      )),
    )
    result.cell(0, 0) shouldBe Some(LiteralValue.StringValue("Alice"))
    result.cell(0, 1) shouldBe Some(LiteralValue.IntValue(30))
    result.cell(0, 2) shouldBe Some(LiteralValue.BoolValue(true))
    result.cell(0, 3) shouldBe Some(LiteralValue.DecimalValue(BigDecimal("75000.50")))
    result.cell(0, 4) shouldBe Some(LiteralValue.TimestampValue(
      java.time.LocalDateTime.parse("2020-01-15T12:00:00").toInstant(java.time.ZoneOffset.UTC)
    ))
  }

  test("decode handles multiple rows with mixed types") {
    val result = TrinoResultDecoder.decode(
      columns = List("id", "name"),
      rawRows = List(
        List(1: Int, "Alice" : Any),
        List(2: Int, "Bob"   : Any),
        List(3: Int, "Carol" : Any),
      ),
    )
    result.rowCount shouldBe 3
    result.cell(0, 0) shouldBe Some(LiteralValue.IntValue(1))
    result.cell(1, 0) shouldBe Some(LiteralValue.IntValue(2))
    result.cell(2, 0) shouldBe Some(LiteralValue.IntValue(3))
    result.cell(0, 1) shouldBe Some(LiteralValue.StringValue("Alice"))
    result.cell(1, 1) shouldBe Some(LiteralValue.StringValue("Bob"))
    result.cell(2, 1) shouldBe Some(LiteralValue.StringValue("Carol"))
  }

  // -- unknown type fallback --

  test("decode maps unknown type to LiteralValue.StringValue (defensive fallback)") {
    // A custom class isn't a JDBC type — the decoder falls back to toString
    case class Custom(value: Int)
    val result = TrinoResultDecoder.decode(
      columns = List("c"),
      rawRows = List(List(Custom(42))),
    )
    result.cell(0, 0) shouldBe Some(LiteralValue.StringValue("Custom(42)"))
  }

  // -- boundary contract --

  test("TrinoResultDecoder is a singleton object") {
    TrinoResultDecoder shouldBe a [TrinoResultDecoder.type]
  }
}