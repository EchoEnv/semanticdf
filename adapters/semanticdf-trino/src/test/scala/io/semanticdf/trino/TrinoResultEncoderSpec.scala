package io.semanticdf.trino

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.semanticdf.core.engine.{ResultError, ResultValue}
import io.semanticdf.core.expr.LiteralValue
import io.semanticdf.core.schema.{Field, SealedDataType}

import java.math.BigDecimal
import java.time.{Instant, LocalDate}

/** Tests for [[TrinoResultEncoder]]: engine-native TrinoResult
  * \u2192 portable `PortableQueryResult`. Mirrors the design
  * \u00a74.5.4 conformance properties. */
class TrinoResultEncoderSpec extends AnyFunSuite with Matchers {

  test("encodes a 2-column TrinoResult correctly") {
    val trinoResult = TrinoResult(
      columns = List("id", "name"),
      rows = List(
        List(LiteralValue.LongValue(1L), LiteralValue.StringValue("alice")),
        List(LiteralValue.LongValue(2L), LiteralValue.StringValue("bob")),
      ),
    )
    val encoded = new TrinoResultEncoder().encode(trinoResult)
    encoded match {
      case Right(pqr) =>
        pqr.rowCount shouldBe 2
        pqr.schema.fields.map(_.name) shouldBe List("id", "name")
        pqr.rows(0).values shouldBe List(ResultValue.IntV(1L), ResultValue.StringV("alice"))
        pqr.rows(1).values shouldBe List(ResultValue.IntV(2L), ResultValue.StringV("bob"))
        pqr.isWellFormed shouldBe true
        pqr.metadata("engine.adaptor.id") shouldBe "trino"
      case Left(err) => fail(s"expected Right, got Left($err)")
    }
  }

  test("returns ShapeMismatch when row count differs from schema") {
    val bad = TrinoResult(
      columns = List("a", "b"),
      rows = List(List(LiteralValue.LongValue(1L))),
    )
    new TrinoResultEncoder().encode(bad) match {
      case Left(ResultError.ShapeMismatch(reason)) =>
        reason should include ("1 values")
        reason should include ("2 fields")
      case other => fail(s"expected Left(ShapeMismatch), got $other")
    }
  }

  test("maps every LiteralValue case to the correct ResultValue") {
    val trinoResult = TrinoResult(
      columns = List("n", "b", "i", "d", "s", "dec", "ts", "dt", "str"),
      rows = List(List(
        LiteralValue.NullValue,
        LiteralValue.BoolValue(true),
        LiteralValue.IntValue(42),
        LiteralValue.LongValue(99L),
        LiteralValue.DoubleValue(3.14),
        LiteralValue.DecimalValue(new java.math.BigDecimal("123.45")),
        LiteralValue.TimestampValue(Instant.parse("2024-01-15T10:30:00Z")),
        LiteralValue.DateValue(LocalDate.parse("2024-01-15")),
        LiteralValue.StringValue("x"),
      )),
    )
    val Right(pqr) = new TrinoResultEncoder().encode(trinoResult)
    pqr.rows(0).values shouldBe List(
      ResultValue.NullV,
      ResultValue.BoolV(true),
      ResultValue.IntV(42L),
      ResultValue.IntV(99L),
      ResultValue.DoubleV(3.14),
      ResultValue.DecimalV(new java.math.BigDecimal("123.45")),
      ResultValue.TimestampV(Instant.parse("2024-01-15T10:30:00Z")),
      ResultValue.DateV(LocalDate.parse("2024-01-15")),
      ResultValue.StringV("x"),
    )
  }

  test("PortableQueryResult round-trips through Java serialization") {
    val schema = io.semanticdf.core.engine.ResultSchema(List(
      Field("a", SealedDataType.Int, nullable = true),
    ))
    val rows = Vector(
      io.semanticdf.core.engine.ResultRow(List(ResultValue.IntV(1L)), schema),
      io.semanticdf.core.engine.ResultRow(List(ResultValue.IntV(2L)), schema),
    )
    val pqr = io.semanticdf.core.engine.PortableQueryResult(schema, rows, Map("source" -> "trino"))
    val out = new java.io.ByteArrayOutputStream()
    val oos = new java.io.ObjectOutputStream(out)
    oos.writeObject(pqr)
    oos.close()
    val ois = new java.io.ObjectInputStream(new java.io.ByteArrayInputStream(out.toByteArray))
    val back = ois.readObject().asInstanceOf[io.semanticdf.core.engine.PortableQueryResult]
    ois.close()
    back shouldBe pqr
  }
}
