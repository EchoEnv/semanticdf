package io.semanticdf.duckdb

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.semanticdf.core.engine.{ResultError, ResultValue}
import io.semanticdf.core.expr.LiteralValue
import io.semanticdf.core.schema.{Field, SealedDataType}

/** Tests for [[DuckDBResultEncoder]]: engine-native DuckDBResult
  * \u2192 portable `PortableQueryResult`. Mirrors the design
  * \u00a74.5.4 conformance properties. */
class DuckDBResultEncoderSpec extends AnyFunSuite with Matchers {

  test("encodes a 2-column DuckDBResult correctly") {
    val duckResult = DuckDBResult(
      columns = List("id", "name"),
      rows = List(
        List(LiteralValue.LongValue(1L), LiteralValue.StringValue("alice")),
        List(LiteralValue.LongValue(2L), LiteralValue.StringValue("bob")),
      ),
    )
    val encoded = new DuckDBResultEncoder().encode(duckResult)
    encoded match {
      case Right(pqr) =>
        pqr.rowCount shouldBe 2
        pqr.rows(0).values shouldBe List(ResultValue.IntV(1L), ResultValue.StringV("alice"))
        pqr.metadata("engine.adaptor.id") shouldBe "duckdb"
      case Left(err) => fail(s"expected Right, got Left($err)")
    }
  }

  test("returns ShapeMismatch when row count differs from schema") {
    val bad = DuckDBResult(
      columns = List("a", "b"),
      rows = List(List(LiteralValue.LongValue(1L))),
    )
    new DuckDBResultEncoder().encode(bad) match {
      case Left(ResultError.ShapeMismatch(reason)) =>
        reason should include ("1 values")
        reason should include ("2 fields")
      case other => fail(s"expected Left(ShapeMismatch), got $other")
    }
  }
}
