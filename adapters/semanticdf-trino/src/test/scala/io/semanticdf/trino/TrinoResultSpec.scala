package io.semanticdf.trino

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.semanticdf.core.expr.LiteralValue

/** Phase 3 follow-up to PRs #367/#374/#379: prove that
  * `TrinoResult.toJson` correctly serializes typed `LiteralValue`
  * cells to a JSON string.
  *
  * ==Why this matters==
  *
  * Mirrors the original Spark library's `df.toJSON.collect()`
  * consumer pattern. MCP responses, log lines, and audit-event
  * payloads all benefit from a single string representation of
  * a query result.
  *
  * ==Why per scala-data-driven-refactor §1==
  *
  * `toJson` is a pure function of the `TrinoResult`'s columns +
  * rows. It belongs on the data class (no behavior side effects,
  * no IO, no external dependencies).
  */
class TrinoResultSpec extends AnyFunSuite with Matchers {

  // -- empty / single-row / multi-row --

  test("toJson returns an empty array for an empty result") {
    val r   = TrinoResult(columns = List("a", "b"), rows = Nil)
    val j   = r.toJson
    j shouldBe "[]"
  }

  test("toJson returns a single object for a single row") {
    val r = TrinoResult(
      columns = List("region", "total"),
      rows    = List(List(
        LiteralValue.StringValue("AA"),
        LiteralValue.LongValue(12345L),
      )),
    )
    r.toJson shouldBe """[{"region":"AA","total":12345}]"""
  }

  test("toJson returns a JSON array for multiple rows") {
    val r = TrinoResult(
      columns = List("region", "total"),
      rows    = List(
        List(LiteralValue.StringValue("AA"), LiteralValue.LongValue(100L)),
        List(LiteralValue.StringValue("BB"), LiteralValue.LongValue(200L)),
      ),
    )
    r.toJson shouldBe """[{"region":"AA","total":100},{"region":"BB","total":200}]"""
  }

  // -- types --

  test("toJson handles LongValue, IntValue, DoubleValue, BoolValue, DecimalValue") {
    val r = TrinoResult(
      columns = List("a", "b", "c", "d", "e"),
      rows    = List(List(
        LiteralValue.LongValue(42L),
        LiteralValue.IntValue(7),
        LiteralValue.DoubleValue(3.14),
        LiteralValue.BoolValue(true),
        LiteralValue.DecimalValue(BigDecimal("100.50")),
      )),
    )
    r.toJson shouldBe """[{"a":42,"b":7,"c":3.14,"d":true,"e":100.50}]"""
  }

  test("toJson handles NullValue as JSON null") {
    val r = TrinoResult(
      columns = List("x"),
      rows    = List(List(LiteralValue.NullValue)),
    )
    r.toJson shouldBe """[{"x":null}]"""
  }

  test("toJson escapes quotes, backslashes, and control chars in StringValue") {
    val r = TrinoResult(
      columns = List("s"),
      rows    = List(List(LiteralValue.StringValue("hello \"world\" \\ \n \r \t"))),
    )
    r.toJson shouldBe """[{"s":"hello \"world\" \\ \n \r \t"}]"""
  }

  test("toJson handles ArrayValue and MapValue as nested collections") {
    val r = TrinoResult(
      columns = List("arr", "map"),
      rows    = List(List(
        LiteralValue.ArrayValue(
          values = List(
            LiteralValue.IntValue(1),
            LiteralValue.IntValue(2),
            LiteralValue.IntValue(3),
          ),
        ),
        LiteralValue.MapValue(
          values = List(
            (LiteralValue.StringValue("x"), LiteralValue.IntValue(10)),
            (LiteralValue.StringValue("y"), LiteralValue.IntValue(20)),
          ),
        ),
      )),
    )
    r.toJson shouldBe """[{"arr":[1,2,3],"map":{"x":10,"y":20}}]"""
  }

  // -- JSON key escaping --

  test("toJson escapes column names containing JSON-significant chars") {
    val r = TrinoResult(
      columns = List("a\"b"),
      rows    = List(List(LiteralValue.IntValue(1))),
    )
    // Column name `a"b` (literal double-quote) must be quoted+escaped
    // inside the JSON key
    r.toJson shouldBe """[{"a\"b":1}]"""
  }
}