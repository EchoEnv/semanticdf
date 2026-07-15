package io.semanticdf.mcp.handlers

import io.semanticdf.Predicate
import io.semanticdf.Predicate.Compare
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import scala.jdk.CollectionConverters._

/** Tests for the JSON predicate adapter — the heart of `query`/`explain`.
  *
  * The adapter is pure (no Spark, no MCP, no I/O). We just exercise every
  * shape the contract documents, plus the error cases. */
class JsonPredicatesSpec extends AnyFunSuite {

  // ---------------------------------------------------------------------------
  // (1) Compare — the 6 ops, including the `eq` shorthand
  // ---------------------------------------------------------------------------

  test("eq / ne / lt / le / gt / ge all parse to the right Compare subtype") {
    val cases = Seq(
      "eq" -> Compare.Eq("carrier", "AA"),
      "ne" -> Compare.Ne("carrier", "AA"),
      "lt" -> Compare.Lt("distance", 500),
      "le" -> Compare.Le("distance", 500),
      "gt" -> Compare.Gt("distance", 500),
      "ge" -> Compare.Ge("distance", 500),
    )
    cases.foreach { case (op, expected) =>
      val json = s"""{ "type": "$op", "field": "${fieldOf(expected)}", "value": ${valueJson(expected)} }"""
      val parsed = JsonPredicates.parse(asJavaMap(json))
      parsed shouldBe expected
    }
  }

  // ---------------------------------------------------------------------------
  // (2) In / not_in
  // ---------------------------------------------------------------------------

  test("in parses to a non-negated Predicate.In") {
    val json = """{ "type": "in", "field": "carrier", "values": ["AA", "UA", "DL"] }"""
    JsonPredicates.parse(asJavaMap(json)) shouldBe Predicate.In("carrier", Seq("AA", "UA", "DL"), negate = false)
  }

  test("not_in parses to a negated Predicate.In") {
    val json = """{ "type": "not_in", "field": "carrier", "values": ["AA"] }"""
    JsonPredicates.parse(asJavaMap(json)) shouldBe Predicate.In("carrier", Seq("AA"), negate = true)
  }

  // ---------------------------------------------------------------------------
  // (3) is_null / is_not_null
  // ---------------------------------------------------------------------------

  test("is_null / is_not_null") {
    JsonPredicates.parse(asJavaMap("""{ "type": "is_null", "field": "origin" }"""))       shouldBe Predicate.IsNull("origin", negate = false)
    JsonPredicates.parse(asJavaMap("""{ "type": "is_not_null", "field": "origin" }""")) shouldBe Predicate.IsNull("origin", negate = true)
  }

  // ---------------------------------------------------------------------------
  // (4) Compound — and / or / not
  // ---------------------------------------------------------------------------

  test("and combines child predicates in order") {
    val json = """{
      "type": "and",
      "predicates": [
        { "type": "eq",  "field": "carrier", "value": "AA" },
        { "type": "gt",  "field": "distance", "value": 500   }
      ]
    }"""
    val expected = Compare.Eq("carrier", "AA").and(Compare.Gt("distance", 500))
    JsonPredicates.parse(asJavaMap(json)) shouldBe expected
  }

  test("or and not work like and") {
    val jsonOr = """{
      "type": "or",
      "predicates": [
        { "type": "eq", "field": "carrier", "value": "AA" },
        { "type": "eq", "field": "carrier", "value": "UA" }
      ]
    }"""
    JsonPredicates.parse(asJavaMap(jsonOr)) shouldBe
      Predicate.Or(Compare.Eq("carrier", "AA"), Compare.Eq("carrier", "UA"))

    val jsonNot = """{
      "type": "not",
      "predicate": { "type": "eq", "field": "carrier", "value": "AA" }
    }"""
    JsonPredicates.parse(asJavaMap(jsonNot)) shouldBe Predicate.Not(Compare.Eq("carrier", "AA"))
  }

  // ---------------------------------------------------------------------------
  // (5) Error paths — INVALID_PREDICATE / UNSUPPORTED_OP
  // ---------------------------------------------------------------------------

  test("missing `type` raises InvalidPredicate") {
    val ex = intercept[JsonPredicates.InvalidPredicate] {
      JsonPredicates.parse(asJavaMap("""{ "field": "carrier", "value": "AA" }"""))
    }
    ex.getMessage should include ("'type'")
  }

  test("missing `field` on Compare raises InvalidPredicate") {
    val ex = intercept[JsonPredicates.InvalidPredicate] {
      JsonPredicates.parse(asJavaMap("""{ "type": "eq", "value": "AA" }"""))
    }
    ex.getMessage should include ("'field'")
  }

  test("missing `values` on in raises InvalidPredicate") {
    val ex = intercept[JsonPredicates.InvalidPredicate] {
      JsonPredicates.parse(asJavaMap("""{ "type": "in", "field": "carrier" }"""))
    }
    ex.getMessage should include ("'values'")
  }

  test("empty `values` on in raises InvalidPredicate") {
    val ex = intercept[JsonPredicates.InvalidPredicate] {
      JsonPredicates.parse(asJavaMap("""{ "type": "in", "field": "carrier", "values": [] }"""))
    }
    ex.getMessage should include ("must not be empty")
  }

  test("and with fewer than 2 children raises InvalidPredicate") {
    val ex = intercept[JsonPredicates.InvalidPredicate] {
      JsonPredicates.parse(asJavaMap("""{
        "type": "and",
        "predicates": [ { "type": "eq", "field": "carrier", "value": "AA" } ]
      }"""))
    }
    ex.getMessage should include ("at least 2")
  }

  test("unknown op raises UnsupportedOp") {
    val ex = intercept[JsonPredicates.UnsupportedOp] {
      JsonPredicates.parse(asJavaMap("""{ "type": "regexp", "field": "carrier", "value": "AA.*" }"""))
    }
    ex.getMessage should include ("regexp")
  }

  test("non-object input raises InvalidPredicate") {
    val ex = intercept[JsonPredicates.InvalidPredicate] {
      JsonPredicates.parse("not a map")
    }
    ex.getMessage should include ("JSON object")
  }

  // ---------------------------------------------------------------------------
  // (6) parseAll — flat-list AND-combiner (server-side helper)
  // ---------------------------------------------------------------------------

  test("parseAll on empty seq returns None (no And wrapper for zero predicates)") {
    JsonPredicates.parseAll(Seq.empty) shouldBe None
  }

  test("parseAll on a single predicate returns Some(predicate) without an And wrapper") {
    JsonPredicates.parseAll(Seq(asJavaMap("""{ "type": "eq", "field": "carrier", "value": "AA" }"""))) shouldBe
      Some(Compare.Eq("carrier", "AA"))
  }

  test("parseAll on multiple predicates returns One And wrapper") {
    val json1 = asJavaMap("""{ "type": "eq", "field": "carrier", "value": "AA" }""")
    val json2 = asJavaMap("""{ "type": "gt", "field": "distance", "value": 100 }""")
    JsonPredicates.parseAll(Seq(json1, json2)) shouldBe
      Some(Predicate.And(Compare.Eq("carrier", "AA"), Compare.Gt("distance", 100)))
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /** Parse a JSON string into a `java.util.Map[String, Any]` via Jackson 2.x,
    * which is what the MCP SDK uses for JSON ↔ Java. We read into a concrete
    * `LinkedHashMap` (so the JSON object shape is preserved) and rely on
    * automatic Object → Java type promotion (`Integer` for `5` etc.). */
  private def asJavaMap(json: String): java.util.Map[String, Any] = {
    val om = new com.fasterxml.jackson.databind.ObjectMapper()
    om.readValue(json, classOf[java.util.LinkedHashMap[String, Any]])
  }

  private def fieldOf(p: Compare): String = p match {
    case Compare.Eq(f, _)   => f
    case Compare.Ne(f, _)   => f
    case Compare.Lt(f, _)   => f
    case Compare.Le(f, _)   => f
    case Compare.Gt(f, _)   => f
    case Compare.Ge(f, _)   => f
  }

  /** Render the `value` of a Compare as JSON literal (for re-emitting). */
  private def valueJson(p: Compare): String = p match {
    case Compare.Eq(_, v)   => jsValue(v)
    case Compare.Ne(_, v)   => jsValue(v)
    case Compare.Lt(_, v)   => jsValue(v)
    case Compare.Le(_, v)   => jsValue(v)
    case Compare.Gt(_, v)   => jsValue(v)
    case Compare.Ge(_, v)   => jsValue(v)
  }

  private def jsValue(v: Any): String = v match {
    case s: String => "\"" + s + "\""
    case other     => other.toString
  }
}
