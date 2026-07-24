package io.semanticdf.mcp.handlers

import io.semanticdf.Predicate

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import scala.jdk.CollectionConverters._

/** Tests for the structured AST predicate adapter. */
class AstPredicatesSpec extends AnyFunSuite {

  // ---------------------------------------------------------------------------
  // Compare: 6 leaf ops
  // ---------------------------------------------------------------------------

  test("eq with string value parses to Compare.Eq") {
    val json = asJavaMap("""{"op": "eq", "left": "carrier", "right": "AA"}""")
    AstPredicates.parse(json) shouldBe Predicate.Compare.Eq("carrier", "AA")
  }

  test("neq with string value parses to Compare.Ne") {
    val json = asJavaMap("""{"op": "neq", "left": "carrier", "right": "AA"}""")
    AstPredicates.parse(json) shouldBe Predicate.Compare.Ne("carrier", "AA")
  }

  test("lt with int value parses to Compare.Lt") {
    val json = asJavaMap("""{"op": "lt", "left": "distance", "right": 500}""")
    AstPredicates.parse(json) shouldBe Predicate.Compare.Lt("distance", 500)
  }

  test("lte with int value parses to Compare.Le") {
    val json = asJavaMap("""{"op": "lte", "left": "distance", "right": 500}""")
    AstPredicates.parse(json) shouldBe Predicate.Compare.Le("distance", 500)
  }

  test("gt with int value parses to Compare.Gt") {
    val json = asJavaMap("""{"op": "gt", "left": "distance", "right": 500}""")
    AstPredicates.parse(json) shouldBe Predicate.Compare.Gt("distance", 500)
  }

  test("gte with int value parses to Compare.Ge") {
    val json = asJavaMap("""{"op": "gte", "left": "distance", "right": 500}""")
    AstPredicates.parse(json) shouldBe Predicate.Compare.Ge("distance", 500)
  }

  // ---------------------------------------------------------------------------
  // Compound
  // ---------------------------------------------------------------------------

  test("and of two compares parses to Predicate.And") {
    val json = asJavaMap("""{
      "op": "and",
      "left":  {"op": "gt", "left": "distance", "right": 500},
      "right": {"op": "eq", "left": "carrier",  "right": "AA"}
    }""")
    AstPredicates.parse(json) shouldBe Predicate.And(
      Predicate.Compare.Gt("distance", 500),
      Predicate.Compare.Eq("carrier",  "AA"),
    )
  }

  test("or of two compares parses to Predicate.Or") {
    val json = asJavaMap("""{
      "op": "or",
      "left":  {"op": "eq", "left": "carrier", "right": "AA"},
      "right": {"op": "eq", "left": "carrier", "right": "UA"}
    }""")
    AstPredicates.parse(json) shouldBe Predicate.Or(
      Predicate.Compare.Eq("carrier", "AA"),
      Predicate.Compare.Eq("carrier", "UA"),
    )
  }

  test("nested: and(gt, or(eq, eq))") {
    val json = asJavaMap("""{
      "op": "and",
      "left":  {"op": "gt",  "left": "distance", "right": 500},
      "right": {
        "op": "or",
        "left":  {"op": "eq", "left": "carrier", "right": "AA"},
        "right": {"op": "eq", "left": "carrier", "right": "UA"}
      }
    }""")
    val expected = Predicate.And(
      Predicate.Compare.Gt("distance", 500),
      Predicate.Or(
        Predicate.Compare.Eq("carrier", "AA"),
        Predicate.Compare.Eq("carrier", "UA"),
      ),
    )
    AstPredicates.parse(json) shouldBe expected
  }

  // ---------------------------------------------------------------------------
  // Value types
  // ---------------------------------------------------------------------------

  test("double values preserved") {
    val json = asJavaMap("""{"op": "gt", "left": "rate", "right": 0.5}""")
    AstPredicates.parse(json) shouldBe Predicate.Compare.Gt("rate", 0.5)
  }

  test("boolean values preserved") {
    val json = asJavaMap("""{"op": "eq", "left": "active", "right": true}""")
    AstPredicates.parse(json) shouldBe Predicate.Compare.Eq("active", true)
  }

  // ---------------------------------------------------------------------------
  // Errors
  // ---------------------------------------------------------------------------

  test("missing op key throws InvalidPredicate") {
    val json = asJavaMap("""{"left": "carrier", "right": "AA"}""")
    val ex = intercept[JsonPredicates.InvalidPredicate] {
      AstPredicates.parse(json)
    }
    assert(ex.getMessage.contains("op"))
  }

  test("unknown op throws UnsupportedOp") {
    val json = asJavaMap("""{"op": "starts_with", "left": "carrier", "right": "AA"}""")
    val ex = intercept[JsonPredicates.UnsupportedOp] {
      AstPredicates.parse(json)
    }
    assert(ex.getMessage.contains("starts_with"))
  }

  test("compare with non-string left throws InvalidPredicate") {
    val json = asJavaMap("""{"op": "eq", "left": 5, "right": "AA"}""")
    val ex = intercept[JsonPredicates.InvalidPredicate] {
      AstPredicates.parse(json)
    }
    assert(ex.getMessage.contains("field name"))
  }

  test("missing left or right throws InvalidPredicate") {
    val json = asJavaMap("""{"op": "eq", "left": "carrier"}""")
    val ex = intercept[JsonPredicates.InvalidPredicate] {
      AstPredicates.parse(json)
    }
    assert(ex.getMessage.contains("right"))
  }

  test("non-object root throws InvalidPredicate") {
    val ex = intercept[JsonPredicates.InvalidPredicate] {
      AstPredicates.parse("not a json object")
    }
    assert(ex.getMessage.toLowerCase.contains("json object"))
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private def asJavaMap(json: String): java.util.Map[String, Any] = {
    val mapper = io.semanticdf.mcp.JsonSupport.scalaMapper()
    mapper.readValue(json, classOf[java.util.Map[String, Object]]).asScala.toMap.asInstanceOf[Map[String, Any]].asJava
  }
}
