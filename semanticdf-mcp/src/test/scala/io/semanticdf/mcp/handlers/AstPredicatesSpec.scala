package io.semanticdf.mcp.handlers

import io.semanticdf.predicate.Predicate

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

  // ---------------------------------------------------------------------------
  // Core ADT entry points (parseCore)
  //
  // Phase 1 consolidation: mirrors the existing parse tests but produces
  // engine-portable io.semanticdf.core.predicate.Predicate directly.
  // Same JSON shape contract; same error types (InvalidPredicate,
  // UnsupportedOp). Kept additive — parse / parse test surface above is
  // unchanged.
  // ---------------------------------------------------------------------------

  test("parseCore: eq with string value produces core Compare.Eq") {
    val json = asJavaMap("""{"op": "eq", "left": "carrier", "right": "AA"}""")
    AstPredicates.parseCore(json) shouldBe
      io.semanticdf.core.predicate.Predicate.Compare.Eq("carrier", "AA")
  }

  test("parseCore: neq with string value produces core Compare.Ne") {
    val json = asJavaMap("""{"op": "neq", "left": "carrier", "right": "AA"}""")
    AstPredicates.parseCore(json) shouldBe
      io.semanticdf.core.predicate.Predicate.Compare.Ne("carrier", "AA")
  }

  test("parseCore: lt with int value produces core Compare.Lt") {
    val json = asJavaMap("""{"op": "lt", "left": "distance", "right": 500}""")
    AstPredicates.parseCore(json) shouldBe
      io.semanticdf.core.predicate.Predicate.Compare.Lt("distance", 500)
  }

  test("parseCore: lte with int value produces core Compare.Le") {
    val json = asJavaMap("""{"op": "lte", "left": "distance", "right": 500}""")
    AstPredicates.parseCore(json) shouldBe
      io.semanticdf.core.predicate.Predicate.Compare.Le("distance", 500)
  }

  test("parseCore: gt with double value produces core Compare.Gt") {
    val json = asJavaMap("""{"op": "gt", "left": "rate", "right": 0.5}""")
    AstPredicates.parseCore(json) shouldBe
      io.semanticdf.core.predicate.Predicate.Compare.Gt("rate", 0.5)
  }

  test("parseCore: gte with int value produces core Compare.Ge") {
    val json = asJavaMap("""{"op": "gte", "left": "active", "right": true}""")
    AstPredicates.parseCore(json) shouldBe
      io.semanticdf.core.predicate.Predicate.Compare.Ge("active", true)
  }

  test("parseCore: and produces core And with recursive children") {
    val json = asJavaMap("""{
      "op": "and",
      "left":  {"op": "eq", "left": "carrier", "right": "AA"},
      "right": {"op": "gt", "left": "pax",     "right": 100}
    }""")
    AstPredicates.parseCore(json) shouldBe io.semanticdf.core.predicate.Predicate.And(
      io.semanticdf.core.predicate.Predicate.Compare.Eq("carrier", "AA"),
      io.semanticdf.core.predicate.Predicate.Compare.Gt("pax", 100),
    )
  }

  test("parseCore: or produces core Or with recursive children") {
    val json = asJavaMap("""{
      "op": "or",
      "left":  {"op": "eq", "left": "carrier", "right": "AA"},
      "right": {"op": "eq", "left": "carrier", "right": "UA"}
    }""")
    AstPredicates.parseCore(json) shouldBe io.semanticdf.core.predicate.Predicate.Or(
      io.semanticdf.core.predicate.Predicate.Compare.Eq("carrier", "AA"),
      io.semanticdf.core.predicate.Predicate.Compare.Eq("carrier", "UA"),
    )
  }

  test("parseCore: deeply nested tree produces nested core And/Or") {
    val json = asJavaMap("""{
      "op": "and",
      "left":  {"op": "or",  "left": {"op": "eq", "left": "carrier", "right": "AA"}, "right": {"op": "eq", "left": "carrier", "right": "UA"}},
      "right": {"op": "gt",  "left": "pax",     "right": 100}
    }""")
    AstPredicates.parseCore(json) shouldBe io.semanticdf.core.predicate.Predicate.And(
      io.semanticdf.core.predicate.Predicate.Or(
        io.semanticdf.core.predicate.Predicate.Compare.Eq("carrier", "AA"),
        io.semanticdf.core.predicate.Predicate.Compare.Eq("carrier", "UA"),
      ),
      io.semanticdf.core.predicate.Predicate.Compare.Gt("pax", 100),
    )
  }

  test("parseCore: throws InvalidPredicate on missing op (same contract as parse)") {
    val ex = intercept[JsonPredicates.InvalidPredicate] {
      AstPredicates.parseCore(asJavaMap("""{"left": "carrier", "right": "AA"}"""))
    }
    ex.getMessage should include ("'op'")
  }

  test("parseCore: throws UnsupportedOp on unknown op (same contract as parse)") {
    val ex = intercept[JsonPredicates.UnsupportedOp] {
      AstPredicates.parseCore(asJavaMap("""{"op": "wat", "left": "carrier", "right": "AA"}"""))
    }
    ex.getMessage should include ("wat")
  }

  test("parseCore: throws InvalidPredicate on non-string left in compare op (same contract as parse)") {
    val ex = intercept[JsonPredicates.InvalidPredicate] {
      AstPredicates.parseCore(asJavaMap("""{"op": "eq", "left": 42, "right": "AA"}"""))
    }
    ex.getMessage should include ("field name")
  }

  test("parseCore / parse parity: same JSON produces equivalent predicates via both APIs") {
    // Load-bearing assertion: the two adapters agree on the JSON contract.
    // parse → original, parseCore → core. Both produce semantically equivalent
    // predicates that, via PredicateConverter, are == to each other.
    val json = asJavaMap("""{
      "op": "and",
      "left":  {"op": "gt", "left": "distance", "right": 100},
      "right": {"op": "eq", "left": "carrier",  "right": "AA"}
    }""")
    val original = AstPredicates.parse(json)
    val core     = AstPredicates.parseCore(json)
    io.semanticdf.predicate.PredicateConverter.toCore(original) shouldBe core
  }
}
