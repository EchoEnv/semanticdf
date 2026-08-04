package io.semanticdf.trino

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.semanticdf.core.predicate.{Predicate => CorePredicate}

/** Phase 2 — prove `SqlLowerer.lower` correctly translates a
  * `CorePredicate` to a Trino-compatible SQL string. The lowerer
  * is a pure function: same input always returns the same output.
  */
class SqlLowererSpec extends AnyFunSuite with Matchers {

  // -- leaf cases: Compare family --

  test("Compare.Eq with string value produces \"field\" = 'value'") {
    SqlLowerer.lower(CorePredicate.Compare.Eq("carrier", "AA")) shouldBe
      "\"carrier\" = 'AA'"
  }

  test("Compare.Eq with numeric value produces \"field\" = number") {
    SqlLowerer.lower(CorePredicate.Compare.Eq("distance", 500)) shouldBe
      "\"distance\" = 500"
  }

  test("Compare.Ne produces \"field\" <> value") {
    SqlLowerer.lower(CorePredicate.Compare.Ne("carrier", "AA")) shouldBe
      "\"carrier\" <> 'AA'"
    SqlLowerer.lower(CorePredicate.Compare.Ne("distance", 500)) shouldBe
      "\"distance\" <> 500"
  }

  test("Compare.Lt produces \"field\" < value") {
    SqlLowerer.lower(CorePredicate.Compare.Lt("distance", 500)) shouldBe
      "\"distance\" < 500"
  }

  test("Compare.Le produces \"field\" <= value") {
    SqlLowerer.lower(CorePredicate.Compare.Le("distance", 500)) shouldBe
      "\"distance\" <= 500"
  }

  test("Compare.Gt produces \"field\" > value") {
    SqlLowerer.lower(CorePredicate.Compare.Gt("distance", 500)) shouldBe
      "\"distance\" > 500"
  }

  test("Compare.Ge produces \"field\" >= value") {
    SqlLowerer.lower(CorePredicate.Compare.Ge("distance", 500)) shouldBe
      "\"distance\" >= 500"
  }

  // -- leaf cases: In / IsNull --

  test("In (non-negated) with multiple values produces IN list") {
    SqlLowerer.lower(CorePredicate.In("carrier", Seq("AA", "UA", "DL"), negate = false)) shouldBe
      "\"carrier\" IN ('AA', 'UA', 'DL')"
  }

  test("In (negated) produces NOT IN list") {
    SqlLowerer.lower(CorePredicate.In("carrier", Seq("AA", "UA"), negate = true)) shouldBe
      "\"carrier\" NOT IN ('AA', 'UA')"
  }

  test("In with empty values list produces empty IN clause") {
    SqlLowerer.lower(CorePredicate.In("carrier", Seq.empty, negate = false)) shouldBe
      "\"carrier\" IN ()"
  }

  test("In with mix of string and numeric values renders each appropriately") {
    SqlLowerer.lower(CorePredicate.In("x", Seq("a", 1, true), negate = false)) shouldBe
      "\"x\" IN ('a', 1, TRUE)"
  }

  test("IsNull produces \"field\" IS NULL") {
    SqlLowerer.lower(CorePredicate.IsNull("carrier", negate = false)) shouldBe
      "\"carrier\" IS NULL"
  }

  test("IsNotNull produces \"field\" IS NOT NULL") {
    SqlLowerer.lower(CorePredicate.IsNull("carrier", negate = true)) shouldBe
      "\"carrier\" IS NOT NULL"
  }

  // -- compound cases --

  test("And with 2 children produces parenthesized AND expression") {
    val p = CorePredicate.And(
      CorePredicate.Compare.Eq("carrier", "AA"),
      CorePredicate.Compare.Gt("distance", 1),
    )
    SqlLowerer.lower(p) shouldBe "(\"carrier\" = 'AA' AND \"distance\" > 1)"
  }

  test("And with 3+ children chains with AND") {
    val p = CorePredicate.And(
      CorePredicate.Compare.Eq("a", 1),
      CorePredicate.Compare.Eq("b", 2),
      CorePredicate.Compare.Eq("c", 3),
    )
    SqlLowerer.lower(p) shouldBe "(\"a\" = 1 AND \"b\" = 2 AND \"c\" = 3)"
  }

  test("And with 0 children is TRUE (vacuous truth)") {
    val p = CorePredicate.And()
    SqlLowerer.lower(p) shouldBe "TRUE"
  }

  test("Or with 2 children produces parenthesized OR expression") {
    val p = CorePredicate.Or(
      CorePredicate.Compare.Eq("carrier", "AA"),
      CorePredicate.Compare.Eq("carrier", "UA"),
    )
    SqlLowerer.lower(p) shouldBe "(\"carrier\" = 'AA' OR \"carrier\" = 'UA')"
  }

  test("Or with 0 children is FALSE (vacuous falsity)") {
    val p = CorePredicate.Or()
    SqlLowerer.lower(p) shouldBe "FALSE"
  }

  test("Not wraps child in NOT (...)") {
    val p = CorePredicate.Not(CorePredicate.Compare.Eq("carrier", "AA"))
    SqlLowerer.lower(p) shouldBe "NOT (\"carrier\" = 'AA')"
  }

  test("Nested compounds: And(Or(Eq, Eq), Gt) lowers correctly") {
    val p = CorePredicate.And(
      CorePredicate.Or(
        CorePredicate.Compare.Eq("a", 1),
        CorePredicate.Compare.Eq("b", 2),
      ),
      CorePredicate.Compare.Gt("c", 3),
    )
    SqlLowerer.lower(p) shouldBe
      "((\"a\" = 1 OR \"b\" = 2) AND \"c\" > 3)"
  }

  test("Deeply nested Not inside And inside Or") {
    val p = CorePredicate.Or(
      CorePredicate.And(
        CorePredicate.Compare.Eq("a", 1),
        CorePredicate.Not(CorePredicate.Compare.Gt("b", 2)),
      ),
      CorePredicate.Compare.Eq("c", 3),
    )
    SqlLowerer.lower(p) shouldBe
      "((\"a\" = 1 AND NOT (\"b\" > 2)) OR \"c\" = 3)"
  }

  // -- value rendering edge cases --

  test("renderValue: string with embedded single quote escapes by doubling") {
    // Via the public lower() entry point — lower(Eq("name", "O'Brien")) →
    // renders value as 'O''Brien' (two single quotes = escaped quote)
    SqlLowerer.lower(CorePredicate.Compare.Eq("name", "O'Brien")) shouldBe
      "\"name\" = 'O''Brien'"
  }

  test("renderValue: null renders as NULL") {
    SqlLowerer.lower(CorePredicate.Compare.Eq("x", null)) shouldBe
      "\"x\" = NULL"
  }

  test("renderValue: boolean renders as TRUE/FALSE") {
    SqlLowerer.lower(CorePredicate.Compare.Eq("active", true)) shouldBe
      "\"active\" = TRUE"
    SqlLowerer.lower(CorePredicate.Compare.Eq("active", false)) shouldBe
      "\"active\" = FALSE"
  }

  // -- pure function invariant --

  test("pure function: same input always returns same output (no shared state)") {
    val p = CorePredicate.And(
      CorePredicate.Compare.Eq("c", "AA"),
      CorePredicate.Compare.Gt("d", 1),
    )
    val a = SqlLowerer.lower(p)
    val b = SqlLowerer.lower(p)
    val c = SqlLowerer.lower(p)
    a shouldBe b
    b shouldBe c
  }
}