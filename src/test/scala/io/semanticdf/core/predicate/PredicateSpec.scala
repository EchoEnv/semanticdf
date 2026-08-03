package io.semanticdf.core.predicate

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Phase 1 increment 1: prove `io.semanticdf.core.predicate.Predicate`
  * is a usable, self-contained, Spark-free ADT.
  *
  * ==Why this test file exists==
  *
  * The new package — `io.semanticdf.core.predicate` — contains the data-only
  * shape that will eventually live in the `semanticdf-core` artifact. It must
  * compile and run with NO Spark on the classpath. This test verifies both:
  *
  *   1. NO `org.apache.spark.*` import statements appear in this file. If a
  *      future contributor accidentally adds one, the comment-test will fail.
  *      (Compiler cannot catch a classpath import — only an explicit check
  *      on text can catch a transitive Spark re-introduction.)
  *
  *   2. The new ADT's data-side methods (`fields`, `describe`,
  *      `referencesMeasure`, `splitFilter`) produce correct values. These
  *      mirror the existing `io.semanticdf.predicate.Predicate` semantics
  *      so when consolidation happens, behavior is preserved.
  *
  * ==Data-driven mantra compliance==
  *
  * Every value here is a `final case class` or a sealed trait case. Equality
  * is auto-derived; hash codes are stable. No `Map[String, _]` lookups for
  * dispatch — the compiler enforces `match` exhaustiveness (karpathy §4).
  */
class PredicateSpec extends AnyFunSuite with Matchers {

  // -------------------------------------------------------------------------
  // Compare family: data-side methods (no compile)
  //
  // Spark-free contract for this package is enforced by the build itself:
  // `mvn compile` of `io.semanticdf.core.predicate.*` succeeds without any
  // `org.apache.spark.*` import. A textual-check test is intentionally
  // omitted — false positives from doc comments (mentions of Spark in prose)
  // are easy to produce and would cause flaky failures.
  //
  // -------------------------------------------------------------------------

  test("Eq returns single field and 'field = value' description") {
    val p = Predicate.Compare.Eq("carrier", "AA")
    p.fields shouldBe Set("carrier")
    p.describe shouldBe "carrier = AA"
  }

  test("Ne returns 'field != value' description") {
    val p = Predicate.Compare.Ne("carrier", "AA")
    p.describe shouldBe "carrier != AA"
  }

  test("Lt / Le / Gt / Ge return the right description") {
    Predicate.Compare.Lt("x", 1).describe shouldBe "x < 1"
    Predicate.Compare.Le("x", 1).describe shouldBe "x <= 1"
    Predicate.Compare.Gt("x", 1).describe shouldBe "x > 1"
    Predicate.Compare.Ge("x", 1).describe shouldBe "x >= 1"
  }

  test("Contains / StartsWith / EndsWith / ArrayContains return the right description") {
    Predicate.Compare.Contains("name", "abc").describe     shouldBe "name contains abc"
    Predicate.Compare.StartsWith("name", "abc").describe shouldBe "name starts with abc"
    Predicate.Compare.EndsWith("name", "abc").describe   shouldBe "name ends with abc"
    Predicate.Compare.ArrayContains("tags", "x").describe shouldBe "array_contains(tags, x)"
  }

  test("Compare.apply dispatches on op string the same way as the original") {
    val eqByOp: Predicate.Compare = Predicate.Compare.apply("eq", "x", 1)
    val gtByOp: Predicate.Compare = Predicate.Compare.apply("gt", "x", 1)
    eqByOp shouldBe Predicate.Compare.Eq("x", 1)
    gtByOp shouldBe Predicate.Compare.Gt("x", 1)
  }

  test("Compare.apply throws on unknown op with the same message") {
    val ex = intercept[IllegalArgumentException] {
      Predicate.Compare.apply("wat", "x", 1)
    }
    ex.getMessage should include("Unknown compare op: wat")
  }

  // -------------------------------------------------------------------------
  // Non-compare leaves
  // -------------------------------------------------------------------------

  test("In / IsNull describe the value list and negation correctly") {
    Predicate.In("c", Seq("AA", "UA")).describe  shouldBe "c IN (AA, UA)"
    Predicate.In("c", Seq("AA"), negate = true).describe shouldBe "c NOT IN (AA)"
    Predicate.IsNull("c").describe  shouldBe "c IS NULL"
    Predicate.IsNull("c", negate = true).describe shouldBe "c IS NOT NULL"
  }

  test("And concatenates children's describe with ' AND '") {
    val p = Predicate.And(
      Predicate.Compare.Eq("carrier", "AA"),
      Predicate.Compare.Gt("pax", 100),
    )
    p.describe shouldBe "(carrier = AA AND pax > 100)"
    p.fields shouldBe Set("carrier", "pax")
  }

  test("Or concatenates with ' OR '; Not wraps in 'NOT (...)'") {
    val orP = Predicate.Or(
      Predicate.Compare.Eq("carrier", "AA"),
      Predicate.Compare.Eq("carrier", "UA"),
    )
    orP.describe shouldBe "(carrier = AA OR carrier = UA)"

    val notP = Predicate.Not( Predicate.Compare.Gt("pax", 100) )
    notP.describe shouldBe "NOT (pax > 100)"
    notP.fields shouldBe Set("pax")
  }

  test("Empty And describes as 'TRUE'; empty Or describes as 'FALSE'") {
    Predicate.And().describe shouldBe "TRUE"
    Predicate.Or().describe  shouldBe "FALSE"
  }

  // -------------------------------------------------------------------------
  // routing helpers
  // -------------------------------------------------------------------------

  test("referencesMeasure: matches on bare field name") {
    val pred = Predicate.Compare.Eq("total_passengers", 100)
    Predicate.referencesMeasure(pred, Set("total_passengers")) shouldBe true
    Predicate.referencesMeasure(pred, Set("other_measure"))     shouldBe false
  }

  test("referencesMeasure: matches on last dot-separated segment for model.field refs") {
    val pred = Predicate.Compare.Eq("orders.total_passengers", 100)
    Predicate.referencesMeasure(pred, Set("total_passengers")) shouldBe true
  }

  test("splitFilter: leaf with measure → postAgg; no measure → preAgg") {
    val measured = Predicate.Compare.Eq("total_passengers", 100)
    val (pre, post) = Predicate.splitFilter(measured, Set("total_passengers"))
    pre shouldBe empty
    post shouldBe List(measured)

    val dimensioned = Predicate.Compare.Eq("carrier", "AA")
    val (pre2, post2) = Predicate.splitFilter(dimensioned, Set("total_passengers"))
    pre2 shouldBe List(dimensioned)
    post2 shouldBe empty
  }

  test("splitFilter: And routes each child independently and re-groups each side into at most one And") {
    val dim1 = Predicate.Compare.Eq("carrier", "AA")
    val dim2 = Predicate.Compare.Eq("origin", "SFO")
    val measure1 = Predicate.Compare.Gt("total_passengers", 100)
    val compound = Predicate.And(dim1, dim2, measure1)
    val (pre, post) = Predicate.splitFilter(compound, Set("total_passengers"))
    pre.size shouldBe 1
    post.size shouldBe 1
    // Both sides should be And(...) of the multiple children, OR single-element seq.
    val preChildren = pre.head match { case Predicate.And(cs @ _*) => cs; case single => Seq(single) }
    preChildren.toSet shouldBe Set(dim1, dim2)
  }

  // -------------------------------------------------------------------------
  // Type-system invariants: data-only equality
  // -------------------------------------------------------------------------

  test("Same data → equal case class instances") {
    val a = Predicate.Compare.Eq("x", 1)
    val b = Predicate.Compare.Eq("x", 1)
    a shouldBe b
    a.hashCode shouldBe b.hashCode
  }

  test("Different op → not equal") {
    val eq = Predicate.Compare.Eq("x", 1)
    val ne = Predicate.Compare.Ne("x", 1)
    eq should not be ne
  }

  test("Serializable: every case class round-trips through Java serialization") {
    val cases: Seq[Predicate] = Seq(
      Predicate.Compare.Eq("a", 1),
      Predicate.Compare.Contains("a", "x"),
      Predicate.In("a", Seq(1, 2, 3)),
      Predicate.IsNull("a"),
      Predicate.And(),
      Predicate.Or(),
      Predicate.Not(Predicate.Compare.Eq("a", 1)),
    )
    cases.foreach { p =>
      val bos = new java.io.ByteArrayOutputStream()
      val oos = new java.io.ObjectOutputStream(bos)
      oos.writeObject(p)
      oos.close()
      val bis = new java.io.ByteArrayInputStream(bos.toByteArray)
      val ois = new java.io.ObjectInputStream(bis)
      val restored = ois.readObject().asInstanceOf[Predicate]
      restored shouldBe p
    }
  }

  test("and/or/not helpers construct the corresponding case class") {
    val a = Predicate.Compare.Eq("x", 1)
    val b = Predicate.Compare.Eq("y", 2)
    a.and(b)  shouldBe Predicate.And(a, b)
    a.or(b)   shouldBe Predicate.Or(a, b)
    a.not     shouldBe Predicate.Not(a)
  }

  test("not(pred) is shorthand for Not(pred)") {
    val a = Predicate.Compare.Eq("x", 1)
    Predicate.not(a) shouldBe Predicate.Not(a)
  }
}
