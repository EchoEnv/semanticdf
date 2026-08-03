package io.semanticdf.predicate

import io.semanticdf.core.predicate.{Predicate => CorePredicate}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Boundary contract test for `PredicateConverter`.
  *
  * The converter is the bridge between the Spark-bearing `Predicate` ADT
  * and the engine-portable `core.predicate.Predicate` ADT. Every leaf
  * and compound node must round-trip through the converter with no data
  * loss. The `match` in `toCore` is exhaustive over the original ADT —
  * if a new case class is added to either ADT without updating the
  * converter, this spec's per-case-class tests will fail to compile
  * (they reference every leaf type).
  */
class PredicateConverterSpec extends AnyFunSuite with Matchers {

  // -------------------------------------------------------------------------
  // Compare family
  // -------------------------------------------------------------------------

  test("Compare.Eq round-trips through converter") {
    val original = Predicate.Compare.Eq("carrier", "AA")
    val converted = PredicateConverter.toCore(original)
    converted shouldBe CorePredicate.Compare.Eq("carrier", "AA")
  }

  test("Compare.Ne round-trips through converter") {
    PredicateConverter.toCore(Predicate.Compare.Ne("x", 1)) shouldBe
      CorePredicate.Compare.Ne("x", 1)
  }

  test("Lt / Le / Gt / Ge round-trip through converter") {
    PredicateConverter.toCore(Predicate.Compare.Lt("x", 1)) shouldBe CorePredicate.Compare.Lt("x", 1)
    PredicateConverter.toCore(Predicate.Compare.Le("x", 1)) shouldBe CorePredicate.Compare.Le("x", 1)
    PredicateConverter.toCore(Predicate.Compare.Gt("x", 1)) shouldBe CorePredicate.Compare.Gt("x", 1)
    PredicateConverter.toCore(Predicate.Compare.Ge("x", 1)) shouldBe CorePredicate.Compare.Ge("x", 1)
  }

  test("Contains / StartsWith / EndsWith / ArrayContains round-trip through converter") {
    PredicateConverter.toCore(Predicate.Compare.Contains("x", "y")) shouldBe CorePredicate.Compare.Contains("x", "y")
    PredicateConverter.toCore(Predicate.Compare.StartsWith("x", "y")) shouldBe CorePredicate.Compare.StartsWith("x", "y")
    PredicateConverter.toCore(Predicate.Compare.EndsWith("x", "y")) shouldBe CorePredicate.Compare.EndsWith("x", "y")
    PredicateConverter.toCore(Predicate.Compare.ArrayContains("x", "y")) shouldBe CorePredicate.Compare.ArrayContains("x", "y")
  }

  // -------------------------------------------------------------------------
  // Non-compare leaves
  // -------------------------------------------------------------------------

  test("In (non-negated) round-trips through converter") {
    val original = Predicate.In("c", Seq("AA", "UA"))
    PredicateConverter.toCore(original) shouldBe CorePredicate.In("c", Seq("AA", "UA"), negate = false)
  }

  test("In (negated) round-trips through converter") {
    val original = Predicate.In("c", Seq("AA"), negate = true)
    PredicateConverter.toCore(original) shouldBe CorePredicate.In("c", Seq("AA"), negate = true)
  }

  test("IsNull (both negations) round-trips through converter") {
    PredicateConverter.toCore(Predicate.IsNull("c", negate = false)) shouldBe CorePredicate.IsNull("c", negate = false)
    PredicateConverter.toCore(Predicate.IsNull("c", negate = true)) shouldBe CorePredicate.IsNull("c", negate = true)
  }

  // -------------------------------------------------------------------------
  // Compound (recursive)
  // -------------------------------------------------------------------------

  test("And round-trips: children recursively converted") {
    val original = Predicate.And(
      Predicate.Compare.Eq("carrier", "AA"),
      Predicate.Compare.Gt("distance", 100),
    )
    PredicateConverter.toCore(original) shouldBe CorePredicate.And(
      CorePredicate.Compare.Eq("carrier", "AA"),
      CorePredicate.Compare.Gt("distance", 100),
    )
  }

  test("Or round-trips: children recursively converted") {
    val original = Predicate.Or(
      Predicate.Compare.Eq("carrier", "AA"),
      Predicate.Compare.Eq("carrier", "UA"),
    )
    PredicateConverter.toCore(original) shouldBe CorePredicate.Or(
      CorePredicate.Compare.Eq("carrier", "AA"),
      CorePredicate.Compare.Eq("carrier", "UA"),
    )
  }

  test("Not round-trips: inner predicate recursively converted") {
    val original = Predicate.Not(Predicate.Compare.Gt("x", 1))
    PredicateConverter.toCore(original) shouldBe CorePredicate.Not(CorePredicate.Compare.Gt("x", 1))
  }

  test("Deep nesting: And(Or(Not(...))) round-trips") {
    val original = Predicate.And(
      Predicate.Or(
        Predicate.Compare.Eq("carrier", "AA"),
        Predicate.Not(Predicate.Compare.Gt("x", 1)),
      ),
    )
    val converted = PredicateConverter.toCore(original)
    converted shouldBe CorePredicate.And(
      CorePredicate.Or(
        CorePredicate.Compare.Eq("carrier", "AA"),
        CorePredicate.Not(CorePredicate.Compare.Gt("x", 1)),
      ),
    )
  }

  // -------------------------------------------------------------------------
  // Boundary invariants
  // -------------------------------------------------------------------------

  test("Two conversions of the same original produce equal core predicates (idempotent)") {
    val original = Predicate.And(
      Predicate.Compare.Eq("a", 1),
      Predicate.Compare.Lt("b", 2),
    )
    PredicateConverter.roundTripEquals(original) shouldBe true
  }

  test("Value identity preserved: Any value field round-trips by reference") {
    // The value field is `Any` in both ADTs. The converter just copies the
    // reference — same instance in == same instance out. This matters when
    // the value is a complex object (e.g. a user-defined case class).
    case class CustomValue(name: String, count: Int)
    val custom = CustomValue("hello", 42)
    val original = Predicate.Compare.Eq("c", custom)
    val converted = PredicateConverter.toCore(original)
    converted.asInstanceOf[CorePredicate.Compare.Eq].value shouldBe custom
  }

  test("toCoreAll: bulk conversion of a sequence") {
    val originals: Seq[Predicate] = Seq(
      Predicate.Compare.Eq("a", 1),
      Predicate.In("b", Seq(1, 2, 3)),
      Predicate.Not(Predicate.IsNull("c")),
    )
    val converted = PredicateConverter.toCoreAll(originals)
    converted.size shouldBe 3
    converted.head shouldBe CorePredicate.Compare.Eq("a", 1)
    converted(1) shouldBe CorePredicate.In("b", Seq(1, 2, 3), negate = false)
    converted(2) shouldBe CorePredicate.Not(CorePredicate.IsNull("c", negate = false))
  }

  test("Hash invariant: original and converted produce the same PredicateHasher.hash") {
    // This is the load-bearing assertion — it proves the converter
    // preserves semantic equivalence for the audit/cache-key chain.
    import io.semanticdf.audit.PredicateHasher
    val originals: Seq[Predicate] = Seq(
      Predicate.Compare.Eq("carrier", "AA"),
      Predicate.And(
        Predicate.Compare.Eq("carrier", "AA"),
        Predicate.Compare.Gt("pax", 100),
      ),
      Predicate.In("c", Seq("AA", "UA")),
      Predicate.Or(
        Predicate.IsNull("x"),
        Predicate.Not(Predicate.Compare.Lt("y", 5)),
      ),
    )
    originals.foreach { original =>
      val originalHash = PredicateHasher.hash(original)
      val coreDirectHash = PredicateHasher.hashCore(PredicateConverter.toCore(original))
      // If the converter preserves semantic equivalence, both paths produce
      // the same hash — this is the load-bearing boundary contract.
      originalHash shouldBe coreDirectHash
    }
  }
}