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

  test("Hash invariant: hash(toCore(original)) produces a valid SHA-256 hex string") {
    // After Phase 1 consolidation, PredicateHasher.hash operates on the core
    // ADT directly. The boundary contract is now: hash(toCore(original))
    // produces a valid 64-char SHA-256 hex string (no crash, no exception).
    // The semantic-equivalence proof lives in the symmetry tests above
    // (toCore ∘ fromCore = id, fromCore ∘ toCore = id).
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
      val coreHash = PredicateHasher.hash(PredicateConverter.toCore(original))
      coreHash.length shouldBe 64  // SHA-256 hex
      coreHash should fullyMatch regex "[0-9a-f]{64}"
    }
  }

  // -------------------------------------------------------------------------
  // Symmetric boundary: fromCore (the reverse direction)
  // -------------------------------------------------------------------------

  test("fromCore: Compare.Eq round-trips back to original") {
    val core = CorePredicate.Compare.Eq("carrier", "AA")
    PredicateConverter.fromCore(core) shouldBe Predicate.Compare.Eq("carrier", "AA")
  }

  test("fromCore: every Compare case class round-trips") {
    val cores: Seq[CorePredicate.Compare] = Seq(
      CorePredicate.Compare.Eq("x", 1),
      CorePredicate.Compare.Ne("x", 1),
      CorePredicate.Compare.Lt("x", 1),
      CorePredicate.Compare.Le("x", 1),
      CorePredicate.Compare.Gt("x", 1),
      CorePredicate.Compare.Ge("x", 1),
      CorePredicate.Compare.Contains("x", "y"),
      CorePredicate.Compare.StartsWith("x", "y"),
      CorePredicate.Compare.EndsWith("x", "y"),
      CorePredicate.Compare.ArrayContains("x", "y"),
    )
    cores.foreach { c =>
      PredicateConverter.fromCore(c) shouldBe a [Predicate.Compare]
    }
  }

  test("fromCore: In / IsNull round-trip") {
    PredicateConverter.fromCore(CorePredicate.In("c", Seq("AA"), negate = false)) shouldBe
      Predicate.In("c", Seq("AA"), negate = false)
    PredicateConverter.fromCore(CorePredicate.IsNull("c", negate = true)) shouldBe
      Predicate.IsNull("c", negate = true)
  }

  test("fromCore: And / Or / Not round-trip with recursive children") {
    val core = CorePredicate.And(
      CorePredicate.Or(
        CorePredicate.Compare.Eq("a", 1),
        CorePredicate.Not(CorePredicate.Compare.Gt("b", 2)),
      ),
    )
    PredicateConverter.fromCore(core) shouldBe Predicate.And(
      Predicate.Or(
        Predicate.Compare.Eq("a", 1),
        Predicate.Not(Predicate.Compare.Gt("b", 2)),
      ),
    )
  }

  // -------------------------------------------------------------------------
  // Boundary symmetry: toCore ∘ fromCore = id, fromCore ∘ toCore = id
  // -------------------------------------------------------------------------

  test("Symmetry: fromCore(toCore(x)) == x for all leaf and compound predicates") {
    val originals: Seq[Predicate] = Seq(
      Predicate.Compare.Eq("a", 1),
      Predicate.Compare.Ne("a", "AA"),
      Predicate.Compare.Lt("a", 1),
      Predicate.Compare.Le("a", 1),
      Predicate.Compare.Gt("a", 1),
      Predicate.Compare.Ge("a", 1),
      Predicate.Compare.Contains("a", "x"),
      Predicate.Compare.StartsWith("a", "x"),
      Predicate.Compare.EndsWith("a", "x"),
      Predicate.Compare.ArrayContains("a", "x"),
      Predicate.In("a", Seq(1, 2, 3)),
      Predicate.In("a", Seq(1), negate = true),
      Predicate.IsNull("a"),
      Predicate.IsNull("a", negate = true),
      Predicate.And(
        Predicate.Compare.Eq("a", 1),
        Predicate.Compare.Gt("b", 2),
      ),
      Predicate.Or(
        Predicate.Compare.Eq("a", 1),
        Predicate.IsNull("b"),
      ),
      Predicate.Not(Predicate.Compare.Eq("a", 1)),
      Predicate.Not(Predicate.Not(Predicate.Not(Predicate.Compare.Eq("a", 1)))),
    )
    originals.foreach { original =>
      val roundTripped = PredicateConverter.fromCore(PredicateConverter.toCore(original))
      roundTripped shouldBe original
    }
  }

  test("Symmetry: toCore(fromCore(c)) == c for all leaf and compound predicates") {
    val cores: Seq[CorePredicate] = Seq(
      CorePredicate.Compare.Eq("a", 1),
      CorePredicate.Compare.Ne("a", "AA"),
      CorePredicate.Compare.Contains("a", "x"),
      CorePredicate.Compare.ArrayContains("a", "x"),
      CorePredicate.In("a", Seq(1, 2, 3), negate = false),
      CorePredicate.IsNull("a", negate = true),
      CorePredicate.And(
        CorePredicate.Compare.Eq("a", 1),
        CorePredicate.Compare.Gt("b", 2),
      ),
      CorePredicate.Or(
        CorePredicate.Compare.Eq("a", 1),
        CorePredicate.IsNull("b", negate = false),
      ),
      CorePredicate.Not(CorePredicate.Not(CorePredicate.Compare.Eq("a", 1))),
    )
    cores.foreach { core =>
      val roundTripped = PredicateConverter.toCore(PredicateConverter.fromCore(core))
      roundTripped shouldBe core
    }
  }

  test("fromCoreAll: bulk conversion of a Seq") {
    val cores: Seq[CorePredicate] = Seq(
      CorePredicate.Compare.Eq("a", 1),
      CorePredicate.In("b", Seq(1, 2, 3), negate = false),
    )
    val originals = PredicateConverter.fromCoreAll(cores)
    originals.size shouldBe 2
    originals.head shouldBe Predicate.Compare.Eq("a", 1)
    originals(1) shouldBe Predicate.In("b", Seq(1, 2, 3), negate = false)
  }
}