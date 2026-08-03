package io.semanticdf.core.field

import io.semanticdf.core.predicate.{Predicate => CorePredicate}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

// The wildcard `import io.semanticdf.core.field._` brings `FieldRefOps`
// (the object) into implicit scope, but Scala 2.13 doesn't propagate
// implicit classes through wildcard imports on the containing package.
// The original `PredicateOpsSpec` works around this by adding a scoped
// `import io.semanticdf.predicate.PredicateOps._` INSIDE the class body.
// We mirror that pattern here.

/** Phase 1 increment 6: prove `io.semanticdf.core.field.FieldRefOps`
  * produces engine-portable `io.semanticdf.core.predicate.Predicate`
  * values from typed field references, mirroring the original
  * `io.semanticdf.predicate.PredicateOps.FieldRefOps` exactly except
  * for the ADT it targets.
  *
  * ==Why this test file exists==
  *
  * The new operator surface is the engine-portable counterpart of the
  * original infix DSL. Each operator must produce the matching
  * `CorePredicate.Compare` / `CorePredicate.In` / `CorePredicate.IsNull`
  * case class so that downstream code (future fluent API migration,
  * wire-format encoders, audit/cache chain) can consume the result
  * without re-constructing it.
  *
  * ==Data-driven mantra compliance==
  *
  * Every assertion checks data shape: the operator produces a specific
  * `CorePredicate` case class with the right `field` and `value`. No
  * `Map`-based dispatch — the operator directly constructs the
  * matching sealed case class. Per
  * `scala-data-driven-refactor` step 1, the boundary here is the
  * operator method; downstream code trusts the produced `CorePredicate`
  * without re-validating.
  */
class FieldRefOpsSpec extends AnyFunSuite with Matchers {
  // Scoped import INSIDE the class body — mirrors the original
  // PredicateOpsSpec's `import io.semanticdf.predicate.PredicateOps._`.
  // Scala 2.13 only brings implicit classes into scope via scoped imports
  // of the containing object — wildcard package imports don't suffice.
  import FieldRefOps._

  // -------------------------------------------------------------------------
  // Test fixtures: typed field references
  // -------------------------------------------------------------------------

  // Phantom type tags. Their static types let us pass dimension refs to
  // dimension-only methods and measure refs to measure-only methods,
  // but the tests below don't depend on the dimension/measure split —
  // they exercise the operators on both kinds.
  sealed trait Carrier
  sealed trait TotalPax

  implicit val carrier: SemanticDimension[Carrier] =
    SemanticDimension.of[Carrier]("carrier")
  implicit val pax: SemanticMeasure[TotalPax] =
    SemanticMeasure.of[TotalPax]("total_passengers")

  // -------------------------------------------------------------------------
  // Compare family operators
  // -------------------------------------------------------------------------

  test("=== produces Compare.Eq with the field's name and the value") {
    val pred: CorePredicate = carrier === "AA"
    pred shouldBe CorePredicate.Compare.Eq("carrier", "AA")
  }

  test("=!= produces Compare.Ne") {
    val pred: CorePredicate = carrier =!= "AA"
    pred shouldBe CorePredicate.Compare.Ne("carrier", "AA")
  }

  test("> produces Compare.Gt") {
    (pax > 100).shouldBe(CorePredicate.Compare.Gt("total_passengers", 100))
  }

  test(">= produces Compare.Ge") {
    (pax >= 100).shouldBe(CorePredicate.Compare.Ge("total_passengers", 100))
  }

  test("< produces Compare.Lt") {
    (pax < 100).shouldBe(CorePredicate.Compare.Lt("total_passengers", 100))
  }

  test("<= produces Compare.Le") {
    (pax <= 100).shouldBe(CorePredicate.Compare.Le("total_passengers", 100))
  }

  // -------------------------------------------------------------------------
  // IsNull family operators
  // -------------------------------------------------------------------------

  test("isNull produces IsNull with negate=false") {
    carrier.isNull shouldBe new CorePredicate.IsNull("carrier", negate = false)
  }

  test("isNotNull produces IsNull with negate=true") {
    carrier.isNotNull shouldBe new CorePredicate.IsNull("carrier", negate = true)
  }

  // -------------------------------------------------------------------------
  // String operators
  // -------------------------------------------------------------------------

  test("contains produces Compare.Contains") {
    carrier.contains("AA") shouldBe CorePredicate.Compare.Contains("carrier", "AA")
  }

  test("startsWith produces Compare.StartsWith") {
    carrier.startsWith("AA") shouldBe CorePredicate.Compare.StartsWith("carrier", "AA")
  }

  test("endsWith produces Compare.EndsWith") {
    carrier.endsWith("AA") shouldBe CorePredicate.Compare.EndsWith("carrier", "AA")
  }

  test("arrayContains produces Compare.ArrayContains") {
    carrier.arrayContains("x") shouldBe CorePredicate.Compare.ArrayContains("carrier", "x")
  }

  // -------------------------------------------------------------------------
  // In operators
  // -------------------------------------------------------------------------

  test("isin produces In (non-negated) with values converted to Seq[Any]") {
    val values: Seq[String] = Seq("AA", "UA", "DL")
    carrier.isin(values) shouldBe CorePredicate.In("carrier", Seq("AA", "UA", "DL"), negate = false)
  }

  test("isin accepts any Iterable (Set, List, etc.)") {
    carrier.isin(Set("AA", "UA")) shouldBe CorePredicate.In("carrier", Seq("AA", "UA"), negate = false)
    carrier.isin(List("AA", "UA")) shouldBe CorePredicate.In("carrier", Seq("AA", "UA"), negate = false)
  }

  test("notin produces In (negated)") {
    carrier.notin(Seq("AA")) shouldBe CorePredicate.In("carrier", Seq("AA"), negate = true)
  }

  // -------------------------------------------------------------------------
  // Combined: fluent chains produce CorePredicate trees
  // -------------------------------------------------------------------------

  test("and / or / not helpers work on CorePredicate") {
    val a = (carrier === "AA")
    val b = (pax > 100)
    val combined = a.and(b)
    combined shouldBe CorePredicate.And(
      CorePredicate.Compare.Eq("carrier", "AA"),
      CorePredicate.Compare.Gt("total_passengers", 100),
    )
    combined.or(b) shouldBe CorePredicate.Or(
      CorePredicate.And(
        CorePredicate.Compare.Eq("carrier", "AA"),
        CorePredicate.Compare.Gt("total_passengers", 100),
      ),
      CorePredicate.Compare.Gt("total_passengers", 100),
    )
    a.not shouldBe CorePredicate.Not(CorePredicate.Compare.Eq("carrier", "AA"))
  }

  test("Complex expression: ((carrier === 'AA') or (pax > 100)) and carrier.isNotNull") {
    val complex = ((carrier === "AA") or (pax > 100)).and(carrier.isNotNull)
    complex shouldBe CorePredicate.And(
      CorePredicate.Or(
        CorePredicate.Compare.Eq("carrier", "AA"),
        CorePredicate.Compare.Gt("total_passengers", 100),
      ),
      new CorePredicate.IsNull("carrier", negate = true),
    )
  }

  // -------------------------------------------------------------------------
  // Compile-time safety (compile-only tests; the value type proves it)
  // -------------------------------------------------------------------------

  test("Result type is CorePredicate, not the original Spark-bearing Predicate") {
    // Type assertion: the infix operator produces an engine-portable
    // core predicate. Downstream code can use this without depending on
    // io.semanticdf.predicate.Predicate (which would force a Spark import).
    val pred: CorePredicate = carrier === "AA"
    // If the operator returned the original Predicate, this assignment
    // would fail to compile (incompatible types).
    val coreTyped: CorePredicate = pred
    coreTyped shouldBe CorePredicate.Compare.Eq("carrier", "AA")
  }

  test("Operators work on both SemanticDimension and SemanticMeasure witnesses") {
    // Both `carrier` (dimension) and `pax` (measure) inherit from
    // SemanticField — the implicit class fires for both kinds.
    val dimPred: CorePredicate = carrier === "AA"
    val measPred: CorePredicate = pax > 100
    dimPred shouldBe CorePredicate.Compare.Eq("carrier", "AA")
    measPred shouldBe CorePredicate.Compare.Gt("total_passengers", 100)
  }

  // -------------------------------------------------------------------------
  // Parity with the original PredicateOps (per the design's invariant)
  // -------------------------------------------------------------------------

  test("Parity: every operator name maps to the matching original") {
    // The new FieldRefOps operators must mirror the original's operator
    // names and shape. This is the design's invariant: the core surface
    // is structurally identical to the original. If the original changes
    // a signature, this test would NOT catch it (because we mirror at
    // import time, not runtime), but it documents the contract.
    val pred: CorePredicate = carrier === "AA"
    pred.getClass.getSimpleName shouldBe "Eq"
    // Compare family (10 case classes)
    Seq(
      carrier === "AA",
      carrier =!= "AA",
      pax < 100,
      pax <= 100,
      pax > 100,
      pax >= 100,
      carrier.contains("AA"),
      carrier.startsWith("AA"),
      carrier.endsWith("AA"),
      carrier.arrayContains("x"),
    ).map(_.getClass.getSimpleName).toSet shouldBe Set(
      "Eq", "Ne", "Lt", "Le", "Gt", "Ge",
      "Contains", "StartsWith", "EndsWith", "ArrayContains",
    )
  }

  // -------------------------------------------------------------------------
  // Data-only verification: every result round-trips through equality
  // -------------------------------------------------------------------------

  test("Every operator result has structural equality with a directly-constructed predicate") {
    // Same field name from `ref.name`, same value, same case class →
    // shouldBe equality holds. This is the data-driven mantra: equality
    // is auto-derived for `final case class`.
    val cases: Seq[(CorePredicate, CorePredicate)] = Seq(
      (carrier === "AA",    CorePredicate.Compare.Eq("carrier", "AA")),
      (carrier =!= "AA",    CorePredicate.Compare.Ne("carrier", "AA")),
      (pax > 100,           CorePredicate.Compare.Gt("total_passengers", 100)),
      (pax >= 100,          CorePredicate.Compare.Ge("total_passengers", 100)),
      (pax < 100,           CorePredicate.Compare.Lt("total_passengers", 100)),
      (pax <= 100,          CorePredicate.Compare.Le("total_passengers", 100)),
      (carrier.contains("AA"),    CorePredicate.Compare.Contains("carrier", "AA")),
      (carrier.startsWith("AA"),  CorePredicate.Compare.StartsWith("carrier", "AA")),
      (carrier.endsWith("AA"),    CorePredicate.Compare.EndsWith("carrier", "AA")),
      (carrier.arrayContains("x"), CorePredicate.Compare.ArrayContains("carrier", "x")),
      (carrier.isin(Seq("AA")),   CorePredicate.In("carrier", Seq("AA"), negate = false)),
      (carrier.notin(Seq("AA")),  CorePredicate.In("carrier", Seq("AA"), negate = true)),
      (carrier.isNull,            new CorePredicate.IsNull("carrier", negate = false)),
      (carrier.isNotNull,         new CorePredicate.IsNull("carrier", negate = true)),
    )
    cases.foreach { case (fromOps, direct) =>
      fromOps shouldBe direct
    }
  }
}