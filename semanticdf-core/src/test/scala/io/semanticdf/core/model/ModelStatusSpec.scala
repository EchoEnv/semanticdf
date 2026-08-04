package io.semanticdf.core.model

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Phase 1 increment 10: prove `io.semanticdf.core.model.ModelStatus`
  * is a usable, self-contained, Spark-free data record + pure helpers.
  *
  * ==Why this test file exists==
  *
  * The new package — `io.semanticdf.core.model` — contains the data-only
  * shape that future engine adapters (Trino, Databricks) will use to
  * surface model lifecycle status. It must compile and run with NO
  * Spark on the classpath. This test verifies both:
  *
  *   1. The sealed trait + 3 case objects behave correctly.
  *   2. The `asString` / `fromString` / `all` helpers are pure
  *      functions — same input always returns the same output, the
  *      wire-stable string contract is preserved.
  *
  * ==Data-driven mantra compliance==
  *
  * Every assertion checks data shape: case object identity, equality,
  * and the pure-function determinism. No `Map`-based dispatch, no
  * closures, no Spark imports. Per `scala-data-driven-refactor`
  * step 1, the sealed trait + case objects are data; the helper
  * functions are pure functions on that data.
  */
class ModelStatusSpec extends AnyFunSuite with Matchers {

  // -------------------------------------------------------------------------
  // Sealed trait + case objects
  // -------------------------------------------------------------------------

  test("ModelStatus has exactly three cases: Draft, Published, Deprecated") {
    val allCases: Set[ModelStatus] =
      Set(ModelStatus.Draft, ModelStatus.Published, ModelStatus.Deprecated)
    allCases.size shouldBe 3
  }

  test("Each case is a distinct singleton") {
    ModelStatus.Draft should not be ModelStatus.Published
    ModelStatus.Published should not be ModelStatus.Deprecated
    ModelStatus.Draft should not be ModelStatus.Deprecated

    ModelStatus.Draft shouldBe ModelStatus.Draft
    ModelStatus.Published shouldBe ModelStatus.Published
    ModelStatus.Deprecated shouldBe ModelStatus.Deprecated
  }

  test("Sealed exhaustiveness: pattern-match over all 3 cases") {
    val examples: Seq[ModelStatus] =
      Seq(ModelStatus.Draft, ModelStatus.Published, ModelStatus.Deprecated)
    examples.foreach {
      case ModelStatus.Draft      => ()
      case ModelStatus.Published  => ()
      case ModelStatus.Deprecated => ()
    }
  }

  // -------------------------------------------------------------------------
  // Wire format: asString
  // -------------------------------------------------------------------------

  test("each status has a stable lowercase wire string") {
    ModelStatus.Draft.asString      shouldBe "draft"
    ModelStatus.Published.asString  shouldBe "published"
    ModelStatus.Deprecated.asString shouldBe "deprecated"
  }

  test("asString values are wire-stable (literal match)") {
    // Per the original's contract: these strings are part of the MCP
    // wire format and the YAML `status:` field. Renaming is a breaking
    // change. This test locks in the contract.
    ModelStatus.Draft.asString      shouldBe "draft"
    ModelStatus.Published.asString  shouldBe "published"
    ModelStatus.Deprecated.asString shouldBe "deprecated"
  }

  // -------------------------------------------------------------------------
  // Parser: fromString
  // -------------------------------------------------------------------------

  test("fromString is case-insensitive on input") {
    ModelStatus.fromString("DRAFT")      shouldBe Some(ModelStatus.Draft)
    ModelStatus.fromString("Published")  shouldBe Some(ModelStatus.Published)
    ModelStatus.fromString("DEPRECATED") shouldBe Some(ModelStatus.Deprecated)
    ModelStatus.fromString("draft")      shouldBe Some(ModelStatus.Draft)
    ModelStatus.fromString("published")  shouldBe Some(ModelStatus.Published)
    ModelStatus.fromString("deprecated") shouldBe Some(ModelStatus.Deprecated)
  }

  test("fromString returns None for unknown values") {
    ModelStatus.fromString("")           shouldBe None
    ModelStatus.fromString("retired")    shouldBe None
    ModelStatus.fromString("published ") shouldBe None  // trailing whitespace rejected
    ModelStatus.fromString("garbage")    shouldBe None
  }

  test("asString round-trips through fromString for all cases") {
    ModelStatus.all.foreach { s =>
      ModelStatus.fromString(s.asString) shouldBe Some(s)
    }
  }

  // -------------------------------------------------------------------------
  // all: helper
  // -------------------------------------------------------------------------

  test("all returns the three known statuses in display order") {
    ModelStatus.all shouldBe Seq(
      ModelStatus.Draft,
      ModelStatus.Published,
      ModelStatus.Deprecated,
    )
  }

  test("all.map(_.asString) yields the wire-stable string set") {
    ModelStatus.all.map(_.asString).toSet shouldBe
      Set("draft", "published", "deprecated")
  }
}