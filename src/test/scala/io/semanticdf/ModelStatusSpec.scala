package io.semanticdf

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Unit tests for [[ModelStatus]] — the sealed trait + companion that
  * carries the lifecycle status of a [[SemanticTable]].
  *
  * The three values are wire-stable. Renaming is a breaking change to
  * MCP `describe_model`, `SemanticManifest`, and the YAML `status:` field.
  */
class ModelStatusSpec extends AnyFunSuite with Matchers {

  // -- wire format ------------------------------------------------------------

  test("each status has a stable lowercase wire string") {
    ModelStatus.Draft.asString       shouldBe "draft"
    ModelStatus.Published.asString   shouldBe "published"
    ModelStatus.Deprecated.asString  shouldBe "deprecated"
  }

  test("asString round-trips through fromString") {
    ModelStatus.all.foreach { s =>
      ModelStatus.fromString(s.asString) shouldBe Some(s)
    }
  }

  // -- parser -----------------------------------------------------------------

  test("fromString is case-insensitive") {
    ModelStatus.fromString("DRAFT")      shouldBe Some(ModelStatus.Draft)
    ModelStatus.fromString("Published")  shouldBe Some(ModelStatus.Published)
    ModelStatus.fromString("DEPRECATED") shouldBe Some(ModelStatus.Deprecated)
  }

  test("fromString returns None for unknown values (caller decides policy)") {
    ModelStatus.fromString("")           shouldBe None
    ModelStatus.fromString("retired")    shouldBe None
    ModelStatus.fromString("published ") shouldBe None  // trailing whitespace rejected
  }

  test("all returns the three known statuses") {
    ModelStatus.all.map(_.asString).toSet shouldBe Set("draft", "published", "deprecated")
  }
}