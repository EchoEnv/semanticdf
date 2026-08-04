package io.semanticdf.core.engine

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Phase 2 contract: prove the `Capability` sealed trait + closed
  * enumeration work as a typed diagnostic name. Pure data, no Spark.
  */
class CapabilitySpec extends AnyFunSuite with Matchers {

  // -- enumeration values --

  test("NestedStructTypes is a Capability with name \"nested-struct-types\"") {
    Capability.NestedStructTypes.name shouldBe "nested-struct-types"
  }

  test("BroadcastJoin is a Capability with name \"broadcast-join\"") {
    Capability.BroadcastJoin.name shouldBe "broadcast-join"
  }

  test("SkewJoin is a Capability with name \"skew-join\"") {
    Capability.SkewJoin.name shouldBe "skew-join"
  }

  test("WindowRanking is a Capability with name \"window-ranking\"") {
    Capability.WindowRanking.name shouldBe "window-ranking"
  }

  test("Materialize is a Capability with name \"materialize\"") {
    Capability.Materialize.name shouldBe "materialize"
  }

  test("LateBinding is a Capability with name \"late-binding\"") {
    Capability.LateBinding.name shouldBe "late-binding"
  }

  // -- Named factory for user-defined capabilities --

  test("Named factory creates a Capability with the given name") {
    val c = Capability.Named("custom-engine-feature")
    c.name shouldBe "custom-engine-feature"
  }

  // -- data-driven contract: equality + hashing --

  test("Two Capabilities with the same name are equal") {
    Capability.Named("x") shouldBe Capability.Named("x")
  }

  test("Two Capabilities with different names are not equal") {
    Capability.Named("x") should not be Capability.Named("y")
  }

  test("Closed enumeration values are distinct singletons") {
    val all: Set[Capability] = Set(
      Capability.NestedStructTypes,
      Capability.BroadcastJoin,
      Capability.SkewJoin,
      Capability.WindowRanking,
      Capability.Materialize,
      Capability.LateBinding,
    )
    all.size shouldBe 6
  }

  test("Enumeration values are case objects (singletons)") {
    Capability.NestedStructTypes shouldBe Capability.NestedStructTypes
  }

  test("Sealed exhaustiveness: pattern-match over the 6 closed cases") {
    val examples: Seq[Capability] = Seq(
      Capability.NestedStructTypes,
      Capability.BroadcastJoin,
      Capability.SkewJoin,
      Capability.WindowRanking,
      Capability.Materialize,
      Capability.LateBinding,
    )
    examples.foreach {
      case Capability.NestedStructTypes => ()
      case Capability.BroadcastJoin      => ()
      case Capability.SkewJoin           => ()
      case Capability.WindowRanking      => ()
      case Capability.Materialize        => ()
      case Capability.LateBinding       => ()
    }
  }
}