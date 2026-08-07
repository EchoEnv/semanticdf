package io.semanticdf.core.engine

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.semanticdf.core.rel.JoinKind

/** Tests for [EngineCapabilities] (PR 9 of the 12-PR triage plan).
  *
  * The structured value object replaces the loose
  * `Map[Capability, String]` that used to be returned by
  * `Engine.describeCapabilities`. These tests pin the contract. */
class EngineCapabilitiesSpec extends AnyFunSuite with Matchers {

  // -- describe(): convenience method --

  test("describe returns Some(description) for described capabilities") {
    val ec = EngineCapabilities(
      identity     = "test-engine",
      descriptions = Map(Capability.BroadcastJoin -> "supports broadcast"),
    )
    ec.describe(Capability.BroadcastJoin) shouldBe Some("supports broadcast")
  }

  test("describe returns None for undescribed capabilities") {
    val ec = EngineCapabilities(
      identity     = "test-engine",
      descriptions = Map.empty,
    )
    ec.describe(Capability.SkewJoin) shouldBe None
  }

  // -- described: Set view of descriptions --

  test("described returns the keys of descriptions") {
    val ec = EngineCapabilities(
      identity     = "test-engine",
      descriptions = Map(
        Capability.BroadcastJoin -> "supports broadcast",
        Capability.SkewJoin -> "supports skew",
      ),
    )
    ec.described shouldBe Set(Capability.BroadcastJoin, Capability.SkewJoin)
  }

  // -- structured fields default to empty/false --

  test("supportedJoinKinds defaults to empty (not advertised)") {
    val ec = EngineCapabilities(
      identity     = "test-engine",
      descriptions = Map.empty,
    )
    ec.supportedJoinKinds shouldBe Set.empty
  }

  test("supportsRollup defaults to false") {
    val ec = EngineCapabilities(
      identity     = "test-engine",
      descriptions = Map.empty,
    )
    ec.supportsRollup shouldBe false
  }

  test("supportsMaterialize defaults to false") {
    val ec = EngineCapabilities(
      identity     = "test-engine",
      descriptions = Map.empty,
    )
    ec.supportsMaterialize shouldBe false
  }

  // -- structured fields populate when set --

  test("supportedJoinKinds can be populated with the 5-case JoinKind ADT") {
    val allFive: Set[JoinKind] = Set(
      JoinKind.Inner, JoinKind.Left, JoinKind.Right, JoinKind.Full, JoinKind.Cross,
    )
    val ec = EngineCapabilities(
      identity           = "test-engine",
      descriptions       = Map.empty,
      supportedJoinKinds = allFive,
    )
    ec.supportedJoinKinds shouldBe allFive
  }

  test("supportsRollup can be set to true") {
    val ec = EngineCapabilities(
      identity       = "test-engine",
      descriptions   = Map.empty,
      supportsRollup = true,
    )
    ec.supportsRollup shouldBe true
  }

  test("supportsMaterialize can be set to true") {
    val ec = EngineCapabilities(
      identity           = "test-engine",
      descriptions       = Map.empty,
      supportsMaterialize = true,
    )
    ec.supportsMaterialize shouldBe true
  }

  // -- data-driven contract: equality + hashing --

  test("Two EngineCapabilities with the same fields are equal") {
    val a = EngineCapabilities(
      identity     = "x",
      descriptions = Map(Capability.BroadcastJoin -> "y"),
    )
    val b = EngineCapabilities(
      identity     = "x",
      descriptions = Map(Capability.BroadcastJoin -> "y"),
    )
    a shouldBe b
    a.hashCode shouldBe b.hashCode
  }

  test("Two EngineCapabilities with different supportedJoinKinds are not equal") {
    val a = EngineCapabilities(
      identity = "x", descriptions = Map.empty,
      supportedJoinKinds = Set(JoinKind.Inner),
    )
    val b = EngineCapabilities(
      identity = "x", descriptions = Map.empty,
      supportedJoinKinds = Set.empty,
    )
    a should not be b
  }

  // -- Engine trait integration --

  test("Engine.describeCapabilities default impl produces an EngineCapabilities from capabilities") {
    // A minimal in-test Engine implementation
    val engine = new Engine[String] {
      override def identity: String = "minimal"
      override def capabilities: Set[Capability] = Set(
        Capability.BroadcastJoin, Capability.SkewJoin,
      )
      override def compile(
          model: io.semanticdf.core.model.Model,
          ctx:   EngineContext,
      ): Either[EngineError, ExecutionPlan[String]] =
        Right(ExecutionPlan(
          engine = EngineIdentity("minimal", "0.0.1", "0.0.1"),
          native = "minimal-plan",
        ))
      override def compile(
          plan: io.semanticdf.core.rel.RelOp,
          ctx:  EngineContext,
      ): Either[EngineError, ExecutionPlan[String]] =
        Right(ExecutionPlan(
          engine = EngineIdentity("minimal", "0.0.1", "0.0.1"),
          native = "minimal-plan",
        ))
      override def execute(
          plan: ExecutionPlan[String],
          ctx:  EngineContext,
      ): Either[EngineError, String] = Right("ok")
      override def explain(
          model: io.semanticdf.core.model.Model,
          ctx:   EngineContext,
      ): Either[EngineError, String] = Right("ok")
    }
    val ec = engine.describeCapabilities
    ec.identity shouldBe "minimal"
    ec.described shouldBe Set(Capability.BroadcastJoin, Capability.SkewJoin)
    ec.supportedJoinKinds shouldBe Set.empty
    ec.supportsRollup shouldBe false
    ec.supportsMaterialize shouldBe false
  }
}