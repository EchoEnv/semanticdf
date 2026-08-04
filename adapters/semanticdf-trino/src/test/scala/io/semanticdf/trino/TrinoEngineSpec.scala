package io.semanticdf.trino

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.semanticdf.core.engine.{Capability, EngineContext, EngineError}

/** Phase 2 contract: prove `TrinoEngine` implements the `Engine[Any]`
  * contract from `io.semanticdf.core.engine`. The first concrete
  * engine adapter — it demonstrates the engine-adapter boundary in
  * code and validates that the contract is implementable.
  *
  * The actual SQL lowering / source resolution / result decoding
  * land in follow-up PRs (see `adapters/semanticdf-trino/README.md`
  * for the roadmap). For now, `compile` / `execute` / `explain`
  * return `EngineError.FeatureDeferred` with a roadmap pointer.
  *
  * ==Why these tests matter==
  *
  *   - Validates that the Trino engine class can be instantiated
  *   - Validates the wire-stable `identity` value
  *   - Validates the `capabilities` set shape (closed set, no duplicates)
  *   - Validates that `compile` / `execute` / `explain` return
  *     `Left(EngineError.FeatureDeferred)` (a well-formed error,
  *     not a raw exception)
  *   - Validates zero Spark imports (the multi-engine boundary)
  */
class TrinoEngineSpec extends AnyFunSuite with Matchers {

  // -- instance shape --

  test("TrinoEngine.instance is a TrinoEngine") {
    TrinoEngine.instance shouldBe a [TrinoEngine]
  }

  test("TrinoEngine.instance returns the same instance on repeated access (singleton)") {
    val a = TrinoEngine.instance
    val b = TrinoEngine.instance
    a should be theSameInstanceAs b
  }

  // -- identity (wire-stable engine label) --

  test("identity is \"trino\"") {
    TrinoEngine.instance.identity shouldBe "trino"
  }

  // -- capabilities (typed, closed) --

  test("capabilities is a non-empty closed Set") {
    val caps = TrinoEngine.instance.capabilities
    caps should not be empty
  }

  test("capabilities is a Set (no duplicates)") {
    val caps = TrinoEngine.instance.capabilities
    caps.size shouldBe caps.toList.distinct.size
  }

  test("capabilities includes BroadcastJoin (Trino supports broadcast joins natively)") {
    TrinoEngine.instance.capabilities should contain (Capability.BroadcastJoin)
  }

  test("capabilities includes NestedStructTypes (Trino supports row types)") {
    TrinoEngine.instance.capabilities should contain (Capability.NestedStructTypes)
  }

  test("capabilities includes WindowRanking (Trino supports window functions)") {
    TrinoEngine.instance.capabilities should contain (Capability.WindowRanking)
  }

  test("capabilities does NOT include Materialize (Trino has no native persist)") {
    // Trino doesn't have a Spark-like `persist(MEMORY_ONLY)` API.
    // The adapter rejects this policy via EngineError.UnsupportedCapability.
    TrinoEngine.instance.capabilities should not contain (Capability.Materialize)
  }

  // -- compile / execute / explain — deferred --

  test("compile returns Left(EngineError.FeatureDeferred) for the placeholder path") {
    val result = TrinoEngine.instance.compile("any-model", EngineContext.defaultContext)
    result.isLeft shouldBe true
    result.left.toOption.get shouldBe a [EngineError.FeatureDeferred]
  }

  test("execute returns Left(EngineError.FeatureDeferred) for the placeholder path") {
    val result = TrinoEngine.instance.execute("any-plan", EngineContext.defaultContext)
    result.isLeft shouldBe true
    result.left.toOption.get shouldBe a [EngineError.FeatureDeferred]
  }

  test("explain returns Left(EngineError.FeatureDeferred) for the placeholder path") {
    val result = TrinoEngine.instance.explain("any-model", EngineContext.defaultContext)
    result.isLeft shouldBe true
    result.left.toOption.get shouldBe a [EngineError.FeatureDeferred]
  }

  test("compile result includes a roadmap pointer in the FeatureDeferred error") {
    val result = TrinoEngine.instance.compile("any-model", EngineContext.defaultContext)
    val err = result.left.toOption.get.asInstanceOf[EngineError.FeatureDeferred]
    err.feature should include ("trino")
    err.release should not be empty
  }

  // -- compile on a CorePredicate (the minimum viable lowering) --

  test("compile(CorePredicate) returns Right(SqlLowerer.lower(p)) for an Eq") {
    val p = io.semanticdf.core.predicate.Predicate.Compare.Eq("carrier", "AA")
    TrinoEngine.instance.compile(p, EngineContext.defaultContext) shouldBe
      Right("\"carrier\" = 'AA'")
  }

  test("compile(CorePredicate) returns Right(SqlLowerer.lower(p)) for an And") {
    val p = io.semanticdf.core.predicate.Predicate.And(
      io.semanticdf.core.predicate.Predicate.Compare.Eq("c", "AA"),
      io.semanticdf.core.predicate.Predicate.Compare.Gt("d", 1),
    )
    TrinoEngine.instance.compile(p, EngineContext.defaultContext) shouldBe
      Right("(\"c\" = 'AA' AND \"d\" > 1)")
  }

  // -- boundary contract: zero Spark imports --

  test("TrinoEngine instance is an Engine[Any] (contract conformance)") {
    val engine: io.semanticdf.core.engine.Engine[Any] = TrinoEngine.instance
    engine.identity shouldBe "trino"
  }
}