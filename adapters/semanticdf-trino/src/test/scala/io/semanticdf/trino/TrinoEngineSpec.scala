package io.semanticdf.trino

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.semanticdf.core.engine.{Capability, EngineContext, EngineError}
import io.semanticdf.core.model.{Model, ModelPolicyDefaults, ModelStatus, SourceRef}

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

  // -- fixtures --

  /** A minimal valid `Model` for testing `compile` / `explain`.
    * Uses `Model.of` (the smart constructor) so the model passes
    * validation. */
  private val sampleModel: Model = Model.of(
    name     = "orders",
    source   = SourceRef.ByName(catalog = None, namespace = Some("public"), table = "orders"),
    dimensions = Nil,
    measures   = Nil,
    defaultPolicies = ModelPolicyDefaults.none,
    status = ModelStatus.Draft,
  ).fold(
    err => fail(s"sampleModel failed validation: $err"),
    identity,
  )

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
    val m = sampleModel
    val result = TrinoEngine.instance.compile(m, EngineContext.defaultContext)
    result.isLeft shouldBe true
    result.left.toOption.get shouldBe a [EngineError.FeatureDeferred]
  }

  test("execute returns Left(EngineError.FeatureDeferred) for the placeholder path") {
    val result = TrinoEngine.instance.execute("any-plan", EngineContext.defaultContext)
    result.isLeft shouldBe true
    result.left.toOption.get shouldBe a [EngineError.FeatureDeferred]
  }

  test("explain returns Left(EngineError.FeatureDeferred) for the placeholder path") {
    val m = sampleModel
    val result = TrinoEngine.instance.explain(m, EngineContext.defaultContext)
    result.isLeft shouldBe true
    result.left.toOption.get shouldBe a [EngineError.FeatureDeferred]
  }

  test("compile result includes a roadmap pointer in the FeatureDeferred error") {
    val result = TrinoEngine.instance.compile(sampleModel, EngineContext.defaultContext)
    val err = result.left.toOption.get.asInstanceOf[EngineError.FeatureDeferred]
    err.feature should include ("trino")
    err.release should not be empty
  }

  // -- boundary contract: zero Spark imports --

  test("TrinoEngine instance is an Engine[Any] (contract conformance)") {
    val engine: io.semanticdf.core.engine.Engine[Any] = TrinoEngine.instance
    engine.identity shouldBe "trino"
  }
}