package io.semanticdf.core.engine

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.semanticdf.core.model.{Model, SourceRef}
import io.semanticdf.core.rel.RelOp

/** Tests for the [[Engine]] trait default behavior. Specifically:
  * the `executePortable` default impl is FAIL-LOUD (per debug-mantra
  * \u00a75: every fix must be verifiable).
  *
  * Pre-v0.3.0, the default was:
  *   `execute(plan, ctx).map { _ => PortableQueryResult(empty schema, empty rows, ...) }`
  * which silently dropped data when an engine didn't override the
  * method. The v0.3.0 pre-tag audit caught this (HIGH-1) and replaced
  * the default with `throw NotImplementedError`. This spec is the
  * regression guard. */
class EngineSpec extends AnyFunSuite with Matchers {

  // Helper: minimal valid Model (name + source). Model.of validates.
  private def sampleModel: Model = Model.of(
    name    = "noop",
    source  = SourceRef.ByName(catalog = Some("hive"), namespace = Some("default"), table = "noop_tbl"),
    dimensions = Nil,
    measures   = Nil,
  ).toOption.get

  // -- Minimal fixture: an Engine that doesn't override executePortable --

  private final class NoPortableEngine extends Engine[String] {
    override def identity: String = "noop"
    override def capabilities: Set[Capability] = Set.empty
    override def compile(model: Model, ctx: EngineContext): Either[EngineError, ExecutionPlan[String]] =
      Right(ExecutionPlan(
        engine = EngineIdentity(identity, "1.0", "0.2.4"),
        native = "noop-native",
      ))
    override def compile(plan: RelOp, ctx: EngineContext): Either[EngineError, ExecutionPlan[String]] =
      compile(sampleModel, ctx)
    override def execute(plan: ExecutionPlan[String], ctx: EngineContext): Either[EngineError, String] =
      Right(plan.native)
    override def explain(model: Model, ctx: EngineContext): Either[EngineError, String] =
      Right(s"explain ${model.name}")
    // NOTE: deliberately does NOT override executePortable
  }

  test("executePortable default throws NotImplementedError (regression guard for v0.3.0 audit HIGH-1)") {
    val engine = new NoPortableEngine
    val model  = sampleModel
    val ctx    = EngineContext.defaultContext
    val plan   = engine.compile(model, ctx).toOption.get

    val ex = intercept[NotImplementedError] {
      engine.executePortable(plan, ctx)
    }
    ex.getMessage should include ("executePortable")
    ex.getMessage should include ("ResultEncoder")
  }

  test("execute (not executePortable) still works on the no-override engine") {
    // Sanity: the fix doesn't break the engine's own execute path.
    val engine = new NoPortableEngine
    val model  = sampleModel
    val ctx    = EngineContext.defaultContext
    val plan   = engine.compile(model, ctx).toOption.get
    engine.execute(plan, ctx) shouldBe Right("noop-native")
  }
}
