package io.semanticdf.core.model

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.semanticdf.core.engine.{AuditPolicy, CachePolicy, MaterializePolicy}

/** Phase 2 contract: prove `ModelPolicyDefaults` is a usable, Spark-
  * free data record + the canonical `none` default. Per scala-data-
  * driven-refactor, this is pure data: composition lives in core;
  * behavior stays in the engine adapter.
  */
class ModelPolicyDefaultsSpec extends AnyFunSuite with Matchers {

  test("ModelPolicyDefaults.none is the canonical default (no policies)") {
    ModelPolicyDefaults.none.materialize shouldBe MaterializePolicy.None
    ModelPolicyDefaults.none.cache shouldBe CachePolicy.NoCache
    ModelPolicyDefaults.none.audit shouldBe AuditPolicy.NoAudit
  }

  test("ModelPolicyDefaults with full policies") {
    val p = ModelPolicyDefaults(
      materialize = MaterializePolicy.MemoryOnly,
      cache       = CachePolicy.ReadThrough,
      audit       = AuditPolicy.EngineDefault,
    )
    p.materialize shouldBe MaterializePolicy.MemoryOnly
    p.cache shouldBe CachePolicy.ReadThrough
    p.audit shouldBe AuditPolicy.EngineDefault
  }

  test("ModelPolicyDefaults is a value, not a singleton — two `none` are equal") {
    val a = ModelPolicyDefaults.none
    val b = ModelPolicyDefaults.none
    a shouldBe b
  }

  test("ModelPolicyDefaults with different materialize are not equal") {
    val a = ModelPolicyDefaults.none
    val b = ModelPolicyDefaults.none.copy(materialize = MaterializePolicy.MemoryOnly)
    a should not be b
  }

  test("ModelPolicyDefaults round-trips through Java serialization") {
    val p = ModelPolicyDefaults(
      materialize = MaterializePolicy.MemoryOnly,
      cache       = CachePolicy.NoCache,
      audit       = AuditPolicy.NoAudit,
    )
    val bos = new java.io.ByteArrayOutputStream()
    val oos = new java.io.ObjectOutputStream(bos)
    oos.writeObject(p)
    oos.close()
    val bis = new java.io.ByteArrayInputStream(bos.toByteArray)
    val ois = new java.io.ObjectInputStream(bis)
    val restored = ois.readObject().asInstanceOf[ModelPolicyDefaults]
    restored shouldBe p
  }
}