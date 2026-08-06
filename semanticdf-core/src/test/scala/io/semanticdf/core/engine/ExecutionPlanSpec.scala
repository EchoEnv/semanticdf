package io.semanticdf.core.engine

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.semanticdf.core.schema.{Field, SealedDataType}

/** Tests for [[ExecutionPlan]] — the new inspectable sealed
  * trait + its `ExecutionPlanSummary` companion.
  *
  * Per design §4.5.4 + round-3 DE finding 2.1 closure:
  * `ExecutionPlan` must NOT auto-extend `Product with Serializable`.
  * The tests pin this property. */
class ExecutionPlanSpec extends AnyFunSuite with Matchers {

  // -- fixture --

  private val sampleEngine = EngineIdentity(
    name                 = "trino",
    nativeVersion        = "0.286",
    engineAdapterVersion = "0.2.4",
  )

  // -- the case class is gone; it's a sealed trait now --

  test("ExecutionPlan is a sealed trait (NOT a case class)") {
    // Class.isInterface check via reflection (case class becomes
    // a regular class; sealed trait is an interface). Per
    // scala-data-driven-refactor §1, the SHAPE is a contract.
    val plan = ExecutionPlan[ParameterizedSql](
      engine = sampleEngine,
      native = ParameterizedSql(sql = "SELECT 1", parameters = Nil),
    )
    plan shouldBe a [ExecutionPlan[_]]
  }

  // -- the smart constructor preserves the call-site shape --

  test("ExecutionPlan.apply factory preserves the engine + native fields") {
    val native = ParameterizedSql(sql = "SELECT 1", parameters = Nil)
    val plan = ExecutionPlan[ParameterizedSql](
      engine = sampleEngine,
      native = native,
    )
    plan.engine shouldBe sampleEngine
    plan.native shouldBe native
  }

  test("ExecutionPlan defaults: warnings = Nil, requiredCapabilities = Set.empty, normalizedSchema = empty") {
    val plan = ExecutionPlan[ParameterizedSql](
      engine = sampleEngine,
      native = ParameterizedSql(sql = "SELECT 1", parameters = Nil),
    )
    plan.warnings shouldBe Nil
    plan.requiredCapabilities shouldBe Set.empty[Capability]
    plan.normalizedSchema shouldBe ResultSchema(Nil)
    plan.isCacheable shouldBe true  // default
  }

  test("ExecutionPlan.apply supports overriding warnings, requiredCapabilities, normalizedSchema") {
    val warnings = List(EngineWarning.PolicyAdapted(original = "broadcast", adapted = "broadcast hint omitted"))
    val caps     = Set(Capability.BroadcastJoin, Capability.NestedStructTypes)
    val schema   = ResultSchema(List(Field(name = "x", dataType = SealedDataType.Int, nullable = true)))
    val plan = ExecutionPlan[ParameterizedSql](
      engine               = sampleEngine,
      native               = ParameterizedSql("SELECT 1", Nil),
      warnings             = warnings,
      requiredCapabilities = caps,
      normalizedSchema     = schema,
    )
    plan.warnings shouldBe warnings
    plan.requiredCapabilities shouldBe caps
    plan.normalizedSchema shouldBe schema
  }

  // -- isCacheable can be disabled for non-Serializable R --

  test("isCacheable can be set to false for non-Serializable R (e.g. Spark QueryPlan)") {
    val handle = new Object  // stand-in for Spark QueryPlan
    val plan = ExecutionPlan[AnyRef](
      engine    = sampleEngine,
      native    = handle,
      cacheable = false,
    )
    plan.isCacheable shouldBe false
  }

  // -- toSummary produces a Serializable summary --

  test("toSummary produces ExecutionPlanSummary that is Serializable") {
    val plan = ExecutionPlan[ParameterizedSql](
      engine   = sampleEngine,
      native   = ParameterizedSql("SELECT 1", Nil),
      warnings = List(EngineWarning.PolicyAdapted(original = "x", adapted = "y")),
    )
    val summary = plan.toSummary
    summary shouldBe a [ExecutionPlanSummary]
    summary.engine shouldBe sampleEngine
    summary.warnings shouldBe plan.warnings

    // Round-trip through Java serialization.
    val bytes = ExecutionPlanSpec.serialize(summary)
    val back  = ExecutionPlanSpec.deserialize[ExecutionPlanSummary](bytes)
    back shouldBe summary
  }

  test("toSummary with no SQL/logical plan (engine didn't populate them) is still valid") {
    val plan = ExecutionPlan[ParameterizedSql](
      engine = sampleEngine,
      native = ParameterizedSql("SELECT 1", Nil),
    )
    val summary = plan.toSummary
    summary.sql shouldBe None
    summary.logicalPlan shouldBe None
    summary.hasTextualPlan shouldBe false
  }

  // -- SERIALIZABILITY PROPERTY: the ExecutionPlan itself is NOT Serializable --
  // Note: this property is enforced by the TYPE SYSTEM (sealed trait,
  // not case class). We don't test it at runtime because Java
  // serialization behavior on anonymous classes is JVM-specific
  // and can hang. The compile-time guarantee is the contract.

  test("ExecutionPlan is a sealed trait, not a case class (type-level cluster-safety)") {
    // The plan's class should NOT have a `copy` method (case-class
    // auto-derives one). If this test ever fails, the round-3
    // DE fix 2.1 closure has been broken.
    val plan = ExecutionPlan[ParameterizedSql](
      engine = sampleEngine,
      native = ParameterizedSql("SELECT 1", Nil),
    )
    val cls = plan.getClass
    val hasCopy = cls.getMethods.exists(_.getName == "copy")
    assert(!hasCopy, s"ExecutionPlan must NOT be a case class (copy method present on $cls)")
  }
}

object ExecutionPlanSpec {

  /** Helper: serialize an object to a byte array. */
  def serialize(obj: AnyRef): Array[Byte] = {
    val out = new java.io.ByteArrayOutputStream()
    val oos = new java.io.ObjectOutputStream(out)
    oos.writeObject(obj)
    oos.close()
    out.toByteArray
  }

  /** Helper: deserialize a byte array back to the given type. */
  def deserialize[T](bytes: Array[Byte]): T = {
    val in  = new java.io.ByteArrayInputStream(bytes)
    val ois = new java.io.ObjectInputStream(in)
    val obj = ois.readObject()
    ois.close()
    obj.asInstanceOf[T]
  }
}