package io.semanticdf.core.engine

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Phase 2 contract: prove `ExecutionPlan` is a usable, Spark-free
  * data record + Serializable round-trip. Per scala-data-driven-
  * refactor, this is pure data: the wrapper SHAPE (engine + native
  * result) is engine-portable; the CONTENT of the native result is
  * engine-specific.
  */
class ExecutionPlanSpec extends AnyFunSuite with Matchers {

  private val testIdentity = EngineIdentity(
    name                 = "trino",
    nativeVersion        = "0.286",
    engineAdapterVersion = "0.2.4",
  )

  // -- case class shape --

  test("ExecutionPlan carries engine + native result") {
    val plan = ExecutionPlan(
      engine = testIdentity,
      native = "SELECT * FROM orders",
    )
    plan.engine shouldBe testIdentity
    plan.native shouldBe "SELECT * FROM orders"
  }

  test("ExecutionPlan with different native types (generic R)") {
    val stringPlan = ExecutionPlan(testIdentity, "SELECT * FROM x")
    stringPlan.native shouldBe "SELECT * FROM x"

    val intPlan = ExecutionPlan(testIdentity, 42)
    intPlan.native shouldBe 42

    val listPlan = ExecutionPlan(testIdentity, List("a", "b"))
    listPlan.native shouldBe List("a", "b")
  }

  // -- equality --

  test("ExecutionPlan is a value, not a singleton — two with same fields are equal") {
    val a = ExecutionPlan(testIdentity, "SELECT 1")
    val b = ExecutionPlan(testIdentity, "SELECT 1")
    a shouldBe b
  }

  test("ExecutionPlan with different engines are not equal") {
    val otherIdentity = EngineIdentity("spark", "3.5.8", "0.2.4")
    val a = ExecutionPlan(testIdentity, "SELECT 1")
    val b = ExecutionPlan(otherIdentity, "SELECT 1")
    a should not be b
  }

  test("ExecutionPlan with different native values are not equal") {
    val a = ExecutionPlan(testIdentity, "SELECT 1")
    val b = ExecutionPlan(testIdentity, "SELECT 2")
    a should not be b
  }

  // -- Serializable round-trip --

  test("ExecutionPlan with String native round-trips through Java serialization") {
    val plan = ExecutionPlan(testIdentity, "SELECT * FROM orders")
    val bos = new java.io.ByteArrayOutputStream()
    val oos = new java.io.ObjectOutputStream(bos)
    oos.writeObject(plan)
    oos.close()
    val bis = new java.io.ByteArrayInputStream(bos.toByteArray)
    val ois = new java.io.ObjectInputStream(bis)
    val restored = ois.readObject().asInstanceOf[ExecutionPlan[String]]
    restored shouldBe plan
  }

  test("ExecutionPlan with Int native round-trips through Java serialization") {
    val plan = ExecutionPlan(testIdentity, 42)
    val bos = new java.io.ByteArrayOutputStream()
    val oos = new java.io.ObjectOutputStream(bos)
    oos.writeObject(plan)
    oos.close()
    val bis = new java.io.ByteArrayInputStream(bos.toByteArray)
    val ois = new java.io.ObjectInputStream(bis)
    val restored = ois.readObject().asInstanceOf[ExecutionPlan[Int]]
    restored shouldBe plan
  }
}