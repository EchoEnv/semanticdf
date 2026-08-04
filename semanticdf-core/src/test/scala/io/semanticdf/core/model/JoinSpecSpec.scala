package io.semanticdf.core.model

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.semanticdf.core.rel.JoinKind

/** Phase 2 contract: prove `JoinSpec` is a usable, Spark-free data
  * record. Per scala-data-driven-refactor, this is pure data: the
  * join-spec SHAPE is engine-portable; the engine-specific compile
  * is in the engine adapter.
  */
class JoinSpecSpec extends AnyFunSuite with Matchers {

  test("JoinSpec carries name + rightModel + kind + keys") {
    val j = JoinSpec(
      name       = "orders_to_customers",
      rightModel = "customers",
      kind       = JoinKind.Left,
      keys       = List(("customer_id", "id")),
    )
    j.name shouldBe "orders_to_customers"
    j.rightModel shouldBe "customers"
    j.kind shouldBe JoinKind.Left
    j.keys.size shouldBe 1
    j.keys.head shouldBe ("customer_id", "id")
  }

  test("JoinSpec with multiple key pairs (composite-key join)") {
    val j = JoinSpec(
      name       = "complex_join",
      rightModel = "regions",
      kind       = JoinKind.Inner,
      keys       = List(
        ("country", "code_country"),
        ("year",    "code_year"),
      ),
    )
    j.keys.size shouldBe 2
  }

  test("JoinSpec with Cross kind (no keys)") {
    val j = JoinSpec(
      name       = "cartesian",
      rightModel = "all_dimensions",
      kind       = JoinKind.Cross,
      keys       = Nil,
    )
    j.kind shouldBe JoinKind.Cross
    j.keys shouldBe Nil
  }

  test("JoinSpec is a value, not a singleton — two with same fields are equal") {
    val a = JoinSpec("j", "m", JoinKind.Inner, List(("a", "b")))
    val b = JoinSpec("j", "m", JoinKind.Inner, List(("a", "b")))
    a shouldBe b
  }

  test("JoinSpec with different kinds are not equal") {
    val a = JoinSpec("j", "m", JoinKind.Inner, Nil)
    val b = JoinSpec("j", "m", JoinKind.Left,  Nil)
    a should not be b
  }

  test("JoinSpec supports all JoinKind cases") {
    val kinds: Set[JoinKind] = Set(
      JoinKind.Inner, JoinKind.Left, JoinKind.Right, JoinKind.Full, JoinKind.Cross,
    )
    val specs: Seq[JoinSpec] = kinds.toSeq.map(k => JoinSpec("j", "m", k, Nil))
    specs.map(_.kind).toSet shouldBe kinds
  }

  test("JoinSpec round-trips through Java serialization") {
    val j = JoinSpec(
      name       = "orders_to_customers",
      rightModel = "customers",
      kind       = JoinKind.Left,
      keys       = List(("customer_id", "id")),
    )
    val bos = new java.io.ByteArrayOutputStream()
    val oos = new java.io.ObjectOutputStream(bos)
    oos.writeObject(j)
    oos.close()
    val bis = new java.io.ByteArrayInputStream(bos.toByteArray)
    val ois = new java.io.ObjectInputStream(bis)
    val restored = ois.readObject().asInstanceOf[JoinSpec]
    restored shouldBe j
  }
}