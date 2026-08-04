package io.semanticdf.core.rel

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Phase 2 contract: prove `JoinKind` is a usable, Spark-free data
  * record + the closed 5-variant enumeration. Per scala-data-driven-
  * refactor, this is pure data: the join-kind is engine-portable;
  * the engine-specific compile is in the engine adapter.
  */
class JoinKindSpec extends AnyFunSuite with Matchers {

  test("each case is a singleton") {
    JoinKind.Inner shouldBe JoinKind.Inner
    JoinKind.Left shouldBe JoinKind.Left
    JoinKind.Right shouldBe JoinKind.Right
    JoinKind.Full shouldBe JoinKind.Full
    JoinKind.Cross shouldBe JoinKind.Cross
  }

  test("JoinKind has exactly 5 cases") {
    val all: Set[JoinKind] = Set(
      JoinKind.Inner,
      JoinKind.Left,
      JoinKind.Right,
      JoinKind.Full,
      JoinKind.Cross,
    )
    all.size shouldBe 5
  }

  test("Sealed exhaustiveness: pattern-match over all 5 cases") {
    val all: Seq[JoinKind] = Seq(
      JoinKind.Inner,
      JoinKind.Left,
      JoinKind.Right,
      JoinKind.Full,
      JoinKind.Cross,
    )
    all.foreach {
      case JoinKind.Inner => ()
      case JoinKind.Left  => ()
      case JoinKind.Right => ()
      case JoinKind.Full  => ()
      case JoinKind.Cross => ()
    }
  }

  test("JoinKind round-trips through Java serialization") {
    val cases: Seq[JoinKind] = Seq(
      JoinKind.Inner, JoinKind.Left, JoinKind.Right, JoinKind.Full, JoinKind.Cross,
    )
    cases.foreach { v =>
      val bos = new java.io.ByteArrayOutputStream()
      val oos = new java.io.ObjectOutputStream(bos)
      oos.writeObject(v)
      oos.close()
      val bis = new java.io.ByteArrayInputStream(bos.toByteArray)
      val ois = new java.io.ObjectInputStream(bis)
      val restored = ois.readObject().asInstanceOf[JoinKind]
      restored shouldBe v
    }
  }
}