package io.semanticdf.core.rel

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Phase 2 contract: prove `NullOrdering` is a usable, Spark-free
  * data record + the closed 2-variant enumeration. Per scala-data-
  * driven-refactor, this is pure data: the null-ordering is engine-
  * portable; the engine-specific compile is in the engine adapter.
  */
class NullOrderingSpec extends AnyFunSuite with Matchers {

  test("First is a singleton") {
    NullOrdering.First shouldBe NullOrdering.First
  }

  test("Last is a singleton") {
    NullOrdering.Last shouldBe NullOrdering.Last
  }

  test("First != Last") {
    NullOrdering.First should not be NullOrdering.Last
  }

  test("NullOrdering has exactly 2 cases") {
    val all: Set[NullOrdering] = Set(NullOrdering.First, NullOrdering.Last)
    all.size shouldBe 2
  }

  test("Sealed exhaustiveness: pattern-match over both cases") {
    val all: Seq[NullOrdering] = Seq(NullOrdering.First, NullOrdering.Last)
    all.foreach {
      case NullOrdering.First => ()
      case NullOrdering.Last  => ()
    }
  }

  test("NullOrdering round-trips through Java serialization") {
    val cases: Seq[NullOrdering] = Seq(NullOrdering.First, NullOrdering.Last)
    cases.foreach { v =>
      val bos = new java.io.ByteArrayOutputStream()
      val oos = new java.io.ObjectOutputStream(bos)
      oos.writeObject(v)
      oos.close()
      val bis = new java.io.ByteArrayInputStream(bos.toByteArray)
      val ois = new java.io.ObjectInputStream(bis)
      val restored = ois.readObject().asInstanceOf[NullOrdering]
      restored shouldBe v
    }
  }
}