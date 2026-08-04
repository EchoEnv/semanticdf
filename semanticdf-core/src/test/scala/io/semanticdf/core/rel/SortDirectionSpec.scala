package io.semanticdf.core.rel

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Phase 2 contract: prove `SortDirection` is a usable, Spark-free
  * data record + the closed 2-variant enumeration. Per scala-data-
  * driven-refactor, this is pure data: the direction is engine-
  * portable; the engine-specific compile is in the engine adapter.
  */
class SortDirectionSpec extends AnyFunSuite with Matchers {

  test("Ascending is a singleton") {
    SortDirection.Ascending shouldBe SortDirection.Ascending
  }

  test("Descending is a singleton") {
    SortDirection.Descending shouldBe SortDirection.Descending
  }

  test("Ascending != Descending") {
    SortDirection.Ascending should not be SortDirection.Descending
  }

  test("SortDirection has exactly 2 cases") {
    val all: Set[SortDirection] = Set(
      SortDirection.Ascending,
      SortDirection.Descending,
    )
    all.size shouldBe 2
  }

  test("Sealed exhaustiveness: pattern-match over both cases") {
    val all: Seq[SortDirection] = Seq(SortDirection.Ascending, SortDirection.Descending)
    all.foreach {
      case SortDirection.Ascending  => ()
      case SortDirection.Descending => ()
    }
  }

  test("SortDirection round-trips through Java serialization") {
    val cases: Seq[SortDirection] = Seq(SortDirection.Ascending, SortDirection.Descending)
    cases.foreach { v =>
      val bos = new java.io.ByteArrayOutputStream()
      val oos = new java.io.ObjectOutputStream(bos)
      oos.writeObject(v)
      oos.close()
      val bis = new java.io.ByteArrayInputStream(bos.toByteArray)
      val ois = new java.io.ObjectInputStream(bis)
      val restored = ois.readObject().asInstanceOf[SortDirection]
      restored shouldBe v
    }
  }
}