package io.semanticdf.core.model

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Phase 2 contract: prove `OnStalePolicy` is a usable, Spark-free
  * data record + the closed 2-variant enumeration. Per scala-data-
  * driven-refactor, this is pure data: the policy is engine-portable;
  * the engine-specific compile is in the engine adapter.
  */
class OnStalePolicySpec extends AnyFunSuite with Matchers {

  test("FallBackToBase is a singleton") {
    OnStalePolicy.FallBackToBase shouldBe OnStalePolicy.FallBackToBase
  }

  test("Error is a singleton") {
    OnStalePolicy.Error shouldBe OnStalePolicy.Error
  }

  test("FallBackToBase != Error") {
    OnStalePolicy.FallBackToBase should not be OnStalePolicy.Error
  }

  test("OnStalePolicy has exactly 2 cases") {
    val all: Set[OnStalePolicy] = Set(
      OnStalePolicy.FallBackToBase,
      OnStalePolicy.Error,
    )
    all.size shouldBe 2
  }

  test("Sealed exhaustiveness: pattern-match over both cases") {
    val all: Seq[OnStalePolicy] = Seq(
      OnStalePolicy.FallBackToBase,
      OnStalePolicy.Error,
    )
    all.foreach {
      case OnStalePolicy.FallBackToBase => ()
      case OnStalePolicy.Error          => ()
    }
  }

  test("OnStalePolicy round-trips through Java serialization") {
    val cases: Seq[OnStalePolicy] = Seq(
      OnStalePolicy.FallBackToBase,
      OnStalePolicy.Error,
    )
    cases.foreach { v =>
      val bos = new java.io.ByteArrayOutputStream()
      val oos = new java.io.ObjectOutputStream(bos)
      oos.writeObject(v)
      oos.close()
      val bis = new java.io.ByteArrayInputStream(bos.toByteArray)
      val ois = new java.io.ObjectInputStream(bis)
      val restored = ois.readObject().asInstanceOf[OnStalePolicy]
      restored shouldBe v
    }
  }
}