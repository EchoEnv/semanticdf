package io.semanticdf.core.model

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Phase 2 contract: prove `ModelValidationError` is a usable,
  * Spark-free data record + the closed 5-variant enumeration.
  * Per scala-data-driven-refactor, this is pure data: the error
  * SHAPE is engine-portable; the engine-specific rendering (MCP
  * ErrorDetail) lives in the engine adapter.
  */
class ModelValidationErrorSpec extends AnyFunSuite with Matchers {

  // -- 5 cases --

  test("InvalidName carries a reason") {
    ModelValidationError.InvalidName("name is blank").reason shouldBe "name is blank"
  }

  test("DuplicateMember carries kind + name") {
    val e = ModelValidationError.DuplicateMember("dimension/measure", "x")
    e.kind shouldBe "dimension/measure"
    e.name shouldBe "x"
  }

  test("UnknownReference carries referent + target") {
    val e = ModelValidationError.UnknownReference("calculatedMeasures", "missing_measure")
    e.referent shouldBe "calculatedMeasures"
    e.target shouldBe "missing_measure"
  }

  test("CalcDepthExceeded carries depth + max") {
    val e = ModelValidationError.CalcDepthExceeded(depth = 10, max = 5)
    e.depth shouldBe 10
    e.max shouldBe 5
  }

  test("ExtensionEnvelopeExceeded carries fieldCount + byteCount") {
    val e = ModelValidationError.ExtensionEnvelopeExceeded(fieldCount = 20, byteCount = 9000)
    e.fieldCount shouldBe 20
    e.byteCount shouldBe 9000
  }

  // -- closed enumeration --

  test("ModelValidationError has exactly 6 cases") {
    val all: Set[ModelValidationError] = Set(
      ModelValidationError.InvalidName("x"),
      ModelValidationError.DuplicateMember("dimension", "x"),
      ModelValidationError.UnknownReference("calc", "x"),
      ModelValidationError.CalcDepthExceeded(1, 1),
      ModelValidationError.ExtensionEnvelopeExceeded(1, 1),
      ModelValidationError.FilterConversionUnsupported("legacy where"),
    )
    all.size shouldBe 6
  }

  test("Sealed exhaustiveness: pattern-match over all 6 cases") {
    val all: Seq[ModelValidationError] = Seq(
      ModelValidationError.InvalidName("x"),
      ModelValidationError.DuplicateMember("dimension", "x"),
      ModelValidationError.UnknownReference("calc", "x"),
      ModelValidationError.CalcDepthExceeded(1, 1),
      ModelValidationError.ExtensionEnvelopeExceeded(1, 1),
      ModelValidationError.FilterConversionUnsupported("legacy where"),
    )
    all.foreach {
      case ModelValidationError.InvalidName(_)                  => ()
      case ModelValidationError.DuplicateMember(_, _)          => ()
      case ModelValidationError.UnknownReference(_, _)         => ()
      case ModelValidationError.CalcDepthExceeded(_, _)        => ()
      case ModelValidationError.ExtensionEnvelopeExceeded(_,_) => ()
      case ModelValidationError.FilterConversionUnsupported(_) => ()
    }
  }

  // -- Serializable --

  test("all 6 cases round-trip through Java serialization") {
    val cases: Seq[ModelValidationError] = Seq(
      ModelValidationError.InvalidName("name is blank"),
      ModelValidationError.DuplicateMember("dimension", "x"),
      ModelValidationError.UnknownReference("calc", "missing"),
      ModelValidationError.CalcDepthExceeded(10, 5),
      ModelValidationError.ExtensionEnvelopeExceeded(20, 9000),
      ModelValidationError.FilterConversionUnsupported("legacy where: predicate"),
    )
    cases.foreach { v =>
      val bos = new java.io.ByteArrayOutputStream()
      val oos = new java.io.ObjectOutputStream(bos)
      oos.writeObject(v)
      oos.close()
      val bis = new java.io.ByteArrayInputStream(bos.toByteArray)
      val ois = new java.io.ObjectInputStream(bis)
      val restored = ois.readObject().asInstanceOf[ModelValidationError]
      restored shouldBe v
    }
  }
}