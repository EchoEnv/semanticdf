package io.semanticdf.core.model

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Phase 2 contract: prove `ExtensionLimits.check` correctly
  * validates inline envelopes against the field + byte limits.
  * Per scala-data-driven-refactor, this is pure data analysis
  * (no engine coupling): the limits are universal invariants.
  */
class ExtensionLimitsSpec extends AnyFunSuite with Matchers {

  test("ExtensionLimits.MaxInlineBytes = 8 * 1024") {
    ExtensionLimits.MaxInlineBytes shouldBe 8 * 1024
  }

  test("ExtensionLimits.MaxFields = 16") {
    ExtensionLimits.MaxFields shouldBe 16
  }

  test("check returns Right(()) for empty envelope") {
    ExtensionLimits.check(Map.empty) shouldBe Right(())
  }

  test("check returns Right(()) for small inline envelope (within limits)") {
    val envelope = Map(
      "description" -> ExtensionValue.String("orders model"),
      "owner"       -> ExtensionValue.String("analytics-team"),
      "version"     -> ExtensionValue.String("1.0"),
    )
    ExtensionLimits.check(envelope) shouldBe Right(())
  }

  test("check returns Right(()) for envelope at exactly MaxFields") {
    val envelope = (1 to ExtensionLimits.MaxFields).map { i =>
      s"key$i" -> ExtensionValue.String("v")
    }.toMap
    ExtensionLimits.check(envelope) shouldBe Right(())
  }

  test("check returns Left(Excess) when exceeding MaxFields") {
    val envelope = (1 to (ExtensionLimits.MaxFields + 1)).map { i =>
      s"key$i" -> ExtensionValue.String("v")
    }.toMap
    val result = ExtensionLimits.check(envelope)
    result.isLeft shouldBe true
    val Left(excess) = result
    excess.fieldCount shouldBe ExtensionLimits.MaxFields + 1
  }

  test("check counts List elements as 0 fields (a field is a name-value pair, not a list element)") {
    val bigList = ExtensionValue.List(
      (1 to 17).map(i => ExtensionValue.String(s"item$i")).toList
    )
    val envelope = Map("big" -> bigList)
    // 1 top-level field ("big") + 0 list-element fields = 1 field, fits within MaxFields=16.
    ExtensionLimits.check(envelope) shouldBe Right(())
  }

  test("check counts Object fields recursively toward MaxFields") {
    val bigObj = ExtensionValue.Object(
      (1 to 17).map(i => s"k$i" -> ExtensionValue.String("v")).toMap
    )
    val envelope = Map("big" -> bigObj)
    val result = ExtensionLimits.check(envelope)
    result.isLeft shouldBe true
  }

  test("check returns Left(Excess) when exceeding MaxInlineBytes") {
    val bigStr = "x" * (ExtensionLimits.MaxInlineBytes + 1)
    val envelope = Map("big" -> ExtensionValue.String(bigStr))
    val result = ExtensionLimits.check(envelope)
    result.isLeft shouldBe true
    val Left(excess) = result
    excess.byteCount should be > ExtensionLimits.MaxInlineBytes
  }

  test("check returns Right(()) for envelope at exactly MaxInlineBytes") {
    // Build a small envelope that fits in 8 KiB.
    val envelope = Map("k" -> ExtensionValue.String("hello"))
    ExtensionLimits.check(envelope) shouldBe Right(())
  }

  test("Excess is a value (carries fieldCount + byteCount)") {
    val e = ExtensionLimits.Excess(fieldCount = 20, byteCount = 9000)
    e.fieldCount shouldBe 20
    e.byteCount shouldBe 9000
  }

  test("Excess round-trips through Java serialization") {
    val e = ExtensionLimits.Excess(fieldCount = 20, byteCount = 9000)
    val bos = new java.io.ByteArrayOutputStream()
    val oos = new java.io.ObjectOutputStream(bos)
    oos.writeObject(e)
    oos.close()
    val bis = new java.io.ByteArrayInputStream(bos.toByteArray)
    val ois = new java.io.ObjectInputStream(bis)
    val restored = ois.readObject().asInstanceOf[ExtensionLimits.Excess]
    restored shouldBe e
  }
}