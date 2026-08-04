package io.semanticdf.core.model

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Phase 2 contract: prove `ExtensionEnvelope` is a usable, Spark-
  * free data record + the smart constructors build the common
  * cases. Per scala-data-driven-refactor, this is pure data: the
  * wrapper SHAPE is engine-portable; the STORAGE is engine-specific.
  */
class ExtensionEnvelopeSpec extends AnyFunSuite with Matchers {

  test("default ExtensionEnvelope is empty (inline=Map.empty, external=None)") {
    val e = ExtensionEnvelope()
    e.inline shouldBe Map.empty
    e.external shouldBe None
  }

  test("ExtensionEnvelope.inlineOnly factory") {
    val e = ExtensionEnvelope.inlineOnly(Map(
      "description" -> ExtensionValue.String("orders model"),
      "owner"       -> ExtensionValue.String("analytics-team"),
    ))
    e.inline.size shouldBe 2
    e.external shouldBe None
  }

  test("ExtensionEnvelope.externalOnly factory") {
    val blob = ExternalExtensionBlob(
      digest     = "sha256:abc",
      uri        = new java.net.URI("s3://b/k"),
      byteLength = 100L,
    )
    val e = ExtensionEnvelope.externalOnly(blob)
    e.inline shouldBe Map.empty
    e.external shouldBe Some(blob)
  }

  test("ExtensionEnvelope with both inline and external") {
    val blob = ExternalExtensionBlob(
      digest     = "sha256:abc",
      uri        = new java.net.URI("s3://b/k"),
      byteLength = 100L,
    )
    val e = ExtensionEnvelope(
      inline   = Map("description" -> ExtensionValue.String("model")),
      external = Some(blob),
    )
    e.inline.size shouldBe 1
    e.external shouldBe Some(blob)
  }

  test("ExtensionEnvelope is a value, not a singleton — two with same fields are equal") {
    val a = ExtensionEnvelope.inlineOnly(Map("k" -> ExtensionValue.String("v")))
    val b = ExtensionEnvelope.inlineOnly(Map("k" -> ExtensionValue.String("v")))
    a shouldBe b
  }

  test("ExtensionEnvelope with nested ExtensionValue round-trips through Java serialization") {
    val e = ExtensionEnvelope.inlineOnly(Map(
      "metadata" -> ExtensionValue.Object(Map(
        "version" -> ExtensionValue.String("1.0"),
        "tags"    -> ExtensionValue.List(List(
          ExtensionValue.String("analytics"),
          ExtensionValue.String("orders"),
        )),
      )),
    ))
    val bos = new java.io.ByteArrayOutputStream()
    val oos = new java.io.ObjectOutputStream(bos)
    oos.writeObject(e)
    oos.close()
    val bis = new java.io.ByteArrayInputStream(bos.toByteArray)
    val ois = new java.io.ObjectInputStream(bis)
    val restored = ois.readObject().asInstanceOf[ExtensionEnvelope]
    restored shouldBe e
  }
}