package io.semanticdf.core.model

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Phase 2 contract: prove `ExternalExtensionBlob` is a usable,
  * Spark-free data record. Per scala-data-driven-refactor, this is
  * pure data: the blob METADATA is engine-portable; the blob CONTENT
  * fetch is in the engine adapter.
  */
class ExternalExtensionBlobSpec extends AnyFunSuite with Matchers {

  test("ExternalExtensionBlob carries digest + uri + byteLength") {
    val b = ExternalExtensionBlob(
      digest     = "sha256:abc123",
      uri        = new java.net.URI("s3://bucket/key"),
      byteLength = 1024L,
    )
    b.digest shouldBe "sha256:abc123"
    b.uri shouldBe new java.net.URI("s3://bucket/key")
    b.byteLength shouldBe 1024L
  }

  test("ExternalExtensionBlob with default mediaType") {
    val b = ExternalExtensionBlob(
      digest     = "sha256:abc123",
      uri        = new java.net.URI("s3://bucket/key"),
      byteLength = 1024L,
    )
    b.mediaType shouldBe "application/vnd.semanticdf.extensions+json"
  }

  test("ExternalExtensionBlob with custom mediaType") {
    val b = ExternalExtensionBlob(
      digest     = "sha256:abc123",
      uri        = new java.net.URI("s3://bucket/key"),
      byteLength = 1024L,
      mediaType  = "application/octet-stream",
    )
    b.mediaType shouldBe "application/octet-stream"
  }

  test("ExternalExtensionBlob is a value, not a singleton — two with same fields are equal") {
    val a = ExternalExtensionBlob(
      "sha256:abc", new java.net.URI("s3://b/k"), 100L,
    )
    val b = ExternalExtensionBlob(
      "sha256:abc", new java.net.URI("s3://b/k"), 100L,
    )
    a shouldBe b
  }

  test("ExternalExtensionBlob with different digests are not equal") {
    val a = ExternalExtensionBlob(
      "sha256:abc", new java.net.URI("s3://b/k"), 100L,
    )
    val b = ExternalExtensionBlob(
      "sha256:def", new java.net.URI("s3://b/k"), 100L,
    )
    a should not be b
  }

  test("ExternalExtensionBlob round-trips through Java serialization") {
    val b = ExternalExtensionBlob(
      digest     = "sha256:abc123",
      uri        = new java.net.URI("s3://bucket/key"),
      byteLength = 1024L,
    )
    val bos = new java.io.ByteArrayOutputStream()
    val oos = new java.io.ObjectOutputStream(bos)
    oos.writeObject(b)
    oos.close()
    val bis = new java.io.ByteArrayInputStream(bos.toByteArray)
    val ois = new java.io.ObjectInputStream(bis)
    val restored = ois.readObject().asInstanceOf[ExternalExtensionBlob]
    restored shouldBe b
  }
}