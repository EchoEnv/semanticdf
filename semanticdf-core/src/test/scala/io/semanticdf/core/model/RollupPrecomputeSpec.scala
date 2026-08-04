package io.semanticdf.core.model

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Phase 2 contract: prove `RollupPrecompute` is a usable, Spark-free
  * data record. Per scala-data-driven-refactor, this is pure data:
  * the precompute SHAPE is engine-portable; the COMPUTATION
  * (resolving the provider) is in the engine adapter.
  */
class RollupPrecomputeSpec extends AnyFunSuite with Matchers {

  test("default RollupPrecompute is empty (rowCount=None, columns=Set.empty, sourceDigest=None)") {
    val p = RollupPrecompute()
    p.rowCount shouldBe None
    p.columns shouldBe Set.empty
    p.sourceDigest shouldBe None
  }

  test("RollupPrecompute with full precompute") {
    val p = RollupPrecompute(
      rowCount     = Some(1000L),
      columns      = Set("region", "category", "total"),
      sourceDigest = Some("sha256:abc123"),
    )
    p.rowCount shouldBe Some(1000L)
    p.columns.size shouldBe 3
    p.sourceDigest shouldBe Some("sha256:abc123")
  }

  test("RollupPrecompute with rowCount only (no columns yet)") {
    val p = RollupPrecompute(rowCount = Some(5000L))
    p.rowCount shouldBe Some(5000L)
    p.columns shouldBe Set.empty
  }

  test("RollupPrecompute with columns only (no rowCount)") {
    val p = RollupPrecompute(columns = Set("a", "b"))
    p.rowCount shouldBe None
    p.columns shouldBe Set("a", "b")
  }

  test("RollupPrecompute is a value, not a singleton — two with same fields are equal") {
    val a = RollupPrecompute(Some(100L), Set("x"), Some("d"))
    val b = RollupPrecompute(Some(100L), Set("x"), Some("d"))
    a shouldBe b
  }

  test("RollupPrecompute round-trips through Java serialization") {
    val p = RollupPrecompute(
      rowCount     = Some(1000L),
      columns      = Set("region", "category", "total"),
      sourceDigest = Some("sha256:abc123"),
    )
    val bos = new java.io.ByteArrayOutputStream()
    val oos = new java.io.ObjectOutputStream(bos)
    oos.writeObject(p)
    oos.close()
    val bis = new java.io.ByteArrayInputStream(bos.toByteArray)
    val ois = new java.io.ObjectInputStream(bis)
    val restored = ois.readObject().asInstanceOf[RollupPrecompute]
    restored shouldBe p
  }
}