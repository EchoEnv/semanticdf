package io.semanticdf.core.engine

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Phase 2 contract: prove `SourceStats` is a usable, Spark-free
  * data record. Per scala-data-driven-refactor, this is pure data:
  * the stats SHAPE is engine-portable; the COMPUTATION (SHOW STATS,
  * ANALYZE TABLE, etc.) is in the engine adapter.
  */
class SourceStatsSpec extends AnyFunSuite with Matchers {

  test("default SourceStats is empty (estimatedRows=None, estimatedBytes=None)") {
    val s = SourceStats()
    s.estimatedRows shouldBe None
    s.estimatedBytes shouldBe None
  }

  test("SourceStats with row count only") {
    val s = SourceStats(estimatedRows = Some(1000L))
    s.estimatedRows shouldBe Some(1000L)
    s.estimatedBytes shouldBe None
  }

  test("SourceStats with byte count only") {
    val s = SourceStats(estimatedBytes = Some(8000L))
    s.estimatedRows shouldBe None
    s.estimatedBytes shouldBe Some(8000L)
  }

  test("SourceStats with both row + byte counts (full stats)") {
    val s = SourceStats(
      estimatedRows  = Some(1_000_000L),
      estimatedBytes = Some(50_000_000L),
    )
    s.estimatedRows shouldBe Some(1_000_000L)
    s.estimatedBytes shouldBe Some(50_000_000L)
  }

  test("realistic: 1M-row table (~80 bytes/row)") {
    val s = SourceStats(
      estimatedRows  = Some(1_000_000L),
      estimatedBytes = Some(80_000_000L),
    )
    s.estimatedBytes.get / s.estimatedRows.get shouldBe 80L
  }

  test("SourceStats is a value, not a singleton — two with same fields are equal") {
    val a = SourceStats(Some(100L), Some(800L))
    val b = SourceStats(Some(100L), Some(800L))
    a shouldBe b
  }

  test("SourceStats with different row counts are not equal") {
    val a = SourceStats(Some(100L), Some(800L))
    val b = SourceStats(Some(200L), Some(800L))
    a should not be b
  }

  test("SourceStats round-trips through Java serialization") {
    val s = SourceStats(
      estimatedRows  = Some(1_000_000L),
      estimatedBytes = Some(80_000_000L),
    )
    val bos = new java.io.ByteArrayOutputStream()
    val oos = new java.io.ObjectOutputStream(bos)
    oos.writeObject(s)
    oos.close()
    val bis = new java.io.ByteArrayInputStream(bos.toByteArray)
    val ois = new java.io.ObjectInputStream(bis)
    val restored = ois.readObject().asInstanceOf[SourceStats]
    restored shouldBe s
  }
}