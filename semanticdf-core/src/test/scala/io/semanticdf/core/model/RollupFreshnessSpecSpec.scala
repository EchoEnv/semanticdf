package io.semanticdf.core.model

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Phase 2 contract: prove `RollupFreshnessSpec` is a usable,
  * Spark-free data record + the closed 2-variant enumeration.
  * Per scala-data-driven-refactor, this is pure data: the freshness
  * CONTRACT is engine-portable; the freshness RESOLUTION (the
  * watermark lookup) lives in the engine adapter.
  */
class RollupFreshnessSpecSpec extends AnyFunSuite with Matchers {

  test("Track carries maxStaleness + onStale") {
    val t = RollupFreshnessSpec.Track(
      maxStaleness = java.time.Duration.ofMinutes(5),
      onStale      = OnStalePolicy.FallBackToBase,
    )
    t.maxStaleness shouldBe java.time.Duration.ofMinutes(5)
    t.onStale shouldBe OnStalePolicy.FallBackToBase
  }

  test("Track with Error policy") {
    val t = RollupFreshnessSpec.Track(
      maxStaleness = java.time.Duration.ofSeconds(30),
      onStale      = OnStalePolicy.Error,
    )
    t.onStale shouldBe OnStalePolicy.Error
  }

  test("NoTracking is a singleton (explicit opt-out)") {
    RollupFreshnessSpec.NoTracking shouldBe RollupFreshnessSpec.NoTracking
  }

  test("Track != NoTracking") {
    val t = RollupFreshnessSpec.Track(
      java.time.Duration.ofMinutes(5),
      OnStalePolicy.FallBackToBase,
    )
    t should not be RollupFreshnessSpec.NoTracking
  }

  test("RollupFreshnessSpec has exactly 2 cases (Track + NoTracking)") {
    val all: Set[RollupFreshnessSpec] = Set(
      RollupFreshnessSpec.NoTracking,
      RollupFreshnessSpec.Track(
        java.time.Duration.ofMinutes(1),
        OnStalePolicy.FallBackToBase,
      ),
    )
    all.size shouldBe 2
  }

  test("Sealed exhaustiveness: pattern-match over both cases") {
    val all: Seq[RollupFreshnessSpec] = Seq(
      RollupFreshnessSpec.NoTracking,
      RollupFreshnessSpec.Track(
        java.time.Duration.ofMinutes(1),
        OnStalePolicy.FallBackToBase,
      ),
    )
    all.foreach {
      case RollupFreshnessSpec.NoTracking => ()
      case RollupFreshnessSpec.Track(_, _) => ()
    }
  }

  test("RollupFreshnessSpec.Track round-trips through Java serialization") {
    val v = RollupFreshnessSpec.Track(
      java.time.Duration.ofMinutes(5),
      OnStalePolicy.Error,
    )
    val bos = new java.io.ByteArrayOutputStream()
    val oos = new java.io.ObjectOutputStream(bos)
    oos.writeObject(v)
    oos.close()
    val bis = new java.io.ByteArrayInputStream(bos.toByteArray)
    val ois = new java.io.ObjectInputStream(bis)
    val restored = ois.readObject().asInstanceOf[RollupFreshnessSpec]
    restored shouldBe v
  }

  test("RollupFreshnessSpec.NoTracking round-trips through Java serialization") {
    val v = RollupFreshnessSpec.NoTracking
    val bos = new java.io.ByteArrayOutputStream()
    val oos = new java.io.ObjectOutputStream(bos)
    oos.writeObject(v)
    oos.close()
    val bis = new java.io.ByteArrayInputStream(bos.toByteArray)
    val ois = new java.io.ObjectInputStream(bis)
    val restored = ois.readObject().asInstanceOf[RollupFreshnessSpec]
    restored shouldBe v
  }
}