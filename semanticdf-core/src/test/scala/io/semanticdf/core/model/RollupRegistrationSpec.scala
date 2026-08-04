package io.semanticdf.core.model

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.semanticdf.core.rel.AggregateFn

/** Phase 2 contract: prove `RollupRegistration` is a usable, Spark-
  * free data record. Per scala-data-driven-refactor, this is pure
  * data: the registration SHAPE is engine-portable; the provider
  * RESOLUTION (calling the thunk) is in the engine adapter.
  */
class RollupRegistrationSpec extends AnyFunSuite with Matchers {

  private val sampleSpec = RollupSpec(
    name       = "orders_by_region",
    baseModel  = "orders",
    dimensions = List("region"),
    measures   = List(RollupMeasureSpec("total", AggregateFn.Sum, "sum_amount")),
    freshness  = RollupFreshnessSpec.NoTracking,
  )

  private val samplePrecompute = RollupPrecompute(
    rowCount = Some(50L),
    columns  = Set("region", "total"),
  )

  private val sampleProvider = ProviderRef.DataFrameSource(
    name       = "ordersByRegion",
    schemaHint = None,
  )

  test("RollupRegistration carries spec + provider + precomputed") {
    val r = RollupRegistration(
      spec        = sampleSpec,
      provider    = sampleProvider,
      precomputed = samplePrecompute,
    )
    r.spec shouldBe sampleSpec
    r.provider shouldBe sampleProvider
    r.precomputed shouldBe samplePrecompute
  }

  test("RollupRegistration with empty precompute (provider just resolved)") {
    val r = RollupRegistration(
      spec        = sampleSpec,
      provider    = sampleProvider,
      precomputed = RollupPrecompute(),
    )
    r.precomputed.rowCount shouldBe None
  }

  test("RollupRegistration is a value, not a singleton — two with same fields are equal") {
    val a = RollupRegistration(sampleSpec, sampleProvider, samplePrecompute)
    val b = RollupRegistration(sampleSpec, sampleProvider, samplePrecompute)
    a shouldBe b
  }

  test("RollupRegistration with different specs are not equal") {
    val otherSpec = sampleSpec.copy(name = "other")
    val a = RollupRegistration(sampleSpec,   sampleProvider, samplePrecompute)
    val b = RollupRegistration(otherSpec,   sampleProvider, samplePrecompute)
    a should not be b
  }

  test("RollupRegistration round-trips through Java serialization") {
    val r = RollupRegistration(
      spec        = sampleSpec,
      provider    = sampleProvider,
      precomputed = samplePrecompute,
    )
    val bos = new java.io.ByteArrayOutputStream()
    val oos = new java.io.ObjectOutputStream(bos)
    oos.writeObject(r)
    oos.close()
    val bis = new java.io.ByteArrayInputStream(bos.toByteArray)
    val ois = new java.io.ObjectInputStream(bis)
    val restored = ois.readObject().asInstanceOf[RollupRegistration]
    restored shouldBe r
  }
}