package io.semanticdf.rollup

import org.scalatest.funsuite.AnyFunSuite

/** Targeted test for H4: public case-class constructor bypasses
  * validation.
  *
  * Before the fix: `Rollup` was a `final case class` with public 7-arg
  * auto-generated apply. Callers could construct invalid instances:
  *   `Rollup("", "", Nil, Nil, -1, Set.empty)`
  *
  * After the fix: `Rollup` is a `final class` with a `private[rollup]`
  * constructor. The only public way to construct is the smart 5-arg
  * `apply` in the companion, which validates inputs and throws on
  * invalid arguments.
  */
class H4CaseClassBypassSpec extends AnyFunSuite {

  test("H4: empty name is rejected by smart constructor") {
    val ex = intercept[IllegalArgumentException] {
      Rollup(
        name = "",
        baseModel = "orders",
        rollupDimensions = Seq("region"),
        rollupMeasures = Seq(RollupMeasure("total", "sum", "total_storage")),
        sourceProvider = () => null,
      )
    }
    assert(ex.getMessage.contains("name"), s"got: ${ex.getMessage}")
  }

  test("H4: empty baseModel is rejected") {
    val ex = intercept[IllegalArgumentException] {
      Rollup(
        name = "r1",
        baseModel = "",
        rollupDimensions = Seq("region"),
        rollupMeasures = Seq(RollupMeasure("total", "sum", "total_storage")),
        sourceProvider = () => null,
      )
    }
    assert(ex.getMessage.contains("baseModel"), s"got: ${ex.getMessage}")
  }

  test("H4: duplicate dimension names rejected") {
    val ex = intercept[IllegalArgumentException] {
      Rollup(
        name = "r1",
        baseModel = "orders",
        rollupDimensions = Seq("region", "region"),
        rollupMeasures = Seq(RollupMeasure("total", "sum", "total_storage")),
        sourceProvider = () => null,
      )
    }
    assert(ex.getMessage.contains("duplicate"), s"got: ${ex.getMessage}")
  }

  test("H4: duplicate measure names rejected") {
    val ex = intercept[IllegalArgumentException] {
      Rollup(
        name = "r1",
        baseModel = "orders",
        rollupDimensions = Seq("region"),
        rollupMeasures = Seq(
          RollupMeasure("total", "sum", "total_storage_1"),
          RollupMeasure("total", "sum", "total_storage_2"),
        ),
        sourceProvider = () => null,
      )
    }
    assert(ex.getMessage.contains("duplicate"), s"got: ${ex.getMessage}")
  }

  test("H4: dimension/measure name collision rejected") {
    val ex = intercept[IllegalArgumentException] {
      Rollup(
        name = "r1",
        baseModel = "orders",
        rollupDimensions = Seq("region"),
        rollupMeasures = Seq(
          RollupMeasure("region", "sum", "region_storage"),
        ),
        sourceProvider = () => null,
      )
    }
    assert(ex.getMessage.contains("collision") || ex.getMessage.contains("conflict"),
      s"got: ${ex.getMessage}")
  }

  test("H4: RollupMeasure with empty name rejected") {
    val ex = intercept[IllegalArgumentException] {
      RollupMeasure("", "sum", "total_storage")
    }
    assert(ex.getMessage.contains("must not be empty"), s"got: ${ex.getMessage}")
  }

  test("H4: RollupMeasure with empty storageCol rejected") {
    val ex = intercept[IllegalArgumentException] {
      RollupMeasure("total", "sum", "")
    }
    assert(ex.getMessage.contains("storageCol"), s"got: ${ex.getMessage}")
  }

  test("H4: RollupMeasure with unsupported aggregator rejected") {
    val ex = intercept[IllegalArgumentException] {
      RollupMeasure("avg_amount", "avg", "avg_storage")
    }
    assert(ex.getMessage.contains("avg") && (ex.getMessage.contains("Unsupported") || ex.getMessage.contains("v0.2.4")),
      s"got: ${ex.getMessage}")
  }
}
