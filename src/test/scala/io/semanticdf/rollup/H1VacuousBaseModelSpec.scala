package io.semanticdf.rollup

import io.semanticdf.{Dimension, Measure, SparkSessionFixture, toSemanticTable}
import org.scalatest.funsuite.AnyFunSuite

/** Targeted test for H-1: vacuous baseModel check in withRollup.
  *
  * The check `require(rollup.baseModel == name.getOrElse(rollup.baseModel), ...)`
  * is vacuously true when the model was constructed with `name = None`,
  * because `name.getOrElse(rollup.baseModel)` returns `rollup.baseModel` always.
  * A rollup with any baseModel can be silently registered on an anonymous model.
  *
  * Fix: use `require(name.contains(rollup.baseModel), ...)` which is false
  * when the model is anonymous.
  */
class H1VacuousBaseModelSpec extends AnyFunSuite with SparkSessionFixture {

  test("H-1 REAL: withRollup on anonymous model (name = None) accepts any baseModel (vacuous)") {
    val spark = this.spark
    import org.apache.spark.sql.functions.{col, sum}
    val src = spark.range(5).toDF("k").groupBy("k").agg(sum("k").as("sum_k"))
    val rollup = Rollup(
      "r1", "ANY_BASE_MODEL", Seq("k"),  // bogus baseModel
      Seq(RollupMeasure("total", "sum", "sum_k")),
      () => src,
    )
    val anonModel = toSemanticTable(spark.range(5).toDF("k"), name = None)
      .withDimensions(Dimension("k", t => t("k")))
    val ex = intercept[IllegalArgumentException] {
      anonModel.withRollup(rollup)
    }
    assert(ex.getMessage.contains("baseModel") || ex.getMessage.contains("anonymous"),
      s"H-1: should reject rollup on anonymous model, got: ${ex.getMessage}")
  }
}
