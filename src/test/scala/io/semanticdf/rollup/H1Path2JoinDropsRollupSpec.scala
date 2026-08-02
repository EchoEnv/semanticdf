package io.semanticdf.rollup

import io.semanticdf.{Dimension, Measure, SparkSessionFixture, toSemanticTable}
import org.scalatest.funsuite.AnyFunSuite

/** Path-2 contract: rollups do NOT survive joins.
  *
  * The v0.2.4 redesign (PR #330, Post329RedesignSpec.scala:92-95) explicitly
  * said: "Joins on SemanticTable are forbidden when a rollup is active
  *  (useRollup returns a different type)."
  *
  * Round-2 tried to preserve rollups across joins via `joinRollups`, but
  * the round-3 review (H-A1) showed this was silently wrong — `RollupQuery.execute`
  * ignores the joined op tree, so the user got the rollup source alone, no join.
  *
  * The cleaner architectural choice is to NOT preserve rollups across joins:
  * after a `join_one`/`join_many`/`join_cross`, the joined model has empty
  * rollups. Re-register on the joined model if needed.
  *
  * This test pins the contract:
  *   1. `model.join_on(dim, ...).listRollups()` is empty
  *   2. `model.join_on(dim, ...).useRollup("r1", registry)` throws
  */
class H1Path2JoinDropsRollupSpec extends AnyFunSuite with SparkSessionFixture {

  test("H1-Path2: rollup is dropped by join (joined model has empty rollups)") {
    val spark = this.spark
    import org.apache.spark.sql.functions.{col, sum}
    val src = spark.range(5).toDF("k").groupBy("k").agg(sum("k").as("sum_k"))
    val rollup = Rollup(
      "r1", "orders", Seq("k"),
      Seq(RollupMeasure("total", "sum", "sum_k")),
      () => src,
    )
    val model = toSemanticTable(spark.range(5).toDF("k"), name = Some("orders"))
      .withDimensions(Dimension("k", t => t("k")))
      .withMeasures(Measure("total", _ => sum(col("k"))))
      .withRollup(rollup)
    val dim = toSemanticTable(spark.range(5).toDF("k"), name = Some("dim"))
      .withDimensions(Dimension("k", t => t("k")))
    val joined = model.join_on(dim, ("k", "k"))
    assert(joined.listRollups().isEmpty,
      s"H1-Path2: rollup should be dropped by join, got ${joined.listRollups().map(_.name)}")
  }

  test("H1-Path2: useRollup on a joined model throws IllegalArgumentException") {
    val spark = this.spark
    import org.apache.spark.sql.functions.{col, sum}
    val src = spark.range(5).toDF("k").groupBy("k").agg(sum("k").as("sum_k"))
    val rollup = Rollup(
      "r1", "orders", Seq("k"),
      Seq(RollupMeasure("total", "sum", "sum_k")),
      () => src,
    )
    val model = toSemanticTable(spark.range(5).toDF("k"), name = Some("orders"))
      .withDimensions(Dimension("k", t => t("k")))
      .withMeasures(Measure("total", _ => sum(col("k"))))
      .withRollup(rollup)
    val dim = toSemanticTable(spark.range(5).toDF("k"), name = Some("dim"))
      .withDimensions(Dimension("k", t => t("k")))
    val joined = model.join_on(dim, ("k", "k"))
    val ex = intercept[IllegalArgumentException] {
      joined.useRollup("r1", RollupRegistry.empty.register("r1", () => src))
    }
    assert(ex.getMessage.contains("r1") || ex.getMessage.contains("rollup"),
      s"got: ${ex.getMessage}")
  }

  test("H-A1: joined.withRollup throws because joined.name is None (vacuous baseModel check)") {
    // H-A1 in round-3 review: "joined.withRollup succeeds silently".
    // Our H-1 fix uses `name.contains(rollup.baseModel)` which is false for None.
    // Verify: a rollup on a joined model can't be re-registered.
    val spark = this.spark
    import org.apache.spark.sql.functions.{col, sum}
    val src = spark.range(5).toDF("k").groupBy("k").agg(sum("k").as("sum_k"))
    val rollup = Rollup(
      "r1", "orders", Seq("k"),
      Seq(RollupMeasure("total", "sum", "sum_k")),
      () => src,
    )
    val model = toSemanticTable(spark.range(5).toDF("k"), name = Some("orders"))
      .withDimensions(Dimension("k", t => t("k")))
      .withMeasures(Measure("total", _ => sum(col("k"))))
      .withRollup(rollup)
    val dim = toSemanticTable(spark.range(5).toDF("k"), name = Some("dim"))
      .withDimensions(Dimension("k", t => t("k")))
    val joined = model.join_on(dim, ("k", "k"))
    assert(joined.name.isEmpty, s"joined.name should be None, got ${joined.name}")
    val ex = intercept[IllegalArgumentException] {
      joined.withRollup(rollup)
    }
    assert(
      ex.getMessage.contains("anonymous") || ex.getMessage.contains("baseModel") || ex.getMessage.contains("name"),
      s"H-A1: should reject rollup on joined model, got: ${ex.getMessage}"
    )
  }
}
