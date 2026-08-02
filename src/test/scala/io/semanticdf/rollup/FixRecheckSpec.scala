package io.semanticdf.rollup

import io.semanticdf.{Dimension, Measure, SparkSessionFixture, toSemanticTable}
import org.apache.spark.sql.functions.sum
import org.scalatest.funsuite.AnyFunSuite

/** Falsification tests for the post-#328 fix. Each test was written
  * BEFORE the fix was applied (where applicable) to confirm the bug.
  * Now run AFTER the fix to ensure no regression. */
class FixRecheckSpec extends AnyFunSuite with SparkSessionFixture {

  test("RECHECK H1-Arch: rollups survives .withDimensions()") {
    val spark = this.spark
    val rollupDf = spark.range(10).toDF("k").groupBy("k").agg(sum("k").as("sum_k"))
    val rollup = Rollup("r1", "orders", Seq("k"),
      Seq(RollupMeasure("total", RollupAggregator.Sum, "sum_k")), () => rollupDf)
    val model = toSemanticTable(spark.range(10).toDF("k"), name = Some("orders"))
      .withDimensions(Dimension("k", t => t("k")))
      .withMeasures(Measure("total", t => sum(t("k"))))
      .withRollup(rollup)
    val afterWithDims = model.withDimensions(Dimension("v", t => t("k")))
    println(s"After withDimensions: rollups=${afterWithDims.listRollups().map(_.name)}")
    assert(afterWithDims.listRollups().map(_.name) == List("r1"),
      s"RECHECK FAIL: rollups should survive .withDimensions() but got: ${afterWithDims.listRollups().map(_.name)}")
  }

  test("RECHECK H1-Arch: rollups survives .withMeasures()") {
    val spark = this.spark
    val rollupDf = spark.range(10).toDF("k").groupBy("k").agg(sum("k").as("sum_k"))
    val rollup = Rollup("r1", "orders", Seq("k"),
      Seq(RollupMeasure("total", RollupAggregator.Sum, "sum_k")), () => rollupDf)
    val model = toSemanticTable(spark.range(10).toDF("k"), name = Some("orders"))
      .withDimensions(Dimension("k", t => t("k")))
      .withMeasures(Measure("total", t => sum(t("k"))))
      .withRollup(rollup)
    val afterWithMeasures = model.withMeasures(Measure("other", t => sum(t("k"))))
    println(s"After withMeasures: rollups=${afterWithMeasures.listRollups().map(_.name)}")
    assert(afterWithMeasures.listRollups().map(_.name) == List("r1"),
      s"RECHECK FAIL: rollups should survive .withMeasures() but got: ${afterWithMeasures.listRollups().map(_.name)}")
  }

  test("RECHECK H1-Arch: rollups survives .groupBy().aggregate()") {
    val spark = this.spark
    val rollupDf = spark.range(10).toDF("k").groupBy("k").agg(sum("k").as("sum_k"))
    val rollup = Rollup("r1", "orders", Seq("k"),
      Seq(RollupMeasure("total", RollupAggregator.Sum, "sum_k")), () => rollupDf)
    val model = toSemanticTable(spark.range(10).toDF("k"), name = Some("orders"))
      .withDimensions(Dimension("k", t => t("k")))
      .withMeasures(Measure("total", t => sum(t("k"))))
      .withRollup(rollup)
    val afterGroupBy = model.groupBy("k").aggregate("total")
    println(s"After groupBy().aggregate(): rollups=${afterGroupBy.listRollups().map(_.name)}")
    assert(afterGroupBy.listRollups().map(_.name) == List("r1"),
      s"RECHECK FAIL: rollups should survive .groupBy().aggregate() but got: ${afterGroupBy.listRollups().map(_.name)}")
  }

  test("RECHECK H1: rollups survives .withRowFilter()") {
    val spark = this.spark
    val rollupDf = spark.range(10).toDF("k").groupBy("k").agg(sum("k").as("sum_k"))
    val rollup = Rollup("r1", "orders", Seq("k"),
      Seq(RollupMeasure("total", RollupAggregator.Sum, "sum_k")), () => rollupDf)
    val model = toSemanticTable(spark.range(10).toDF("k"), name = Some("orders"))
      .withDimensions(Dimension("k", t => t("k")))
      .withMeasures(Measure("total", t => sum(t("k"))))
      .withRollup(rollup)
    val afterRowFilter = model.withRowFilter("k_pos", "k > 0", description = None, metadata = Map.empty)
    println(s"After withRowFilter: rollups=${afterRowFilter.listRollups().map(_.name)}")
    assert(afterRowFilter.listRollups().map(_.name) == List("r1"),
      s"RECHECK FAIL: rollups should survive .withRowFilter() but got: ${afterRowFilter.listRollups().map(_.name)}")
  }

  test("RECHECK M1: rollups survives .join_one()") {
    val spark = this.spark
    val rollupDf = spark.range(10).toDF("k").groupBy("k").agg(sum("k").as("sum_k"))
    val rollup = Rollup("r1", "orders", Seq("k"),
      Seq(RollupMeasure("total", RollupAggregator.Sum, "sum_k")), () => rollupDf)
    val model = toSemanticTable(spark.range(10).toDF("k"), name = Some("orders"))
      .withDimensions(Dimension("k", t => t("k")))
      .withMeasures(Measure("total", t => sum(t("k"))))
      .withRollup(rollup)
    val other = toSemanticTable(spark.range(10).toDF("k2"), name = Some("other"))
      .withDimensions(Dimension("k2", t => t("k2")))
    val afterJoin = model.join_one(other, (l, r) => l("k") === r("k2"))
    println(s"After join_one: rollups=${afterJoin.listRollups().map(_.name)}")
    assert(afterJoin.listRollups().map(_.name) == List("r1"),
      s"RECHECK FAIL: rollups should survive .join_one() but got: ${afterJoin.listRollups().map(_.name)}")
  }

  test("RECHECK H2: useRollup + withRowFilter + query + execute works") {
    val spark = this.spark
    val rollupDf = spark.range(10).toDF("k").groupBy("k").agg(sum("k").as("sum_k"))
    val rollup = Rollup("r1", "orders", Seq("k"),
      Seq(RollupMeasure("total", RollupAggregator.Sum, "sum_k")), () => rollupDf)
    val model = toSemanticTable(spark.range(10).toDF("k"), name = Some("orders"))
      .withDimensions(Dimension("k", t => t("k")))
      .withMeasures(Measure("total", t => sum(t("k"))))
      .withRollup(rollup)
    val registry = RollupRegistry.empty.register("r1", () => rollupDf)
    val active = model.useRollup("r1", registry)
      .withRowFilter("k_pos", "k > 0", description = None, metadata = Map.empty)
    val result = active.query(measures = Seq("total"), dimensions = Seq("k"))
      .execute(spark).collect()
    assert(result.length == 9, s"RECHECK H2 FAIL: withRowFilter on useRollup should work, got ${result.length} rows")
  }
}
