package io.semanticdf.rollup
import org.apache.spark.sql.functions.col

import io.semanticdf.{Dimension, Measure, SemanticTable, SparkSessionFixture, toSemanticTable}
import io.semanticdf.rollup.{Rollup, RollupAggregator, RollupMeasure, RollupRegistry}
import org.apache.spark.sql.functions.sum
import org.scalatest.funsuite.AnyFunSuite

/** Targeted test for H1: rollup metadata propagation.
  *
  * The bug: withRollup(r) stores the rollup in the new SemanticTable,
  * but the existing 82+ setters (where, having, orderBy, limit, withHint,
  * withRowFilter, withDimensions, withMeasures, withTransforms, version,
  * status, etc.) don't propagate the rollups field. So fluent chain
  * operations silently drop the rollup.
  *
  * Fix: every `new SemanticTable(...)` call must pass
  * `rollups = this.rollups` to preserve the metadata.
  */
class H1PropagationSpec extends AnyFunSuite with SparkSessionFixture {

  test("H1 REAL: withRollup(r).version(2) loses the rollup") {
    val spark = this.spark
    val rollupDf = spark.range(10).toDF("k").groupBy("k").agg(sum("k").as("sum_k"))
    val rollup = Rollup("r1", "orders", Seq("k"),
      Seq(RollupMeasure("total", "sum", "sum_k")), () => rollupDf)
    val model = toSemanticTable(spark.range(10).toDF("k"), name = Some("orders"))
      .withDimensions(Dimension("k", t => t("k")))
      .withMeasures(Measure("total", _ => sum(col("k"))))
      .withRollup(rollup)
    val afterVersion = model.version(2)
    assert(afterVersion.listRollups().map(_.name) == List("r1"),
      s"H1 REAL: rollup should survive .version(2) but got ${afterVersion.listRollups().map(_.name)}")
  }

  test("H1: where() preserves rollup") {
    val spark = this.spark
    val rollupDf = spark.range(10).toDF("k").groupBy("k").agg(sum("k").as("sum_k"))
    val rollup = Rollup("r1", "orders", Seq("k"),
      Seq(RollupMeasure("total", "sum", "sum_k")), () => rollupDf)
    val model = toSemanticTable(spark.range(10).toDF("k"), name = Some("orders"))
      .withDimensions(Dimension("k", t => t("k")))
      .withMeasures(Measure("total", _ => sum(col("k"))))
      .withRollup(rollup)
    val afterWhere = model.where(io.semanticdf.predicate.Predicate.Compare.Gt("k", 0))
    assert(afterWhere.listRollups().map(_.name) == List("r1"),
      s"H1 REAL: rollup should survive .where() but got ${afterWhere.listRollups().map(_.name)}")
  }

  test("H1: orderBy() preserves rollup") {
    val spark = this.spark
    val rollupDf = spark.range(10).toDF("k").groupBy("k").agg(sum("k").as("sum_k"))
    val rollup = Rollup("r1", "orders", Seq("k"),
      Seq(RollupMeasure("total", "sum", "sum_k")), () => rollupDf)
    val model = toSemanticTable(spark.range(10).toDF("k"), name = Some("orders"))
      .withDimensions(Dimension("k", t => t("k")))
      .withMeasures(Measure("total", _ => sum(col("k"))))
      .withRollup(rollup)
    val afterOrderBy = model.orderBy(io.semanticdf.SortKey.asc("k"))
    assert(afterOrderBy.listRollups().map(_.name) == List("r1"),
      s"H1 REAL: rollup should survive .orderBy() but got ${afterOrderBy.listRollups().map(_.name)}")
  }
}
