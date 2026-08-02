package io.semanticdf.rollup

import io.semanticdf.{Dimension, Measure, SparkSessionFixture, toSemanticTable}
import io.semanticdf.predicate.Predicate
import org.apache.spark.sql.functions.sum
import org.scalatest.funsuite.AnyFunSuite

/** Regression tests for post-#328 audit fixes.
  *
  * Each test pins a specific claim from the audit. If the fix regresses,
  * the test fails.
  */
class Post328FixSpec extends AnyFunSuite with SparkSessionFixture {

  // ---- Fix 1: H1-DE — visitor no longer crashes on SemanticRollupOp ----

  test("Fix 1 (H1-DE): useRollup + where() doesn't throw MatchError") {
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
    // Before fix: MatchError on SemanticRollupOp
    val afterWhere = active.where(Predicate.Compare.Gt("k", 0))
    assert(afterWhere.listRollups().map(_.name) == List("r1"),
      s"Fix 1: rollups should survive .where() but got: ${afterWhere.listRollups().map(_.name)}")
  }

  // ---- Fix 2: H1-Arch — rollups propagates through setters ----

  test("Fix 2 (H1-Arch): rollups survives .where()") {
    val spark = this.spark
    val rollupDf = spark.range(10).toDF("k").groupBy("k").agg(sum("k").as("sum_k"))
    val rollup = Rollup("r1", "orders", Seq("k"),
      Seq(RollupMeasure("total", RollupAggregator.Sum, "sum_k")), () => rollupDf)
    val model = toSemanticTable(spark.range(10).toDF("k"), name = Some("orders"))
      .withDimensions(Dimension("k", t => t("k")))
      .withMeasures(Measure("total", t => sum(t("k"))))
      .withRollup(rollup)
    val afterWhere = model.where(Predicate.Compare.Gt("k", 0))
    assert(afterWhere.listRollups().map(_.name) == List("r1"),
      s"Fix 2: rollups should survive .where() but got: ${afterWhere.listRollups().map(_.name)}")
  }

  // ---- Fix 3: H2-Arch — query() skip-aggregate works after wrappers ----

  test("Fix 3 (H2-Arch): useRollup + where + query + execute still uses rollup") {
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
      .where(Predicate.Compare.Gt("k", 0))  // excludes k=0
    val result = active.query(measures = Seq("total"), dimensions = Seq("k"))
      .execute(spark).collect()
    // k=1..9, so 9 rows. (k=0 fails the where.)
    assert(result.length == 9, s"Fix 3: expected 9 rows (k=1..9) but got ${result.length}")
  }

  // ---- Fix 4: H2-DE — grain mismatch throws clear error ----

  test("Fix 4 (H2-DE): query at subset of rollup grain throws clear error") {
    val spark = this.spark
    import org.apache.spark.sql.types.StringType
    import org.apache.spark.sql.functions.{col => fcol, sum => fsum}
    val raw = spark.range(10).toDF("k")
      .withColumn("region", (fcol("k") % 3).cast(StringType))
      .withColumn("category", (fcol("k") % 5).cast(StringType))
    val rollupDf = raw.groupBy("region", "category").agg(fsum("k").as("sum_k"))
    val rollup = Rollup("r1", "orders", Seq("region", "category"),
      Seq(RollupMeasure("total", RollupAggregator.Sum, "sum_k")), () => rollupDf)
    val model = toSemanticTable(raw, name = Some("orders"))
      .withDimensions(
        Dimension("region", t => t("region")),
        Dimension("category", t => t("category")),
      )
      .withMeasures(Measure("total", t => fsum(t("k"))))
      .withRollup(rollup)
    val registry = RollupRegistry.empty.register("r1", () => rollupDf)
    val active = model.useRollup("r1", registry)
    // Query with just "region" — grain mismatch (rollup is (region, category))
    val ex = intercept[IllegalArgumentException] {
      active.query(measures = Seq("total"), dimensions = Seq("region"))
        .execute(spark).collect()
    }
    assert(ex.getMessage.contains("grain") || ex.getMessage.contains("mismatch") ||
           ex.getMessage.contains("rollup"),
      s"Fix 4: error should mention grain/mismatch, got: ${ex.getMessage}")
  }

  // ---- Fix 5: H3-DE — having on rollup throws clear error (v0.2.4 limitation) ----

  test("Fix 5 (H3-DE): having on rollup throws clear error") {
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
    // having on rollup: should throw clear error (v0.2.4 limitation, v0.3.x will translate)
    val ex = intercept[IllegalArgumentException] {
      active.query(measures = Seq("total"), dimensions = Seq("k"),
                   having = Some(Predicate.Compare.Gt("total", 5)))
        .execute(spark).collect()
    }
    assert(ex.getMessage.contains("having") || ex.getMessage.contains("v0.3.x"),
      s"Fix 5: error should mention having/v0.3.x, got: ${ex.getMessage}")
  }
}
