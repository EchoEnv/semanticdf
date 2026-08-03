package io.semanticdf.rollup

import io.semanticdf.{Dimension, Measure, SparkSessionFixture, toSemanticTable}
import io.semanticdf.predicate.Predicate
import org.scalatest.funsuite.AnyFunSuite

/** Targeted test for H2: WHERE clause validation uses projected schema
  * instead of source schema.
  *
  * The bug: in RollupQuery.execute(), WHERE validation uses
  * `projected.schema.fieldNames` which is only the dim + measure columns.
  * If the source has additional columns (e.g., a date column that the
  * user wants to filter on), the WHERE is incorrectly rejected.
  *
  * Fix: validate predicate columns against `rollup.precomputedColumns`
  * (the source schema) AND apply WHERE to the source BEFORE projection.
  */
class H2WhereSchemaSpec extends AnyFunSuite with SparkSessionFixture {

  test("H2 REAL: WHERE on a source-only column is rejected") {
    // Source has: region, date, region_total
    // Projection: region (dim), region_total (measure)
    // User filters: date > "2024-01-01"
    // Bug: rejected because "date" not in projection
    val spark = this.spark
    val rollupDf = spark.range(10).toDF("region")
      .withColumn("date", org.apache.spark.sql.functions.lit("2024-06-01"))
      .groupBy("region", "date")
      .agg(org.apache.spark.sql.functions.sum("region").as("region_total"))

    val rollup = Rollup(
      name = "r1",
      baseModel = "orders",
      rollupDimensions = Seq("region"),
      rollupMeasures = Seq(RollupMeasure("region_total", "sum", "region_total")),
      sourceProvider = () => rollupDf,
    )
    val registry = RollupRegistry.empty.register("r1", () => rollupDf)
    val model = toSemanticTable(spark.range(10).toDF("region"), name = Some("orders"))
      .withDimensions(Dimension("region", t => t("region")))
      .withMeasures(Measure("region_total", _ => org.apache.spark.sql.functions.sum(org.apache.spark.sql.functions.col("region"))))
      .withRollup(rollup)
    val query = model.useRollup("r1", registry)
      .withWhere(Predicate.Compare.Eq("date", "2024-06-01"))

    // After the fix, this should NOT throw
    val result = query.execute(spark)
    val cols = result.columns.toList
    assert(cols.contains("region"), s"expected region in output: $cols")
  }

  test("H2: WHERE on a column NOT in source is still rejected") {
    // User filters on "nonexistent_col" - should be rejected
    val spark = this.spark
    val rollupDf = spark.range(10).toDF("region")
      .groupBy("region")
      .agg(org.apache.spark.sql.functions.sum("region").as("region_total"))
    val rollup = Rollup(
      name = "r1",
      baseModel = "orders",
      rollupDimensions = Seq("region"),
      rollupMeasures = Seq(RollupMeasure("region_total", "sum", "region_total")),
      sourceProvider = () => rollupDf,
    )
    val registry = RollupRegistry.empty.register("r1", () => rollupDf)
    val model = toSemanticTable(spark.range(10).toDF("region"), name = Some("orders"))
      .withDimensions(Dimension("region", t => t("region")))
      .withMeasures(Measure("region_total", _ => org.apache.spark.sql.functions.sum(org.apache.spark.sql.functions.col("region"))))
      .withRollup(rollup)
    val query = model.useRollup("r1", registry)
      .withWhere(Predicate.Compare.Eq("nonexistent_col", "x"))

    val ex = intercept[IllegalStateException] {
      query.execute(spark)
    }
    assert(ex.getMessage.contains("nonexistent_col") || ex.getMessage.contains("not") || ex.getMessage.contains("aren't in the rollup"),
      s"got: ${ex.getMessage}")
  }
}
