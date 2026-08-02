package io.semanticdf.rollup

import io.semanticdf.{Dimension, Measure, SparkSessionFixture, toSemanticTable}
import io.semanticdf.predicate.Predicate
import org.scalatest.funsuite.AnyFunSuite

/** Targeted test for H2-α: predicateToColumn rejects 4 valid Compare
  * subtypes (Contains, StartsWith, EndsWith, ArrayContains).
  *
  * The match in RollupQuery.execute() handles Eq/Ne/Lt/Le/Gt/Ge but
  * silently throws for Contains/StartsWith/EndsWith/ArrayContains,
  * even though the error message claims "Compare" is supported.
  */
class H2PredicateCompareSpec extends AnyFunSuite with SparkSessionFixture {

  test("H2-α: Predicate.Compare.Contains works in rollup WHERE") {
    val spark = this.spark
    val src = spark.range(3).toDF("k")
      .withColumn("name", org.apache.spark.sql.functions.lit("hello"))
      .groupBy("name")
      .agg(org.apache.spark.sql.functions.count("k").as("cnt"))
    val rollup = Rollup("r1", "t", Seq("name"),
      Seq(RollupMeasure("cnt", "count", "cnt")), () => src)
    val model = toSemanticTable(src, name = Some("t"))
      .withDimensions(Dimension("name", t => t("name")))
      .withMeasures(Measure("cnt", _ => org.apache.spark.sql.functions.count(org.apache.spark.sql.functions.col("k"))))
      .withRollup(rollup)
    val registry = RollupRegistry.empty.register("r1", () => src)
    val result = model.useRollup("r1", registry)
      .withWhere(Predicate.Compare.Contains("name", "hell"))
      .execute(spark)
    assert(result.count() == 1, s"expected 1 row, got ${result.count()}")
  }

  test("H2-α: Predicate.Compare.StartsWith works in rollup WHERE") {
    val spark = this.spark
    val src = spark.range(3).toDF("k")
      .withColumn("name", org.apache.spark.sql.functions.lit("hello"))
      .groupBy("name")
      .agg(org.apache.spark.sql.functions.count("k").as("cnt"))
    val rollup = Rollup("r1", "t", Seq("name"),
      Seq(RollupMeasure("cnt", "count", "cnt")), () => src)
    val model = toSemanticTable(src, name = Some("t"))
      .withDimensions(Dimension("name", t => t("name")))
      .withMeasures(Measure("cnt", _ => org.apache.spark.sql.functions.count(org.apache.spark.sql.functions.col("k"))))
      .withRollup(rollup)
    val registry = RollupRegistry.empty.register("r1", () => src)
    val result = model.useRollup("r1", registry)
      .withWhere(Predicate.Compare.StartsWith("name", "hel"))
      .execute(spark)
    assert(result.count() == 1, s"expected 1 row, got ${result.count()}")
  }
}
