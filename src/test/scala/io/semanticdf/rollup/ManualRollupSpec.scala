package io.semanticdf.rollup

import io.semanticdf.{Dimension, Measure, SemanticTable, SparkSessionFixture, toSemanticTable}
import io.semanticdf.audit.AuditSink
import io.semanticdf.cache.ResultCache

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, lit, sum}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Manual rollups feature tests for v0.2.4.
  *
  * Each test pins a falsifiable claim from the design plan (per
  * debug-mantra): every PR ships tests that would fail if the
  * implementation were wrong.
  */
class ManualRollupSpec extends AnyFunSuite with Matchers with SparkSessionFixture {

  // ---- RollupAggregator ----

  test("parse: 'sum' and 'count' return Some; Min/Max/Avg/Stddev return None (deferred)") {
    RollupAggregator.parse("sum").isDefined shouldBe true
    RollupAggregator.parse("count").isDefined shouldBe true
    RollupAggregator.parse("SUM").isDefined shouldBe true  // case-insensitive
    RollupAggregator.parse("min").isEmpty shouldBe true      // deferred
    RollupAggregator.parse("max").isEmpty shouldBe true      // deferred
    RollupAggregator.parse("avg").isEmpty shouldBe true      // deferred
    RollupAggregator.parse("stddev").isEmpty shouldBe true   // deferred
    RollupAggregator.parse("unknown").isEmpty shouldBe true
  }

  test("Sum and Count are exact-additive at any grain") {
    RollupAggregator.Sum.canReAggregate(Set("region"), Set("region", "category")) shouldBe true
    RollupAggregator.Sum.canReAggregate(Set("region", "category"), Set("region")) shouldBe true
    RollupAggregator.Count.canReAggregate(Set("region"), Set("region", "category")) shouldBe true
    RollupAggregator.Count.canReAggregate(Set("region", "category"), Set("region")) shouldBe true
  }

  // ---- RollupMeasure smart constructor ----

  test("RollupMeasure: unsupported aggregator throws IllegalArgumentException") {
    val ex = intercept[IllegalArgumentException] {
      RollupMeasure("avg_amount", "avg", "avg_col")
    }
    ex.getMessage should include ("avg")
    ex.getMessage should include ("v0.2.4 supports: sum, count")
  }

  test("RollupMeasure: empty storageCol throws IllegalArgumentException") {
    intercept[IllegalArgumentException] {
      RollupMeasure("total", "sum", "")
    }
  }

  // ---- Rollup smart constructor ----

  private def buildOrdersDf(spark: SparkSession, n: Int = 100) = {
    spark.range(n).toDF("k").withColumn("v", (col("k") * 3 % 7).cast("string"))
  }

  private def buildModel(spark: SparkSession): SemanticTable = {
    val df = buildOrdersDf(spark)
    toSemanticTable(df, name = Some("orders"))
      .withDimensions(
        Dimension("k", t => t("k")),
        Dimension("v", t => t("v")),
      )
      .withMeasures(Measure("total", _ => sum(df("k"))))
  }

  test("Rollup: smart constructor precomputes stats from source") {
    val spark = this.spark
    val df = buildOrdersDf(spark, n = 100)
    val rollup = Rollup(
      name             = "r1",
      baseModel        = "orders",
      rollupDimensions = Seq("k"),
      rollupMeasures   = Seq(RollupMeasure("total", "sum", "sum_k")),
      sourceProvider   = () => df.groupBy("k").agg(sum("k").as("sum_k")),
    )
    rollup.precomputedRowCount shouldBe 100L
    rollup.precomputedColumns shouldBe Set("k", "sum_k")
  }

  test("Rollup: dimension not in source columns throws IllegalArgumentException") {
    val spark = this.spark
    val df = buildOrdersDf(spark)
    intercept[IllegalArgumentException] {
      Rollup(
        name             = "r1",
        baseModel        = "orders",
        rollupDimensions = Seq("nonexistent"),
        rollupMeasures   = Seq(RollupMeasure("total", "sum", "sum_k")),
        sourceProvider   = () => df.groupBy("k").agg(sum("k").as("sum_k")),
      )
    }
  }

  test("Rollup: storage column not in source throws IllegalArgumentException") {
    val spark = this.spark
    val df = buildOrdersDf(spark)
    intercept[IllegalArgumentException] {
      Rollup(
        name             = "r1",
        baseModel        = "orders",
        rollupDimensions = Seq("k"),
        rollupMeasures   = Seq(RollupMeasure("total", "sum", "missing_col")),
        sourceProvider   = () => df.groupBy("k").agg(sum("k").as("sum_k")),
      )
    }
  }

  // ---- SemanticTable.withRollup / listRollups / findRollup ----

  test("withRollup: register a rollup, listRollups returns it") {
    val spark = this.spark
    val model = buildModel(spark)
    val rollup = Rollup(
      name             = "r1",
      baseModel        = "orders",
      rollupDimensions = Seq("k"),
      rollupMeasures   = Seq(RollupMeasure("total", "sum", "sum_k")),
      sourceProvider   = () => spark.range(10).toDF("k").groupBy("k").agg(sum("k").as("sum_k")),
    )
    val modelWith = model.withRollup(rollup)
    modelWith.listRollups() shouldBe List(rollup)
    modelWith.findRollup("r1") shouldBe Some(rollup)
    modelWith.findRollup("nope") shouldBe None
  }

  test("withRollup: baseModel mismatch throws IllegalArgumentException") {
    val spark = this.spark
    val model = buildModel(spark)  // baseModel = "orders"
    val rollup = Rollup(
      name             = "r1",
      baseModel        = "OTHER_MODEL",  // mismatch
      rollupDimensions = Seq("k"),
      rollupMeasures   = Seq(RollupMeasure("total", "sum", "sum_k")),
      sourceProvider   = () => spark.range(10).toDF("k").groupBy("k").agg(sum("k").as("sum_k")),
    )
    intercept[IllegalArgumentException] {
      model.withRollup(rollup)
    }
  }

  test("withRollup: registering a name twice REPLACES the old (re-register pattern)") {
    val spark = this.spark
    val model = buildModel(spark)
    val src1 = spark.range(10).toDF("k").groupBy("k").agg(sum("k").as("sum_k"))
    val src2 = spark.range(20).toDF("k").groupBy("k").agg(sum("k").as("sum_k"))
    val r1 = Rollup("r1", "orders", Seq("k"), Seq(RollupMeasure("total", "sum", "sum_k")), () => src1)
    val r2 = Rollup("r1", "orders", Seq("k"), Seq(RollupMeasure("total", "sum", "sum_k")), () => src2)
    val modelFinal = model.withRollup(r1).withRollup(r2)
    modelFinal.listRollups() should have size 1
    modelFinal.findRollup("r1").map(_.precomputedRowCount) shouldBe Some(20L)
  }

  // ---- useRollup + execute(spark) ----

  test("useRollup + execute(spark): returns rollup-projected rows") {
    val spark = this.spark
    val baseDf = buildOrdersDf(spark, n = 1000)
    val rollupDf = baseDf.groupBy("k").agg(sum("k").as("sum_k"))  // 1000 rows
    val model = buildModel(spark).withRollup(Rollup(
      name             = "r1",
      baseModel        = "orders",
      rollupDimensions = Seq("k"),
      rollupMeasures   = Seq(RollupMeasure("total", "sum", "sum_k")),
      sourceProvider   = () => rollupDf,
    ))
    val registry = RollupRegistry.empty.register("r1", () => rollupDf)
    val result = model.useRollup("r1", registry)
      .query(measures = Seq("total"), dimensions = Seq("k"))
      .execute(spark)
      .collect()
    result.length shouldBe 1000  // one row per k (since k is unique in range(n))
  }

  test("useRollup: throws if rollup not registered on model") {
    val spark = this.spark
    val model = buildModel(spark)
    val registry = RollupRegistry.empty
    intercept[IllegalArgumentException] {
      model.useRollup("nope", registry)
    }
  }

  test("useRollup: throws if registry doesn't contain the rollup") {
    val spark = this.spark
    val rollupDf = spark.range(10).toDF("k").groupBy("k").agg(sum("k").as("sum_k"))
    val model = buildModel(spark).withRollup(Rollup(
      name             = "r1",
      baseModel        = "orders",
      rollupDimensions = Seq("k"),
      rollupMeasures   = Seq(RollupMeasure("total", "sum", "sum_k")),
      sourceProvider   = () => rollupDf,
    ))
    val emptyRegistry = RollupRegistry.empty  // no providers
    intercept[IllegalArgumentException] {
      model.useRollup("r1", emptyRegistry)
    }
  }

  test("execute(spark) without useRollup: falls through to base (no-op for rollup)") {
    val spark = this.spark
    val model = buildModel(spark)
    val result = model.query(measures = Seq("total"), dimensions = Seq("k"))
      .execute(spark)
      .collect()
    result.length should be > 0
  }

  test("execute(spark) with useRollup: registry is captured at useRollup time") {
    // The registry is captured at useRollup time. After that, execute(spark)
    // works without the registry being re-passed.
    val spark = this.spark
    val rollupDf = spark.range(10).toDF("k").groupBy("k").agg(sum("k").as("sum_k"))
    val model = buildModel(spark).withRollup(Rollup(
      name             = "r1",
      baseModel        = "orders",
      rollupDimensions = Seq("k"),
      rollupMeasures   = Seq(RollupMeasure("total", "sum", "sum_k")),
      sourceProvider   = () => rollupDf,
    ))
    val active = model.useRollup("r1", RollupRegistry.empty.register("r1", () => rollupDf))
    val result = active.query(measures = Seq("total"), dimensions = Seq("k"))
      .execute(spark)
      .collect()
    result.length shouldBe 10
  }
}