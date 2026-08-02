package io.semanticdf.rollup

import org.scalatest.matchers.should.Matchers
import io.semanticdf.{Dimension, Measure, SemanticTable, SparkSessionFixture, toSemanticTable}
import io.semanticdf.predicate.Predicate
import io.semanticdf.SortKey
import org.apache.spark.sql.functions.sum
import org.scalatest.funsuite.AnyFunSuite

/** Manual rollups feature tests for v0.2.4 (redesign).
  *
  * The previous design had `useRollup` return a SemanticTable with
  * `root = SemanticRollupOp(...)`, which caused 19+ HIGH-severity bugs
  * across 5 audit cycles. The new design returns a separate
  * [[RollupQuery]] type that has NO interaction with the existing
  * fluent chain.
  *
  * Each test pins a falsifiable claim from the design. If the test
  * passes, the design is correct.
  */
class ManualRollupSpec extends AnyFunSuite with Matchers with SparkSessionFixture {

  // ---- RollupAggregator ----

  test("parse: 'sum' and 'count' return Some; Min/Max/Avg/Stddev return None (deferred)") {
    RollupAggregator.parse("sum").isDefined shouldBe true
    RollupAggregator.parse("count").isDefined shouldBe true
    RollupAggregator.parse("SUM").isDefined shouldBe true
    RollupAggregator.parse("min").isEmpty shouldBe true
    RollupAggregator.parse("max").isEmpty shouldBe true
    RollupAggregator.parse("avg").isEmpty shouldBe true
    RollupAggregator.parse("stddev").isEmpty shouldBe true
    RollupAggregator.parse("unknown").isEmpty shouldBe true
  }

  test("Sum and Count are exact-additive at any grain") {
    RollupAggregator.Sum.canReAggregate(Set("region"), Set("region", "category")) shouldBe true
    RollupAggregator.Count.canReAggregate(Set("region", "category"), Set("region")) shouldBe true
  }

  // ---- RollupMeasure smart constructor ----

  test("RollupMeasure: unsupported aggregator throws IllegalArgumentException") {
    val ex = intercept[IllegalArgumentException] {
      RollupMeasure("avg_amount", "avg", "avg_col")
    }
    ex.getMessage should include ("v0.2.4 supports: sum, count")
  }

  // ---- Rollup smart constructor ----

  private def buildOrdersDf(spark: org.apache.spark.sql.SparkSession, n: Int = 100) = {
    spark.range(n).toDF("k").withColumn("v", (org.apache.spark.sql.functions.col("k") * 3 % 7).cast("string"))
  }

  private def buildModel(spark: org.apache.spark.sql.SparkSession): SemanticTable = {
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

  // ---- SemanticTable.withRollup / listRollups / findRollup / useRollup ----

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
    val model = buildModel(spark)
    val rollup = Rollup(
      name             = "r1",
      baseModel        = "OTHER_MODEL",
      rollupDimensions = Seq("k"),
      rollupMeasures   = Seq(RollupMeasure("total", "sum", "sum_k")),
      sourceProvider   = () => spark.range(10).toDF("k").groupBy("k").agg(sum("k").as("sum_k")),
    )
    intercept[IllegalArgumentException] {
      model.withRollup(rollup)
    }
  }

  test("withRollup: registering a name twice REPLACES the old") {
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

  // ---- useRollup: returns RollupQuery (not SemanticTable) ----

  test("useRollup returns RollupQuery, not SemanticTable") {
    val spark = this.spark
    val rollupDf = spark.range(10).toDF("k").groupBy("k").agg(sum("k").as("sum_k"))
    val rollup = Rollup("r1", "orders", Seq("k"),
      Seq(RollupMeasure("total", "sum", "sum_k")), () => rollupDf)
    val model = buildModel(spark).withRollup(rollup)
    val registry = RollupRegistry.empty.register("r1", () => rollupDf)
    val active = model.useRollup("r1", registry)
    // The return type is RollupQuery, NOT SemanticTable.
    // This is the key design decision: the type system prevents the
    // 5+ bugs we found in the v1 design.
    assert(active.isInstanceOf[RollupQuery],
      s"useRollup should return RollupQuery, got ${active.getClass.getName}")
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
    val rollup = Rollup("r1", "orders", Seq("k"),
      Seq(RollupMeasure("total", "sum", "sum_k")), () => rollupDf)
    val model = buildModel(spark).withRollup(rollup)
    val emptyRegistry = RollupRegistry.empty
    intercept[IllegalArgumentException] {
      model.useRollup("r1", emptyRegistry)
    }
  }

  // ---- RollupQuery.execute ----

  test("RollupQuery.execute: returns rollup-projected rows") {
    val spark = this.spark
    val rollupDf = spark.range(10).toDF("k").groupBy("k").agg(sum("k").as("sum_k"))
    val rollup = Rollup("r1", "orders", Seq("k"),
      Seq(RollupMeasure("total", "sum", "sum_k")), () => rollupDf)
    val model = buildModel(spark).withRollup(rollup)
    val registry = RollupRegistry.empty.register("r1", () => rollupDf)
    val result = model.useRollup("r1", registry).execute(spark).collect()
    result.length shouldBe 10
  }

  // ---- RollupQuery.withWhere / withOrderBy / withLimit ----

  test("RollupQuery.withWhere: applies predicate to rollup data") {
    val spark = this.spark
    val rollupDf = spark.range(10).toDF("k").groupBy("k").agg(sum("k").as("sum_k"))
    val rollup = Rollup("r1", "orders", Seq("k"),
      Seq(RollupMeasure("total", "sum", "sum_k")), () => rollupDf)
    val model = buildModel(spark).withRollup(rollup)
    val registry = RollupRegistry.empty.register("r1", () => rollupDf)
    val result = model.useRollup("r1", registry)
      .withWhere(Predicate.Compare.Gt("k", 0))
      .execute(spark)
      .collect()
    // k > 0: 9 rows (k=1..9)
    result.length shouldBe 9
  }

  test("RollupQuery.withOrderBy + withLimit: works") {
    val spark = this.spark
    val rollupDf = spark.range(10).toDF("k").groupBy("k").agg(sum("k").as("sum_k"))
    val rollup = Rollup("r1", "orders", Seq("k"),
      Seq(RollupMeasure("total", "sum", "sum_k")), () => rollupDf)
    val model = buildModel(spark).withRollup(rollup)
    val registry = RollupRegistry.empty.register("r1", () => rollupDf)
    val result = model.useRollup("r1", registry)
      .withOrderBy(SortKey.desc("k"))
      .withLimit(3)
      .execute(spark)
      .collect()
    result.length shouldBe 3
    // k=9, k=8, k=7
    result(0).getAs[Long]("k") shouldBe 9L
    result(2).getAs[Long]("k") shouldBe 7L
  }

  test("RollupQuery.withWhere: throws clear error for missing columns") {
    val spark = this.spark
    val rollupDf = spark.range(10).toDF("k").groupBy("k").agg(sum("k").as("sum_k"))
    val rollup = Rollup("r1", "orders", Seq("k"),
      Seq(RollupMeasure("total", "sum", "sum_k")), () => rollupDf)
    val model = buildModel(spark).withRollup(rollup)
    val registry = RollupRegistry.empty.register("r1", () => rollupDf)
    val ex = intercept[IllegalStateException] {
      model.useRollup("r1", registry)
        .withWhere(Predicate.Compare.Gt("v", "x"))  // v is in rollup source
        .execute(spark)
        .collect()
    }
    ex.getMessage should (include("WHERE") and include("rollup"))
  }
}
