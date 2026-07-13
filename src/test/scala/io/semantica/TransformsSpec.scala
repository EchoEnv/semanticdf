package io.semantica

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Tests for the new `transforms:` YAML block and `withTransforms(...)` Scala DSL.
  *
  * Transforms are per-row computations applied to the source data at model-load
  * time. They produce additional source columns that downstream measures can
  * aggregate. Equivalent to dbt's staging models / LookML's `derived_table`.
  */
class TransformsSpec extends AnyFunSuite with Matchers with SparkSessionFixture {

  // -------------------------------------------------------------------------
  // Scala DSL: withTransforms
  // -------------------------------------------------------------------------

  test("withTransforms adds a per-row column to the source") {
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row("A", java.sql.Date.valueOf("2024-01-05"), java.sql.Date.valueOf("2024-01-07")),
        Row("B", java.sql.Date.valueOf("2024-02-10"), java.sql.Date.valueOf("2024-02-15")),
      )),
      StructType(Seq(
        StructField("id", StringType),
        StructField("admission",  DateType),
        StructField("discharge", DateType),
      )),
    )
    val st = toSemanticTable(df, name = Some("visits"))
      .withTransforms(
        Transform("los_days",
          t => datediff(t("discharge"), t("admission"))),
      )
      .withMeasures(
        Measure("avg_los", t => sum(t("los_days")) / count(lit(1))),
      )

    val rows = st.groupBy().aggregate("avg_los").execute(spark).collect()
    assert(rows.length == 1)
    // los_days is int (datediff returns int). sum(los_days) = 7 (Long).
    // count(1) = 2 (Long). The calc layer promotes to Double for division,
    // so 7 / 2 = 3.5. Use Math.abs to avoid scalatest's `+-` reporting
    // false negatives on exact values.
    val avgLos = rows.head.getAs[Number](0).doubleValue()
    assert(math.abs(avgLos - 3.5) < 1e-9,
      s"expected avg_los=3.5, got $avgLos")
  }

  test("withTransforms applies multiple transforms in declaration order") {
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row("X", 100.0, 5.0),
        Row("Y",  50.0, 3.0),
      )),
      StructType(Seq(
        StructField("id",  StringType),
        StructField("a",   DoubleType),
        StructField("b",   DoubleType),
      )),
    )
    val st = toSemanticTable(df, name = Some("data"))
      .withTransforms(
        // Order matters: `total` depends on `a + b` which is added by the first transform.
        // No — actually, a + b uses source cols. The second transform uses the first's
        // output. This is the test for inter-transform dependencies.
        Transform("sum_ab", t => t("a") + t("b")),
        Transform("double_sum", t => t("sum_ab") * 2),
      )
      .withMeasures(Measure("final_value", t => sum(t("double_sum"))))

    val rows = st.groupBy().aggregate("final_value").execute(spark).collect()
    // ((100+5) * 2) + ((50+3) * 2) = 210 + 106 = 316.
    // The framework may produce Long or Double depending on promotion path;
    // use Number + doubleValue for the comparison.
    val finalValue = rows.head.getAs[Number](0).doubleValue()
    assert(math.abs(finalValue - 316.0) < 1e-9,
      s"expected 316, got $finalValue")
  }

  test("withTransforms supports window functions (lag, row_number)") {
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row("C1", java.sql.Date.valueOf("2024-01-01")),
        Row("C1", java.sql.Date.valueOf("2024-01-10")),
        Row("C1", java.sql.Date.valueOf("2024-02-05")),
        Row("C2", java.sql.Date.valueOf("2024-01-15")),
        Row("C2", java.sql.Date.valueOf("2024-02-20")),
      )),
      StructType(Seq(
        StructField("customer_id", StringType),
        StructField("visit_date",  DateType),
      )),
    )
    val st = toSemanticTable(df, name = Some("visits"))
      .withTransforms(
        // Per-row lag with window — proves transforms handle window functions.
        Transform("prev_visit",
          t => lag(t("visit_date"), 1).over(
            Window.partitionBy("customer_id").orderBy("visit_date"))),
        // Per-row rank — another window function.
        Transform("visit_rank",
          t => row_number().over(
            Window.partitionBy("customer_id").orderBy("visit_date"))),
      )
      .withMeasures(
        Measure("visit_count", t => count(lit(1))),
      )

    val rows = st.groupBy("customer_id")
      .aggregate("visit_count", "visit_rank")
      .execute(spark).collect()
    assert(rows.length == 2, s"expected 2 customer rows, got ${rows.length}")
    val byCustomer = rows.map(r => r.getAs[String](0) -> r.getAs[Long](1)).toMap
    assert(byCustomer("C1") == 3L, s"expected C1=3 visits, got ${byCustomer("C1")}")
    assert(byCustomer("C2") == 2L, s"expected C2=2 visits, got ${byCustomer("C2")}")
  }

  // -------------------------------------------------------------------------
  // Scala DSL: withTransforms on a joined model
  // -------------------------------------------------------------------------

  test("withTransforms applies to joined-model source data") {
    val leftDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row("A", 1L), Row("A", 2L), Row("B", 3L),
      )),
      StructType(Seq(StructField("k", StringType), StructField("v", LongType))),
    )
    val rightDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row("A", 100L), Row("B", 200L))),
      StructType(Seq(StructField("k", StringType), StructField("factor", LongType))),
    )
    val orders = toSemanticTable(leftDf, name = Some("orders"))
      .withDimensions(Dimension("k", t => t("k")))
    val customers = toSemanticTable(rightDf, name = Some("customers"))
      .withDimensions(Dimension("k", t => t("k")), Dimension("factor", t => t("factor")))
    // Transforms see per-row source columns (no aggregates). Reference
    // `v` and `factor` directly, then aggregate `weighted` after.
    val joined = orders.join_one(customers, (l, r) => l("k") === r("k"))
      .withTransforms(
        Transform("weighted", t => t("v") * t("factor")),
      )
      .groupBy("k")
      .aggregate("weighted")
      .execute(spark).collect()
    val byK = joined.map(r => r.getAs[String]("k") -> r.getAs[Long]("weighted")).toMap
    // sum(weighted) per k: A = 1*100 + 2*100 = 300, B = 3*200 = 600
    assert(byK("A") == 300L, s"expected A weighted=300, got ${byK("A")}")
    assert(byK("B") == 600L, s"expected B weighted=600, got ${byK("B")}")
  }

  // -------------------------------------------------------------------------
  // YAML: transforms: block
  // -------------------------------------------------------------------------

  test("YAML transforms: block produces a working model") {
    val yaml =
      """
        |visits:
        |  table: visits_csv
        |  description: "Patient visits"
        |  transforms:
        |    los_days:
        |      expr: "datediff(discharge, admission)"
        |      description: "Length of stay in days"
        |  measures:
        |    visit_count:
        |      expr: "count(1)"
        |    total_los:
        |      expr: "sum(los_days)"
        |  calculated_measures:
        |    avg_los:
        |      expr: "total_los / visit_count"
        |""".stripMargin
    val tmp = java.io.File.createTempFile("transforms-yaml-test", ".yml")
    tmp.deleteOnExit()
    val w = new java.io.PrintWriter(tmp); w.write(yaml); w.close()

    val visitsDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row("V1", java.sql.Date.valueOf("2024-01-05"), java.sql.Date.valueOf("2024-01-07")),
        Row("V2", java.sql.Date.valueOf("2024-02-10"), java.sql.Date.valueOf("2024-02-15")),
      )),
      StructType(Seq(
        StructField("visit_id",  StringType),
        StructField("admission", DateType),
        StructField("discharge", DateType),
      )),
    )
    val tables = Map("visits_csv" -> visitsDf)
    val models = YamlLoader.load(tmp.getAbsolutePath, tables)
    val visits = models("visits")

    // The transform column 'los_days' is now part of the model's source.
    val rows = visits.groupBy().aggregate("avg_los", "visit_count").execute(spark).collect()
    assert(rows.length == 1)
    assert(rows.head.getAs[Long]("visit_count") == 2L)
    // (2 + 5) / 2 = 3.5 (the calc layer promotes to Double for division).
    val avgLos = rows.head.getAs[Number]("avg_los").doubleValue()
    assert(math.abs(avgLos - 3.5) < 1e-9, s"expected avg_los=3.5, got $avgLos")
  }

  test("YAML transforms: block supports window functions in expr") {
    // max(visit_rank) can't go in calculated_measures: (CalcExpr doesn't
    // support function calls). Put it in measures: (Spark expr supports
    // aggregates on any column including transform-added ones).
    val yaml =
      """
        |visits:
        |  table: visits_csv
        |  transforms:
        |    visit_rank:
        |      expr: "row_number() over (partition by customer_id order by visit_date)"
        |  measures:
        |    visit_count:
        |      expr: "count(1)"
        |    top_visit_rank:
        |      expr: "max(visit_rank)"
        |""".stripMargin
    val tmp = java.io.File.createTempFile("transforms-yaml-window-test", ".yml")
    tmp.deleteOnExit()
    val w = new java.io.PrintWriter(tmp); w.write(yaml); w.close()

    val visitsDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row("C1", java.sql.Date.valueOf("2024-01-01")),
        Row("C1", java.sql.Date.valueOf("2024-01-10")),
        Row("C1", java.sql.Date.valueOf("2024-02-05")),
        Row("C2", java.sql.Date.valueOf("2024-01-15")),
      )),
      StructType(Seq(
        StructField("customer_id", StringType),
        StructField("visit_date",  DateType),
      )),
    )
    val tables = Map("visits_csv" -> visitsDf)
    val visits = YamlLoader.load(tmp.getAbsolutePath, tables)("visits")
    val rows = visits.groupBy("customer_id")
      .aggregate("visit_count", "top_visit_rank")
      .execute(spark).collect()
    assert(rows.length == 2, s"expected 2 customer rows, got ${rows.length}")
    val byCustomer = rows.map(r => r.getAs[String]("customer_id") -> r.getAs[Number]("top_visit_rank").longValue()).toMap
    // For each customer, the max rank equals the visit count.
    assert(byCustomer("C1") == 3L, s"expected C1 max_rank=3, got ${byCustomer("C1")}")
    assert(byCustomer("C2") == 1L, s"expected C2 max_rank=1, got ${byCustomer("C2")}")
  }

  // -------------------------------------------------------------------------
  // Single source of truth: YAML and Scala DSL produce the same result
  // -------------------------------------------------------------------------

  test("YAML transforms: and Scala DSL withTransforms produce equivalent results") {
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row("A", 10L, 5L),
        Row("B", 20L, 3L),
      )),
      StructType(Seq(
        StructField("id", StringType),
        StructField("a", LongType),
        StructField("b", LongType),
      )),
    )
    val tables = Map("data_csv" -> df)

    // Approach 1: YAML
    val yaml =
      """
        |data:
        |  table: data_csv
        |  transforms:
        |    diff:
        |      expr: "a - b"
        |  measures:
        |    total_diff:
        |      expr: "sum(diff)"
        |""".stripMargin
    val tmp = java.io.File.createTempFile("transforms-equiv-test", ".yml")
    tmp.deleteOnExit()
    val w = new java.io.PrintWriter(tmp); w.write(yaml); w.close()
    val yamlResult = YamlLoader.load(tmp.getAbsolutePath, tables)("data")
      .groupBy().aggregate("total_diff").execute(spark).collect().head
      .getAs[Long](0)

    // Approach 2: Scala DSL — same result
    val dslResult = toSemanticTable(df, name = Some("data"))
      .withTransforms(Transform("diff", t => t("a") - t("b")))
      .withMeasures(Measure("total_diff", t => sum(t("diff"))))
      .groupBy().aggregate("total_diff").execute(spark).collect().head
      .getAs[Long](0)

    assert(yamlResult == dslResult, s"YAML=$yamlResult, DSL=$dslResult (must be equal)")
    assert(yamlResult == 22L, s"expected 22, got $yamlResult")  // (10-5) + (20-3) = 5+17 = 22
  }
}