package io.semanticdf

import org.apache.spark.sql.{Row, SparkSession, functions => F}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite

/** Tests for [[TypedArithmetic]] — Phase E3 typed-arithmetic DSL.
  *
  * Verifies:
  *  1. Backward compatibility — the untyped form keeps working unchanged.
  *  2. The typed form produces the same numeric result as the untyped form.
  *  3. Type parameters are erased at runtime — different [T, U, R]
  *     combinations exercise the same code path.
  *
  * Compile-time rejection of non-numeric types (e.g. `String`) is documented
  * in [[TypedArithmetic]] and not exercised here — would require a separate
  * test file that doesn't compile, which is awkward under ScalaTest.
  *
  * The test lambdas use `sum(t("a"))` / `sum(t("b"))` so the framework
  * classifies them as base measures (not calc), and the underlying
  * Spark expression is a ratio / difference / etc. of aggregates. */
class TypedArithmeticSpec extends AnyFunSuite with SparkSessionFixture {

  private def tinyNumDf(spark: SparkSession) = {
    val schema = StructType(Seq(
      StructField("k", StringType, nullable = false),
      StructField("a", LongType, nullable = false),
      StructField("b", LongType, nullable = false)))
    val rows = spark.sparkContext.parallelize(Seq(
      Row("x", 10L, 2L),
      Row("y", 30L, 4L),
      Row("x", 5L,  1L),
    ))
    spark.createDataFrame(rows, schema)
  }

  private def tinyIntDf(spark: SparkSession) = {
    val schema = StructType(Seq(
      StructField("k", StringType, nullable = false),
      StructField("a", IntegerType, nullable = false),
      StructField("b", IntegerType, nullable = false)))
    val rows = spark.sparkContext.parallelize(Seq(
      Row("x", 10, 3),
      Row("y", 30, 4),
    ))
    spark.createDataFrame(rows, schema)
  }

  test("untyped form is unchanged (backward compat)") {
    implicit val s: SparkSession = spark
    val df = tinyNumDf(s)
    val model = toSemanticTable(df, name = Some("m"))
      .withDimensions(Dimension("k", t => t("k")))
      .withMeasures(Measure("ratio", t => F.sum(t("a")) / F.sum(t("b"))))
    val rows = model.groupBy("k").aggregate("ratio").toDataFrame(spark).collect()
    assert(rows.length == 2, s"Expected 2 group rows (k=x, k=y), got ${rows.length}")
  }

  test("typed divide produces the same result as the untyped form") {
    implicit val s: SparkSession = spark
    val df = tinyNumDf(s)
    val model = toSemanticTable(df, name = Some("m"))
      .withDimensions(Dimension("k", t => t("k")))
      .withMeasures(Measure("ratio", t =>
        TypedArithmetic.divide[Long, Long, Double](F.sum(t("a")), F.sum(t("b")))))
    val byKey = model.groupBy("k").aggregate("ratio").toDataFrame(spark).collect()
      .map(r => r.getString(0) -> r.getDouble(1)).toMap
    // k=x: (10+5) / (2+1) = 15/3 = 5
    // k=y: 30/4 = 7.5
    assert(math.abs(byKey("x") - 5.0)  < 0.01, s"k=x: expected 5.0, got ${byKey("x")}")
    assert(math.abs(byKey("y") - 7.5)  < 0.01, s"k=y: expected 7.5, got ${byKey("y")}")
  }

  test("typed plus, minus, multiply work for Long, Long, Long") {
    implicit val s: SparkSession = spark
    val df = tinyNumDf(s)
    val model = toSemanticTable(df, name = Some("m"))
      .withDimensions(Dimension("k", t => t("k")))
      .withMeasures(
        Measure("s", t => TypedArithmetic.plus[Long, Long, Long](F.sum(t("a")), F.sum(t("b")))),
        Measure("d", t => TypedArithmetic.minus[Long, Long, Long](F.sum(t("a")), F.sum(t("b")))),
        Measure("p", t => TypedArithmetic.multiply[Long, Long, Long](F.sum(t("a")), F.sum(t("b")))),
      )
    val rows = model.groupBy("k").aggregate("s", "d", "p").toDataFrame(spark).collect()
    val byKey = rows.map(r => r.getString(0) -> (r.getLong(1), r.getLong(2), r.getLong(3))).toMap
    // k=x: s = 15+3 = 18, d = 15-3 = 12, p = 15*3 = 45
    val (sx, dx, px) = byKey("x")
    assert(sx == 18L, s"plus k=x: $sx")
    assert(dx == 12L, s"minus k=x: $dx")
    assert(px == 45L, s"multiply k=x: $px")
  }

  test("type parameters are erased — Int, Long, Double all hit the same code path") {
    implicit val s: SparkSession = spark
    // Use Int columns to verify the [T, U, R] type assertions are erased
    // at runtime: divide[Int, Int, Double] is the same code path as
    // divide[Long, Long, Double] after type erasure.
    val df = tinyIntDf(s)
    val model = toSemanticTable(df, name = Some("m"))
      .withDimensions(Dimension("k", t => t("k")))
      .withMeasures(Measure("r", t =>
        TypedArithmetic.divide[Int, Int, Double](F.sum(t("a")), F.sum(t("b")))))
    val byKey = model.groupBy("k").aggregate("r").toDataFrame(spark).collect()
      .map(r => r.getString(0) -> r.getDouble(1)).toMap
    // k=x: 10/3 ≈ 3.333
    // k=y: 30/4 = 7.5
    assert(math.abs(byKey("x") - 3.333) < 0.01, s"10/3 ≈ 3.333, got ${byKey("x")}")
    assert(math.abs(byKey("y") - 7.5)   < 0.01, s"30/4 = 7.5, got ${byKey("y")}")
  }

  test("typed arithmetic composes: divide of multiply of (plus, minus) in one lambda") {
    implicit val s: SparkSession = spark
    val df = tinyNumDf(s)
    // For each row: (sum(a) + sum(b)) * (sum(a) - sum(b)) / sum(b)
    //   k=x: (15+3) * (15-3) / 3 = 18 * 12 / 3 = 72
    //   k=y: (30+4) * (30-4) / 4 = 34 * 26 / 4 = 221
    val model = toSemanticTable(df, name = Some("m"))
      .withDimensions(Dimension("k", t => t("k")))
      .withMeasures(Measure("q", t =>
        TypedArithmetic.divide[Long, Long, Double](
          TypedArithmetic.multiply[Long, Long, Long](
            TypedArithmetic.plus[Long, Long, Long](F.sum(t("a")), F.sum(t("b"))),
            TypedArithmetic.minus[Long, Long, Long](F.sum(t("a")), F.sum(t("b")))),
          F.sum(t("b")))))
    val byKey = model.groupBy("k").aggregate("q").toDataFrame(spark).collect()
      .map(r => r.getString(0) -> r.getDouble(1)).toMap
    assert(math.abs(byKey("x") - 72.0)  < 0.01, s"k=x: expected 72.0, got ${byKey("x")}")
    assert(math.abs(byKey("y") - 221.0) < 0.01, s"k=y: expected 221.0, got ${byKey("y")}")
  }
}
