package io.semanticdf

import org.apache.spark.sql.{Row, SparkSession, functions => F}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite

/** Tests for [[TypedMeasure]] / `Measure.typed[T]` — Phase E2/E3 typed-measure
  * factory.
  *
  * Verifies:
  *  1. Backward compat — `Measure("name", fn)` (untyped) keeps working.
  *  2. The new `Measure.typed[T]("name", fn)` factory compiles, runs, and
  *     returns a [[Measure]] that works with existing `withMeasures`.
  *  3. The typed form composes with [[TypedArithmetic]] (typed lambda body
  *     using `divide`, `plus`, etc.).
  *  4. Type parameters are erased at runtime — different `[T]` for the same
  *     body produces the same output. */
class TypedMeasureSpec extends AnyFunSuite with SparkSessionFixture {

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

  test("untyped form is unchanged (backward compat — no overload regression)") {
    implicit val s: SparkSession = spark
    val df = tinyNumDf(s)
    val model = toSemanticTable(df, name = Some("m"))
      .withDimensions(Dimension("k", t => t("k")))
      .withMeasures(Measure("ratio", t => F.sum(t("a")) / F.sum(t("b"))))
    val rows = model.groupBy("k").aggregate("ratio").toDataFrame(spark).collect()
    assert(rows.length == 2)
  }

  test("Measure.typed[Double](name, fn) compiles, runs, returns Measure") {
    implicit val s: SparkSession = spark
    val df = tinyNumDf(s)
    val typedMeasure: Measure = Measure.typed[Double]("ratio", t =>
      TypedArithmetic.divide[Long, Long, Double](F.sum(t("a")), F.sum(t("b"))))
    // The factory returns a plain Measure (not Measure[Double]); existing
    // withMeasures accepts it.
    val model = toSemanticTable(df, name = Some("m"))
      .withDimensions(Dimension("k", t => t("k")))
      .withMeasures(typedMeasure)
    val byKey = model.groupBy("k").aggregate("ratio").toDataFrame(spark).collect()
      .map(r => r.getString(0) -> r.getDouble(1)).toMap
    // k=x: (10+5) / (2+1) = 5; k=y: 30/4 = 7.5
    assert(math.abs(byKey("x") - 5.0)  < 0.01, s"k=x: expected 5.0, got ${byKey("x")}")
    assert(math.abs(byKey("y") - 7.5)  < 0.01, s"k=y: expected 7.5, got ${byKey("y")}")
  }

  test("typed measure composes with TypedArithmetic in one expression") {
    implicit val s: SparkSession = spark
    val df = tinyNumDf(s)
    // (sum(a) + sum(b)) * (sum(a) - sum(b)) / sum(b)
    //   k=x: (15+3) * (15-3) / 3 = 18 * 12 / 3 = 72
    //   k=y: (30+4) * (30-4) / 4 = 34 * 26 / 4 = 221
    val m: Measure = Measure.typed[Double]("q", t =>
      TypedArithmetic.divide[Long, Long, Double](
        TypedArithmetic.multiply[Long, Long, Long](
          TypedArithmetic.plus[Long, Long, Long](F.sum(t("a")), F.sum(t("b"))),
          TypedArithmetic.minus[Long, Long, Long](F.sum(t("a")), F.sum(t("b")))),
        F.sum(t("b"))))
    val model = toSemanticTable(df, name = Some("m"))
      .withDimensions(Dimension("k", t => t("k")))
      .withMeasures(m)
    val byKey = model.groupBy("k").aggregate("q").toDataFrame(spark).collect()
      .map(r => r.getString(0) -> r.getDouble(1)).toMap
    assert(math.abs(byKey("x") - 72.0)  < 0.01, s"k=x: expected 72.0, got ${byKey("x")}")
    assert(math.abs(byKey("y") - 221.0) < 0.01, s"k=y: expected 221.0, got ${byKey("y")}")
  }

  test("different [T] for the same body produce the same output (T erased)") {
    implicit val s: SparkSession = spark
    val df = tinyNumDf(s)
    // Same arithmetic body, declared as Double and as Float. At runtime
    // T is erased, so both produce a Column. Spark's /  coerces to
    // double; the values should be identical.
    val typedDouble: Measure = Measure.typed[Double]("r_d", t =>
      TypedArithmetic.divide[Long, Long, Double](F.sum(t("a")), F.sum(t("b"))))
    val typedFloat: Measure = Measure.typed[Float]("r_f", t =>
      TypedArithmetic.divide[Long, Long, Float](F.sum(t("a")), F.sum(t("b"))))
    val model = toSemanticTable(df, name = Some("m"))
      .withDimensions(Dimension("k", t => t("k")))
      .withMeasures(typedDouble, typedFloat)
    val byKey = model.groupBy("k").aggregate("r_d", "r_f").toDataFrame(spark).collect()
      .map(r => r.getString(0) -> (r.getDouble(1), r.getDouble(2))).toMap
    // For each k, the two values should be equal (within float precision).
    byKey.foreach { case (k, (d, f)) =>
      assert(math.abs(d - f) < 0.001, s"k=$k: typed Double ($d) and typed Float ($f) differ")
    }
  }

  test("typed and untyped forms produce the same result for the same arithmetic") {
    implicit val s: SparkSession = spark
    val df = tinyNumDf(s)
    val untyped = toSemanticTable(df, name = Some("m"))
      .withDimensions(Dimension("k", t => t("k")))
      .withMeasures(Measure("r", t => F.sum(t("a")) / F.sum(t("b"))))
    val typed = toSemanticTable(df, name = Some("m"))
      .withDimensions(Dimension("k", t => t("k")))
      .withMeasures(Measure.typed[Double]("r", t =>
        TypedArithmetic.divide[Long, Long, Double](F.sum(t("a")), F.sum(t("b")))))
    val untypedRows = untyped.groupBy("k").aggregate("r").toDataFrame(spark).collect()
      .map(r => r.getString(0) -> r.getDouble(1)).toMap
    val typedRows = typed.groupBy("k").aggregate("r").toDataFrame(spark).collect()
      .map(r => r.getString(0) -> r.getDouble(1)).toMap
    untypedRows.keys.foreach { k =>
      assert(math.abs(untypedRows(k) - typedRows(k)) < 0.001,
        s"k=$k: untyped=${untypedRows(k)}, typed=${typedRows(k)}")
    }
  }
}
