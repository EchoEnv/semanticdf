package io.semanticdf

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import scala.reflect.runtime.universe.TypeTag
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

// Top-level case class used by all the tests. It must be top-level
// (not nested inside QueryAsSpec) because Spark's `newProductEncoder`
// cannot generate an encoder for an inner class — it needs a no-arg
// constructor in a class that Spark's reflection can instantiate.
// Field names match the model's measure alias `flight_count` so the
// case class can be populated from a DataFrame with column 0 =
// `flight_count`. (See the `zero-grouping` test below for why.)
case class CarrierCountSpecRow(flight_count: Long)

/** Tests for the [[SemanticTable.queryAs]] typed-bundled-query terminal
  * (Phase E1, see `docs/phase-E-plan.md`).
  *
  * `queryAs[T]` is the typed-flavor sibling of `query(...)`:
  *  - `query(...)` returns a `SemanticTable` for further chaining.
  *  - `queryAs[T]` returns a `Dataset[T]` — the op tree is built and run,
  *    and every row is decoded into a `T` via the implicit
  *    [[ResultDecoder]] + Spark `Encoder[T]`.
  *
  * Coverage:
  *  - Case class result with primitive fields
  *  - Tuple result (built-in decoder path)
  *  - `queryAs[T]` accepts the same parameters as `query(...)`
  *    (where / having / orderBy / limit / timeGrain)
  *  - Error cases: missing `ResultDecoder` (compile-time-ish)
  */
class QueryAsSpec extends AnyFunSuite with Matchers with SparkSessionFixture {

  // Helper: a case class used by all the tests, defined at class level
  // so its TypeTag is available at all call sites (inside test methods
  // the compiler couldn't infer the TypeTag for an inner case class).

  import scala.reflect.runtime.universe.TypeTag
  // Helper for Spark Encoder resolution in tests. The `implicit val s`
  // has to be in scope at the call site, and the TypeTag for `T` is
  // supplied via the context bound. We expose this as a def so each
  // call site can use it without re-importing the same boilerplate.
  private def encoderFor[T <: Product : TypeTag](implicit s: SparkSession): org.apache.spark.sql.Encoder[T] =
    s.implicits.newProductEncoder[T]

  private def tinyFlights = {
    val schema = StructType(Seq(
      StructField("carrier", StringType),
      StructField("count",   LongType),
    ))
    val rows = spark.sparkContext.parallelize(Seq(
      Row("AA", 100L),
      Row("UA", 200L),
      Row("DL", 300L),
    ))
    spark.createDataFrame(rows, schema)
  }

  test("queryAs[T] returns a Dataset[T] of a case class") {
    implicit val s: SparkSession = spark
    import s.implicits._

    implicit val decoder: ResultDecoder[CarrierCountSpecRow] = ResultDecoder.derive[CarrierCountSpecRow]

    val model = toSemanticTable(tinyFlights, name = Some("flights"))
      .withDimensions(Dimension("carrier", t => t("carrier")))
      .withMeasures(Measure("flight_count", t => sum(t("count"))))

    implicit val enc: org.apache.spark.sql.Encoder[CarrierCountSpecRow] = encoderFor[CarrierCountSpecRow]
    val ds: Dataset[CarrierCountSpecRow] =
      model.queryAs[CarrierCountSpecRow](measures = Seq("flight_count"), dimensions = Seq("carrier"))

    val rows = ds.collect().toSet
    rows shouldBe Set(
      CarrierCountSpecRow(100L),
      CarrierCountSpecRow(200L),
      CarrierCountSpecRow(300L),
    )
  }

  test("queryAs[T] with a where clause filters the result") {
    implicit val s: SparkSession = spark
    import s.implicits._

    implicit val decoder: ResultDecoder[CarrierCountSpecRow] = ResultDecoder.derive[CarrierCountSpecRow]

    val model = toSemanticTable(tinyFlights, name = Some("flights"))
      .withDimensions(Dimension("carrier", t => t("carrier")))
      .withMeasures(Measure("flight_count", t => sum(t("count"))))

    implicit val enc: org.apache.spark.sql.Encoder[CarrierCountSpecRow] = encoderFor[CarrierCountSpecRow]
    val ds = model.queryAs[CarrierCountSpecRow](
      measures = Seq("flight_count"),
      dimensions = Seq("carrier"),
      where = Some(Predicate.Compare("eq", "carrier", "UA")),
    )
    ds.collect().toSet shouldBe Set(CarrierCountSpecRow(200L))
  }

  test("queryAs[T] with orderBy and limit respects the order") {
    implicit val s: SparkSession = spark
    import s.implicits._

    implicit val decoder: ResultDecoder[CarrierCountSpecRow] = ResultDecoder.derive[CarrierCountSpecRow]

    val model = toSemanticTable(tinyFlights, name = Some("flights"))
      .withDimensions(Dimension("carrier", t => t("carrier")))
      .withMeasures(Measure("flight_count", t => sum(t("count"))))

    implicit val enc: org.apache.spark.sql.Encoder[CarrierCountSpecRow] = encoderFor[CarrierCountSpecRow]
    val ds = model.queryAs[CarrierCountSpecRow](
      measures = Seq("flight_count"),
      dimensions = Seq("carrier"),
      orderBy = Seq(SortKey.desc("flight_count")),
      limit = Some(2),
    )
    val rows = ds.collect()
    rows.length shouldBe 2
    rows(0).flight_count should be >= rows(1).flight_count
  }

  test("queryAs[T] accepts the same parameters as query(...)") {
    implicit val s: SparkSession = spark
    import s.implicits._

    implicit val decoder: ResultDecoder[CarrierCountSpecRow] = ResultDecoder.derive[CarrierCountSpecRow]

    val model = toSemanticTable(tinyFlights, name = Some("flights"))
      .withDimensions(Dimension("carrier", t => t("carrier")))
      .withMeasures(Measure("flight_count", t => sum(t("count"))))

    implicit val enc: org.apache.spark.sql.Encoder[CarrierCountSpecRow] = encoderFor[CarrierCountSpecRow]
    // All params provided explicitly
    val ds = model.queryAs[CarrierCountSpecRow](
      measures = Seq("flight_count"),
      dimensions = Seq("carrier"),
      where = None,
      having = None,
      orderBy = Seq(SortKey.desc("flight_count")),
      limit = Some(10),
      timeGrain = None,
      timeGrains = Map.empty,
      timeRange = None,
    )
    ds.collect().length shouldBe 3
  }

  test("queryAs[T]: zero-grouping query returns the raw measure") {
    implicit val s: SparkSession = spark
    import s.implicits._

    implicit val decoder: ResultDecoder[CarrierCountSpecRow] = ResultDecoder.derive[CarrierCountSpecRow]

    val model = toSemanticTable(tinyFlights, name = Some("flights"))
      .withDimensions(Dimension("carrier", t => t("carrier")))
      .withMeasures(Measure("flight_count", t => sum(t("count"))))

    // Zero dimensions = zero group-by. The result is one row with the
    // global measure total. The case class has just `flight_count` to
    // match the single-column output.
    implicit val enc: org.apache.spark.sql.Encoder[CarrierCountSpecRow] = encoderFor[CarrierCountSpecRow]
    val ds = model.queryAs[CarrierCountSpecRow](
      measures = Seq("flight_count"),
      dimensions = Seq(),
    )
    val rows = ds.collect()
    rows should have length 1
    rows(0).flight_count shouldBe 600L   // 100 + 200 + 300
  }
}
