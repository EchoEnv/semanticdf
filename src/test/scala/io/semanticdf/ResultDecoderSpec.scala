package io.semanticdf

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

/** Tests for the [[ResultDecoder]] typeclass and [[SemanticTable.collectAs]]
  * typed-terminal API.
  *
  * Covers:
  *  - Each built-in primitive decoder reads column 0 correctly.
  *  - `collectAs[T]` plumbs a Spark DataFrame through the implicit decoder.
  *  - A user-supplied decoder (tuple, custom case class) works.
  *  - A missing decoder gives a clear compile error (verified at the
  *    call site, not via a runtime test). */
class ResultDecoderSpec extends AnyFunSuite with SparkSessionFixture with FlightsFixture {

  // ---------------------------------------------------------------------------
  // Built-in primitive decoders
  // ---------------------------------------------------------------------------

  test("stringDecoder reads column 0 of a Row as String") {
    val row = Row("hello")
    ResultDecoder[String].decode(row) shouldBe "hello"
  }

  test("intDecoder reads column 0 of a Row as Int") {
    val row = Row(42)
    ResultDecoder[Int].decode(row) shouldBe 42
  }

  test("longDecoder reads column 0 of a Row as Long") {
    val row = Row(1234567890123L)
    ResultDecoder[Long].decode(row) shouldBe 1234567890123L
  }

  test("doubleDecoder reads column 0 of a Row as Double") {
    val row = Row(3.14)
    ResultDecoder[Double].decode(row) shouldBe 3.14
  }

  test("floatDecoder reads column 0 of a Row as Float") {
    val row = Row(2.5f)
    ResultDecoder[Float].decode(row) shouldBe 2.5f
  }

  test("booleanDecoder reads column 0 of a Row as Boolean") {
    val row = Row(true)
    ResultDecoder[Boolean].decode(row) shouldBe true
  }

  // ---------------------------------------------------------------------------
  // collectAs[T] — typed terminal on SemanticTable
  // ---------------------------------------------------------------------------

  test("collectAs[String] returns the first column as strings") {
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row("AA"), Row("UA"))),
      StructType(Seq(StructField("carrier", StringType))),
    )
    val table = toSemanticTable(df, name = Some("flights"))
    val result = table.collectAs[String](spark)
    result shouldBe Seq("AA", "UA")
  }

  test("collectAs[Long] returns the first column as longs") {
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(100L), Row(200L))),
      StructType(Seq(StructField("count", LongType))),
    )
    val table = toSemanticTable(df, name = Some("counts"))
    val result = table.collectAs[Long](spark)
    result shouldBe Seq(100L, 200L)
  }

  // ---------------------------------------------------------------------------
  // Custom decoder — user-supplied ResultDecoder for a tuple / case class
  // ---------------------------------------------------------------------------

  test("custom tuple decoder reads multiple columns") {
    // A 2-column DataFrame + a user decoder that reads both.
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row("AA", 100L), Row("UA", 200L))),
      StructType(Seq(
        StructField("carrier", StringType),
        StructField("count",   LongType),
      )),
    )
    implicit val pairDecoder: ResultDecoder[(String, Long)] =
      new ResultDecoder[(String, Long)] {
        def decode(row: Row): (String, Long) = (row.getString(0), row.getLong(1))
      }
    val table = toSemanticTable(df, name = Some("flights"))
    val result = table.collectAs[(String, Long)](spark)
    result shouldBe Seq(("AA", 100L), ("UA", 200L))
  }

  test("custom case-class decoder — manual instance, no derivation") {
    case class CarrierCount(carrier: String, count: Long)
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row("AA", 100L))),
      StructType(Seq(
        StructField("carrier", StringType),
        StructField("count",   LongType),
      )),
    )
    implicit val ccDecoder: ResultDecoder[CarrierCount] =
      new ResultDecoder[CarrierCount] {
        def decode(row: Row): CarrierCount = CarrierCount(row.getString(0), row.getLong(1))
      }
    val table = toSemanticTable(df, name = Some("flights"))
    val result = table.collectAs[CarrierCount](spark)
    result shouldBe Seq(CarrierCount("AA", 100L))
  }
}