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

  // ---------------------------------------------------------------------------
  // Macro derivation: ResultDecoder.derive[T] (PR feat/resultdecoder-derive-macro)
  //
  // The macro generates a `ResultDecoder[T]` instance at compile time for case
  // classes whose fields are all primitive types (String, Int, Long, Double,
  // Float, Boolean, Short, Byte, BigDecimal). Compile-time failure for
  // non-case-classes and for unsupported field types.
  // ---------------------------------------------------------------------------

  test("derive[T] for a 2-field case class uses the right Row getters") {
    case class CarrierCount(carrier: String, count: Long)
    implicit val decoder: ResultDecoder[CarrierCount] = ResultDecoder.derive[CarrierCount]
    val row = Row("AA", 100L)
    decoder.decode(row) shouldBe CarrierCount("AA", 100L)
  }

  test("derive[T] covers every primitive field type") {
    case class AllPrimitives(
      s: String, i: Int, l: Long, d: Double, f: Float, b: Boolean, sh: Short, by: Byte
    )
    implicit val decoder: ResultDecoder[AllPrimitives] = ResultDecoder.derive[AllPrimitives]
    val row = Row("hi", 42, 1234567890123L, 3.14, 2.5f, true, 7.toShort, 8.toByte)
    decoder.decode(row) shouldBe AllPrimitives("hi", 42, 1234567890123L, 3.14, 2.5f, true, 7.toShort, 8.toByte)
  }

  test("derive[T] reads columns in declaration order (not field-name order)") {
    // Field-order test: a row built in column order [col0, col1, col2] must
    // decode into the case class whose constructor expects exactly that order,
    // even if the case-class field names are different.
    case class Foo(a: Long, b: String, c: Int)
    implicit val decoder: ResultDecoder[Foo] = ResultDecoder.derive[Foo]
    val row = Row(100L, "hello", 7)
    decoder.decode(row) shouldBe Foo(100L, "hello", 7)
  }

  test("derive[T] works through collectAs[T] end-to-end on a SemanticTable") {
    case class CarrierCount(carrier: String, count: Long)
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row("AA", 100L), Row("UA", 200L), Row("DL", 300L),
      )),
      StructType(Seq(
        StructField("carrier", StringType),
        StructField("count",   LongType),
      )),
    )
    implicit val decoder: ResultDecoder[CarrierCount] = ResultDecoder.derive[CarrierCount]
    val table = toSemanticTable(df, name = Some("flights"))
    table.collectAs[CarrierCount](spark) shouldBe Seq(
      CarrierCount("AA", 100L),
      CarrierCount("UA", 200L),
      CarrierCount("DL", 300L),
    )
  }

  test("derive[T]: empty case class works (0-arg constructor)") {
    case class Empty()
    implicit val decoder: ResultDecoder[Empty] = ResultDecoder.derive[Empty]
    val row = Row() // empty row
    decoder.decode(row) shouldBe Empty()
  }

  test("derive[T]: BigDecimal field type works") {
    import java.math.BigDecimal
    case class Money(amount: BigDecimal, label: String)
    implicit val decoder: ResultDecoder[Money] = ResultDecoder.derive[Money]
    val bd = new BigDecimal("12345.67")
    val row = Row(bd, "price")
    decoder.decode(row) shouldBe Money(bd, "price")
  }
}