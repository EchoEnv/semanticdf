package io.semanticdf.core.model

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.semanticdf.core.expr.{Expr, LiteralValue}
import io.semanticdf.core.schema.SealedDataType

/** Phase 2 contract: prove `Dimension` is a usable, Spark-free data
  * record + the smart constructor `Dimension.field` builds the
  * common case correctly. Per scala-data-driven-refactor, this is
  * pure data: the dimension SHAPE is engine-portable; the engine-
  * specific compile is in the engine adapter.
  */
class DimensionSpec extends AnyFunSuite with Matchers {

  test("Dimension.field smart constructor builds a simple field-ref dimension") {
    val d = Dimension.field("region", SealedDataType.Varchar)
    d.name shouldBe "region"
    d.expr shouldBe Expr.FieldRef("region")
    d.dataType shouldBe Some(SealedDataType.Varchar)
  }

  test("Dimension.field with various types") {
    Dimension.field("k",       SealedDataType.BigInt).dataType shouldBe Some(SealedDataType.BigInt)
    Dimension.field("amount",  SealedDataType.Decimal(precision = 10, scale = 2))
      .dataType shouldBe Some(SealedDataType.Decimal(10, 2))
    Dimension.field("flag",    SealedDataType.Boolean).dataType shouldBe Some(SealedDataType.Boolean)
  }

  test("Dimension with derived expression (not a simple field ref)") {
    val d = Dimension(
      name = "region_code",
      expr = Expr.FunctionCall("SUBSTR", Seq(
        Expr.FieldRef("country"),
        Expr.Literal(LiteralValue.IntValue(1), SealedDataType.Int),
        Expr.Literal(LiteralValue.IntValue(2), SealedDataType.Int),
      )),
      dataType = Some(SealedDataType.Varchar),
    )
    d.expr shouldBe a [Expr.FunctionCall]
  }

  test("Dimension with default dataType = None") {
    val d = Dimension(name = "raw", expr = Expr.FieldRef("raw"))
    d.dataType shouldBe None
  }

  test("Dimension is a value, not a singleton — two with same fields are equal") {
    val a = Dimension.field("region", SealedDataType.Varchar)
    val b = Dimension.field("region", SealedDataType.Varchar)
    a shouldBe b
  }

  test("Dimension.field result equals the structural constructor") {
    val a = Dimension.field("x", SealedDataType.Int)
    val b = Dimension(name = "x", expr = Expr.FieldRef("x"), dataType = Some(SealedDataType.Int))
    a shouldBe b
  }

  test("Dimension round-trips through Java serialization") {
    val d = Dimension(
      name = "amount",
      expr = Expr.Multiply(
        Expr.FieldRef("price"),
        Expr.Literal(LiteralValue.LongValue(1L), SealedDataType.BigInt),
      ),
      dataType = Some(SealedDataType.BigInt),
    )
    val bos = new java.io.ByteArrayOutputStream()
    val oos = new java.io.ObjectOutputStream(bos)
    oos.writeObject(d)
    oos.close()
    val bis = new java.io.ByteArrayInputStream(bos.toByteArray)
    val ois = new java.io.ObjectInputStream(bis)
    val restored = ois.readObject().asInstanceOf[Dimension]
    restored shouldBe d
  }
}