package io.semanticdf.core.model

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.semanticdf.core.expr.{Expr, LiteralValue}
import io.semanticdf.core.schema.SealedDataType

/** Phase 2 contract: prove `FilterSpec` is a usable, Spark-free data
  * record. Per scala-data-driven-refactor, this is pure data: the
  * filter-spec SHAPE is engine-portable; the engine-specific compile
  * is in the engine adapter.
  */
class FilterSpecSpec extends AnyFunSuite with Matchers {

  test("FilterSpec carries name + predicate (Expr)") {
    val f = FilterSpec(
      name      = "active_only",
      predicate = Expr.GreaterThan(
        Expr.FieldRef("status"),
        Expr.Literal(LiteralValue.IntValue(0), SealedDataType.Int),
      ),
    )
    f.name shouldBe "active_only"
    f.predicate shouldBe a [Expr.GreaterThan]
  }

  test("FilterSpec with IsNull predicate") {
    val f = FilterSpec(
      name      = "missing_email",
      predicate = Expr.IsNull(Expr.FieldRef("email")),
    )
    f.predicate shouldBe a [Expr.IsNull]
  }

  test("FilterSpec with compound predicate (And/Or)") {
    val f = FilterSpec(
      name      = "active_and_recent",
      predicate = Expr.And(
        Expr.GreaterThan(
          Expr.FieldRef("last_login"),
          Expr.Literal(LiteralValue.LongValue(0L), SealedDataType.BigInt),
        ),
        Expr.IsNotNull(Expr.FieldRef("email")),
      ),
    )
    f.predicate shouldBe a [Expr.And]
  }

  test("FilterSpec is a value, not a singleton — two with same fields are equal") {
    val a = FilterSpec("f", Expr.Literal(LiteralValue.BoolValue(true), SealedDataType.Boolean))
    val b = FilterSpec("f", Expr.Literal(LiteralValue.BoolValue(true), SealedDataType.Boolean))
    a shouldBe b
  }

  test("FilterSpec with different names are not equal") {
    val a = FilterSpec("a", Expr.Literal(LiteralValue.BoolValue(true), SealedDataType.Boolean))
    val b = FilterSpec("b", Expr.Literal(LiteralValue.BoolValue(true), SealedDataType.Boolean))
    a should not be b
  }

  test("FilterSpec round-trips through Java serialization") {
    val f = FilterSpec(
      name      = "amount_positive",
      predicate = Expr.GreaterThan(
        Expr.FieldRef("amount"),
        Expr.Literal(LiteralValue.LongValue(0L), SealedDataType.BigInt),
      ),
    )
    val bos = new java.io.ByteArrayOutputStream()
    val oos = new java.io.ObjectOutputStream(bos)
    oos.writeObject(f)
    oos.close()
    val bis = new java.io.ByteArrayInputStream(bos.toByteArray)
    val ois = new java.io.ObjectInputStream(bis)
    val restored = ois.readObject().asInstanceOf[FilterSpec]
    restored shouldBe f
  }
}