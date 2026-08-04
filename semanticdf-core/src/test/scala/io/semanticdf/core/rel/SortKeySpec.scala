package io.semanticdf.core.rel

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.semanticdf.core.expr.{Expr, LiteralValue}
import io.semanticdf.core.schema.SealedDataType

/** Phase 2 contract: prove `SortKey` (the relational-plan sort key
  * with `expression: Expr + direction + nullOrdering`) is a usable,
  * Spark-free data record + Serializable round-trip. Per scala-data-
  * driven-refactor, this is pure data: the sort-key SHAPE is engine-
  * portable; the engine-specific compile is in the engine adapter.
  *
  * Note: this `SortKey` is in `core.rel.*` (the relational IR), not
  * the simpler Phase 1 mirror at `core.field.SortKey`. The two
  * coexist intentionally — see SortKey.scala for the rationale.
  */
class SortKeySpec extends AnyFunSuite with Matchers {

  private val sampleExpr = Expr.FieldRef("amount")

  test("SortKey carries expression + direction + nullOrdering") {
    val k = SortKey(sampleExpr, SortDirection.Ascending, NullOrdering.Last)
    k.expression shouldBe sampleExpr
    k.direction shouldBe SortDirection.Ascending
    k.nullOrdering shouldBe NullOrdering.Last
  }

  test("SortKey with default-friendly combinations: 2 directions x 2 null orderings") {
    val all: Set[SortKey] = Set(
      SortKey(sampleExpr, SortDirection.Ascending, NullOrdering.First),
      SortKey(sampleExpr, SortDirection.Ascending, NullOrdering.Last),
      SortKey(sampleExpr, SortDirection.Descending, NullOrdering.First),
      SortKey(sampleExpr, SortDirection.Descending, NullOrdering.Last),
    )
    all.size shouldBe 4
  }

  test("SortKey is a value, not a singleton — two with same fields are equal") {
    val a = SortKey(sampleExpr, SortDirection.Ascending, NullOrdering.Last)
    val b = SortKey(sampleExpr, SortDirection.Ascending, NullOrdering.Last)
    a shouldBe b
  }

  test("SortKey with different expressions are not equal") {
    val a = SortKey(Expr.FieldRef("a"), SortDirection.Ascending, NullOrdering.Last)
    val b = SortKey(Expr.FieldRef("b"), SortDirection.Ascending, NullOrdering.Last)
    a should not be b
  }

  test("SortKey with different directions are not equal") {
    val a = SortKey(sampleExpr, SortDirection.Ascending, NullOrdering.Last)
    val b = SortKey(sampleExpr, SortDirection.Descending, NullOrdering.Last)
    a should not be b
  }

  test("SortKey with different null orderings are not equal") {
    val a = SortKey(sampleExpr, SortDirection.Ascending, NullOrdering.First)
    val b = SortKey(sampleExpr, SortDirection.Ascending, NullOrdering.Last)
    a should not be b
  }

  test("realistic: sort by multiple keys (one list of SortKey)") {
    val keys = List(
      SortKey(Expr.FieldRef("region"), SortDirection.Ascending, NullOrdering.Last),
      SortKey(Expr.FieldRef("amount"), SortDirection.Descending, NullOrdering.First),
    )
    keys.size shouldBe 2
    keys(0).direction shouldBe SortDirection.Ascending
    keys(1).direction shouldBe SortDirection.Descending
  }

  test("SortKey round-trips through Java serialization") {
    val k = SortKey(
      Expr.Add(Expr.FieldRef("a"), Expr.Literal(LiteralValue.IntValue(1), SealedDataType.Int)),
      SortDirection.Descending,
      NullOrdering.First,
    )
    val bos = new java.io.ByteArrayOutputStream()
    val oos = new java.io.ObjectOutputStream(bos)
    oos.writeObject(k)
    oos.close()
    val bis = new java.io.ByteArrayInputStream(bos.toByteArray)
    val ois = new java.io.ObjectInputStream(bis)
    val restored = ois.readObject().asInstanceOf[SortKey]
    restored shouldBe k
  }
}