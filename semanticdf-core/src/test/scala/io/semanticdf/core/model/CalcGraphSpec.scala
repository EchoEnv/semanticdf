package io.semanticdf.core.model

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.semanticdf.core.expr.{Expr, LiteralValue}
import io.semanticdf.core.rel.AggregateFn
import io.semanticdf.core.schema.SealedDataType

/** Phase 2 contract: prove `CalcGraph.checkAcyclicAndDepth` correctly
  * detects cycles and depth violations in a calc-measure DAG.
  * Per scala-data-driven-refactor, this is pure data analysis: the
  * DAG SHAPE is engine-portable; the depth CAP is engine-specific
  * (passed as a parameter).
  */
class CalcGraphSpec extends AnyFunSuite with Matchers {

  // -- helpers --

  private def measureRef(name: String): Expr = Expr.MeasureRef(name)

  private def calc(name: String, refs: List[String]): CalculatedMeasure =
    CalculatedMeasure(
      name = name,
      expr = refs match {
        case Nil       => Expr.Literal(LiteralValue.IntValue(0), SealedDataType.Int)
        case ref :: Nil => Expr.Add(measureRef(ref), Expr.Literal(LiteralValue.IntValue(0), SealedDataType.Int))
        case _         => Expr.Add(
          measureRef(refs.head),
          refs.tail.foldLeft[Expr](Expr.Literal(LiteralValue.IntValue(0), SealedDataType.Int))((acc, r) =>
            Expr.Add(acc, measureRef(r)),
          ),
        )
      },
    )

  // -- acyclic DAGs --

  test("empty calc list returns Right(0)") {
    val r = CalcGraph.checkAcyclicAndDepth(Set.empty, Nil, maxDepthBound = 5)
    r shouldBe Right(0)
  }

  test("single calc (no refs) has depth 0") {
    val c = calc("c1", Nil)
    CalcGraph.checkAcyclicAndDepth(Set("c1"), List(c), maxDepthBound = 5) shouldBe Right(0)
  }

  test("linear chain c1 -> c2 -> c3 has depth 2") {
    val c1 = calc("c1", Nil)
    val c2 = calc("c2", List("c1"))
    val c3 = calc("c3", List("c2"))
    val r = CalcGraph.checkAcyclicAndDepth(
      Set("c1", "c2", "c3"), List(c1, c2, c3), maxDepthBound = 5,
    )
    r shouldBe Right(2)
  }

  test("diamond DAG: c1 -> {c2, c3} -> c4 has depth 2") {
    val c1 = calc("c1", Nil)
    val c2 = calc("c2", List("c1"))
    val c3 = calc("c3", List("c1"))
    val c4 = calc("c4", List("c2", "c3"))
    val r = CalcGraph.checkAcyclicAndDepth(
      Set("c1", "c2", "c3", "c4"), List(c1, c2, c3, c4), maxDepthBound = 5,
    )
    r shouldBe Right(2)
  }

  // -- depth violations --

  test("depth exceeds bound returns Left(depth)") {
    val c1 = calc("c1", Nil)
    val c2 = calc("c2", List("c1"))
    val c3 = calc("c3", List("c2"))
    val r = CalcGraph.checkAcyclicAndDepth(
      Set("c1", "c2", "c3"), List(c1, c2, c3), maxDepthBound = 1,
    )
    r.isLeft shouldBe true
    val Left(depth) = r
    depth should be > 1
  }

  // -- cycles --

  test("self-cycle: c1 -> c1 returns Left") {
    val c1 = calc("c1", List("c1"))
    val r = CalcGraph.checkAcyclicAndDepth(Set("c1"), List(c1), maxDepthBound = 5)
    r.isLeft shouldBe true
  }

  test("two-cycle: c1 -> c2 -> c1 returns Left") {
    val c1 = calc("c1", List("c2"))
    val c2 = calc("c2", List("c1"))
    val r = CalcGraph.checkAcyclicAndDepth(
      Set("c1", "c2"), List(c1, c2), maxDepthBound = 5,
    )
    r.isLeft shouldBe true
  }

  test("three-cycle: c1 -> c2 -> c3 -> c1 returns Left") {
    val c1 = calc("c1", List("c2"))
    val c2 = calc("c2", List("c3"))
    val c3 = calc("c3", List("c1"))
    val r = CalcGraph.checkAcyclicAndDepth(
      Set("c1", "c2", "c3"), List(c1, c2, c3), maxDepthBound = 5,
    )
    r.isLeft shouldBe true
  }

  // -- realistic --

  test("realistic: total_revenue = sum / count (depth 1)") {
    // total_revenue references the base measures sum and count.
    val total = CalculatedMeasure(
      name = "total_revenue",
      expr = Expr.Divide(measureRef("sum"), measureRef("cnt")),
    )
    // Note: only "total_revenue" is a calc measure. The base
    // measures (sum, cnt) are not in the calc set, so they don't
    // contribute to depth.
    CalcGraph.checkAcyclicAndDepth(
      Set("total_revenue"), List(total), maxDepthBound = 5,
    ) shouldBe Right(0)
  }
}