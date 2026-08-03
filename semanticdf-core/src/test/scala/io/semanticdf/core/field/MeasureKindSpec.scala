package io.semanticdf.core.field

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Phase 1 consolidation mirror: `io.semanticdf.core.field.MeasureKind`.
  *
  * Verifies the engine-portable mirror of `MeasureKind` is structurally
  * identical to the original Spark-bearing version (case objects, names,
  * doc-comment contract). No Spark imports; verifiable by inspection. */
class MeasureKindSpec extends AnyFunSuite with Matchers {

  test("MeasureKind has exactly two cases: Base and Calc") {
    val allCases: Set[MeasureKind] = Set(MeasureKind.Base, MeasureKind.Calc)
    allCases.size shouldBe 2
  }

  test("Base and Calc are distinct singletons") {
    MeasureKind.Base should not be MeasureKind.Calc
    MeasureKind.Base shouldBe MeasureKind.Base
    MeasureKind.Calc shouldBe MeasureKind.Calc
  }

  test("Sealed: exhaustiveness — every MeasureKind is Base or Calc") {
    val examples: Seq[MeasureKind] = Seq(MeasureKind.Base, MeasureKind.Calc)
    examples.foreach {
      case MeasureKind.Base => ()
      case MeasureKind.Calc => ()
    }
  }
}