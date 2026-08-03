package io.semanticdf.core.field

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Phase 1 consolidation mirror: `io.semanticdf.core.field.SortKey`.
  *
  * Verifies the engine-portable mirror of `SortKey` is structurally
  * identical to the original Spark-bearing version's data parts:
  * - `Asc(name)` and `Desc(name)` case classes with `name: String`
  * - `nameOf` accessor extracts the column name from any SortKey
  * - Sealed exhaustiveness over `Asc` and `Desc`
  *
  * No Spark imports; verifiable by inspection. */
class SortKeySpec extends AnyFunSuite with Matchers {

  test("Asc carries the column name") {
    SortKey.Asc("carrier").name shouldBe "carrier"
  }

  test("Desc carries the column name") {
    SortKey.Desc("total_passengers").name shouldBe "total_passengers"
  }

  test("Asc and Desc are distinct subtypes") {
    val a = SortKey.Asc("x")
    val d = SortKey.Desc("x")
    a should not be d
    a shouldBe a
    d shouldBe d
  }

  test("nameOf returns the column name from any SortKey") {
    SortKey.nameOf(SortKey.Asc("carrier")) shouldBe "carrier"
    SortKey.nameOf(SortKey.Desc("pax")) shouldBe "pax"
  }

  test("Equality: same name + direction => equal") {
    SortKey.Asc("x") shouldBe SortKey.Asc("x")
    SortKey.Desc("x") shouldBe SortKey.Desc("x")
    SortKey.Asc("x") should not be SortKey.Desc("x")
    SortKey.Asc("x") should not be SortKey.Asc("y")
  }

  test("Sealed exhaustiveness: pattern-match over Asc/Desc covers all cases") {
    val examples: Seq[SortKey] = Seq(SortKey.Asc("a"), SortKey.Desc("b"))
    examples.foreach {
      case SortKey.Asc(_) => ()
      case SortKey.Desc(_) => ()
    }
  }

  test("nameOf works on a generic SortKey (not concrete subtype)") {
    val generic: SortKey = SortKey.Asc("generic_col")
    SortKey.nameOf(generic) shouldBe "generic_col"
  }
}