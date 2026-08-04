package io.semanticdf.core.model

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Phase 2 contract: prove `ProviderRef` is a usable, Spark-free
  * data record + the closed 2-variant enumeration. Pure data —
  * no behavior, no engine coupling. Per scala-data-driven-refactor,
  * the provider IDENTITY lives in core; the actual closure lives
  * in the driver-local registry (engine-specific).
  */
class ProviderRefSpec extends AnyFunSuite with Matchers {

  // -- ProviderRef.DataFrameSource --

  test("DataFrameSource carries name") {
    val p = ProviderRef.DataFrameSource("orders_2024")
    p.name shouldBe "orders_2024"
  }

  test("DataFrameSource defaults schemaHint to None") {
    val p = ProviderRef.DataFrameSource("orders")
    p.schemaHint shouldBe None
  }

  test("DataFrameSource with schemaHint carries it") {
    val p = ProviderRef.DataFrameSource("orders", Some(List("id", "amount", "ts")))
    p.schemaHint shouldBe Some(List("id", "amount", "ts"))
  }

  // -- ProviderRef.TableResolver --

  test("TableResolver carries name") {
    val p = ProviderRef.TableResolver("sales")
    p.name shouldBe "sales"
  }

  // -- closed enumeration + sealed exhaustiveness --

  test("ProviderRef has exactly 2 cases: DataFrameSource, TableResolver") {
    val all: Set[ProviderRef] = Set(
      ProviderRef.DataFrameSource("x"),
      ProviderRef.TableResolver("y"),
    )
    all.size shouldBe 2
  }

  test("Sealed exhaustiveness: pattern-match over both cases") {
    val examples: Seq[ProviderRef] = Seq(
      ProviderRef.DataFrameSource("x"),
      ProviderRef.TableResolver("y"),
    )
    examples.foreach {
      case ProviderRef.DataFrameSource(_, _)    => ()
      case ProviderRef.TableResolver(_)         => ()
    }
  }

  // -- equality + Product with Serializable --

  test("ProviderRef equality: same data => equal") {
    ProviderRef.DataFrameSource("x") shouldBe ProviderRef.DataFrameSource("x")
    ProviderRef.TableResolver("y") shouldBe ProviderRef.TableResolver("y")
  }

  test("ProviderRef different variants with same name are not equal") {
    ProviderRef.DataFrameSource("x") should not be ProviderRef.TableResolver("x")
  }
}