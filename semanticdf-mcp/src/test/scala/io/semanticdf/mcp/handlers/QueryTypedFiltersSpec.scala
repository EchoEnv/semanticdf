package io.semanticdf.mcp.handlers

import io.semanticdf.core.engine.MCPQueryRequest
import io.semanticdf.core.expr.{Expr, LiteralValue}
import io.semanticdf.core.model.FilterSpec
import io.semanticdf.core.schema.SealedDataType
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Tests for v0.3.1 Phase C2: typed filters in MCPQueryRequest.
 *
 * Verifies the `filters` field shape (typed `FilterSpec` list) and
 * its interaction with the existing `where` field (raw SQL).
 *
 * The end-to-end application of typed filters through the Spark
 * engine is covered in semanticdf-spark's `SparkEngineProviderPortableSpec`.
 *
 * Per scala-error-handling: unsupported predicate shapes surface as
 * typed `EngineError.UnsupportedCapability` at the boundary, not as
 * thrown exceptions. Conversion tests live in semanticdf-spark's
 * `PredicateToExprConverterSpec`.
 */
class QueryTypedFiltersSpec extends AnyFunSuite with Matchers {

  private def filterByRegion(name: String): FilterSpec = FilterSpec(
    name      = name,
    predicate = Expr.Equal(
      Expr.FieldRef("region"),
      Expr.Literal(LiteralValue.StringValue("us"), SealedDataType.Varchar)
    )
  )

  test("MCPQueryRequest.filters defaults to Nil when omitted") {
    val req = MCPQueryRequest(model = "x")
    req.filters shouldBe Nil
    // Backward compat: previous default fields still work.
    req.where shouldBe None
    req.dimensions shouldBe Seq.empty
    req.measures shouldBe Seq.empty
    req.limit shouldBe None
    req.timeGrain shouldBe None
    req.timeRange shouldBe None
  }

  test("MCPQueryRequest.filters carries typed FilterSpec list") {
    val f = filterByRegion("where")
    val req = MCPQueryRequest(model = "x", filters = List(f))
    req.filters shouldBe List(f)
    req.filters.head.name shouldBe "where"
    req.filters.head.predicate shouldBe a [Expr.Equal]
  }

  test("MCPQueryRequest.filters is additive with where (both fields coexist)") {
    // Per the design doc: precedence is filters (typed) then where (raw SQL).
    val req = MCPQueryRequest(
      model   = "x",
      filters = List(filterByRegion("typed")),
      where   = Some("raw = 'sql'"))
    req.filters should have size 1
    req.where shouldBe Some("raw = 'sql'")
  }

  test("FilterSpec predicate supports compound (And/Or) shapes") {
    // Per scala-data-driven-refacer: Expr is pure data; the
    // engine-portable filter can be any Expr shape.
    val compound = FilterSpec(
      name = "active_and_recent",
      predicate = Expr.And(
        Expr.GreaterThan(
          Expr.FieldRef("last_login"),
          Expr.Literal(LiteralValue.LongValue(0L), SealedDataType.BigInt)
        ),
        Expr.IsNotNull(Expr.FieldRef("email"))
      )
    )
    val req = MCPQueryRequest(model = "x", filters = List(compound))
    req.filters.head.predicate shouldBe a [Expr.And]
  }
}
