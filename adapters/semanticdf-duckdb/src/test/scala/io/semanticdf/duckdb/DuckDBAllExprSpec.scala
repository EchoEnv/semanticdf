package io.semanticdf.duckdb

import io.semanticdf.core.expr.{Expr, LiteralValue}
import io.semanticdf.core.model.{CalculatedMeasure, Dimension, Measure, Model, SourceRef}
import io.semanticdf.core.rel.{AggregateCall, AggregateFn}
import io.semanticdf.core.schema.SealedDataType

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Tests for [[DuckDBQueryCompiler]]'s `Expr.All` lowerer (v0.3.1
 * Gap 2 closure). Mirrors the Trino spec.
 */
class DuckDBAllExprSpec extends AnyFunSuite with Matchers {

  private val compiler = new DuckDBQueryCompiler

  private def byName: SourceRef.ByName = SourceRef.ByName(
    catalog   = Some("hive"),
    namespace = Some("silver"),
    table     = "orders",
  )

  private def pctModel: Model = Model.of(
    name    = "orders",
    source  = byName,
    dimensions = List(Dimension.field("region", SealedDataType.Varchar)),
    measures = List(
      Measure("total_amount", AggregateCall(AggregateFn.Sum, Some(Expr.FieldRef("amount")), "total_amount")),
    ),
    calculatedMeasures = List(
      CalculatedMeasure(
        name = "pct_of_total",
        expr = Expr.Divide(
          Expr.FieldRef("amount"),
          Expr.All("total_amount"),
        ),
      ),
    ),
  ).fold(err => fail(s"Model.of failed: $err"), identity)

  test("compile resolves All(name) to the named measure's alias column") {
    val sql = compiler.compile(pctModel, Map.empty).sql
    sql should include ("SUM(\"amount\") AS \"total_amount\"")
  }

  test("compile emits the calculated measure expression using the total_amount alias") {
    val sql = compiler.compile(pctModel, Map.empty).sql
    sql should include ("\"amount\" / \"total_amount\"")
  }

  test("compile includes measure inputs in GROUP BY when All is used") {
    val sql = compiler.compile(pctModel, Map.empty).sql
    sql should include ("GROUP BY \"region\", \"amount\"")
  }

  test("Model.of rejects All references to unknown measures at the validation boundary") {
    val result = Model.of(
      name    = "orders",
      source  = byName,
      dimensions = List(Dimension.field("region", SealedDataType.Varchar)),
      measures = List(
        Measure("total_amount", AggregateCall(AggregateFn.Sum, Some(Expr.FieldRef("amount")), "total_amount")),
      ),
      calculatedMeasures = List(
        CalculatedMeasure(
          name = "pct",
          expr = Expr.Divide(Expr.FieldRef("amount"), Expr.All("nonexistent")),
        ),
      ),
    )
    result.swap.toOption.get shouldBe a [io.semanticdf.core.model.ModelValidationError]
  }
}
