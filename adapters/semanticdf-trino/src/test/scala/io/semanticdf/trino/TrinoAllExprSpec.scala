package io.semanticdf.trino

import io.semanticdf.core.expr.{Expr, LiteralValue}
import io.semanticdf.core.model.{CalculatedMeasure, Dimension, Measure, Model, SourceRef}
import io.semanticdf.core.rel.{AggregateCall, AggregateFn}
import io.semanticdf.core.schema.SealedDataType

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Tests for [[TrinoQueryCompiler]]'s `Expr.All` lowerer (v0.3.1
 * Gap 2 closure). `Expr.All(name)` lowers to
 * `SUM(<input>) OVER (PARTITION BY <dims>)` — the SQL equivalent
 * of Spark's window function.
 *
 * Per scala-spark-batch-bugs §1: assert the actual SQL emission,
 * not just compile success.
 * Per scala-spark-batch-bugs §3: verify the schema drift (per-row
 * vs per-group) is what we expect.
 */
class TrinoAllExprSpec extends AnyFunSuite with Matchers {

  private val compiler = new TrinoQueryCompiler

  private def byName: SourceRef.ByName = SourceRef.ByName(
    catalog   = Some("hive"),
    namespace = Some("silver"),
    table     = "orders",
  )

  /** Model: region (dim), total_amount (measure: SUM(amount)),
    * pct_of_total (calc: amount / All(total_amount)). */
  private def pctModel: Model = Model.of(
    name    = "orders",
    source  = byName,
    dimensions = List(Dimension.field("region", SealedDataType.Varchar)),
    measures = List(
      Measure.aggregate("total_amount", AggregateFn.Sum, Expr.FieldRef("amount")),
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
    // Per the legacy t.all semantics: All(name) → FieldRef(name).
    // The measure is computed first via GROUP BY + SUM(amount);
    // the All reference then reads the resulting per-group total.
    val sql = compiler.compile(pctModel, Map.empty).sql
    sql should include ("SUM(\"amount\") AS \"total_amount\"")
  }

  test("compile emits the calculated measure expression using the total_amount alias") {
    val sql = compiler.compile(pctModel, Map.empty).sql
    sql should include ("\"amount\" / \"total_amount\"")
  }

  test("compile includes measure inputs in GROUP BY when All is used (so per-row data survives)") {
    // Per scala-spark-batch-bugs §3: schema-drift awareness.
    // When Expr.All is used, the per-row input columns must be in
    // GROUP BY to survive aggregation (otherwise amount/total_amount
    // can't both exist in the SELECT). The compiler emits both
    // region AND amount in GROUP BY.
    val sql = compiler.compile(pctModel, Map.empty).sql
    sql should include ("GROUP BY \"region\", \"amount\"")
  }

  test("Model.of rejects All references to unknown measures at the validation boundary") {
    // Per scala-data-driven-refactor §1: validation happens at the
    // boundary (Model.of returns Either[ModelValidationError, Model]).
    // The validator's `unresolvedRef` check catches the unknown-
    // measure reference before the compiler ever sees it.
    val result = Model.of(
      name    = "orders",
      source  = byName,
      dimensions = List(Dimension.field("region", SealedDataType.Varchar)),
      measures = List(
        Measure.aggregate("total_amount", AggregateFn.Sum, Expr.FieldRef("amount")),
      ),
      calculatedMeasures = List(
        CalculatedMeasure(
          name = "pct",
          expr = Expr.Divide(Expr.FieldRef("amount"), Expr.All("nonexistent")),
        ),
      ),
    )
    result.swap.toOption.get shouldBe a [io.semanticdf.core.model.ModelValidationError]
    result.swap.toOption.get.toString should include ("nonexistent")
  }
}
