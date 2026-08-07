package io.semanticdf.duckdb

import io.semanticdf.core.engine.ParameterizedSql
import io.semanticdf.core.expr.{Expr, LiteralValue}
import io.semanticdf.core.model.{Dimension, Measure, Model, SourceRef}
import io.semanticdf.core.rel.{AggregateCall, AggregateFn}
import io.semanticdf.core.schema.SealedDataType
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Phase 2 contract: prove [[DuckDBQueryCompiler]] walks a portable
  * `Model` and emits the correct DuckDB SQL.
  *
  * Per scala-data-driven-refactor \u00a71: the compiler is pure behavior \u2014
  * given a Model, it produces a deterministic SQL string. Same input
  * \u2192 same output. No IO, no state.
  *
  * ==v0.3.1 backward-compat==
  *
  * The v0.3.0 release shipped with only 6 of 16 `AggregateFn` cases
  * implemented; the remaining 10 fell through to a generic
  * `${fn.toString.toUpperCase}(x)` fallback that emitted WRONG SQL
  * (e.g. `PERCENTILE_CONTINUOUS(x)` instead of DuckDB's
  * `QUANTILE_CONT(x, 0.5)`). This spec pins the correct mappings. */
class DuckDBQueryCompilerSpec extends AnyFunSuite with Matchers {

  private val compiler = new DuckDBQueryCompiler

  /** Build a minimal model with one measure (using the supplied
    * aggregate function) over `amount`. */
  private def modelWith(fn: AggregateFn): Model = Model.of(
    name       = "test_model",
    source     = SourceRef.ByName(catalog = Some("hive"), namespace = Some("silver"), table = "orders"),
    dimensions = List(Dimension.field("region", SealedDataType.Varchar)),
    measures   = List(Measure("m", AggregateCall(fn, Some(Expr.FieldRef("amount")), "m"))),
  ).fold(err => fail(s"Model.of failed: $err"), identity)

  // -- basic aggregates (shipped in v0.3.0) --

  test("Sum renders as SUM(amount)") {
    val sql = compiler.compile(modelWith(AggregateFn.Sum)).sql
    sql should include ("SUM(\"amount\") AS \"m\"")
  }

  test("Count renders as COUNT(*)") {
    val sql = compiler.compile(modelWith(AggregateFn.Count)).sql
    sql should include ("COUNT(*) AS \"m\"")
  }

  test("CountDistinct renders as COUNT(DISTINCT amount)") {
    val sql = compiler.compile(modelWith(AggregateFn.CountDistinct)).sql
    sql should include ("COUNT(DISTINCT \"amount\") AS \"m\"")
  }

  test("Avg renders as AVG(amount)") {
    val sql = compiler.compile(modelWith(AggregateFn.Avg)).sql
    sql should include ("AVG(\"amount\") AS \"m\"")
  }

  test("Min renders as MIN(amount)") {
    val sql = compiler.compile(modelWith(AggregateFn.Min)).sql
    sql should include ("MIN(\"amount\") AS \"m\"")
  }

  test("Max renders as MAX(amount)") {
    val sql = compiler.compile(modelWith(AggregateFn.Max)).sql
    sql should include ("MAX(\"amount\") AS \"m\"")
  }

  // -- advanced aggregates (v0.3.1 backward-compat) --

  test("StddevSample renders as STDDEV_SAMP(amount)") {
    val sql = compiler.compile(modelWith(AggregateFn.StddevSample)).sql
    sql should include ("STDDEV_SAMP(\"amount\") AS \"m\"")
  }

  test("StddevPopulation renders as STDDEV_POP(amount)") {
    val sql = compiler.compile(modelWith(AggregateFn.StddevPopulation)).sql
    sql should include ("STDDEV_POP(\"amount\") AS \"m\"")
  }

  test("VarianceSample renders as VAR_SAMP(amount)") {
    val sql = compiler.compile(modelWith(AggregateFn.VarianceSample)).sql
    sql should include ("VAR_SAMP(\"amount\") AS \"m\"")
  }

  test("VariancePopulation renders as VAR_POP(amount)") {
    val sql = compiler.compile(modelWith(AggregateFn.VariancePopulation)).sql
    sql should include ("VAR_POP(\"amount\") AS \"m\"")
  }

  test("Median renders as MEDIAN(amount)") {
    val sql = compiler.compile(modelWith(AggregateFn.Median)).sql
    sql should include ("MEDIAN(\"amount\") AS \"m\"")
  }

  test("PercentileContinuous renders as QUANTILE_CONT(amount, 0.5)") {
    // Per v0.3.1 backward-compat: hardcode percentile=0.5 (median) until
    // the portable AggregateCall shape carries a percentile arg. See
    // docs/design/v0.3.1-feature-parity-backlog.md Gap 5.
    val sql = compiler.compile(modelWith(AggregateFn.PercentileContinuous)).sql
    sql should include ("QUANTILE_CONT(\"amount\", 0.5) AS \"m\"")
  }

  test("PercentileDiscrete renders as QUANTILE_DISC(amount, 0.5)") {
    val sql = compiler.compile(modelWith(AggregateFn.PercentileDiscrete)).sql
    sql should include ("QUANTILE_DISC(\"amount\", 0.5) AS \"m\"")
  }

  test("ApproxPercentile renders as APPROX_QUANTILE(amount, 0.5)") {
    val sql = compiler.compile(modelWith(AggregateFn.ApproxPercentile)).sql
    sql should include ("APPROX_QUANTILE(\"amount\", 0.5) AS \"m\"")
  }

  test("First renders as FIRST(amount)") {
    // DuckDB ordered-set aggregate; default order is insertion order.
    // Future: support ORDER BY clause for the aggregate.
    val sql = compiler.compile(modelWith(AggregateFn.First)).sql
    sql should include ("FIRST(\"amount\") AS \"m\"")
  }

  test("Last renders as LAST(amount)") {
    val sql = compiler.compile(modelWith(AggregateFn.Last)).sql
    sql should include ("LAST(\"amount\") AS \"m\"")
  }
}
