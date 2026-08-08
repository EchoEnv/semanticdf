package io.semanticdf.duckdb

import java.time.{Duration, Instant}

import io.semanticdf.core.expr.Expr
import io.semanticdf.core.model.{Dimension, Measure, Model, OnStalePolicy, RollupFreshnessSpec, RollupMeasureSpec, RollupSpec, SourceRef}
import io.semanticdf.core.rel.AggregateFn
import io.semanticdf.core.schema.SealedDataType

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** v0.3.1 Gap 6 closure: [[DuckDBQueryCompiler.compile]] now
  * selects a covering fresh rollup at compile time and substitutes
  * the FROM clause with the rollup's `SourceRef`.
  *
  * Per the user constraint "For behavior (rollup selection, etc.)
  * use the same API as the original Spark library": behavior
  * mirrors [[io.semanticdf.trino.TrinoQueryCompiler]] exactly.
  *
  * Per scala-spark-batch-bugs §1: assert the actual SQL output,
  * not just compile success. */
class DuckDBRollupCompileSpec extends AnyFunSuite with Matchers {

  private val compiler = new DuckDBQueryCompiler

  private val byName: SourceRef.ByName = SourceRef.ByName(
    catalog   = Some("hive"),
    namespace = Some("silver"),
    table     = "orders",
  )

  private val rollupSource: SourceRef.ByName = SourceRef.ByName(
    catalog   = Some("hive"),
    namespace = Some("silver"),
    table     = "orders_rollup_region",
  )

  private val fixedNow: Instant = Instant.parse("2025-01-15T12:00:00Z")

  private def modelWithRollup(
      source: SourceRef,
      rollup: RollupSpec,
  ): Model = {
    val attempt = Model.of(
      name      = "test_model",
      source    = source,
      dimensions = List(Dimension.field("region", SealedDataType.Varchar)),
      measures  = List(
        Measure.aggregate("total", AggregateFn.Sum, Expr.FieldRef("amount")),
      ),
      rollups   = List(rollup),
    )
    attempt.fold(err => fail(s"Model.of failed: $err"), identity)
  }

  private def coveringRollup(
      name: String,
      freshness: RollupFreshnessSpec,
  ): RollupSpec = RollupSpec(
    name       = name,
    baseModel  = "test_model",
    dimensions = List("region"),
    measures   = List(RollupMeasureSpec(
      name       = "total",
      aggregator = AggregateFn.Sum,
      storageCol = "total_amount",
    )),
    freshness  = freshness,
  )

  // -- NoTracking: always fresh --

  test("compile uses rollup source for NoTracking rollup") {
    val m = modelWithRollup(
      source = byName,
      rollup = coveringRollup("r1", RollupFreshnessSpec.NoTracking),
    )
    val sql = compiler.compile(
      model = m,
      rollupSources = Map("r1" -> rollupSource),
      rollupWatermarks = Map.empty,
      now = fixedNow,
    ).sql
    sql should include ("FROM \"hive\".\"silver\".\"orders_rollup_region\"")
  }

  test("compile emits rollup name as comment for NoTracking rollup") {
    val m = modelWithRollup(
      source = byName,
      rollup = coveringRollup("r1", RollupFreshnessSpec.NoTracking),
    )
    val sql = compiler.compile(
      model = m,
      rollupSources = Map("r1" -> rollupSource),
      now = fixedNow,
    ).sql
    sql should include ("-- using rollup 'r1'")
  }

  // -- Track: fresh watermark --

  test("compile uses rollup source for Track rollup with fresh watermark") {
    val m = modelWithRollup(
      source = byName,
      rollup = coveringRollup("r1", RollupFreshnessSpec.Track(
        maxStaleness = Duration.ofHours(1),
        onStale      = OnStalePolicy.FallBackToBase,
      )),
    )
    val freshWatermark = fixedNow.minus(Duration.ofMinutes(30))
    val sql = compiler.compile(
      model = m,
      rollupSources = Map("r1" -> rollupSource),
      rollupWatermarks = Map("r1" -> freshWatermark),
      now = fixedNow,
    ).sql
    sql should include ("FROM \"hive\".\"silver\".\"orders_rollup_region\"")
  }

  // -- Track: stale watermark + FallBackToBase --

  test("compile uses base source for Track rollup with stale watermark + FallBackToBase") {
    val m = modelWithRollup(
      source = byName,
      rollup = coveringRollup("r1", RollupFreshnessSpec.Track(
        maxStaleness = Duration.ofHours(1),
        onStale      = OnStalePolicy.FallBackToBase,
      )),
    )
    val staleWatermark = fixedNow.minus(Duration.ofHours(2))
    val sql = compiler.compile(
      model = m,
      rollupSources = Map("r1" -> rollupSource),
      rollupWatermarks = Map("r1" -> staleWatermark),
      now = fixedNow,
    ).sql
    sql should include ("FROM \"hive\".\"silver\".\"orders\"")
    sql should not include "using rollup"
  }

  // -- Track: missing watermark --

  test("compile treats Track rollup with no watermark as stale") {
    val m = modelWithRollup(
      source = byName,
      rollup = coveringRollup("r1", RollupFreshnessSpec.Track(
        maxStaleness = Duration.ofHours(1),
        onStale      = OnStalePolicy.FallBackToBase,
      )),
    )
    val sql = compiler.compile(
      model = m,
      rollupSources = Map("r1" -> rollupSource),
      rollupWatermarks = Map.empty,
      now = fixedNow,
    ).sql
    sql should include ("FROM \"hive\".\"silver\".\"orders\"")
  }

  // -- No rollup in model --

  test("compile uses base source when model has no rollups") {
    val attempt = Model.of(
      name      = "test_model",
      source    = byName,
      dimensions = List(Dimension.field("region", SealedDataType.Varchar)),
      measures  = List(
        Measure.aggregate("total", AggregateFn.Sum, Expr.FieldRef("amount")),
      ),
    )
    val m = attempt.fold(err => fail(s"Model.of failed: $err"), identity)
    val sql = compiler.compile(m, now = fixedNow).sql
    sql should include ("FROM \"hive\".\"silver\".\"orders\"")
    sql should not include "using rollup"
  }

  // -- Backward-compat: pre-v0.3.1 3-arg signature still works --

  test("compile accepts the pre-v0.3.1 3-arg signature (backward-compat)") {
    val attempt = Model.of(
      name      = "test_model",
      source    = byName,
      dimensions = List(Dimension.field("region", SealedDataType.Varchar)),
      measures  = List(
        Measure.aggregate("total", AggregateFn.Sum, Expr.FieldRef("amount")),
      ),
    )
    val m = attempt.fold(err => fail(s"Model.of failed: $err"), identity)
    val sql = compiler.compile(m).sql  // only model arg; defaults apply
    sql should include ("FROM \"hive\".\"silver\".\"orders\"")
    sql should not include "using rollup"
  }
}