package io.semanticdf.core.model

import io.semanticdf.core.rel.AggregateFn

/** Engine-portable rollup-measure spec ADT — Phase 2 contract.
  * Mirrors the design doc §4.5.2 "RollupMeasureSpec" (portable
  * mirror of the spark-coupled `RollupMeasure` in the existing
  * `rollup/Rollup.scala`).
  *
  * ==Why a separate type from the existing `io.semanticdf.rollup.RollupMeasure`==
  *
  * The spark-coupled `RollupMeasure` uses `RollupAggregator`
  * (a sealed trait with only `Sum` + `Count` cases — the v0.2.4
  * closure). The portable `RollupMeasureSpec` uses `AggregateFn`
  * (16 cases from PR #360: Sum, Count, CountDistinct, Avg, Min,
  * Max, StddevSample, StddevPopulation, VarianceSample,
  * VariancePopulation, Median, PercentileContinuous,
  * PercentileDiscrete, ApproxPercentile, First, Last). The portable
  * version is richer — it covers the rollup closure expansion
  * called out in the multi-engine design.
  *
  * ==Why a separate type from `Measure.aggregate(...)`==
  *
  * `Measure.aggregate(...)` declares how the engine builds a
  * measure from a source column. `RollupMeasureSpec` declares how
  * the rollup STORES a pre-aggregated measure (with a `storageCol`
  * that names the column in the rollup table). They have the same
  * `aggregator` (Sum, Count, etc.) but different storage semantics.
  *
  * ==Why `storageCol: String` (not a column ref)==
  *
  * The portable rollup spec carries the storage column NAME
  * (matching the design). The rollup table's actual columns are
  * resolved by the engine adapter at registration time (via
  * `RollupRegistration`). The portable model doesn't need to know
  * about the rollup's columns until then.
  *
  * ==Why core (engine-portable)==
  *
  * The rollup-measure spec is universal — every SQL engine has
  * `SUM(x)` / `COUNT(*)` / etc. The engine-specific compile (Spark's
  * `functions.sum`, Trino's `SUM` keyword, etc.) lives in the
  * engine adapter.
  *
  * ==Data-driven mantra compliance==
  *
  * - Pure data: case class (no behavior)
  * - Equality auto-derived
  * - `Product with Serializable`
  *
  * ==Boundary contract==
  *
  * Zero Spark imports. Verifiable by:
  * `grep -r 'org.apache.spark' semanticdf-core/src/main/scala/io/semanticdf/core/model/RollupMeasureSpec.scala`
  */
final case class RollupMeasureSpec(
    name:       String,
    aggregator: AggregateFn,
    storageCol: String,
) extends Product with Serializable