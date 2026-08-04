package io.semanticdf.core.rel

/** Engine-portable aggregate-function ADT — Phase 2 contract.
  * Mirrors the design doc §4.5.2 "AggregateFn" (16 cases total:
  * Sum, Count, CountDistinct, Avg, Min, Max, StddevSample,
  * StddevPopulation, VarianceSample, VariancePopulation, Median,
  * PercentileContinuous, PercentileDiscrete, ApproxPercentile,
  * First, Last).
  *
  * ==Why a sealed ADT (not a String or Map)==
  *
  * The design has 16 aggregate functions, each with engine-specific
  * compile semantics (Spark's `sum`/`avg`/`stddev_samp`/etc.,
  * Trino's `SUM`/`AVG`/`STDDEV_SAMP`/etc.). A `String` field would
  * let callers pass `"sum"` / `"SUM"` / `"SUMM"` (case-insensitive
  * typos) — silent failures at engine-compile time.
  *
  * Per scala-data-driven-refactor §3 ("A rule becomes data only
  * when it must change without a deploy"): the aggregate-function
  * set is FIXED at compile time (16 cases, defined by the design).
  * A sealed ADT gives compiler-checked exhaustiveness — the
  * engine adapter's match statement is forced to handle every
  * case. A Map would be a downgrade (silent defaulting on typo'd
  * keys).
  *
  * ==Why 16 cases (not fewer, not more)==
  *
  * The set covers the aggregates needed by the portable model:
  *   - **Additive (4)**: Sum, Count, CountDistinct, First
  *   - **Non-additive (2)**: Min, Max
  *   - **Algebraic (5)**: Avg, StddevSample, StddevPopulation,
  *     VarianceSample, VariancePopulation
  *   - **Order-statistic (4)**: Median, PercentileContinuous,
  *     PercentileDiscrete, ApproxPercentile
  *   - **Position (1)**: Last
  *
  * Exact `Median` is intentionally separate from
  * `ApproxPercentile`. The two are not interchangeable: an exact
  * median gives a deterministic answer; an approximate percentile
  * gives a faster but lossy answer. The design enforces this
  * distinction at the ADT level so engines can't silently swap
  * them.
  *
  * ==Why core (engine-portable)==
  *
  * Aggregate functions are universal across query engines. Every
  * SQL engine has these 16 functions (or a subset that maps
  * cleanly to them). The engine-specific compile (Spark's
  * `functions.sum`/`avg`/etc., Trino's `SUM`/`AVG`/`STDDEV_SAMP`/
  * etc.) lives in the engine adapter.
  *
  * ==Data-driven mantra compliance==
  *
  * - Pure data: sealed trait + 16 case objects
  * - Equality auto-derived (case objects)
  * - `Product with Serializable`
  *
  * ==Boundary contract==
  *
  * Zero Spark imports. Verifiable by:
  * `grep -r 'org.apache.spark' semanticdf-core/src/main/scala/io/semanticdf/core/rel/AggregateFn.scala`
  */
sealed trait AggregateFn extends Product with Serializable

object AggregateFn {

  /** Sum of values. Additive — can be re-aggregated from finer
    * grain rollups. Maps to Spark's `sum`, Trino's `SUM`. */
  case object Sum extends AggregateFn

  /** Count of rows (or non-null values if `input` is given).
    * Additive — can be re-aggregated from finer grain rollups.
    * Maps to Spark's `count`, Trino's `COUNT`. */
  case object Count extends AggregateFn

  /** Count of distinct values. NOT additive — requires
    * distinct-count re-aggregation, which is more expensive than
    * Sum/Count. Maps to Spark's `countDistinct`, Trino's
    * `APPROX_DISTINCT` (or `COUNT(DISTINCT ...)`). */
  case object CountDistinct extends AggregateFn

  /** Average of values. Algebraic — requires both Sum and Count
    * from the rollup. Maps to Spark's `avg`, Trino's `AVG`. */
  case object Avg extends AggregateFn

  /** Minimum value. Non-additive (partial-additive at most) —
    * re-aggregation only valid at exact rollup grain. Maps to
    * Spark's `min`, Trino's `MIN`. */
  case object Min extends AggregateFn

  /** Maximum value. Non-additive (partial-additive at most) —
    * re-aggregation only valid at exact rollup grain. Maps to
    * Spark's `max`, Trino's `MAX`. */
  case object Max extends AggregateFn

  /** Sample standard deviation. Algebraic — requires Sum, Sum of
    * squares, and Count. Maps to Spark's `stddev_samp`, Trino's
    * `STDDEV_SAMP`. */
  case object StddevSample extends AggregateFn

  /** Population standard deviation. Algebraic — requires Sum,
    * Sum of squares, and Count. Maps to Spark's `stddev_pop`,
    * Trino's `STDDEV_POP`. */
  case object StddevPopulation extends AggregateFn

  /** Sample variance. Algebraic — requires Sum, Sum of squares,
    * and Count. Maps to Spark's `var_samp`, Trino's
    * `VAR_SAMP`. */
  case object VarianceSample extends AggregateFn

  /** Population variance. Algebraic — requires Sum, Sum of
    * squares, and Count. Maps to Spark's `var_pop`, Trino's
    * `VAR_POP`. */
  case object VariancePopulation extends AggregateFn

  /** Exact median (50th percentile). Order-statistic — NOT
    * replaceable by approximate percentile (the design enforces
    * this distinction at the ADT level). Maps to Spark's
    * `median` (3.x+) or `expr"percentile_approx(..., 0.5)"` for
    * older versions, Trino's `MEDIAN` / `APPROX_PERCENTILE`. */
  case object Median extends AggregateFn

  /** Continuous percentile (linear interpolation between order
    * statistics). Order-statistic — distinct from
    * `PercentileDiscrete` (which uses the nearest-rank method).
    * The percentile is in `arguments` (e.g. `LiteralValue(0.95)`).
    * Maps to Spark's `percentile_approx`, Trino's
    * `APPROX_PERCENTILE`. */
  case object PercentileContinuous extends AggregateFn

  /** Discrete percentile (nearest-rank method, no interpolation).
    * Order-statistic — distinct from `PercentileContinuous`.
    * The percentile is in `arguments`. Maps to Trino's
    * `APPROX_PERCENTILE` with `k=1`. */
  case object PercentileDiscrete extends AggregateFn

  /** Approximate percentile (lossy, faster than exact). Order-
    * statistic — explicitly distinct from `Median` (the design
    * does NOT allow approximate to silently replace exact). The
    * percentile is in `arguments`. Maps to Spark's
    * `percentile_approx`, Trino's `APPROX_PERCENTILE`. */
  case object ApproxPercentile extends AggregateFn

  /** First value seen (by input order). Additive only when the
    * input is naturally ordered (e.g. a primary key). Maps to
    * Spark's `first`, Trino's arbitrary / ordered FIRST_VALUE. */
  case object First extends AggregateFn

  /** Last value seen (by input order). Additive only when the
    * input is naturally ordered. Maps to Spark's `last`,
    * Trino's `LAST_VALUE`. */
  case object Last extends AggregateFn
}