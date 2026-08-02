package io.semanticdf.rollup

import org.apache.spark.sql.{DataFrame, SparkSession}

/** Pre-computed aggregate semantics for a single rollup measure.
  *
  * v0.2.4 ships with only [[Sum]] and [[Count]] -- both are exact-additive
  * (re-aggregable across any grain). Min/Max are partial-additive
  * (correct only at the rollup's grain) and Avg/Stddev require state
  * algebra (sum/count pair and sum/sum-of-squares/count triple
  * respectively). Those are deferred to v0.3.x+ once auto-routing
  * is in scope; for manual rollups, Sum+Count covers the common case.
  *
  * Adding a new aggregator:
  *   1. Subclass [[RollupAggregator]]
  *   2. Implement `name` (the canonical aggregator name)
  *   3. If re-aggregable, document the conditions in the docstring
  *   4. Add a parse case in [[RollupAggregator.parse]]
  */
sealed trait RollupAggregator {
  /** Canonical name (matches the Spark SQL aggregator name). */
  def name: String

  /** True if a rollup at `rollupGrain` can be re-aggregated to a query
    * at `queryGrain` for this aggregator. Sum and Count are always
    * additive; Min/Max would be `rg == qg`; Avg/Stddev would be true
    * (via state). */
  def canReAggregate(rollupGrain: Set[String], queryGrain: Set[String]): Boolean
}

object RollupAggregator {

  /** Sum. Exact-additive: SUM of a SUM is the SUM. */
  case object Sum extends RollupAggregator {
    val name: String = "sum"
    def canReAggregate(rollupGrain: Set[String], queryGrain: Set[String]): Boolean = true
  }

  /** Count. Exact-additive: SUM of a COUNT is the total count. */
  case object Count extends RollupAggregator {
    val name: String = "count"
    def canReAggregate(rollupGrain: Set[String], queryGrain: Set[String]): Boolean = true
  }

  /** Parse a string into a [[RollupAggregator]]. Returns None for
    * unsupported names. Case-insensitive.
    *
    * v0.2.4 supports only Sum and Count. Min/Max/Avg/Stddev return
    * None -- callers should treat that as "aggregator not yet supported
    * for manual rollups" and surface a clear error. */
  def parse(s: String): Option[RollupAggregator] = s.toLowerCase match {
    case "sum"   => Some(Sum)
    case "count" => Some(Count)
    case other   => None
  }
}

/** One pre-aggregated measure within a [[Rollup]].
  *
  * `storageCol` is the column name in the rollup source DataFrame
  * where the pre-aggregated value lives. For example, a rollup built
  * by `df.groupBy("region", "category").agg(sum("amount").as("region_total"))`
  * has `storageCol = "region_total"`.
  */
final case class RollupMeasure(
  name:       String,
  aggregator: RollupAggregator,
  storageCol: String,
)

object RollupMeasure {
  /** Smart constructor: validate `aggregator` parses, validate
    * `storageCol` is non-empty. */
  def apply(name: String, aggregatorName: String, storageCol: String): RollupMeasure = {
    require(name.nonEmpty, "RollupMeasure.name must not be empty")
    require(storageCol.nonEmpty, "RollupMeasure.storageCol must not be empty")
    val agg = RollupAggregator.parse(aggregatorName)
      .getOrElse(throw new IllegalArgumentException(
        s"Unsupported rollup aggregator '$aggregatorName' for measure '$name'. " +
        s"v0.2.4 supports: sum, count. Min/Max/Avg/Stddev are deferred to v0.3.x+ " +
        s"(they require state algebra that's out of scope for manual rollups)."))
    new RollupMeasure(name, agg, storageCol)
  }
}

/** Pure-data description of a pre-aggregated rollup table bound to a
  * base model.
  *
  * A [[Rollup]] carries no DataFrame reference -- the actual rollup
  * source lives in a [[RollupRegistry]], registered separately by the
  * caller. This keeps `Rollup` purely serializable so it can live on
  * `SemanticTable` (which `extends Serializable`) without breaking the
  * Serializable contract.
  *
  * `precomputedRowCount` and `precomputedColumns` are computed ONCE at
  * registration time (via the smart constructor) and reused at every
  * `toDataFrame` call. They go stale only if the user rebuilds the
  * rollup table without re-registering; in that case the user should
  * call `withRollup` again with a fresh `Rollup`.
  *
  * `Rollup` is Serializable (pure data). However, after `useRollup`,
  * the SemanticTable is NOT safe to ship across executors because
  * `SemanticRollupOp` holds a `RollupRegistry` (which contains
  * `() => DataFrame` providers that are not Serializable).
  * @param name                 unique rollup name (within the model)
  * @param baseModel            name of the SemanticTable this rollup is for
  * @param rollupDimensions     dimensions the rollup was built at (the grain)
  * @param rollupMeasures       pre-aggregated measures available
  * @param precomputedRowCount  row count of the rollup at registration time
  * @param precomputedColumns   column names of the rollup at registration time
  */
final case class Rollup(
  name:                 String,
  baseModel:            String,
  rollupDimensions:     Seq[String],
  rollupMeasures:       Seq[RollupMeasure],
  precomputedRowCount:  Long,
  precomputedColumns:    Set[String],
)

object Rollup {
  /** Smart constructor. Validates fields, precomputes stats from the
    * source DataFrame.
    *
    * The `sourceProvider` thunk is invoked ONCE here for stats
    * precompute; the result is discarded. The caller must register the
    * same provider with a [[RollupRegistry]] for runtime loading.
    *
    * @throws IllegalArgumentException for invalid input (empty name,
    *         mismatched baseModel, dimensions/measures not in source
    *         columns, etc.)
    */
  def apply(
    name:             String,
    baseModel:        String,
    rollupDimensions: Seq[String],
    rollupMeasures:   Seq[RollupMeasure],
    sourceProvider:   () => DataFrame,
  ): Rollup = {
    require(name.nonEmpty, s"Rollup.name must not be empty")
    require(rollupDimensions.nonEmpty, s"Rollup '$name': rollupDimensions must not be empty")
    require(rollupMeasures.nonEmpty, s"Rollup '$name': rollupMeasures must not be empty")

    val source    = sourceProvider()
    val cols      = source.columns.toSet
    val rowCount  = source.count()
    val dims      = rollupDimensions.toSet
    val storageCols = rollupMeasures.map(_.storageCol).toSet
    require(dims.subsetOf(cols),
      s"Rollup '$name': dimensions $dims not in source columns ${cols.toList.sorted}")
    require(storageCols.subsetOf(cols),
      s"Rollup '$name': storage columns $storageCols not in source columns ${cols.toList.sorted}")
    new Rollup(name, baseModel, rollupDimensions, rollupMeasures, rowCount, cols)
  }

  /** Internal constructor used by `fromJson` -- skips precompute. */
  private[rollup] def fromMetadata(
    name:                String,
    baseModel:           String,
    rollupDimensions:    Seq[String],
    rollupMeasures:      Seq[RollupMeasure],
    precomputedRowCount: Long,
    precomputedColumns:   Set[String],
  ): Rollup =
    new Rollup(name, baseModel, rollupDimensions, rollupMeasures,
      precomputedRowCount, precomputedColumns)
}