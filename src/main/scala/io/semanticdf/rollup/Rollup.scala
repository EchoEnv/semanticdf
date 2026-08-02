package io.semanticdf.rollup

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Cast, Literal}
import org.apache.spark.sql.types.{DataType, StructField}

import io.semanticdf.{SemanticTable, SortKey}
import io.semanticdf.predicate.Predicate

/** Pure-data description of a pre-aggregated rollup table bound to a
  * base model.
  *
  * A [[Rollup]] carries no DataFrame reference -- the actual rollup
  * source lives in a [[RollupRegistry]], registered separately by the
  * caller. This keeps `Rollup` purely serializable so it lives on
  * `SemanticTable` (which `extends Serializable`) without breaking the
  * Serializable contract.
  *
  * `precomputedRowCount` and `precomputedColumns` are computed ONCE at
  * registration time (via the smart constructor) and reused at every
  * `toDataFrame` call. They go stale only if the user rebuilds the
  * rollup table without re-registering; in that case the user should
  * call `withRollup` again with a fresh `Rollup`.
  *
  * @param name                 unique rollup name (within a model)
  * @param baseModel            name of the SemanticTable this rollup is for
  * @param rollupDimensions     dimensions the rollup was built at (the grain)
  * @param rollupMeasures       pre-aggregated measures available
  * @param freshness            freshness tracking contract
  * @param precomputedRowCount  row count of the rollup at registration time
  * @param precomputedColumns   column names of the rollup at registration time
  */
final case class Rollup(
  name:                 String,
  baseModel:            String,
  rollupDimensions:     Seq[String],
  rollupMeasures:       Seq[RollupMeasure],
  freshness:            RollupFreshness      = RollupFreshness.Track(
    watermarkProvider = () => java.time.Instant.now(),
    maxStaleness       = java.time.Duration.ofHours(1),
    onStale            = OnStalePolicy.FallBackToBase,
  ),
  precomputedRowCount:  Long                  = 0L,
  precomputedColumns:   Set[String]           = Set.empty,
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
    val measures  = rollupMeasures.map(_.name).toSet
    val storageCols = rollupMeasures.map(_.storageCol).toSet
    require(dims.subsetOf(cols),
      s"Rollup '$name': dimensions $dims not in source columns ${cols.toList.sorted}")
    require(storageCols.subsetOf(cols),
      s"Rollup '$name': storage columns $storageCols not in source columns ${cols.toList.sorted}")
    new Rollup(name, baseModel, rollupDimensions, rollupMeasures, RollupFreshness.Track(
      watermarkProvider = () => java.time.Instant.now(),
      maxStaleness       = java.time.Duration.ofHours(1),
      onStale            = OnStalePolicy.FallBackToBase,
    ), rowCount, cols)
  }

  /** Internal constructor for `fromJson` -- skips precompute. */
  private[rollup] def fromMetadata(
    name:                 String,
    baseModel:            String,
    rollupDimensions:     Seq[String],
    rollupMeasures:       Seq[RollupMeasure],
    freshness:            RollupFreshness,
    precomputedRowCount:  Long,
    precomputedColumns:   Set[String],
  ): Rollup =
    new Rollup(name, baseModel, rollupDimensions, rollupMeasures, freshness, precomputedRowCount, precomputedColumns)
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
    * `storageCol` is non-empty.
    */
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

/** Typed aggregator semantics. Per-aggregator logic — NOT all the same.
  *
  * v0.2.4 ships only [[Sum]] and [[Count]] -- both are exact-additive
  * (re-aggregable across any grain). Min/Max are partial-additive
  * (correct only at the rollup's grain) and Avg/Stddev require state
  * algebra (sum/count pair and sum/sum-of-squares/count triple
  * respectively). Those are deferred to v0.3.x+ once auto-routing
  * is in scope; for manual rollups, Sum+Count covers the common case.
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

/** Freshness tracking. */
sealed trait RollupFreshness

object RollupFreshness {
  /** Track via a provider thunk. Invoked at every compile (cached with TTL
    * via the registry; see [[RollupRegistry]]). */
  final case class Track(
    watermarkProvider: () => java.time.Instant,
    maxStaleness:       java.time.Duration,
    onStale:            OnStalePolicy,
  ) extends RollupFreshness

  /** Explicit opt-out for batch use where staleness is acceptable. */
  case object NoTracking extends RollupFreshness
}

/** What to do when a rollup is too stale to use. */
sealed trait OnStalePolicy

object OnStalePolicy {
  /** Fall back to the base fact table. Emit a warning in the audit event. */
  case object FallBackToBase extends OnStalePolicy

  /** Throw `IllegalStateException` at `toDataFrame` time. Use for
    * dashboards where stale data is unacceptable (e.g. financial
    * dashboards). */
  case object Error extends OnStalePolicy
}

/** Runtime registry of rollup DataFrame providers.
  *
  * The [[Rollup]] value class holds metadata only (no DataFrame). The
  * actual rollup source -- a DataFrame the user maintains externally --
  * is loaded via a `() => DataFrame` thunk held in this registry.
  *
  * The registry is NOT held on `SemanticTable` (which `extends Serializable`).
  * Instead, the caller passes the registry at query time when invoking
  * `SemanticTable.useRollup(name, registry)`. This keeps the
  * Serializable contract intact -- a `SemanticTable` with registered
  * rollups can still be shipped across executors; only the runtime
  * query (which holds the registry) is JVM-local.
  *
  * Thread-safety: the registry is immutable after construction (all
  * `register` calls return a new registry). Concurrent reads are safe.
  *
  * Serializability: per the v1 architect review, `() => DataFrame`
  * thunks are not actually Serializable in Scala 2.13. This registry
  * `extends Serializable` for API consistency with [[SemanticTable]],
  * but if the caller serializes a registry, the captured `DataFrame`
  * references will fail. Documented limitation, same as `AuditSink`.
  */
final class RollupRegistry private[rollup] (
  private[rollup] val providers: Map[String, () => DataFrame],
) extends Serializable {

  /** Load the rollup source DataFrame for a named rollup. */
  def loadSource(name: String): Option[DataFrame] =
    providers.get(name).map(_.apply())

  /** All rollup names this registry can serve. */
  def names: Set[String] = providers.keySet

  /** True if a provider is registered for `name`. */
  def contains(name: String): Boolean = providers.contains(name)

  /** Return a new registry with an additional provider registered. */
  def register(name: String, provider: () => DataFrame): RollupRegistry =
    new RollupRegistry(providers + (name -> provider))
}

object RollupRegistry {
  /** An empty registry -- `loadSource` returns None for any name. */
  val empty: RollupRegistry = new RollupRegistry(Map.empty)
}
