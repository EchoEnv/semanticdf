package io.semanticdf.rollup

import org.apache.spark.sql.DataFrame

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
  * rollups can still be shipped across executors if needed; the
  * registry travels with the query, not the model.
  *
  * Typical usage:
  * {{{
  *   val registry = RollupRegistry.empty
  *     .register("orders_by_region_category",
  *               _ => spark.read.parquet("/path/to/rollup"))
  *   val result = ordersModel
  *     .useRollup("orders_by_region_category", registry)
  *     .query(measures = Seq("total", "count"), dimensions = Seq("region", "category"))
  *     .execute(spark)
  * }}}
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