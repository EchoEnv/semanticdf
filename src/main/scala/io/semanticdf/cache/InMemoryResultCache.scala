package io.semanticdf.cache

import scala.collection.mutable

/** Bounded in-memory LRU cache, backed by `java.util.LinkedHashMap`
  * in access-order mode. On overflow, the least-recently-accessed
  * entry is evicted.
  *
  * Thread-safe: all public methods synchronize on the internal
  * `LinkedHashMap`. This is coarse-grained but the cache's hot
  * path (one `get` + at most one `put` per query) doesn't need
  * more.
  *
  * Memory: holds up to `maxEntries` distinct queries' worth of
  * `Array[Row]` + `StructType`. Each row's cost is
  * `O(num_columns × column_type_size)`. For most analytical
  * workloads (hundreds of rows, tens of columns) the per-entry
  * cost is KB-scale; for high-cardinality workloads it can be
  * hundreds of MB per entry. Pick `maxEntries` accordingly. */
private[cache] final class InMemoryResultCache(maxEntries: Int) extends ResultCache {

  /** One entry in the cache: the cached result + the (model, version) tag
    * (or `("", 0)` if the caller didn't tag it via [[putWithModel]]).
    * Keeping the model and version alongside the key is what makes
    * [[invalidateByModelAndVersion]] O(1) per (model, version) pair. */
  private final case class Entry(result: CachedResult, model: String, version: Int)

  private val lock = new Object

  // `accessOrder = true` makes the LinkedHashMap reorder entries on
  // every get/put so the eldest entry is the LRU one. We override
  // `removeEldestEntry` to evict on overflow.
  private val map = new java.util.LinkedHashMap[String, Entry](
    /* initialCapacity */ 16,
    /* loadFactor      */ 0.75f,
    /* accessOrder     */ true,
  ) {
    override def removeEldestEntry(eldest: java.util.Map.Entry[String, Entry]): Boolean = {
      val shouldEvict = size > InMemoryResultCache.this.maxEntries
      // Only clean up the sidecar when actually evicting. Running
      // the cleanup unconditionally would remove non-evicted
      // entries from the sidecar after every put, defeating
      // invalidation.
      if (shouldEvict) {
        val e = eldest.getValue
        if (e.model.nonEmpty) {
          InMemoryResultCache.this.removeFromSidecar(eldest.getKey, e.model, e.version)
        }
      }
      shouldEvict
    }
  }

  /** Sidecar index from (model, version) to the set of cache keys
    * tagged with that model+version. Used by both [[invalidateModel]]
    * (walks all versions) and [[invalidateByModelAndVersion]] (single
    * lookup). Lazily maintained; entries that were stored without a
    * model tag (`put` without a model) are not in this map.
    *
    * Memory cost: one entry per distinct (model, version) pair that
    * has been put with a tag. */
  private val byModelAndVersion = mutable.HashMap.empty[(String, Int), mutable.Set[String]]

  /** Helper: remove `key` from the sidecar at (model, version).
    * Drops the sidecar entry entirely if its key-set becomes empty. */
  private def removeFromSidecar(key: String, model: String, version: Int): Unit = {
    byModelAndVersion.get((model, version)) match {
      case Some(set) =>
        set.remove(key)
        if (set.isEmpty) byModelAndVersion.remove((model, version))
      case None =>
    }
  }

  def get(key: String): Option[CachedResult] = lock.synchronized {
    Option(map.get(key)).map(_.result)
  }

  override def put(key: String, value: CachedResult): Unit = lock.synchronized {
    map.put(key, Entry(value, "", 0))
  }

  override def putWithModel(key: String, value: CachedResult, model: String): Unit =
    putWithModelAndVersion(key, value, model, 0)

  override def putWithModelAndVersion(
      key: String, value: CachedResult, model: String, version: Int): Unit = lock.synchronized {
    val prev = map.get(key)
    if (prev != null && prev.model.nonEmpty) {
      // Re-tagging: drop the key from the previous sidecar entry.
      removeFromSidecar(key, prev.model, prev.version)
    }
    // `put` triggers `removeEldestEntry` (synchronous, on this thread).
    map.put(key, Entry(value, model, version))
    if (model.nonEmpty) {
      byModelAndVersion
        .getOrElseUpdate((model, version), mutable.Set.empty)
        .add(key)
    }
  }

  override def invalidateModel(name: String): Int = lock.synchronized {
    // Collect all keys across this model's versions, then drop them
    // from both the cache and the sidecar. The walk is over the
    // sidecar's distinct (model, version) entries, not over the
    // cache's per-key entries — so O(distinct_versions) which is
    // typically 1–3.
    val collected = mutable.Set.empty[String]
    byModelAndVersion.toList.foreach { case ((m, _), keys) =>
      if (m == name) collected ++= keys
    }
    if (collected.isEmpty) 0
    else {
      val removed = collected.size
      collected.foreach { k =>
        val e = map.remove(k)
        if (e != null && e.model.nonEmpty) {
          removeFromSidecar(k, e.model, e.version)
        }
      }
      removed
    }
  }

  override def invalidateByModelAndVersion(name: String, version: Int): Int = lock.synchronized {
    byModelAndVersion.remove((name, version)) match {
      case Some(keys) if keys.nonEmpty =>
        val removed = keys.size
        keys.foreach { k =>
          val e = map.remove(k)
          if (e != null && e.model.nonEmpty) {
            // Best-effort sidecar cleanup; the entry is already gone
            // from the sidecar by virtue of `remove` above.
            ()
          }
        }
        removed
      case _ => 0
    }
  }

  /** Drop every retained entry. Exposed for tests. */
  override def clear(): Unit = lock.synchronized {
    map.clear()
    byModelAndVersion.clear()
  }

  /** Snapshot the keys in LRU order (oldest first). */
  override def keys(): Seq[String] = lock.synchronized {
    import scala.jdk.CollectionConverters._
    map.keySet.asScala.toSeq
  }
}
