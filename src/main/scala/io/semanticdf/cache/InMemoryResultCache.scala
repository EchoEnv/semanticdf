package io.semanticdf.cache

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
  * hundreds of MB per entry. Pick `maxEntries` accordingly.
  *
  * The internal maps are `java.util.HashMap` / `HashSet` (which
  * are `Serializable`) rather than `scala.collection.mutable.*`
  * (which are not). This is required for cluster-mode safety:
  * when a `SemanticTable` is captured in a closure and shipped
  * to executors, the `resultCache` field's stored data must
  * round-trip through Java serialization. The cache's *contents*
  * are driver-side (the `mutable.HashMap` holds query results in
  * memory); in cluster mode the cache should typically be empty
  * per executor. We still make it Serializable so the cache can
  * cross JVM boundaries when needed. */
private[cache] final class InMemoryResultCache(maxEntries: Int) extends ResultCache {

  /** One entry in the cache: the cached result + the (model, version) tag
    * (or `("", 0)` if the caller didn't tag it via [[putWithModel]]).
    * Keeping the model and version alongside the key is what makes
    * [[invalidateByModelAndVersion]] O(1) per (model, version) pair. */
  private final case class Entry(result: CachedResult, model: String, version: Int)

  private val lock = new Object

  // `accessOrder = true` makes the LinkedHashMap reorder entries on
  // every get/put so the eldest entry is the LRU one. We override
  // `removeEldestEntry` to evict on overflow. (java.util.LinkedHashMap
  // IS Serializable, so this is cluster-mode safe.)
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
    * lookup). Lazily maintained; entries that were stored without a model
    * tag (`put` without a model) are not in this map.
    *
    * Memory cost: one entry per distinct (model, version) pair that
    * has been put with a tag. `java.util.HashMap` / `HashSet` are
    * Serializable (the Scala `mutable.*` variants are not). */
  private val byModelAndVersion = new java.util.HashMap[(String, Int), java.util.Set[String]]()

  /** Helper: remove `key` from the sidecar at (model, version).
    * Drops the sidecar entry entirely if its key-set becomes empty. */
  private def removeFromSidecar(key: String, model: String, version: Int): Unit = {
    val mv = (model, version)
    val set = byModelAndVersion.get(mv)
    if (set != null) {
      set.remove(key)
      if (set.isEmpty) byModelAndVersion.remove(mv)
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
      val mv = (model, version)
      val existing = byModelAndVersion.get(mv)
      val set =
        if (existing != null) existing
        else {
          val s = new java.util.HashSet[String]()
          byModelAndVersion.put(mv, s)
          s
        }
      set.add(key)
    }
  }

  override def invalidateModel(name: String): Int = lock.synchronized {
    // Walk the sidecar once to collect keys for matching model across all versions,
    // then remove them from both the cache and the sidecar. O(distinct-versions
    // for this model); in practice models typically have 1–3 versions.
    import scala.jdk.CollectionConverters._
    val matching = scala.collection.mutable.Set.empty[String]
    val it = byModelAndVersion.entrySet().iterator()
    while (it.hasNext) {
      val e = it.next()
      if (e.getKey._1 == name) matching ++= e.getValue.asScala
    }
    if (matching.isEmpty) 0
    else {
      val removed = matching.size
      matching.foreach { k =>
        val e = map.remove(k)
        if (e != null && e.model.nonEmpty) {
          removeFromSidecar(k, e.model, e.version)
        }
      }
      removed
    }
  }

  override def invalidateByModelAndVersion(name: String, version: Int): Int = lock.synchronized {
    val set = byModelAndVersion.remove((name, version))
    if (set == null || set.isEmpty) 0
    else {
      val removed = set.size()
      val it = set.iterator()
      while (it.hasNext) map.remove(it.next())
      removed
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
