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

  /** One entry in the cache: the cached result + the model name it
    * was tagged with (or `""` if the caller didn't tag it via
    * [[putWithModel]]). Keeping the model alongside the key is
    * what makes [[invalidateModel]] O(1) per model instead of
    * O(n) over the whole buffer. */
  private final case class Entry(result: CachedResult, model: String)

  private val lock = new Object

  // `accessOrder = true` makes the LinkedHashMap reorder entries on
  // every get/put so the eldest entry is the LRU one. We override
  // `removeEldestEntry` to evict on overflow.
  private val map = new java.util.LinkedHashMap[String, Entry](
    /* initialCapacity */ 16,
    /* loadFactor      */ 0.75f,
    /* accessOrder     */ true,
  ) {
    override def removeEldestEntry(eldest: java.util.Map.Entry[String, Entry]): Boolean =
      size > InMemoryResultCache.this.maxEntries
  }

  /** Sidecar index from model name to the set of cache keys tagged
    * with that model. Used by [[invalidateModel]]. Lazily
    * maintained; entries that were stored without a model tag
    * (`put` without a model) are not in this map. Memory cost:
    * one set per distinct model that has been put with a tag. */
  private val byModel = scala.collection.mutable.HashMap.empty[String, scala.collection.mutable.Set[String]]

  def get(key: String): Option[CachedResult] = lock.synchronized {
    Option(map.get(key)).map(_.result)
  }

  override def put(key: String, value: CachedResult): Unit = lock.synchronized {
    putWithModel(key, value, "")
  }

  override def putWithModel(key: String, value: CachedResult, model: String): Unit = lock.synchronized {
    val prev = map.get(key)
    if (prev != null && prev.model.nonEmpty) {
      byModel.get(prev.model).foreach(_.remove(key))
    }
    // `put` triggers `removeEldestEntry` (synchronous, on this thread).
    map.put(key, Entry(value, model))
    if (model.nonEmpty) {
      byModel.getOrElseUpdate(model, scala.collection.mutable.Set.empty).add(key)
    }
  }

  override def invalidateModel(name: String): Int = lock.synchronized {
    byModel.get(name) match {
      case Some(keys) if keys.nonEmpty =>
        val toRemove = keys.toList
        keys.clear()
        toRemove.foreach { k =>
          map.remove(k)
          Option(map.get(k))  // no-op; just clarity
        }
        // The above `keys` is the SAME set backing the map; removing
        // by key was already done via the caller's iterator. We've
        // cleared it. Now also clear the outer map entry.
        byModel.remove(name)
        toRemove.size
      case _ => 0
    }
  }

  /** Drop every retained entry. Exposed for tests. */
  override def clear(): Unit = lock.synchronized {
    map.clear()
    byModel.clear()
  }

  /** Snapshot the keys in LRU order (oldest first). */
  override def keys(): Seq[String] = lock.synchronized {
    import scala.jdk.CollectionConverters._
    map.keySet.asScala.toSeq
  }
}
