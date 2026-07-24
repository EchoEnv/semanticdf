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

  private val lock = new Object

  // `accessOrder = true` makes the LinkedHashMap reorder entries on
  // every get/put so the eldest entry is the LRU one. We override
  // `removeEldestEntry` to evict on overflow.
  private val map = new java.util.LinkedHashMap[String, CachedResult](
    /* initialCapacity */ 16,
    /* loadFactor      */ 0.75f,
    /* accessOrder     */ true,
  ) {
    override def removeEldestEntry(eldest: java.util.Map.Entry[String, CachedResult]): Boolean =
      size > InMemoryResultCache.this.maxEntries
  }

  def get(key: String): Option[CachedResult] = lock.synchronized {
    Option(map.get(key))
  }

  def put(key: String, value: CachedResult): Unit = lock.synchronized {
    // `put` triggers `removeEldestEntry` (synchronous, on this thread).
    map.put(key, value)
  }

  /** Drop every retained entry. Exposed for tests. */
  def clear(): Unit = lock.synchronized {
    map.clear()
  }

  /** Snapshot the keys in LRU order (oldest first). */
  def keys(): Seq[String] = lock.synchronized {
    import scala.jdk.CollectionConverters._
    map.keySet.asScala.toSeq
  }
}
