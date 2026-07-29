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

  @deprecated("Use invalidateModel(name). The cache key uses the "
    + "YAML-declared version while the journal's CURRENT_VERSION is a "
    + "separate counter; the two never match.", "0.2.2")
  override def invalidateByModelAndVersion(name: String, version: Int): Int = lock.synchronized {
    // Walk BOTH the row-form sidecar and the journaled-form
    // sidecar. The journaled sidecar is a separate invalidation
    // key; this method walks both. The platform currently
    // only calls invalidateModel (which DOES walk both), but the
    // trait still exposes this method, so any library caller
    // would silently leak journaled entries if the model-version
    // tag were used.
    val rowSet = byModelAndVersion.remove((name, version))
    val rowCount =
      if (rowSet == null || rowSet.isEmpty) 0
      else {
        val n = rowSet.size()
        val it = rowSet.iterator()
        while (it.hasNext) map.remove(it.next())
        n
      }
    val journaledSet = byModelAndVersionJournaled.remove((name, version))
    val journaledCount =
      if (journaledSet == null || journaledSet.isEmpty) 0
      else {
        val n = journaledSet.size()
        val it = journaledSet.iterator()
        while (it.hasNext) journaledMap.remove(it.next())
        n
      }
    rowCount + journaledCount
  }

  /** Drop every retained entry. Exposed for tests. */
  override def clear(): Unit = lock.synchronized {
    map.clear()
    byModelAndVersion.clear()
    journaledMap.clear()
    byModelAndVersionJournaled.clear()
  }

  // ===================================================================
  // Journaled-form cache (bypasses Array[Row] rebuild)
  // ===================================================================

  /** LRU map for journaled-form entries (the form Restate journals:
    * a plain {@code List<Object[]>} of cells per row + column
    * metadata). Caching this form avoids the redundant
    * {@code Array[Row]} rebuild that the v0.2.2 path incurred on
    * every cache miss. */
  private val journaledMap = new java.util.LinkedHashMap[String, JournaledEntry](
    /* initialCapacity */ 16,
    /* loadFactor      */ 0.75f,
    /* accessOrder     */ true,
  ) {
    override def removeEldestEntry(eldest: java.util.Map.Entry[String, JournaledEntry]): Boolean = {
      val shouldEvict = size > InMemoryResultCache.this.maxEntries
      if (shouldEvict) {
        val e = eldest.getValue
        if (e.model.nonEmpty) {
          InMemoryResultCache.this.removeFromJournaledSidecar(eldest.getKey, e.model, e.version)
        }
      }
      shouldEvict
    }
  }

  /** Sidecar index for journaled entries (parallel to
    * [[byModelAndVersion]]). */
  private val byModelAndVersionJournaled =
    new java.util.HashMap[(String, Int), java.util.Set[String]]()

  private case class JournaledEntry(value: AnyRef, model: String, version: Int)

  private def removeFromJournaledSidecar(key: String, model: String, version: Int): Unit = {
    val mv = (model, version)
    val set = byModelAndVersionJournaled.get(mv)
    if (set != null) {
      set.remove(key)
      if (set.isEmpty) byModelAndVersionJournaled.remove(mv)
    }
  }

  override def getJournaled(key: String): Option[AnyRef] = lock.synchronized {
    Option(journaledMap.get(key)).map(_.value)
  }

  override def putJournaledWithModelAndVersion(
      key: String, value: AnyRef, model: String, version: Int): Unit = lock.synchronized {
    val prev = journaledMap.get(key)
    if (prev != null && prev.model.nonEmpty) {
      removeFromJournaledSidecar(key, prev.model, prev.version)
    }
    journaledMap.put(key, JournaledEntry(value, model, version))
    if (model.nonEmpty) {
      val mv = (model, version)
      val existing = byModelAndVersionJournaled.get(mv)
      val set =
        if (existing != null) existing
        else {
          val s = new java.util.HashSet[String]()
          byModelAndVersionJournaled.put(mv, s)
          s
        }
      set.add(key)
    }
  }

  /** Per-key in-flight completions for the journaled-form
    * single-flight path (parallel to [[inFlight]] for the row
    * form). */
  private val inFlightJournaled =
    new java.util.concurrent.ConcurrentHashMap[String, java.util.concurrent.CompletableFuture[AnyRef]]()

  override def getOrComputeJournaled(
      key: String,
      compute: java.util.function.Supplier[AnyRef]): AnyRef = {
    getJournaled(key) match {
      case Some(v) => return v
      case None    => ()
    }
    val ours = new java.util.concurrent.CompletableFuture[AnyRef]()
    val prior = inFlightJournaled.putIfAbsent(key, ours)
    if (prior != null) {
      // Lost the race; wait for the winner. Re-set the thread
      // interrupt flag if Future.get() cleared it (PR-fix: B-1 from
      // post-#278 review).
      try { return prior.get(); }
      catch {
        case e: java.util.concurrent.ExecutionException =>
          throw e.getCause
        case e: InterruptedException =>
          Thread.currentThread().interrupt()
          throw new RuntimeException(
            "getOrComputeJournaled: interrupted while waiting on the "
              + "in-flight compute (key=" + key + ")",
            e)
      }
    } else {
      try {
        val v = compute.get()
        // Caller is REQUIRED to put from inside the compute closure
        // via putJournaledWithModelAndVersion(key, v, model, version)
        // (the QueryService does this). Don't fall back to an
        // untagged put here — entries with model="" are
        // uninvalidateable.
        ours.complete(v)
        v
      } catch {
        case t: Throwable =>
          ours.completeExceptionally(t)
          throw t
      } finally {
        inFlightJournaled.remove(key, ours)
      }
    }
  }

  /** Extend [[invalidateModel]] to also clear journaled entries.
    * We override the existing [[invalidateModel]] definition (not
    * add a new method). The body merges both caches' invalidations.
    */
  override def invalidateModel(name: String): Int = lock.synchronized {
    import scala.jdk.CollectionConverters._
    // Original sidecar invalidation (CachedResult entries)
    val matchingRow = scala.collection.mutable.Set.empty[String]
    val itRow = byModelAndVersion.entrySet().iterator()
    while (itRow.hasNext) {
      val e = itRow.next()
      if (e.getKey._1 == name) matchingRow ++= e.getValue.asScala
    }
    val removedRow = matchingRow.size
    matchingRow.foreach { k =>
      val e = map.remove(k)
      if (e != null && e.model.nonEmpty) {
        removeFromSidecar(k, e.model, e.version)
      }
    }
    // Journaled entries
    val matchingJournaled = scala.collection.mutable.Set.empty[String]
    val itJ = byModelAndVersionJournaled.entrySet().iterator()
    while (itJ.hasNext) {
      val e = itJ.next()
      if (e.getKey._1 == name) matchingJournaled ++= e.getValue.asScala
    }
    matchingJournaled.foreach { k =>
      val e = journaledMap.remove(k)
      if (e != null && e.model.nonEmpty) {
        removeFromJournaledSidecar(k, e.model, e.version)
      }
    }
    removedRow + matchingJournaled.size
  }

  // ===================================================================
  // Single-flight on cache miss (thundering herd)
  // ===================================================================

  /** Per-key in-flight completions for single-flight read-through.
    * Used by [[getOrCompute]] to coalesce N concurrent misses for
    * the same key into exactly ONE `compute.get()` invocation.
    *
    * <p>Memory: at most one entry per distinct concurrent cache
    * key during the compute window — typically a handful. Cleared
    * via compare-and-remove on the ConcurrentHashMap so a slow
    * `compute` can't be stranded.
    *
    * <p>Without this, N concurrent identical first-time queries
    * (the LLM-agent stampede pattern) all miss the cache, all run
    * the full Spark job, and only the last put wins. The cache
    * becomes a net negative — every caller pays the Spark cost on
    * the first miss. */
  private val inFlight =
    new java.util.concurrent.ConcurrentHashMap[String, java.util.concurrent.CompletableFuture[CachedResult]]()

  override def getOrCompute(
      key: String,
      compute: java.util.function.Supplier[CachedResult]): CachedResult = {
    // Fast path: another thread already populated the cache.
    get(key) match {
      case Some(v) => return v
      case None    => ()
    }

    // Try to claim the in-flight slot for this key. If another
    // thread already claimed it, wait on their future instead of
    // re-running the compute.
    val ours = new java.util.concurrent.CompletableFuture[CachedResult]()
    val prior = inFlight.putIfAbsent(key, ours)
    if (prior != null) {
      // Lost the race; wait for the winner. Re-set the thread
      // interrupt flag if Future.get() cleared it (PR-fix: B-1
      // from post-#278 review). Without this, a stale interrupt
      // can poison the next Restate handler call on this thread.
      try prior.get()
      catch {
        case e: java.util.concurrent.ExecutionException =>
          // The in-flight compute failed; propagate the cause.
          throw e.getCause
        case e: InterruptedException =>
          Thread.currentThread().interrupt()
          throw new RuntimeException(
            "getOrCompute: interrupted while waiting on the in-flight "
              + "compute (key=" + key + ")",
            e)
      }
    } else {
      // We won the race; run the compute.
      try {
        val v = compute.get()
        // Repopulate the cache with the fresh value BEFORE completing
        // the future — so a waiter that wakes up and checks the cache
        // sees the value. The `put` call here does NOT use the
        // single-arg overload (which would tag with model="") — the
        // caller is expected to call `putWithModelAndVersion` from
        // inside the compute closure if model-tagging is needed
        // (the QueryService does this).
        if (get(key).isEmpty) {
          put(key, v)
        }
        ours.complete(v)
        v
      } catch {
        case t: Throwable =>
          // On failure, we still complete the future so any
          // concurrent waiters (who reached `prior.get()` BEFORE we
          // did the remove below) wake up with the same exception.
          // After this point, no NEW caller can see the future —
          // they either hit the cache (success path) or do their
          // own compute (failure path; failure isn't cached by
          // design — a transient backend hiccup shouldn't poison
          // the key forever).
          ours.completeExceptionally(t)
          throw t
      } finally {
        // Only remove if it's still ours — a successful concurrent
        // put on a DIFFERENT key never enters this map, so the
        // compare-and-remove is safe.
        inFlight.remove(key, ours)
      }
    }
  }

  /** Snapshot the keys in LRU order (oldest first). */
  override def keys(): Seq[String] = lock.synchronized {
    import scala.jdk.CollectionConverters._
    map.keySet.asScala.toSeq
  }
}
