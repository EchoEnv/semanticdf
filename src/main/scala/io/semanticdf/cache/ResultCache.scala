package io.semanticdf.cache

/** Pluggable destination for cached query results.
  *
  * == Why ==
  *
  * Repeated queries on the same model with the same filter are common
  * in LLM-agent loops: an agent re-asks the same question while
  * reasoning, or several agents in the same session hit the same
  * semantic table. A result cache collapses these into one Spark
  * job — the first call materialises the rows, every subsequent
  * call rebuilds a `DataFrame` from those rows without touching
  * Spark's planner.
  *
  * == Contract ==
  *
  *   - `get(key)` returns the cached value if present, else `None`.
  *   - `put(key, value)` records the value. May evict a prior entry.
  *   - Keys are stable strings (SHA-256 in the canonical case). Two
  *     equivalent queries produce the same key — see
  *     [[io.semanticdf.audit.PredicateHasher]] for the hash definition
  *     on `where` / `having`.
  *   - Sinks must be thread-safe — the library calls `get` and `put`
  *     from the calling thread.
  *   - Implementations should be fast on the hit path (a single
  *     `get` call returns the cached value) and unbounded in the
  *     number of distinct keys they may be asked to store. The
  *     default `inMemory(maxEntries)` evicts the LRU entry on
  *     overflow.
  *   - The default sink is `NoOp` — no caching, no overhead. Opt
  *     in via [[io.semanticdf.SemanticTable.withResultCache]].
  *
  * == Cost note ==
  *
  * On a cache miss, the in-memory implementation
  * ([[InMemoryResultCache]]) rebuilds the cached result via
  * `parallelize(rows.toSeq) + schema`. This is O(n) over the result
  * set — for a 10k-row × 20-column result, that's ~200k fields
  * re-allocated on every miss. For result sets >1MB, prefer a
  * different cache backend or no cache.
  *
  * The cache key itself ([[CacheKey.forRequest]]) is a SHA-256 over
  * the full request shape: model, measures, dimensions, where hash,
  * having hash, `orderBy` (direction + columns), `limit` (None vs
  * Some), and the time-grain / time-range / per-dimension-grains
  * fields (post-v0.1.17). Hashing is microseconds; not a hot-path
  * concern. */
trait ResultCache extends Serializable {

  /** Look up a cached result. Returns `None` on miss; the caller
    * is responsible for executing the query and calling [[put]]. */
  def get(key: String): Option[CachedResult]

  /** Record a cached result. May evict a prior entry if the cache
    * is bounded. Must not throw.
    *
    * The single-arg overload stores the result without a model
    * association — [[invalidateModel]] won't see these entries.
    * Use the three-arg overload (or call `putWithModel`) when you
    * want invalidation by model. */
  def put(key: String, value: CachedResult): Unit = putWithModel(key, value, "")

  /** Record a cached result tagged with a model name. The model tag
    * enables [[invalidateModel]] to drop all entries for a given
    * model in one call. Pass `""` if the entry has no model
    * association; the single-arg [[put]] does that for you.
    *
    * Default implementation just calls [[put]] with the model
    * ignored — caches that don't track model can ignore the
    * argument. Overriding implementations (like
    * [[InMemoryResultCache]]) maintain a sidecar map from model
    * name to keys for O(1) lookup. */
  def putWithModel(key: String, value: CachedResult, model: String): Unit = put(key, value)

  /** 4-arg variant: also record the model version for sidecar-based
    * invalidation. Default delegates to [[putWithModel]] with
    * `version = 0`. Concrete caches are encouraged to override
    * this to track (model, version) → keys for [[invalidateByModelAndVersion]]
    * lookups. */
  def putWithModelAndVersion(
      key: String, value: CachedResult, model: String, version: Int): Unit =
    putWithModel(key, value, model)

  /** Drop every entry tagged with the given model name. Returns
    * the number of entries actually removed. Default: 0 (no-op
    * for caches that don't track models). The lookup is O(1) for
    * caches that maintain a model→keys sidecar; O(n) otherwise. */
  def invalidateModel(name: String): Int = 0

  /** Drop every entry tagged with the given model name AND version.
    *
    * Used by the v0.2.0 auto-invalidation mechanism: after a model
    * version bump, the cache key naturally differs (mv= section in
    * the canonical form), so old-version entries become unreachable
    * and LRU evicts them eventually. This hook lets operators
    * actively release memory for old-version entries (e.g. on
    * persistent backends like Redis) instead of waiting for LRU.
    *
    * Returns the number of entries actually removed. Default: 0
    * (no-op for caches that don't track versions). The default
    * implementation does an O(n) scan over all entries; concrete
    * caches are encouraged to maintain a sidecar for O(1). */
  def invalidateByModelAndVersion(name: String, version: Int): Int = 0

  /** Return the keys currently held by this cache, in LRU order
    * (oldest first). Default: empty (non-retentive caches have no
    * keys to report). Used by leak tests and observability tooling
    * to assert bounded memory. */
  def keys(): Seq[String] = Seq.empty

  /** Drop every retained entry. Default: no-op (non-retentive caches
    * have nothing to clear). Used by tests to assert GC reclaim
    * after dropping references. */
  def clear(): Unit = ()

  // --- Journaled-form API (PR #276) ---

  /** Look up a cached journaled-form value. Returns `None` on miss.
    * The journaled form is whatever the platform-side cache
    * already serializes for the Restate journal boundary (a
    * {@code RestateCachedRow} in v0.2.2). Caching this form
    * avoids the redundant {@code Array[Row]} rebuild that the
    * v0.2.2 path incurred on every cache miss (see issue #276).
    *
    * <p>The library doesn't know about {@code RestateCachedRow}
    * — the value type is {@code AnyRef} so the trait stays
    * platform-independent. Concrete caches that don't want this
    * optimization leave the default no-op.
    */
  def getJournaled(key: String): Option[AnyRef] = None

  /** Record a journaled-form value tagged with model + version.
    * Default impl: no-op. Concrete caches that want the
    * performance benefit (skipping the {@code Array[Row]}
    * rebuild) override this and pair it with [[getJournaled]].
    */
  def putJournaledWithModelAndVersion(
      key: String, value: AnyRef, model: String, version: Int): Unit = ()

  /** Single-flight read-through for journaled-form values.
    *
    * <p><b>Default impl is missing on purpose.</b> The previous
    * default did a non-atomic `get → compute → put` via
    * [[putJournaledWithModelAndVersion]] with `model=""`, which
    * produced entries that [[invalidateModel]] could never reach
    * (the sidecar skips entries with empty model). Caches that
    * override [[getJournaled]] and [[putJournaledWithModelAndVersion]]
    * MUST also override this method — throwing from the default
    * surfaces the contract violation at the call site rather than
    * silently leaking entries.
    *
    * <p><b>Contract for an override:</b>
    * <ul>
    *   <li>On HIT, return the cached value without invoking
    *       `compute`.</li>
    *   <li>On MISS, coalesce N concurrent identical calls for the
    *       same `key` into ONE `compute.get()` invocation. The
    *       typical implementation is a per-key
    *       `ConcurrentHashMap[String, CompletableFuture]`.</li>
    *   <li>If `compute` throws, propagate the exception to ALL
    *       waiters for this key (not just the winner). The cache
    *       is NOT populated on failure.</li>
    *   <li>The compute closure is responsible for tagging any
    *       `putJournaledWithModelAndVersion` call with the correct
    *       `model` and `version` (the trait cannot supply these
    *       because it doesn't know the caller's intent).</li>
    * </ul>
    *
    * <p>See [[InMemoryResultCache.getOrComputeJournaled]] for the
    * production single-flight pattern.
    */
  def getOrComputeJournaled(
      key: String,
      compute: java.util.function.Supplier[AnyRef]): AnyRef = {
    // The default impl is unsatisfiable: the row-form put(tag="")
    // leaks uninvalidateable entries. The platform is the only
    // intended consumer; its cache (InMemoryResultCache) overrides
    // this method. Anyone subclassing ResultCache with their own
    // getJournaled / putJournaledWithModelAndVersion override MUST
    // also override getOrComputeJournaled — the default in the
    // row-form getOrCompute silently swallows the thundering herd
    // (no single-flight), and replicating that for the journaled
    // path would push the same bug into the platform's hot path.
    throw new UnsupportedOperationException(
      "ResultCache.getOrComputeJournaled: no default implementation. "
        + "Caches that override getJournaled and "
        + "putJournaledWithModelAndVersion must also override "
        + "getOrComputeJournaled (see InMemoryResultCache for the "
        + "single-flight pattern).")
  }

  /** Single-flight read-through: if `key` is in the cache, return the
    * cached value; if not, invoke `compute` to produce it, store it
    * under `key` (with NO model tag — the caller is responsible for
    * tagging separately if needed), and return the produced value.
    *
    * <p><b>Concurrency contract</b>: for the same `key`, this method
    * MUST guarantee that `compute.get()` is invoked AT MOST ONCE
    * even under concurrent access from N threads. Implementations
    * typically achieve this via a per-key in-flight map (see
    * [[InMemoryResultCache]]). The default implementation does NOT
    * guarantee single-flight — it uses a non-atomic `get → compute →
    * put` pattern which admits the cache stampede bug for caches that
    * don't override. Concrete caches used in production should
    * override this method.
    *
    * <p>If `compute` throws, the exception propagates to ALL waiters
    * for this key (caller-visible semantics: "the in-flight compute
    * failed"), and the cache is NOT populated.
    *
    * @param key     cache key (see [[get]])
    * @param compute supplier that produces the value on miss
    * @return the cached or freshly-computed value
    */
  def getOrCompute(
      key: String,
      compute: java.util.function.Supplier[CachedResult]): CachedResult = {
    get(key) match {
      case Some(v) => v
      case None =>
        val v = compute.get()
        put(key, v)
        v
    }
  }
}

object ResultCache {

  /** A cache that drops every put and returns `None` on every get.
    * The default. Opt-in by passing a real cache to
    * [[io.semanticdf.SemanticTable.withResultCache]]. */
  val NoOp: ResultCache = new ResultCache {
    override def get(key: String): Option[CachedResult] = None
    override def put(key: String, value: CachedResult): Unit = ()
  }

  /** A bounded in-memory LRU cache. `maxEntries` (default 256)
    * caps the number of distinct query results held in memory;
    * on overflow, the least-recently-accessed entry is evicted.
    *
    * Thread-safe via `synchronized` on the internal map. */
  def inMemory(maxEntries: Int = 256): ResultCache =
    new InMemoryResultCache(maxEntries)
}
