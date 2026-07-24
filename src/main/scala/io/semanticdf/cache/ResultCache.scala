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
  *     in via [[io.semanticdf.SemanticTable.withResultCache]]. */
trait ResultCache {

  /** Look up a cached result. Returns `None` on miss; the caller
    * is responsible for executing the query and calling [[put]]. */
  def get(key: String): Option[CachedResult]

  /** Record a cached result. May evict a prior entry if the cache
    * is bounded. Must not throw. */
  def put(key: String, value: CachedResult): Unit

  /** Return the keys currently held by this cache, in LRU order
    * (oldest first). Default: empty (non-retentive caches have no
    * keys to report). Used by leak tests and observability tooling
    * to assert bounded memory. */
  def keys(): Seq[String] = Seq.empty

  /** Drop every retained entry. Default: no-op (non-retentive caches
    * have nothing to clear). Used by tests to assert GC reclaim
    * after dropping references. */
  def clear(): Unit = ()
}

object ResultCache {

  /** A cache that drops every put and returns `None` on every get.
    * The default. Opt-in by passing a real cache to
    * [[io.semanticdf.SemanticTable.withResultCache]]. */
  val NoOp: ResultCache = new ResultCache {
    def get(key: String): Option[CachedResult] = None
    def put(key: String, value: CachedResult): Unit = ()
  }

  /** A bounded in-memory LRU cache. `maxEntries` (default 256)
    * caps the number of distinct query results held in memory;
    * on overflow, the least-recently-accessed entry is evicted.
    *
    * Thread-safe via `synchronized` on the internal map. */
  def inMemory(maxEntries: Int = 256): ResultCache =
    new InMemoryResultCache(maxEntries)
}
