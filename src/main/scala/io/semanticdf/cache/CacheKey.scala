package io.semanticdf.cache

import io.semanticdf.audit.{PredicateHasher, QueryRequest => AuditQueryRequest}

/** Build a stable, canonical cache key from a captured query request.
  *
  * Two queries share a cache entry iff they ask for the same data:
  *   - same model
  *   - same model `version` (auto-invalidation: a version bump produces
  *     a different cache key, so old entries become unreachable and
  *     LRU evicts them; this is the v0.1.17-review's recommended
  *     auto-invalidation mechanism)
  *   - same measures (in the order the user asked — the measure
  *     order doesn't change the result but is part of the request
  *     contract; a future refactor that puts measures in a different
  *     column would break caller assumptions if we sorted)
  *   - same dimensions (in the order the user asked — the column
  *     order is part of the result, e.g. for positional row arrays
  *     returned by the MCP `query` tool)
  *   - same `where` predicate (via the canonical SHA-256 hash from
  *     [[PredicateHasher]])
  *   - same `having` predicate
  *   - same `orderBy` (direction matters: `carrier asc` ≠ `carrier desc`)
  *   - same `limit` (None ≠ Some(10) — uncapped vs. capped differ)
  *
  * Returns `None` if the request is empty (no model), so callers can
  * short-circuit on degenerate input.
  *
  * The output is a lowercase hex SHA-256 — 64 chars, stable across
  * JVMs and platforms. The actual value is opaque to the cache
  * (the cache treats it as a string key); only the canonical-form
  * string matters for the equivalence contract.
  *
  * The length-prefixed encoding rules used to build the canonical
  * string live in [[LengthPrefixed]] — extracted there so future
  * hash-keyed storage can reuse the same convention without
  * duplicating the parser logic.
  */
object CacheKey {

  /** Default row cap for the cache miss collect path.
    *
    * Lives here (foundational hashing layer) rather than in [[CacheBridge]]
    * (Java facade) so the constant can be referenced without dragging
    * the facade's import into the cache-key module. Java callers should
    * use [[CacheBridge.defaultMaxRows]] (the canonical Java accessor).
    */
  val DefaultMaxRows: Int = 100000

  def forRequest(req: AuditQueryRequest, maxRows: Int): Option[String] = {
    if (req.model == null || req.model.isEmpty) None
    else {
      // Every field is length-prefixed. Without length prefixes,
      // delimiter-based encoding admits collisions: `Seq("a,b")`
      // and `Seq("a","b")` both encode as `"a,b"`, returning the
      // wrong cached rows. Fixed for the time
      // fields; later extended to the rest of the request
      // shape.
      val modelPart   = LengthPrefixed.encodeString(req.model)
      val measuresPart = LengthPrefixed.encodeList(req.measures)
      val dimsPart    = LengthPrefixed.encodeList(req.dimensions)
      // whereHash / havingHash are SHA-256 hex strings (64 chars,
      // [0-9a-f]) — no delimiter or length collision possible. Keep
      // them as-is for readability.
      //
      // Phase 1 consolidation: PredicateHasher internally converts via
      // [[io.semanticdf.predicate.PredicateConverter.toCore]] to the
      // engine-portable core.predicate ADT for the actual hash computation.
      // Callers pass the Spark-bearing Predicate directly — no API change.
      val whereHash  = req.where.map(PredicateHasher.hash).getOrElse("")
      val havingHash = req.having.map(PredicateHasher.hash).getOrElse("")
      // Order-preserving: the user-requested column order is part of
      // the result contract. Length-prefixing the pairs preserves
      // it without ambiguity.
      val orderByPart = LengthPrefixed.encodePairList(req.orderBy)
      val limitPart  = req.limit.map(_.toString).getOrElse("none")
      val grainPart  = LengthPrefixed.encodeOptString(req.timeGrain)
      val grainsPart = LengthPrefixed.encodeMap(req.timeGrains)
      val rangePart  = LengthPrefixed.encodeOptPair(req.timeRange)
      // `mv` is the model-version segment — included as a length-prefixed
      // int so it gets the same prefix handling as other int-valued fields
      // (version 10 vs version 100 will not collide).
      val versionPart = LengthPrefixed.encodeOptString(Option(req.version).filter(_ != 0).map(_.toString))
      // `mr` is the maxRows segment — included ONLY when non-default so
      // existing cache entries (built before this field was added) keep
      // working. A user who sets withMaxRows(n) gets a different cache
      // key from the default-maxRows path, so the cap is honoured on
      // both miss and hit.
      val maxRowsPart = LengthPrefixed.encodeOptString(
        Option(maxRows).filter(_ != DefaultMaxRows).map(_.toString))
      val canonical = s"m=$modelPart|mv=$versionPart|me=$measuresPart|dim=$dimsPart" +
        s"|w=$whereHash|h=$havingHash|ob=$orderByPart|lim=$limitPart" +
        s"|tg=$grainPart|tgs=$grainsPart|tr=$rangePart" +
        s"|mr=$maxRowsPart"
      Some(LengthPrefixed.sha256(canonical))
    }
  }

  /** Backward-compatible single-arg overload. Defaults maxRows to
    * [[DefaultMaxRows]] so existing callers (tests, platform code)
    * keep working without modification. New code should pass maxRows
    * explicitly. */
  def forRequest(req: AuditQueryRequest): Option[String] =
    forRequest(req, DefaultMaxRows)

  /** SHA-256 of the canonical string, lowercased hex. Delegates to
    * [[LengthPrefixed.sha256]]; kept here for source compatibility
    * with existing callers (e.g. `ResultCacheSpec`). */
  def sha256(s: String): String = LengthPrefixed.sha256(s)
}
