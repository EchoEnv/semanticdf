package io.semanticdf.cache

import io.semanticdf.SortKey
import io.semanticdf.audit.{PredicateHasher, QueryRequest => AuditQueryRequest}

/** Build a stable, canonical cache key from a captured query request.
  *
  * Two queries share a cache entry iff they ask for the same data:
  *   - same model
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
  */
object CacheKey {

  def forRequest(req: AuditQueryRequest): Option[String] = {
    if (req.model == null || req.model.isEmpty) None
    else {
      val measures   = req.measures.mkString(",")
      val dimensions = req.dimensions.mkString(",")
      val whereHash  = req.where.map(PredicateHasher.hash).getOrElse("")
      val havingHash = req.having.map(PredicateHasher.hash).getOrElse("")
      // Order-preserving encoding: the user-requested column order is
      // part of the result contract (esp. for positional row arrays
      // in the MCP wire). Sorting here would conflate semantically-
      // different requests.
      val orderBy   = req.orderBy.map { case (name, dir) => s"$name:$dir" }.mkString(",")
      val limitPart = req.limit.map(_.toString).getOrElse("none")
      // Time-grain fields change the executed query (atTimeGrain
      // truncates time dimensions, timeRange adds a range filter), so
      // they must be in the cache key. Encode the per-dimension map
      // deterministically by sorting the keys.
      val grainPart = req.timeGrain.getOrElse("none")
      val grainsPart = req.timeGrains.toSeq.sortBy(_._1).map { case (k, v) => s"$k:$v" }.mkString(",")
      val rangePart = req.timeRange.map { case (s, e) => s"$s..$e" }.getOrElse("none")
      val canonical = s"m=${req.model}|me=$measures|dim=$dimensions" +
        s"|w=$whereHash|h=$havingHash|ob=$orderBy|lim=$limitPart" +
        s"|tg=$grainPart|tgs=$grainsPart|tr=$rangePart"
      Some(sha256(canonical))
    }
  }

  /** SHA-256 of the canonical string, lowercased hex. */
  def sha256(s: String): String = {
    val bytes = java.security.MessageDigest.getInstance("SHA-256")
      .digest(s.getBytes("UTF-8"))
    bytes.map(b => f"${b & 0xff}%02x").mkString
  }
}
