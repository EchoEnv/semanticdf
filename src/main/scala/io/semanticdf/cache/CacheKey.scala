package io.semanticdf.cache

import io.semanticdf.SortKey
import io.semanticdf.audit.{PredicateHasher, QueryRequest => AuditQueryRequest}

/** Build a stable, canonical cache key from a captured query request.
  *
  * Two queries share a cache entry iff they ask for the same data:
  *   - same model
  *   - same measures (in any order — `measures` is a set from the
  *     caller's perspective)
  *   - same dimensions
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
      val measures   = req.measures.sorted.mkString(",")
      val dimensions = req.dimensions.sorted.mkString(",")
      val whereHash  = req.where.map(PredicateHasher.hash).getOrElse("")
      val havingHash = req.having.map(PredicateHasher.hash).getOrElse("")
      val canonical = s"m=${req.model}|me=$measures|dim=$dimensions" +
        s"|w=$whereHash|h=$havingHash"
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
