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
      // they must be in the cache key. The previous delimiter-based
      // encoding (PR #186) admitted collisions — e.g.
      //   `timeGrain = None` vs `Some("none")` both encoded as `none`
      //   `Map("a" -> "b,c:d")` vs `Map("a" -> "b", "c" -> "d")` both
      //     encoded as `a:b,c:d`
      //   `timeRange = Some(("a..b","c"))` vs `Some(("a","b..c"))` both
      //     encoded as `a..b..c`
      // The fix: length-prefixed encoding makes the parser
      // unambiguous regardless of what characters appear in values.
      val grainPart  = encodeOptString(req.timeGrain)
      val grainsPart = encodeMap(req.timeGrains)
      val rangePart  = encodeOptPair(req.timeRange)
      val canonical = s"m=${req.model}|me=$measures|dim=$dimensions" +
        s"|w=$whereHash|h=$havingHash|ob=$orderBy|lim=$limitPart" +
        s"|tg=$grainPart|tgs=$grainsPart|tr=$rangePart"
      Some(sha256(canonical))
    }
  }

  /** Length-prefixed Option encoding.
    *   - `None`   → `"N"` (presence sentinel)
    *   - `Some(v)` → `"S<len>:<v>"` (presence + length + value)
    *
    * `None` and `Some("")` are distinct: `"N"` vs `"S0:"`. And
    * `Some("N")` is `"S1:N"`, also distinct from `None`. */
  private def encodeOptString(o: Option[String]): String = o match {
    case None    => "N"
    case Some(v) => s"S${v.length}:$v"
  }

  /** Length-prefixed Map encoding.
    *   - empty → `"0:"` (zero entries)
    *   - non-empty → `"<n>:k<lk>:<k>=v<lv>:<v>;k<lk>:<k>=v<lv>:<v>;..."`
    *
    * Each entry's key and value are length-prefixed, so the
    * parser can recover boundaries regardless of what characters
    * appear inside values. Entries are sorted by key for
    * determinism. */
  private def encodeMap(m: Map[String, String]): String =
    if (m.isEmpty) "0:"
    else {
      val entries = m.toSeq.sortBy(_._1).map { case (k, v) =>
        s"k${k.length}:$k=v${v.length}:$v"
      }.mkString(";")
      s"${m.size}:$entries"
    }

  /** Length-prefixed Option[(String, String)] encoding.
    *   - `None`        → `"N"`
    *   - `Some((s, e))` → `"P<ls>:<s>,<le>:<e>"`
    *
    * Uses `P` to distinguish from the Option[String] encoding
    * (which uses `S`). The `,` inside the Some case is just a
    * visual separator; the length prefix makes parsing
    * unambiguous. */
  private def encodeOptPair(p: Option[(String, String)]): String = p match {
    case None           => "N"
    case Some((s, e))   => s"P${s.length}:$s,${e.length}:$e"
  }

  /** SHA-256 of the canonical string, lowercased hex. */
  def sha256(s: String): String = {
    val bytes = java.security.MessageDigest.getInstance("SHA-256")
      .digest(s.getBytes("UTF-8"))
    bytes.map(b => f"${b & 0xff}%02x").mkString
  }
}
