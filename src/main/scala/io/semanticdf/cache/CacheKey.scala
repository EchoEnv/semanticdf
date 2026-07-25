package io.semanticdf.cache

import io.semanticdf.SortKey
import io.semanticdf.audit.{PredicateHasher, QueryRequest => AuditQueryRequest}

import scala.util.chaining._

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
      // Every field is length-prefixed. Without length prefixes,
      // delimiter-based encoding admits collisions: `Seq("a,b")`
      // and `Seq("a","b")` both encode as `"a,b"`, returning the
      // wrong cached rows. PR #186 fixed this for the time
      // fields; PR #188 extends it to the rest of the request
      // shape.
      val modelPart   = encodeString(req.model)
      val measuresPart = encodeList(req.measures)
      val dimsPart    = encodeList(req.dimensions)
      // whereHash / havingHash are SHA-256 hex strings (64 chars,
      // [0-9a-f]) — no delimiter or length collision possible. Keep
      // them as-is for readability.
      val whereHash  = req.where.map(PredicateHasher.hash).getOrElse("")
      val havingHash = req.having.map(PredicateHasher.hash).getOrElse("")
      // Order-preserving: the user-requested column order is part of
      // the result contract. Length-prefixing the pairs preserves
      // it without ambiguity.
      val orderByPart = encodePairList(req.orderBy)
      val limitPart  = req.limit.map(_.toString).getOrElse("none")
      val grainPart  = encodeOptString(req.timeGrain)
      val grainsPart = encodeMap(req.timeGrains)
      val rangePart  = encodeOptPair(req.timeRange)
      val canonical = s"m=$modelPart|me=$measuresPart|dim=$dimsPart" +
        s"|w=$whereHash|h=$havingHash|ob=$orderByPart|lim=$limitPart" +
        s"|tg=$grainPart|tgs=$grainsPart|tr=$rangePart"
      Some(sha256(canonical))
    }
  }

  /** Length-prefixed required string encoding.
    *   - `"foo"` → `"S3:foo"`
    *
    * The `S` prefix marks the field as present. The length prefix
    * makes the encoding unambiguous regardless of the string's
    * contents. */
  private def encodeString(s: String): String = s"S${s.length}:$s"

  /** Length-prefixed list of strings.
    *   - empty    → `"0:"` (zero entries)
    *   - non-empty → `"<n>:S<l>:<s>;S<l>:<s>;..."`
    *
    * Entries appear in the input order (NOT sorted — the order
    * of measures/dimensions is part of the result contract). */
  private def encodeList(items: Seq[String]): String =
    if (items.isEmpty) "0:"
    else items.map(s => s"S${s.length}:$s").mkString(";").pipe(prefixSize)

  /** Length-prefixed list of (name, value) pairs.
    *   - empty    → `"0:"`
    *   - non-empty → `"<n>:P<ln>:<n>,<lv>:<v>;P<ln>:<n>,<lv>:<v>;..."`
    *
    * Same shape as the time-range pair encoding; reused here so
    * `orderBy` items are encoded the same way. */
  private def encodePairList(items: Seq[(String, String)]): String =
    if (items.isEmpty) "0:"
    else items.map { case (n, v) => s"P${n.length}:$n,${v.length}:$v" }.mkString(";").pipe(prefixSize)

  /** Helper: prefix a comma-joined string with its entry count. */
  private def prefixSize(s: String): String = s"${s.count(_ == ';') + 1}:$s"

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
