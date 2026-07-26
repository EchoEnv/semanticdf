package io.semanticdf.cache

import scala.util.chaining._

/** Length-prefixed value encoders. Used by [[CacheKey]] to build
  * unambiguous canonical strings for hashing.
  *
  * == Why length-prefixed? ==
  *
  * Delimiter-based encoding admits collisions: `Seq("a,b")` and
  * `Seq("a","b")` both encode as `"a,b"`, returning the wrong
  * cached rows. Length-prefixing every field disambiguates the
  * parser regardless of what characters appear inside values.
  *
  * PR #187 added length prefixes to the time fields; PR #188
  * extended the pattern to the rest of the request shape. This
  * object centralizes the encoding rules so future hash-keyed
  * storage can reuse them without duplicating the convention.
  *
  * == Format summary ==
  *
  *   - `encodeString`: `"S<len>:<value>"` (e.g. `"foo"` → `"S3:foo"`)
  *   - `encodeList`: `"<n>:S<len>:<s>;S<len>:<s>;..."` (order-preserving)
  *   - `encodePairList`: `"<n>:P<ln>:<n>,<lv>:<v>;..."` (order-preserving)
  *   - `encodeMap`: `"<n>:k<lk>:<k>=v<lv>:<v>;..."` (sorted by key)
  *   - `encodeOptString`: `None` → `"N"`, `Some(v)` → `"S<len>:<v>"`
  *   - `encodeOptPair`: `None` → `"N"`, `Some((s,e))` → `"P<ls>:<s>,<le>:<e>"`
  *
  * `None` and `Some("")` are distinct: `"N"` vs `"S0:"`. And
  * `Some("N")` is `"S1:N"`, also distinct from `None`. The `S`,
  * `N`, `P`, and `k`/`v` prefixes disambiguate field types so
  * the parser can recover boundaries regardless of input.
  */
object LengthPrefixed {

  /** SHA-256 of the input string, lowercased hex (64 chars).
    * Public so callers doing hash-keyed storage can reuse the
    * same digest algorithm. */
  def sha256(s: String): String = {
    val bytes = java.security.MessageDigest.getInstance("SHA-256")
      .digest(s.getBytes("UTF-8"))
    bytes.map(b => f"${b & 0xff}%02x").mkString
  }

  /** Length-prefixed required string encoding.
    *   - `"foo"` → `"S3:foo"`
    *
    * The `S` prefix marks the field as present. The length prefix
    * makes the encoding unambiguous regardless of the string's
    * contents. */
  def encodeString(s: String): String = s"S${s.length}:$s"

  /** Length-prefixed list of strings.
    *   - empty    → `"0:"` (zero entries)
    *   - non-empty → `"<n>:S<l>:<s>;S<l>:<s>;..."`
    *
    * Entries appear in the input order (NOT sorted — the order
    * of measures/dimensions is part of the result contract). */
  def encodeList(items: Seq[String]): String =
    if (items.isEmpty) "0:"
    else items.map(s => s"S${s.length}:$s").mkString(";").pipe(prefixSize)

  /** Length-prefixed list of (name, value) pairs.
    *   - empty    → `"0:"`
    *   - non-empty → `"<n>:P<ln>:<n>,<lv>:<v>;P<ln>:<n>,<lv>:<v>;..."`
    *
    * Same shape as the time-range pair encoding; reused here so
    * `orderBy` items are encoded the same way. */
  def encodePairList(items: Seq[(String, String)]): String =
    if (items.isEmpty) "0:"
    else items.map { case (n, v) => s"P${n.length}:$n,${v.length}:$v" }.mkString(";").pipe(prefixSize)

  /** Helper: prefix a semicolon-joined string with its entry count. */
  def prefixSize(s: String): String = s"${s.count(_ == ';') + 1}:$s"

  /** Length-prefixed Option encoding.
    *   - `None`   → `"N"` (presence sentinel)
    *   - `Some(v)` → `"S<len>:<v>"` (presence + length + value)
    *
    * `None` and `Some("")` are distinct: `"N"` vs `"S0:"`. And
    * `Some("N")` is `"S1:N"`, also distinct from `None`. */
  def encodeOptString(o: Option[String]): String = o match {
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
  def encodeMap(m: Map[String, String]): String =
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
  def encodeOptPair(p: Option[(String, String)]): String = p match {
    case None           => "N"
    case Some((s, e))   => s"P${s.length}:$s,${e.length}:$e"
  }
}
