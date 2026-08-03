package io.semanticdf.audit

import io.semanticdf.core.predicate.Predicate
import io.semanticdf.predicate.PredicateConverter

/** Stable, canonical hash of a `Predicate` tree.
  *
  * Two predicates hash to the same value iff they describe the same
  * filter — same field, same op, same value, same structure. The hash
  * is independent of construction order for the commutative cases
  * (`And` / `Or` sort their children by canonical form) and is
  * independent of whether the user expressed the predicate in
  * library form or via the v0.1.16 MCP AST wire form.
  *
  * == How ==
  *
  * We walk the tree and emit a stable canonical string in prefix
  * notation, then SHA-256 it. Children of `And` / `Or` are sorted
  * by their canonical form before hashing, so `A and B` and
  * `B and A` produce the same hash. Leaves use a `op|field|value`
  * triple.
  *
  * The canonical form is **not** SQL. It is a small, deterministic
  * notation designed for hashing — using SQL would tie the hash to
  * the wire format and make it brittle to formatting changes.
  *
  * ==Engine-portable migration (Phase 1 consolidation)==
  *
  * Since v0.2.5, the data-side operations of this hasher run on
  * `io.semanticdf.core.predicate.Predicate` (the engine-portable ADT)
  * rather than `io.semanticdf.predicate.Predicate` (the Spark-bearing
  * one). The public API still accepts the Spark-bearing `Predicate`
  * (so all existing callers — `CacheKey`, `SemanticTableCore`,
  * `SemanticTableStreaming`, `AuditSpec`, etc. — continue to compile
  * unchanged); conversion happens here at the boundary.
  *
  * The hash function itself is data-only — it walks a tree of
  * `final case class` nodes, applies a canonicalizing rewrite, and
  * SHA-256s the result. No Spark or engine behaviour is invoked; the
  * data-side contract is what matters here.
  */
object PredicateHasher {

  /** SHA-256 of the canonical form, lowercased hex.
    *
    * Accepts the Spark-bearing `Predicate` from the public API. Internally
    * converts to the engine-portable core ADT before walking — the walk
    * is data-only, so it works on the core type without any engine. */
  def hash(predicate: io.semanticdf.predicate.Predicate): String = {
    import java.security.MessageDigest
    val canonical = canonicalize(predicate)
    val bytes = MessageDigest.getInstance("SHA-256").digest(canonical.getBytes("UTF-8"))
    bytes.map(b => f"${b & 0xff}%02x").mkString
  }

  /** Stable string form. Useful for debugging ("what does this
    * hash map to?"). NOT the hash itself.
    *
    * Public API accepts the Spark-bearing `Predicate`; the walk runs on
    * the engine-portable core ADT. */
  def canonicalize(p: io.semanticdf.predicate.Predicate): String =
    canonicalizeCore(PredicateConverter.toCore(p))

  /** Core-direct hash path. Operates on the engine-portable ADT only.
    *
    * Useful when the caller already has a `core.predicate.Predicate`
    * (e.g. built from the engine-portable API directly, or in tests
    * verifying the converter boundary). Avoids the round-trip through
    * `io.semanticdf.predicate.Predicate` that the public `hash` performs.
    *
    * Hash output is identical to [[hash]] for semantically equivalent
    * predicates — verified by [[io.semanticdf.predicate.PredicateConverterSpec]]
    * and the audit-log equivalence contract. */
  def hashCore(predicate: Predicate): String = {
    import java.security.MessageDigest
    val canonical = canonicalizeCore(predicate)
    val bytes = MessageDigest.getInstance("SHA-256").digest(canonical.getBytes("UTF-8"))
    bytes.map(b => f"${b & 0xff}%02x").mkString
  }

  /** Core-data-side walk. Operates on the engine-portable ADT only —
    * no Spark imports needed. Recursive over the 15 case classes of
    * `io.semanticdf.core.predicate.Predicate`. */
  private def canonicalizeCore(p: Predicate): String = p match {
    case Predicate.Compare.Eq(f, v)         => s"eq($f,${stableValue(v)})"
    case Predicate.Compare.Ne(f, v)         => s"ne($f,${stableValue(v)})"
    case Predicate.Compare.Lt(f, v)         => s"lt($f,${stableValue(v)})"
    case Predicate.Compare.Le(f, v)         => s"le($f,${stableValue(v)})"
    case Predicate.Compare.Gt(f, v)         => s"gt($f,${stableValue(v)})"
    case Predicate.Compare.Ge(f, v)         => s"ge($f,${stableValue(v)})"
    case Predicate.Compare.Contains(f, v)    => s"contains($f,${stableValue(v)})"
    case Predicate.Compare.StartsWith(f, v) => s"starts_with($f,${stableValue(v)})"
    case Predicate.Compare.EndsWith(f, v)   => s"ends_with($f,${stableValue(v)})"
    case Predicate.Compare.ArrayContains(f, v) => s"array_contains($f,${stableValue(v)})"
    case Predicate.In(f, vs, false)        => s"in($f,${vs.map(stableValue).sorted.mkString(",")})"
    case Predicate.In(f, vs, true)         => s"not_in($f,${vs.map(stableValue).sorted.mkString(",")})"
    case Predicate.IsNull(f, false)        => s"is_null($f)"
    case Predicate.IsNull(f, true)         => s"is_not_null($f)"
    case Predicate.Not(inner)              => s"not(${canonicalizeCore(inner)})"
    // And/Or are varargs. Sort the children's canonical forms so
    // associativity doesn't change the hash (a and (b and c) hashes
    // the same as (a and b) and c).
    case Predicate.And(children @ _*)      => s"and(${children.map(canonicalizeCore).sorted.mkString(",")})"
    case Predicate.Or(children @ _*)       => s"or(${children.map(canonicalizeCore).sorted.mkString(",")})"
  }

  /** Render a value (number, string, boolean, null) deterministically.
    * Numbers use Java's `toString`. Strings are wrapped in single
    * quotes (escaping any embedded single quote as `''`). Booleans
    * are lowercase. `null` is the literal token. */
  private def stableValue(v: Any): String = v match {
    case null           => "null"
    case s: String      => "'" + s.replace("'", "''") + "'"
    case b: Boolean     => b.toString
    case n: Number      => n.toString
    case other          => other.toString
  }
}
