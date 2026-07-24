package io.semanticdf.audit

import io.semanticdf.Predicate

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
  * the wire format and make it brittle to formatting changes. */
object PredicateHasher {

  /** SHA-256 of the canonical form, lowercased hex. */
  def hash(predicate: Predicate): String = {
    import java.security.MessageDigest
    val canonical = canonicalize(predicate)
    val bytes = MessageDigest.getInstance("SHA-256").digest(canonical.getBytes("UTF-8"))
    bytes.map(b => f"${b & 0xff}%02x").mkString
  }

  /** Stable string form. Useful for debugging ("what does this 
    * hash map to?"). NOT the hash itself. */
  def canonicalize(p: Predicate): String = p match {
    case Predicate.Compare.Eq(f, v)  => s"eq($f,${stableValue(v)})"
    case Predicate.Compare.Ne(f, v)  => s"ne($f,${stableValue(v)})"
    case Predicate.Compare.Lt(f, v)  => s"lt($f,${stableValue(v)})"
    case Predicate.Compare.Le(f, v)  => s"le($f,${stableValue(v)})"
    case Predicate.Compare.Gt(f, v)  => s"gt($f,${stableValue(v)})"
    case Predicate.Compare.Ge(f, v)  => s"ge($f,${stableValue(v)})"
    case Predicate.In(f, vs, false)  => s"in($f,${vs.map(stableValue).sorted.mkString(",")})"
    case Predicate.In(f, vs, true)   => s"not_in($f,${vs.map(stableValue).sorted.mkString(",")})"
    case Predicate.IsNull(f, false)  => s"is_null($f)"
    case Predicate.IsNull(f, true)   => s"is_not_null($f)"
    case Predicate.Not(inner)        => s"not(${canonicalize(inner)})"
    case Predicate.And(left, right)  => s"and(${sortCommutative(left, right)})"
    case Predicate.Or(left, right)   => s"or(${sortCommutative(left, right)})"
  }

  /** `and(A, B)` and `and(B, A)` should hash to the same value.
    * Sort children by their canonical form, comma-separated. */
  private def sortCommutative(left: Predicate, right: Predicate): String = {
    val lc = canonicalize(left)
    val rc = canonicalize(right)
    if (lc <= rc) s"$lc,$rc" else s"$rc,$lc"
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
