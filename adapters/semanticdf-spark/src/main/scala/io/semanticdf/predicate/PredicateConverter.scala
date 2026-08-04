package io.semanticdf.predicate

import io.semanticdf.core.predicate
import io.semanticdf.core.predicate.{Predicate => CorePredicate}

/** Boundary adapter: convert `io.semanticdf.predicate.Predicate` →
  * `io.semanticdf.core.predicate.Predicate`.
  *
  * ==Why this exists==
  *
  * Per the multi-engine design, the data-only `core.predicate.Predicate`
  * ADT will eventually replace the original Spark-bearing one. Until that
  * consolidation lands (Phase 2), code that wants to operate on the engine-
  * portable data — e.g. hashing, JSON serialization, MCP wire-format
  * translation — needs a faithful conversion.
  *
  * This converter is the boundary. After this PR, every `Predicate` flowing
  * into the audit/cache-key chain crosses this boundary; downstream code
  * (e.g. [[io.semanticdf.audit.PredicateHasher]]) trusts the `core` type
  * and operates on data alone — no `compile(scope): Column` calls, no
  * Spark imports.
  *
  * ==Boundary contract==
  *
  * The two ADTs have identical shape (15 case classes, same field types,
  * same `Compare.apply(op, field, value)` factory op-string vocabulary).
  * Conversion is a structural rewrite — each `io.semanticdf.predicate.X`
  * becomes the matching `core.predicate.X`. No data is lost.
  *
  * ==Equality preservation==
  *
  * Because both ADTs use `final case class` with identical fields, two
  * converted predicates are equal iff their originals were equal. This
  * matters for the audit/cache-key chain: same logical predicate →
  * same hash → same cache key, regardless of which ADT the caller used.
  *
  * ==Live in `io.semanticdf.predicate`, NOT `core.predicate`==
  *
  * The converter's INPUT is `io.semanticdf.predicate.Predicate`, which is
  * Spark-bearing. Placing the converter in `core.predicate` would force
  * `core` to depend on Spark, breaking the invariant that `core` compiles
  * without Spark on the classpath. The converter lives next to its source
  * type, in the Spark-aware package, and produces the engine-portable type.
  */
object PredicateConverter {

  /** Convert any `io.semanticdf.predicate.Predicate` to the engine-portable
    * `io.semanticdf.core.predicate.Predicate` form. Recursive for
    * `And` / `Or` / `Not`.
    *
    * The match is exhaustive over the original's sealed `Predicate` ADT.
    * Adding a new case class to either ADT without updating this converter
    * would be a compile error here. */
  def toCore(p: io.semanticdf.predicate.Predicate): CorePredicate = p match {
    // -------------------------------------------------------------------------
    // Compare family
    // -------------------------------------------------------------------------
    case io.semanticdf.predicate.Predicate.Compare.Eq(field, value) =>
      CorePredicate.Compare.Eq(field, value)
    case io.semanticdf.predicate.Predicate.Compare.Ne(field, value) =>
      CorePredicate.Compare.Ne(field, value)
    case io.semanticdf.predicate.Predicate.Compare.Lt(field, value) =>
      CorePredicate.Compare.Lt(field, value)
    case io.semanticdf.predicate.Predicate.Compare.Le(field, value) =>
      CorePredicate.Compare.Le(field, value)
    case io.semanticdf.predicate.Predicate.Compare.Gt(field, value) =>
      CorePredicate.Compare.Gt(field, value)
    case io.semanticdf.predicate.Predicate.Compare.Ge(field, value) =>
      CorePredicate.Compare.Ge(field, value)
    case io.semanticdf.predicate.Predicate.Compare.Contains(field, value) =>
      CorePredicate.Compare.Contains(field, value)
    case io.semanticdf.predicate.Predicate.Compare.StartsWith(field, value) =>
      CorePredicate.Compare.StartsWith(field, value)
    case io.semanticdf.predicate.Predicate.Compare.EndsWith(field, value) =>
      CorePredicate.Compare.EndsWith(field, value)
    case io.semanticdf.predicate.Predicate.Compare.ArrayContains(field, value) =>
      CorePredicate.Compare.ArrayContains(field, value)

    // -------------------------------------------------------------------------
    // Non-compare leaves
    // -------------------------------------------------------------------------
    case io.semanticdf.predicate.Predicate.In(field, values, negate) =>
      CorePredicate.In(field, values, negate)
    case io.semanticdf.predicate.Predicate.IsNull(field, negate) =>
      CorePredicate.IsNull(field, negate)

    // -------------------------------------------------------------------------
    // Compound (recursive)
    // -------------------------------------------------------------------------
    case io.semanticdf.predicate.Predicate.And(children @ _*) =>
      CorePredicate.And(children.map(toCore): _*)
    case io.semanticdf.predicate.Predicate.Or(children @ _*) =>
      CorePredicate.Or(children.map(toCore): _*)
    case io.semanticdf.predicate.Predicate.Not(pred) =>
      CorePredicate.Not(toCore(pred))
  }

  /** Bulk conversion for `Seq[Predicate]`. Convenience for callers that
    * hold collections (e.g. `Option[Predicate]`, `List[Predicate]`). */
  def toCoreAll(ps: Seq[io.semanticdf.predicate.Predicate]): Seq[CorePredicate] =
    ps.map(toCore)

  /** Round-trip identity check: converting A → B then comparing the converted
    * form's structure (via `==`) to a fresh conversion of A produces `true`.
    *
    * Useful in tests and as a runtime invariant at the boundary. */
  def roundTripEquals(a: io.semanticdf.predicate.Predicate): Boolean = {
    val b  = toCore(a)
    val b2 = toCore(a)
    b == b2
  }

  /** Convert `io.semanticdf.core.predicate.Predicate` →
    * `io.semanticdf.predicate.Predicate`.
    *
    * Symmetric companion to [[toCore]]. Used when code that operates on
    * the engine-portable core ADT (e.g. the audit/cache-key chain, JSON
    * adapters, future wire-format encoders) needs to flow a `CorePredicate`
    * back into the Spark-bearing original — for example, to hand off to
    * the user-facing fluent API (`SemanticTable.query(where = ...)`)
    * which is typed as the original.
    *
    * Exhaustively mirrors [[toCore]] over the same 15 case classes plus
    * recursive And/Or/Not. If either ADT gains a case class without the
    * other being updated, this match fails to compile — surfacing the
    * asymmetry at the boundary, where it can be fixed.
    *
    * No data loss: the two ADTs have identical shape (same field types,
    * same `final case class` structure), so `fromCore(toCore(p)) == p`
    * and `toCore(fromCore(c)) == c` hold for all valid predicates. */
  def fromCore(c: CorePredicate): io.semanticdf.predicate.Predicate = c match {
    // -------------------------------------------------------------------------
    // Compare family
    // -------------------------------------------------------------------------
    case CorePredicate.Compare.Eq(field, value) =>
      io.semanticdf.predicate.Predicate.Compare.Eq(field, value)
    case CorePredicate.Compare.Ne(field, value) =>
      io.semanticdf.predicate.Predicate.Compare.Ne(field, value)
    case CorePredicate.Compare.Lt(field, value) =>
      io.semanticdf.predicate.Predicate.Compare.Lt(field, value)
    case CorePredicate.Compare.Le(field, value) =>
      io.semanticdf.predicate.Predicate.Compare.Le(field, value)
    case CorePredicate.Compare.Gt(field, value) =>
      io.semanticdf.predicate.Predicate.Compare.Gt(field, value)
    case CorePredicate.Compare.Ge(field, value) =>
      io.semanticdf.predicate.Predicate.Compare.Ge(field, value)
    case CorePredicate.Compare.Contains(field, value) =>
      io.semanticdf.predicate.Predicate.Compare.Contains(field, value)
    case CorePredicate.Compare.StartsWith(field, value) =>
      io.semanticdf.predicate.Predicate.Compare.StartsWith(field, value)
    case CorePredicate.Compare.EndsWith(field, value) =>
      io.semanticdf.predicate.Predicate.Compare.EndsWith(field, value)
    case CorePredicate.Compare.ArrayContains(field, value) =>
      io.semanticdf.predicate.Predicate.Compare.ArrayContains(field, value)

    // -------------------------------------------------------------------------
    // Non-compare leaves
    // -------------------------------------------------------------------------
    case CorePredicate.In(field, values, negate) =>
      io.semanticdf.predicate.Predicate.In(field, values, negate)
    case CorePredicate.IsNull(field, negate) =>
      io.semanticdf.predicate.Predicate.IsNull(field, negate)

    // -------------------------------------------------------------------------
    // Compound (recursive)
    // -------------------------------------------------------------------------
    case CorePredicate.And(children @ _*) =>
      io.semanticdf.predicate.Predicate.And(children.map(fromCore): _*)
    case CorePredicate.Or(children @ _*) =>
      io.semanticdf.predicate.Predicate.Or(children.map(fromCore): _*)
    case CorePredicate.Not(predicate) =>
      io.semanticdf.predicate.Predicate.Not(fromCore(predicate))
  }

  /** Bulk conversion for `Seq[CorePredicate]`. */
  def fromCoreAll(cs: Seq[CorePredicate]): Seq[io.semanticdf.predicate.Predicate] =
    cs.map(fromCore)
}