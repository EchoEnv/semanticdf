package io.semanticdf.core.field

import io.semanticdf.core.predicate.{Predicate => CorePredicate}

/** Infix typed-predicate operators on typed field references —
  * Phase 1 increment 6: engine-portable variant.
  *
  * Companion to the typed-field-reference pattern
  * (`io.semanticdf.core.field.SemanticField[T]` /
  * `SemanticDimension[T]` / `SemanticMeasure[T]`). Lets the user write
  * the natural infix form, producing the engine-portable
  * `io.semanticdf.core.predicate.Predicate` ADT directly:
  *
  * {{{
  *   import io.semanticdf.core.field.FieldRefOps._
  *   import io.semanticdf.core.predicate.Predicate
  *
  *   val pred: Predicate = carrier === "AA"
  *   val pred2: Predicate = pax > 500L
  *   val pred3: Predicate = carrier.isNotNull
  * }}}
  *
  * ==Why this exists==
  *
  * The original `io.semanticdf.predicate.PredicateOps.FieldRefOps` (added in
  * an earlier phase) constructs the Spark-bearing `io.semanticdf.predicate.Predicate`
  * ADT. The core ADT was deliberately left without an operator surface
  * during the initial mirror (PR #339) — adding the core operators would
  * have forced a premature consolidation of the fluent API. Now that
  * PR #340-#342 have established the consolidation milestone (audit/cache
  * chain operating end-to-end on core), the core ADT can be completed with
  * its own infix operator surface without touching any existing code.
  *
  * ==Contract==
  *
  * Mirrors the original `PredicateOps.FieldRefOps` exactly except for the
  * ADT it produces:
  *
  *   original: `io.semanticdf.predicate.PredicateOps.FieldRefOps`
  *             → produces `io.semanticdf.predicate.Predicate` (Spark-bearing)
  *   new:      `io.semanticdf.core.field.FieldRefOps`
  *             → produces `io.semanticdf.core.predicate.Predicate` (engine-portable)
  *
  * The two implicit classes operate on DIFFERENT `SemanticField` types
  * (one in `io.semanticdf`, one in `io.semanticdf.core.field`), so they
  * never collide at a call site — Scala's implicit resolution picks the
  * one whose target type matches the imported `SemanticField`.
  *
  * ==Compile-time safety==
  *
  * Same as the original: the implicit class accepts any
  * [[SemanticField]] (the parent of [[SemanticDimension]] and
  * [[SemanticMeasure]]). Passing a measure where a dimension is
  * expected is a compile error at the call site (different types).
  *
  * ==Runtime cost: minimal==
  *
  * Same as the original: a regular class (not a value class — Scala 2.13
  * disallows a value class wrapping a value class, and [[FieldRef]] is
  * itself a value class). One small object allocated per call site (the
  * wrapper holds just one ref). The `CorePredicate` itself is allocated
  * anyway, so the net cost is one extra object per infix predicate.
  * Negligible for typical queries.
  *
  * ==No memory leak==
  *
  * Same as the original: the returned `CorePredicate` is held by the
  * caller's chain and discarded after query compilation. The implicit
  * class wrapper is GC'd along with the predicate.
  *
  * ==Zero user API change==
  *
  * This is a NEW, additive surface. The original `PredicateOps.FieldRefOps`
  * and all existing fluent-API code (which uses the original) continue to
  * work unchanged. Users opt into the core operators by importing this
  * object — no breaking change.
  *
  * To use:
  * {{{
  *   import io.semanticdf.core.field.FieldRefOps._
  * }}}
  */
object FieldRefOps {

  /** Infix predicate operators on any
    * [[io.semanticdf.core.field.SemanticField]] (the core typeclass).
    * The implicit conversion fires at the call site for any
    * `SemanticField[T]` (the parent type of `SemanticDimension` and
    * `SemanticMeasure`), so dimension refs and measure refs both get
    * the same operator set in a single implicit step.
    *
    * Produces engine-portable [[io.semanticdf.core.predicate.Predicate]]
    * — no Spark imports needed. */
  implicit class FieldRefOps[T](val ref: SemanticField[T]) {

    /** `ref === value` — `Compare.Eq(name, value)`. */
    def ===(value: Any): CorePredicate = CorePredicate.Compare.Eq(ref.name, value)

    /** `ref =!= value` — `Compare.Ne(name, value)`. (No `!=` because
      * Scala reserves it for universal equality; `=!=` is the conventional
      * "not equal" operator in this style.) */
    def =!=(value: Any): CorePredicate = CorePredicate.Compare.Ne(ref.name, value)

    /** `ref > value` — `Compare.Gt(name, value)`. */
    def >(value: Any): CorePredicate = CorePredicate.Compare.Gt(ref.name, value)

    /** `ref >= value` — `Compare.Ge(name, value)`. */
    def >=(value: Any): CorePredicate = CorePredicate.Compare.Ge(ref.name, value)

    /** `ref < value` — `Compare.Lt(name, value)`. */
    def <(value: Any): CorePredicate = CorePredicate.Compare.Lt(ref.name, value)

    /** `ref <= value` — `Compare.Le(name, value)`. */
    def <=(value: Any): CorePredicate = CorePredicate.Compare.Le(ref.name, value)

    /** `ref.isNull` — `IsNull(name, negate = false)`. */
    def isNull: CorePredicate = new CorePredicate.IsNull(ref.name, negate = false)

    /** `ref.isNotNull` — `IsNull(name, negate = true)`. */
    def isNotNull: CorePredicate = new CorePredicate.IsNull(ref.name, negate = true)

    /** `ref contains value` — `Compare.Contains(name, value)`.
      * Note: the phantom `T` tag doesn't carry the column's value type,
      * so `pax contains "5"` compiles (pax is a measure) but fails at
      * runtime in the engine's compile step. The value parameter is
      * `Any` to match the rest of the infix surface. */
    def contains(value: Any): CorePredicate = CorePredicate.Compare.Contains(ref.name, value)

    /** `ref startsWith value` — `Compare.StartsWith(name, value)`. */
    def startsWith(value: Any): CorePredicate = CorePredicate.Compare.StartsWith(ref.name, value)

    /** `ref endsWith value` — `Compare.EndsWith(name, value)`. */
    def endsWith(value: Any): CorePredicate = CorePredicate.Compare.EndsWith(ref.name, value)

    /** `ref arrayContains value` — `Compare.ArrayContains(name, value)`.
      * The column should be an array type; engine's compile step
      * catches type mismatches. */
    def arrayContains(value: Any): CorePredicate = CorePredicate.Compare.ArrayContains(ref.name, value)

    /** `ref isin values` — `In(name, values.toSeq, negate = false)`.
      * Accepts any `Iterable` (Seq, List, Set, etc.) — converted to a
      * `Seq[Any]` for the underlying `CorePredicate.In` case class. */
    def isin(values: Iterable[Any]): CorePredicate =
      CorePredicate.In(ref.name, values.toSeq, negate = false)

    /** `ref notin values` — `In(name, values.toSeq, negate = true)`. */
    def notin(values: Iterable[Any]): CorePredicate =
      CorePredicate.In(ref.name, values.toSeq, negate = true)
  }
}