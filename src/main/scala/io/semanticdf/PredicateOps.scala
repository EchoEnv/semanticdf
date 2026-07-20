package io.semanticdf

/** Infix typed-predicate operators on typed field references.
  *
  * Companion to the typed-field-reference pattern (`SemanticField[T]` /
  * `SemanticDimension[T]` / `SemanticMeasure[T]`). Lets the user write
  * the natural form
  *
  * {{{
  *   flights.where(carrier === "AA")
  *   flights.where(pax > 500L)
  *   flights.where(carrier.isNotNull)
  * }}}
  *
  * instead of the verbose factory form
  *
  * {{{
  *   flights.where(Predicate.Eq(carrier, "AA"))
  *   flights.where(Predicate.Gt(pax, 500L))
  *   flights.where(Predicate.isNotNull(carrier))
  * }}}
  *
  * '''Compile-time safety.''' The implicit class accepts any
  * [[SemanticField]] (the parent of [[SemanticDimension]] and
  * [[SemanticMeasure]]). The methods use the field's name from the
  * typed witness — the same name the verbose form's factories use.
  * Passing a measure where a dimension is expected is a compile error
  * at the call site (different types).
  *
  * '''Runtime cost: minimal.''' The implicit class is a regular class
  * (not a value class — Scala 2.13 disallows a value class wrapping
  * a value class, and [[FieldRef]] is itself a value class). The
  * allocation cost is one small object per call site (the wrapper
  * holds just one ref). The predicate itself is allocated anyway,
  * so the net cost is one extra object per infix predicate. Negligible
  * for typical queries.
  *
  * '''No memory leak.''' Same as the underlying `Predicate` — the
  * returned predicate is held by the caller's `where(...)` chain
  * and discarded after query compilation.
  *
  * '''Backward compat.''' The verbose form (`Predicate.Eq(...)`,
  * `Predicate.Gt(...)`, etc.) continues to work unchanged. The
  * infix form is additive.
  *
  * To use, import once:
  *
  * {{{
  *   import io.semanticdf.PredicateOps._
  * }}}
  */
object PredicateOps {

  /** Infix predicate operators on any [[SemanticField]]. The implicit
    * conversion fires at the call site for any `SemanticField[T]`
    * (the parent type of `SemanticDimension` and `SemanticMeasure`),
    * so dimension refs and measure refs both get the same operator
    * set in a single implicit step. */
  implicit class FieldRefOps[T](val ref: SemanticField[T]) {

    /** `ref === value` — `Compare.Eq(name, value)`. */
    def ===(value: Any): Predicate = Predicate.Compare.Eq(ref.name, value)

    /** `ref =!= value` — `Compare.Ne(name, value)`. (No `!=` because
      * Scala reserves it for universal equality; `=!=` is the conventional
      * "not equal" operator in this style.) */
    def =!=(value: Any): Predicate = Predicate.Compare.Ne(ref.name, value)

    /** `ref > value` — `Compare.Gt(name, value)`. */
    def >(value: Any): Predicate = Predicate.Compare.Gt(ref.name, value)

    /** `ref >= value` — `Compare.Ge(name, value)`. */
    def >=(value: Any): Predicate = Predicate.Compare.Ge(ref.name, value)

    /** `ref < value` — `Compare.Lt(name, value)`. */
    def <(value: Any): Predicate = Predicate.Compare.Lt(ref.name, value)

    /** `ref <= value` — `Compare.Le(name, value)`. */
    def <=(value: Any): Predicate = Predicate.Compare.Le(ref.name, value)

    /** `ref.isNull` — `IsNull(name, negate = false)`. */
    def isNull: Predicate = new Predicate.IsNull(ref.name, negate = false)

    /** `ref.isNotNull` — `IsNull(name, negate = true)`. */
    def isNotNull: Predicate = new Predicate.IsNull(ref.name, negate = true)

    /** `ref contains value` — string substring search
      * (`Compare.Contains(name, value)` → `scope(name).contains(lit(value))`).
      * Note: the phantom `T` tag doesn't carry the column's value type,
      * so `pax contains "5"` compiles (pax is a measure) but fails at
      * runtime. The value parameter is `Any` to match the rest of the
      * infix surface. */
    def contains(value: Any): Predicate = Predicate.Compare.Contains(ref.name, value)

    /** `ref startsWith value` — string prefix
      * (`Compare.StartsWith(name, value)` → `scope(name).startsWith(lit(value))`). */
    def startsWith(value: Any): Predicate = Predicate.Compare.StartsWith(ref.name, value)

    /** `ref endsWith value` — string suffix
      * (`Compare.EndsWith(name, value)` → `scope(name).endsWith(lit(value))`). */
    def endsWith(value: Any): Predicate = Predicate.Compare.EndsWith(ref.name, value)

    /** `ref arrayContains value` — array membership
      * (`Compare.ArrayContains(name, value)` → `array_contains(scope(name), lit(value))`).
      * The column should be an array type; Spark's runtime catches type mismatches. */
    def arrayContains(value: Any): Predicate = Predicate.Compare.ArrayContains(ref.name, value)

    /** `ref isin values` — membership test
      * (`In(name, values.toSeq, negate = false)` → `scope(name).isin(values: _*)`).
      * Accepts any `Iterable` (Seq, List, Set, etc.) — converted to a
      * `Seq[Any]` for the underlying `Predicate.In` case class. */
    def isin(values: Iterable[Any]): Predicate = Predicate.In(ref.name, values.toSeq, negate = false)

    /** `ref notin values` — negated membership test
      * (`In(name, values.toSeq, negate = true)` → `!scope(name).isin(values: _*)`).
      * The natural counterpart of `isin`. */
    def notin(values: Iterable[Any]): Predicate = Predicate.In(ref.name, values.toSeq, negate = true)
  }
}
