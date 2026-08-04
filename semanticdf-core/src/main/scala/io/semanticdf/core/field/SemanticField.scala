package io.semanticdf.core.field

/** Engine-portable typed field references — Phase 1 increment 2.
  *
  * Mirrors `io.semanticdf.SemanticField` (the original). The original file is
  * already Spark-free — it only declares a sealed `FieldKind` ADT, sealed
  * typeclass witnesses (`SemanticField[T]`, `SemanticDimension[T]`,
  * `SemanticMeasure[T]`), and a value-class wrapper (`FieldRef[T]`).
  *
  * ==Why this exists==
  *
  * The downstream multi-engine design ([[`io.semanticdf.docs.design.multi-engine-design`]])
  * carves a `semanticdf-core` artifact off the existing library. The new
  * artifact's contract requires the typed field-reference typeclasses and the
  * `FieldRef` carrier to compile without Spark on the classpath. This file
  * establishes that contract in the `core.field` package while leaving the
  * original Spark-bearing API untouched. Consolidation is scheduled in
  * Phase 2 — until callers migrate, both packages coexist.
  *
  * ==Architecture (unchanged from the original)==
  *
  * Three layers:
  *
  *   1. [[FieldKind]] — sealed: Dimension or Measure. The only discriminator.
  *   2. [[SemanticField]] / [[SemanticDimension]] / [[SemanticMeasure]] —
  *      typeclasses. Each instance carries a `name` and a fixed `kind`.
  *      `SemanticDimension[T]` and `SemanticMeasure[T]` are subtypes that pin
  *      `kind` at construction time so the compiler can dispatch on them.
  *   3. [[FieldRef]] — value-class wrapper the user actually passes. Wraps
  *      the witness with T preserved on the static type. Implicit conversions
  *      turn `SemanticDimension[X]` / `SemanticMeasure[X]` into `FieldRef[X]`
  *      automatically.
  *
  * ==Compile-time guarantees (vs runtime typo-detection today)==
  *
  *   - passing a measure-typed ref to `groupByDimensions(...)` → compile error
  *     (the implicit `SemanticDimension[MeasureType]` does not exist).
  *   - passing a dimension-typed ref to `aggregateMeasures(...)` → compile error.
  *   - typos in field names are caught at the **declaration site** of the
  *     implicit val. Every use site references the typed handle, so a typo
  *     happens once instead of once-per-call-site.
  *
  * ==Runtime cost== — zero. The typed 1-arity overloads read `.name` from
  * the underlying witness and delegate to the existing string-based method.
  *
  * ==Data-driven mantra compliance==
  *
  * Every member of this file is data:
  *   - `FieldKind` is a sealed ADT with two case objects
  *   - `SemanticField[T]` is a sealed typeclass with two abstract methods
  *   - `SemanticDimension[T]` / `SemanticMeasure[T]` pin the discriminator
  *   - `FieldRef[T]` is a value class with no behavior
  *   - The implicit conversions wrap one type into another — pure data
  *
  * No `Map`-based dispatch. No `String` lookup tables. No behavior beyond
  * `name` retrieval and the typeclass `kind` pin. Per the
  * `scala-data-driven-refactor` mantra step 1, this is the boundary at which
  * "is this a dimension or a measure" is encoded as a type, not as a flag.
  */
sealed trait FieldKind
object FieldKind {
  /** A column you can groupBy / filter on. */
  case object Dimension extends FieldKind
  /** An aggregate-able measure. */
  case object Measure   extends FieldKind
}

/** The base typeclass. Every typed field reference is an instance of this. */
sealed trait SemanticField[T] {
  /** The underlying field name as it appears in dimensions/measures. */
  def name: String
  /** Whether this ref is a dimension or a measure — drives groupBy vs aggregate. */
  def kind: FieldKind
}

/** A field that's a dimension (you can groupBy / filter on it).
  *
  * Compile-time evidence for `groupByDimensions` overloads. */
trait SemanticDimension[T] extends SemanticField[T] {
  final def kind: FieldKind = FieldKind.Dimension
}

/** A field that's a measure (you can aggregate / having over it).
  *
  * Compile-time evidence for `aggregateMeasures` overloads. */
trait SemanticMeasure[T] extends SemanticField[T] {
  final def kind: FieldKind = FieldKind.Measure
}

/** Convenience builders for typed field refs.
  *
  * Most users only need these — register an implicit val once per field, then
  * reference it everywhere. */
object SemanticDimension {
  /** Build a typed dimension ref by name.
    *
    * The parameter is named `n` rather than `name` to avoid a forward-reference
    * inside the anonymous class: `override val name: String = name` resolves
    * the RHS `name` to the parameter (which shadows the val), but the val is
    * bound before the body so the forward reference is null at init time. */
  def of[T](n: String): SemanticDimension[T] = new SemanticDimension[T] {
    override val name: String = n
  }
}

object SemanticMeasure {
  /** Build a typed measure ref by name. See [[SemanticDimension.of]] on the
    * `n` parameter naming. */
  def of[T](n: String): SemanticMeasure[T] = new SemanticMeasure[T] {
    override val name: String = n
  }
}

/** Phantom-typed field-ref carrier for API call sites.
  *
  * The user holds a `SemanticDimension[X]` or `SemanticMeasure[X]` instance;
  * this value-class wrapper makes that witness passable as an argument whose
  * static type preserves X. Without it, Scala 2.13's type inference would
  * confuse the phantom parameter with the witness type and the implicit
  * lookup would search for `SemanticDimension[SemanticDimension[X]]`.
  *
  * Implicit conversions in [[FieldRef]]'s companion wrap the user's
  * `SemanticDimension`/`SemanticMeasure` instances transparently — call sites
  * pass the witness directly; this wrapper is held only for the duration of
  * the method call.
  *
  * Carries no behaviour of its own — the underlying witness's `name` and
  * `kind` are accessed via the typeclass implicit at the call site.
  */
final class FieldRef[T](val underlying: SemanticField[T]) extends AnyVal

object FieldRef {
  /** Auto-wrap a typed dimension ref when passed to a method expecting a `FieldRef`. */
  implicit def fromDimension[T](d: SemanticDimension[T]): FieldRef[T] = new FieldRef[T](d)

  /** Auto-wrap a typed measure ref when passed to a method expecting a `FieldRef`. */
  implicit def fromMeasure[T](m: SemanticMeasure[T]): FieldRef[T] = new FieldRef[T](m)
}