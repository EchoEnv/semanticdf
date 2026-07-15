package io.semanticdf

/** Phantom-typed field references for compile-time safety.
  *
  * Replaces stringly-typed field references (`aggregate("foo")`, `groupBy("bar")`,
  * `Predicate.Compare("eq", "carrier", "AA")`) with type-driven handles the
  * compiler can check.
  *
  * ==Architecture==
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
  * The 5+ arity `...All` variants pay a small O(n) cost to kind-check the
  * sequence at runtime.
  *
  * ==Backwards compatible== — this typeclass is purely additive. The
  * string-based `groupBy(String*)` / `aggregate(String*)` /
  * `Predicate.Compare(...)` keep working unchanged.
  *
  * ==Usage==
  * {{{
  * // 1. Declare phantom tags per field:
  * object Flights {
  *   sealed trait Carrier
  *   sealed trait TotalPax
  *
  *   implicit val carrier: SemanticDimension[Carrier] =
  *     SemanticDimension.of[Carrier]("carrier")
  *   implicit val pax: SemanticMeasure[TotalPax] =
  *     SemanticMeasure.of[TotalPax]("total_passengers")
  * }
  *
  * // 2. Use:
  * import Flights._
  *
  * table
  *   .groupByDimensions(carrier)                          // type-checked: dimension
  *   .aggregateMeasures(pax)                                // type-checked: measure
  *   .where(Predicate.Gt(pax, 100))                        // type-checked: any field
  *
  * // Mixing a measure into groupBy is a compile error:
  * // .groupByDimensions(pax)
  * //   ^^ could not find implicit SemanticDimension[TotalPax]
  * }}}
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
  * Compile-time evidence for `groupByDimensions` overloads in [[SemanticTable]]. */
trait SemanticDimension[T] extends SemanticField[T] {
  final def kind: FieldKind = FieldKind.Dimension
}

/** A field that's a measure (you can aggregate / having over it).
  *
  * Compile-time evidence for `aggregateMeasures` overloads in [[SemanticTable]]. */
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
