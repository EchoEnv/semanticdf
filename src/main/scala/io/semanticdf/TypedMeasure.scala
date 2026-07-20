package io.semanticdf

import org.apache.spark.sql.Column

/** Generic typed measure support (Phase E2/E3, see `docs/phase-E-plan.md`).
  *
  * Lets the user write a typed measure via `Measure[T](name, fn)`. The phantom
  * `T` is the static return type of the lambda. The typed form composes
  * with [[TypedArithmetic]] (which has its own compile-time checks on the
  * arithmetic ops).
  *
  * {{{
  *   import io.semanticdf.TypedArithmetic.divide
  *
  *   Measure[Double]("avg_passengers", t =>
  *     divide[Long, Long, Double](t("total_passengers"), t("flight_count")))
  * }}}
  *
  * The `Measure` API itself stays non-generic — `Measure.apply[T]` returns
  * the untyped `Measure`, which works with every existing call site. The
  * type parameter is purely a compile-time check on the lambda body; at
  * runtime, T is erased and the typed lambda is lowered to a plain
  * `SemanticScope => Column`. No public API change. No binary-incompatible
  * change. No reflection.
  *
  * '''Runtime cost: zero on the hot path.''' [[TypedColumn]] is a value
  * class (`extends AnyVal`); in the same compilation unit, instances are
  * unboxed to plain `Column` references. Across the library boundary, one
  * small allocation per typed value (~16 bytes). For a typical typed
  * measure with 3 arithmetic ops, that's ~3 boxings per query — well
  * within "low overhead".
  *
  * '''No memory leak.''' The internal [[TypedScopeAdapter]] is created
  * fresh at the start of each lambda evaluation and discarded at the
  * end. The `TypedColumn` wrappers are local to the lambda body. Nothing
  * is retained past the enclosing measure's evaluation. */

/** A phantom-typed wrapper around a Spark [[Column]]. The phantom T encodes
  * the static type the user asserts about the column. T is erased at
  * runtime.
  *
  * Value class — zero allocation in the same compilation unit; one
  * boxing per value when crossing the library boundary. The boxing
  * allocates one object holding the underlying Column reference. The
  * GC pressure is negligible. */
final class TypedColumn[T] private[semanticdf] (val column: Column) extends AnyVal {
  override def toString: String = s"TypedColumn($column)"
}

object TypedColumn {
  /** Implicit conversion from [[TypedColumn]] to plain [[Column]]. Used by
    * [[TypedArithmetic]] functions that take plain `Column` args. The
    * conversion is a field access — zero runtime cost (one `getfield`
    * bytecode). */
  implicit def toColumn[T](tc: TypedColumn[T]): Column = tc.column
}

/** Typed equivalent of [[SemanticScope]]. The `t` parameter in a typed
  * measure lambda is a [[TypedSemanticScope]]. Each `t(name)` call returns
  * a [[TypedColumn]] carrying the user-declared static type of the field. */
trait TypedSemanticScope {
  /** Type-asserting field access. The user declares T (the static type
    * of the column being read). T is phantom; there is no runtime check. */
  def apply[T](name: String): TypedColumn[T]

  /** Type-asserting percent-of-total reference. */
  def all[T](name: String): TypedColumn[T]
}

/** Internal: bridges the untyped [[SemanticScope]] to [[TypedSemanticScope]].
  * Created fresh per lambda evaluation. The adapter holds only a reference
  * to the underlying scope; nothing else. */
private[semanticdf] final class TypedScopeAdapter(underlying: SemanticScope) extends TypedSemanticScope {
  override def apply[T](name: String): TypedColumn[T] = new TypedColumn[T](underlying(name))
  override def all[T](name: String): TypedColumn[T] = new TypedColumn[T](underlying.all(name))
}

/** Generic case class for typed measures. The phantom T is the static
  * return type of the lambda. At runtime, T is erased; the typed lambda
  * is bridged to the untyped `SemanticScope => Column` form via
  * [[toMeasure]]. */
final case class TypedMeasure[T](
    name: String,
    expr: TypedSemanticScope => TypedColumn[T],
    description: Option[String] = None,
    metadata: Map[String, String] = Map.empty,
    exprString: Option[String] = None,
) {
  /** Lower to the untyped [[Measure]] so existing code paths work
    * unchanged. The typed lambda is invoked once per scope evaluation;
    * the resulting `TypedColumn[T]` is unwrapped to its underlying
    * `Column` at the boundary. */
  private[semanticdf] def toMeasure: Measure = {
    val typed = expr
    Measure(
      name = name,
      expr = (scope: SemanticScope) => typed(new TypedScopeAdapter(scope)).column,
      description = description,
      metadata = metadata,
      exprString = exprString,
    )
  }
}
