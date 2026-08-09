package io.semanticdf.core.manifest

/** Engine-portable filter declaration — YAML shape.
  *
  * Per the v0.3.2 design doc: portable reader intermediate. The
  * reader (3A.2) converts this to `core.model.FilterSpec` (which
  * has `predicate: core.expr.Expr`, not a raw SQL string).
  *
  * ==Why `expr: String`==
  *
  * Per design doc §6.3: portable YAML holds raw SQL strings. The
  * reader converts via `PredicateToExprConverter` (which is the
  * existing converter from the v0.3.1 Predicate→Expr work, PR #418
  * / #419 / #420).
  *
  * ==Data-driven mantra compliance==
  *
  * - Pure data: `final case class` (no behavior)
  * - `extends Product with Serializable`
  * - `Option[T]` with default `None` for optional fields */
final case class PortableFilter(
    name:        String,
    expr:        String,
    description: Option[String] = None,
) extends Product with Serializable
