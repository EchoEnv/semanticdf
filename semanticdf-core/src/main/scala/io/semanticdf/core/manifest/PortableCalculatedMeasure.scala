package io.semanticdf.core.manifest

/** Engine-portable calculated-measure declaration — YAML shape.
  *
  * Per the v0.3.2 design doc: portable reader intermediate. The
  * reader (3A.2) converts this to `core.model.CalculatedMeasure`
  * (which has `expr: core.expr.Expr`, not a raw SQL string).
  *
  * ==Why `expr: String`==
  *
  * Per design doc §6.3: portable YAML holds raw SQL strings.
  * Mirrors the existing legacy YAML's `expr: "..."` shape.
  *
  * ==Data-driven mantra compliance==
  *
  * - Pure data: `final case class` (no behavior)
  * - `extends Product with Serializable`
  * - `Option[T]` with default `None` for optional fields */
final case class PortableCalculatedMeasure(
    name:        String,
    expr:        String,
    description: Option[String] = None,
) extends Product with Serializable
