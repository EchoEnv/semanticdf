package io.semanticdf.core.manifest

/** Engine-portable dimension declaration — YAML shape.
  *
  * Per the v0.3.2 design doc: this is the portable reader's
  * intermediate representation. The reader (3A.2) converts it
  * to `core.model.Dimension` (which has `expr: core.expr.Expr`,
  * not a raw SQL string).
  *
  * ==Why `expr: String` (not `core.expr.Expr`)==
  *
  * Per design doc §6.3: portable YAML holds raw SQL strings.
  * Mirrors the existing legacy YAML format's `expr: "..."` shape.
  *
  * ==Data-driven mantra compliance==
  *
  * - Pure data: `final case class` (no behavior)
  * - `extends Product with Serializable`
  * - Optional fields use `Option[T]` with default `None` */
final case class PortableDimension(
    name:        String,
    expr:        String,
    description: Option[String] = None,
) extends Product with Serializable
