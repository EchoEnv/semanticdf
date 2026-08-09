package io.semanticdf.core.manifest

/** Engine-portable measure declaration — YAML shape.
  *
  * Per the v0.3.2 design doc: portable reader intermediate. The
  * reader (3A.2) converts this to `core.model.Measure` (which
  * has `expr: AggregateCall`, not a raw SQL string).
  *
  * ==Why `kind` is a string (not a typed ADT)==
  *
  * Per the v0.3.2 design doc §6.5: v1 uses raw SQL strings for
  * the aggregate expression. The `kind` field is informational
  * (for documentation and round-trip preservation). Future work
  * (post-v0.3.2) can convert this to a typed `AggregateFn` enum
  * if needed.
  *
  * ==Data-driven mantra compliance==
  *
  * - Pure data: `final case class` (no behavior)
  * - `extends Product with Serializable`
  * - `Option[T]` with default `None` for optional fields */
final case class PortableMeasure(
    name:        String,
    expr:        String,
    kind:        Option[String]    = None,
    description: Option[String]    = None,
) extends Product with Serializable
