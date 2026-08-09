package io.semanticdf.core.manifest

/** Engine-portable rollup declaration — YAML shape (v1).
  *
  * Per the v0.3.2 design doc §6.5: v1 uses raw YAML for rollups.
  * The reader (3A.2) converts to `core.model.RollupSpec`.
  *
  * ==Why this shape (vs. the typed `core.model.RollupSpec`)==
  *
  * Rollup definitions have rich semantics (grain, freshness
  * specs, rollup measures). For v1, we capture the essential
  * shape (name + grain + measures) and let the reader fill in
  * defaults for freshness + execution policy. Future work
  * (post-v0.3.2) can extend this to a fully-typed shape.
  *
  * ==Data-driven mantra compliance==
  *
  * - Pure data: `final case class` (no behavior)
  * - `extends Product with Serializable`
  * - `Option[T]` with default `None` for optional fields */
final case class PortableRollup(
    name:     String,
    grain:    String,
    measures: List[String],
    description: Option[String] = None,
) extends Product with Serializable
