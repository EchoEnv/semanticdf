package io.semanticdf.core.manifest

/** Engine-portable join declaration — YAML shape.
  *
  * Per the v0.3.2 design doc: portable reader intermediate. The
  * reader (3A.2) converts this to `core.model.JoinSpec`.
  *
  * ==Why `kind` is a String (not the typed `core.rel.JoinKind`)==
  *
  * Per the v0.3.2 design doc §6.3: v1 uses raw string values for
  * the kind (matches legacy YAML's `"one" | "many" | "cross"`
  * convention). The reader maps to the typed `JoinKind` ADT
  * via a small dispatcher. Mapping per the existing
  * `ModelBridge.toModel`:
  *
  *   "one"   -> JoinKind.Inner
  *   "many"  -> JoinKind.Inner
  *   "cross" -> JoinKind.Cross
  *
  * ==Data-driven mantra compliance==
  *
  * - Pure data: `final case class` (no behavior)
  * - `extends Product with Serializable`
  * - `Option[T]` with default `None` for optional fields */
final case class PortableJoin(
    name:        String,
    kind:        String,
    leftSource:  String,
    rightSource: String,
    keys:        List[String],
    description: Option[String] = None,
) extends Product with Serializable
