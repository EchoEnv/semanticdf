package io.semanticdf.core.manifest

/** Engine-portable manifest status — YAML shape (sealed ADT).
  *
  * Per the v0.3.2 design doc §6.4: v1 uses the legacy wire
  * format string values (`"draft"`, `"published"`, `"deprecated"`).
  *
  * The reader (3A.2) converts to `core.model.ModelStatus` (which
  * is the canonical domain type). The portable type is the YAML
  * shape; the domain type is the validated shape.
  *
  * ==Why `parseStatus` lives here (not in the reader)==
  *
  * The status parse is a single string-to-typed-value mapping.
  * Putting it on the sealed trait's companion keeps the
  * conversion logic co-located with the type definition.
  * The reader just calls `PortableStatus.parse(s)` and matches
  * on the result.
  *
  * ==Why `String` round-trip via toString / parse==
  *
  * Per the v0.3.2 design doc §6.4: legacy YAML uses string
  * values. For round-trip preservation (YAML → portable →
  * YAML → portable), we need a stable string representation.
  * `toString` provides it; `parse` reads it back.
  *
  * ==Data-driven mantra compliance==
  *
  * - Pure data: sealed trait + 3 case objects
  * - Equality auto-derived (case objects are singletons)
  * - `extends Product with Serializable` */
sealed trait PortableStatus extends Product with Serializable {
  /** Wire-stable string form. Matches the legacy YAML format. */
  def asString: String = this match {
    case PortableStatus.Draft      => "draft"
    case PortableStatus.Published  => "published"
    case PortableStatus.Deprecated => "deprecated"
  }
}

object PortableStatus {
  case object Draft      extends PortableStatus
  case object Published  extends PortableStatus
  case object Deprecated extends PortableStatus

  /** Parse a legacy YAML string into a typed `PortableStatus`.
    *
    * Returns `None` if the string doesn't match any known status
    * (the reader surfaces this as a `ManifestError.UnknownStatus`). */
  def parse(s: String): Option[PortableStatus] = s.toLowerCase match {
    case "draft"      => Some(Draft)
    case "published"  => Some(Published)
    case "deprecated" => Some(Deprecated)
    case _            => None
  }
}
