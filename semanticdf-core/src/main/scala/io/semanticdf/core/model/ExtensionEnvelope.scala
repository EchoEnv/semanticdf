package io.semanticdf.core.model

/** Engine-portable extension-envelope ADT — Phase 2 contract.
  * Mirrors the design doc §4.4.1 "ExtensionEnvelope".
  *
  * An [[ExtensionEnvelope]] wraps a model's extensions: either
  * INLINE (the extensions fit within the 8 KiB + 16 fields limit)
  * or EXTERNAL (the extensions exceed the limit and are stored
  * as a content-addressed blob).
  *
  * ==Why a wrapper (vs. just `Map[String, ExtensionValue]`)==
  *
  * The wrapper distinguishes the two storage modes:
  *   - `inline` — the extensions are embedded in the model (no
  *     external fetch)
  *   - `external` — the extensions are stored as a
  *     [[ExternalExtensionBlob]] (the reader fetches at use site)
  *
  * Per the design's finding 14: "Larger payloads are fully
  * externalized as content-addressed blobs; catalog properties
  * retain digest, URI, length, and media type."
  *
  * ==Why `inline: Map[String, ExtensionValue]` is the common case==
  *
  * Most models have small extensions (a description string, a
  * couple of tags). The inline case covers these without an
  * external fetch. The validator (Group 3c) calls
  * [[ExtensionLimits.check]] to ensure the inline case stays
  * within limits.
  *
  * ==Why `external: Option[ExternalExtensionBlob]` is `Option`==
  *
  * A model without external extensions has `external = None`.
  * The wrapper's invariant is: at least one of `inline` (non-empty)
  * or `external` (Some) must be present. The validator enforces
  * this invariant.
  *
  * ==Why core (engine-portable)==
  *
  * The wrapper SHAPE (inline + external) is universal across
  * engines. The actual STORAGE (S3, database, file system) is
  * engine-specific — that's in the engine adapter.
  *
  * ==Data-driven mantra compliance==
  *
  * - Pure data: case class (no behavior)
  * - Equality auto-derived
  * - `Product with Serializable`
  *
  * ==Boundary contract==
  *
  * Zero Spark imports. Verifiable by:
  * `grep -r 'org.apache.spark' semanticdf-core/src/main/scala/io/semanticdf/core/model/ExtensionEnvelope.scala`
  */
final case class ExtensionEnvelope(
    inline:   Map[String, ExtensionValue]      = Map.empty,
    external: Option[ExternalExtensionBlob]    = None,
) extends Product with Serializable

object ExtensionEnvelope {

  /** Construct an envelope from inline extensions only (the common
    * case). The validator (Group 3c) checks that the inline map
    * fits within the [[ExtensionLimits]]. */
  def inlineOnly(values: Map[String, ExtensionValue]): ExtensionEnvelope =
    ExtensionEnvelope(inline = values, external = None)

  /** Construct an envelope from an external blob only. The reader
    * fetches the blob at use site, per the engine's network
    * policy. */
  def externalOnly(blob: ExternalExtensionBlob): ExtensionEnvelope =
    ExtensionEnvelope(inline = Map.empty, external = Some(blob))
}