package io.semanticdf.core.model

/** Engine-portable external-extension-blob ADT — Phase 2 contract.
  * Mirrors the design doc §4.4.1 "ExternalExtensionBlob".
  *
  * An [[ExternalExtensionBlob]] describes a content-addressed
  * extension blob that's stored externally (e.g. in S3, in a
  * database, on a file system) rather than inline in the model.
  * Used when the inline envelope exceeds the limits (8 KiB or 16
  * fields — see [[ExtensionLimits]]).
  *
  * ==Why external blobs exist==
  *
  * Per the design: "Limits apply to canonical UTF-8 JSON; the 16
  * fields are counted recursively. Larger payloads are fully
  * externalized as content-addressed blobs; catalog properties
  * retain digest, URI, length, and media type."
  *
  * The portable model carries the METADATA (digest, URI, length,
  * media type) — the actual blob content lives elsewhere (read at
  * use site, never embedded in the model).
  *
  * ==Why `digest: String` (a content hash)==
  *
  * The digest is the canonical identity of the blob. The reader
  * verifies the digest matches the content (integrity check); if
  * not, the read fails. Per the design's finding 14 ("data is
  * never truncated"), the failure mode is "reject the publication",
  * not "silently truncate".
  *
  * ==Why `byteLength: Long`==
  *
  * The reader verifies the byte count matches the actual size
  * (size check); if not, the read fails. Same integrity guarantee
  * as the digest.
  *
  * ==Why `mediaType: String = "application/vnd.semanticdf.extensions+json"`==
  *
  * The default MIME type matches the design's convention. Callers
  * can override (e.g. for non-JSON extension payloads), but the
  * default is JSON.
  *
  * ==Why `uri: java.net.URI` (not `String`)==
  *
  * `URI` is a structured representation of the URI scheme/host/
  * path/fragment. The reader validates the scheme/host against an
  * allow-list (per the design's finding 14); `URI` makes the
  * allow-list check easier than parsing a `String`.
  *
  * ==Why core (engine-portable)==
  *
  * The blob METADATA (digest + URI + length + media type) is
  * universal across engines. The blob CONTENT (the actual JSON
  * payload) is fetched at use site, per the engine's network /
  * storage policy.
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
  * `grep -r 'org.apache.spark' semanticdf-core/src/main/scala/io/semanticdf/core/model/ExternalExtensionBlob.scala`
  */
final case class ExternalExtensionBlob(
    digest:    String,
    uri:       java.net.URI,
    byteLength: Long,
    mediaType: String = "application/vnd.semanticdf.extensions+json",
) extends Product with Serializable