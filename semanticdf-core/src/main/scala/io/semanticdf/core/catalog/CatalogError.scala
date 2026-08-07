package io.semanticdf.core.catalog

/** Engine-portable catalog-error ADT \u2014 PR 10 of the v0.3.0
  * deferred-work triage.
  *
  * Mirrors the design \u00a75.3 spec verbatim. Every catalog adapter
  * returns one of these cases from `publish` / `discover` /
  * `list`. The closed ADT forces the consumer (MCP, CLI,
  * programmatic) to handle each case explicitly.
  *
  * ==Per-case semantics==
  *
  * - [Conflict]: the requested publish / discover / list
  *   operation was rejected because of a CAS mismatch, identity
  *   collision, or pre-condition failure. The adapter has NO
  *   visibility into the current state (e.g. permission
  *   denied or transport failure during lookup).
  * - [StaleConflict]: a CAS publish was rejected because the
  *   current digest doesn't match `expectedDigest`. `current` is
  *   the entity's current ref so the caller can retry.
  * - [Unauthorized]: the caller doesn't have permission for the
  *   requested operation. The adapter should not have surfaced
  *   any entity state to the caller.
  * - [Network]: the underlying transport (REST, Thrift, JDBC)
  *   failed. `reason` carries the underlying error message.
  * - [Unsupported]: the adapter doesn't support the requested
  *   operation (e.g. publish to a read-only catalog).
  * - [MalformedManifest]: the manifest document failed validation
  *   (missing fields, invalid types). `reason` carries the
  *   validation error.
  *
  * ==Why [Conflict] and [StaleConflict] are separate cases (H4)==
  *
  * Per the SWE review of PR #410: a CAS rejection always has a
  * `current` ref to report (that's the whole point of CAS). A
  * plain [Conflict] never has a `current` ref (otherwise it
  * would be a [StaleConflict]). Splitting the two cases:
  *   - forces the adapter to declare which kind of conflict it
  *     encountered
  *   - matches the [PublishResult.Inserted] / [Updated] / [Conflict]
  *     ADT style (per the SWE review H1)
  *   - eliminates the `Option` on `current`, making the contract
  *     more ergonomic at consumer sites
  *
  * ==Boundary contract==
  *
  * Zero Spark imports. Verifiable by:
  * `grep -r 'org.apache.spark' semanticdf-core/src/main/scala/io/semanticdf/core/catalog/CatalogError.scala`
  *
  * ==Data-driven mantra compliance==
  *
  * - Pure data (sealed trait + final case classes)
  * - Equality auto-derived
  * - `Product with Serializable`
  */
sealed trait CatalogError extends Product with Serializable

object CatalogError {

  /** The requested operation was rejected because of a
    * concurrency or identity conflict. The adapter has NO
    * visibility into the current state (e.g. permission
    * denied or transport failure during lookup). For CAS
    * rejections, see [StaleConflict]. */
  final case class Conflict(reason: String) extends CatalogError

  /** A CAS publish was rejected because the current digest
    * doesn't match `expectedDigest`. `current` is the entity's
    * current ref so the caller can retry from the new state. */
  final case class StaleConflict(
      reason:  String,
      current: CatalogRef,
  ) extends CatalogError

  /** The caller doesn't have permission for the requested
    * operation. The adapter should not have surfaced any
    * entity state to the caller. */
  final case class Unauthorized(reason: String) extends CatalogError

  /** The underlying transport (REST, Thrift, JDBC) failed.
    * `reason` carries the underlying error message. */
  final case class Network(reason: String) extends CatalogError

  /** The adapter doesn't support the requested operation
    * (e.g. publish to a read-only catalog, or a feature the
    * specific catalog implementation hasn't implemented). */
  final case class Unsupported(reason: String) extends CatalogError

  /** The manifest document failed validation (missing fields,
    * invalid types). `reason` carries the validation error. */
  final case class MalformedManifest(reason: String) extends CatalogError
}