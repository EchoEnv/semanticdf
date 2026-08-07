package io.semanticdf.core.catalog

/** Engine-portable catalog-adapter contract \u2014 added in v0.3.0.
  *
  * Mirrors design \u00a75.3. Every catalog adapter (Unity Catalog, Hive
  * Metastore, custom catalogs) implements this trait, which
  * defines the closed set of operations:
  *
  *   1. `publish`  \u2014 write an entity to the catalog (with mode)
  *   2. `discover` \u2014 read an entity from the catalog by ref
  *   3. `list`     \u2014 list entities matching a filter
  *
  * ==Why a trait (not a concrete HTTP / Thrift client)==
  *
  * Per scala-data-driven-refacer \u00a71: the BEHAVIOR (calling UC
  * REST, calling HMS Thrift, parsing responses) is adapter-
  * specific. The CONTRACT (the methods the consumer needs) is in
  * this trait \u2014 a small abstraction justified by testability
  * (consumers inject fakes for tests).
  *
  * ==Why the typed [ManifestDocument] is `Any` for v1 (NOT `Nothing`)==
  *
  * The full [ManifestDocument] v2 spec is deferred to v0.3.1
  * (Manifest v2 + dual reader). For now, the trait's CONTRACT uses
  * `Any` as a placeholder; the actual type lands when the manifest
  * v2 work merges.
  *
  * Why not `Nothing`? A trait with `def publish(doc: Nothing, ...)` is
  * implementable (`override def publish(doc: Nothing, ...) = ???`) but
  * UNCALLABLE: `Nothing` is uninhabited, so callers cannot construct a
  * value to pass. Tests would have to use `null` (rejected by `Nothing`),
  * `???` (throws immediately at the call site), or some trick like
  * `null.asInstanceOf[Nothing]` (unsound). `Any` is the lesser evil:
  * it permits any value at call sites, but the trait stays callable,
  * implementable, and the transition to the real `ManifestDocument` ADT
  * is mechanical (replace `Any` with the real type).
  *
  * ==Why `Either[CatalogError, T]` (vs. exceptions or `Try`)==
  *
  * The design uses `Either[CatalogError, T]` everywhere for
  * typed failures. Exceptions are reserved for exceptional
  * conditions (programmer error, OOM). `Try` doesn't carry
  * typed error info.
  *
  * ==Data-driven mantra compliance==
  *
  * - Pure contract (trait with abstract methods)
  * - `Serializable` for adapter registration across the wire
  * - Zero Spark imports (the contract is engine-portable; the
  *   implementation lives in adapters)
  *
  * ==Boundary contract==
  *
  * Zero Spark imports. Verifiable by:
  * `grep -r 'org.apache.spark' semanticdf-core/src/main/scala/io/semanticdf/core/catalog/CatalogAdapter.scala`
  */
trait CatalogAdapter extends Serializable {

  /** Engine-portable placeholder for the v2 manifest.
    *
    * Aliased to `Any` (not `Nothing`) for the reasons documented
    * in the trait header: `Nothing` is implementable but uncallable.
    * PR 6 (Manifest v2 + dual reader) will replace this with the
    * real `ManifestDocument` ADT. The transition is mechanical:
    * `type ManifestDocument = Any` → `type ManifestDocument = TheRealADT`. */
  type ManifestDocument = Any

  /** The catalog this adapter serves. Used for routing +
    * identification. */
  def catalog: String

  /** Publish an entity to the catalog. The publication mode
    * ([PublishMode]) determines how the call interacts with
    * existing publications of the same identity.
    *
    * Per the DE re-review of PR #410 (#4): the caller passes an
    * explicit [CatalogIdentity] (NOT derived from the doc) so
    * the target is always clear. For CAS, this means the
    * `expectedDigest` in [PublishMode.CompareAndSet] refers to
    * the entity at THIS identity.
    *
    * ==Atomicity contract (per DE re-review N5)==
    *
    * Implementations MUST be server-side-atomic per identity.
    * Specifically:
    * - Two concurrent `Upsert` calls on the same identity MUST NOT
    *   both succeed (one returns `Conflict`, the other `Inserted` /
    *   `Updated`).
    * - Two concurrent `CompareAndSet` calls on the same identity
    *   with the same `expectedDigest` MUST NOT both succeed
    *   (exactly one returns `Updated`; the other returns `Conflict`
    *   with the new `current` ref).
    * - The atomicity mechanism (CAS, OCC, server-side locks) is
    *   implementation-defined; the contract is on the OBSERVABLE
    *   result, not the mechanism.
    *
    * Returns:
    * - `Right(PublishResult.Inserted)` if the entity was created
    * - `Right(PublishResult.Updated)` if the entity was updated
    * - `Right(PublishResult.Conflict)` if the mode precondition
    *   was not met (e.g. CreateOnly on existing entity, CAS digest
    *   mismatch)
    * - `Left(CatalogError.Unauthorized)` if the caller doesn't have
    *   permission
    * - `Left(CatalogError.Network)` if the transport failed
    * - `Left(CatalogError.Unsupported)` if the adapter doesn't
    *   support the requested entity kind
    * - `Left(CatalogError.MalformedManifest)` if the manifest
    *   failed validation */
  def publish(
      identity: CatalogIdentity,
      doc:      CatalogAdapter#ManifestDocument,    // type alias for Any; the real ManifestDocument lands in v0.3.1
      as:       CatalogEntity,
      mode:     PublishMode,
  ): Either[CatalogError, PublishResult]

  /** Discover an entity from the catalog by ref. Returns
    * `Right(None)` if the entity doesn't exist at the given
    * ref (NOT a 404 — a missing entity is a normal case).
    *
    * Per the DE re-review of PR #410 (#3) + #1 (CRITICAL N1):
    * the lookup is keyed on `ref.identity` (same key `publish`
    * uses), then the FULL ref (version + digest) is compared
    * against the stored entry:
    * - exact match (stored == requested) → `Right(Some(doc))`
    * - same identity, different version/digest (stale) → `Right(None)`
    * - no entry at the identity (absent) → `Right(None)`
    *
    * Stale and absent both return `Right(None)`. If the caller
    * needs to distinguish (e.g. for "force refresh" logic), they
    * should issue a `list` with the identity as a filter and
    * read the current ref from the result.
    *
    * @return the entity if the full ref matches, `None` if it
    *         doesn't (stale or absent), `Left(CatalogError)`
    *         if the operation failed. */
  def discover(
      ref: CatalogRef,
  ): Either[CatalogError, Option[CatalogAdapter#ManifestDocument]]

  /** List entities matching the filter. Returns `Right(Nil)` if no
    * entities match.
    *
    * Per the DE re-review N4: implementations MUST return
    * results in a deterministic order (insertion order is
    * recommended for adapters with stable local state; the
    * production adapter is free to choose any consistent
    * order, but it MUST be the same order across calls given
    * the same state). This is required for stable pagination.
    *
    * @return the matching entries (possibly empty), `Left(CatalogError)`
    *         if the operation failed. */
  def list(
      filter: CatalogFilter,
  ): Either[CatalogError, List[CatalogEntry]]
}