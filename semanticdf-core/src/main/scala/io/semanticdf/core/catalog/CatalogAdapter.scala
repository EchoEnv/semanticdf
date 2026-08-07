package io.semanticdf.core.catalog

/** Engine-portable catalog-adapter contract \u2014 PR 10 of the v0.3.0
  * deferred-work triage.
  *
  * Mirrors the design \u00a75.3 spec. Every catalog adapter (Unity
  * Catalog, Hive Metastore, custom catalogs) implements this
  * trait. The trait defines the closed set of operations:
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
  * ==Why the typed [ManifestDocument] is `Any` for v1==
  *
  * The full [ManifestDocument] v2 spec is deferred to PR 6 (Manifest
  * v2 + dual reader). For PR 10, we declare the trait's CONTRACT
  * using `Any` as a placeholder; the actual type lands when PR 6
  * merges. The trait remains stable across the placeholder-to-real
  * transition (only the `publish` / `discover` signatures change).
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

  /** The catalog this adapter serves. Used for routing +
    * identification. */
  def catalog: String

  /** Publish an entity to the catalog. The publication mode
    * ([PublishMode]) determines how the call interacts with
    * existing publications of the same identity.
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
      doc: Any,                  // PR 6 placeholder: ManifestDocument v2
      as:  CatalogEntity,
      mode: PublishMode,
  ): Either[CatalogError, PublishResult]

  /** Discover an entity from the catalog by ref. Returns
    * `Right(None)` if the entity doesn't exist at the given
    * ref (NOT a 404 \u2014 a missing entity is a normal case).
    *
    * @return the entity if it exists, `None` if it doesn't,
    *         `Left(CatalogError)` if the operation failed. */
  def discover(
      ref: CatalogRef,
  ): Either[CatalogError, Option[Any]]  // PR 6 placeholder: Option[ManifestDocument]

  /** List entities matching the filter. Returns `Right(Nil)` if no
    * entities match.
    *
    * @return the matching entries (possibly empty), `Left(CatalogError)`
    *         if the operation failed. */
  def list(
      filter: CatalogFilter,
  ): Either[CatalogError, List[CatalogEntry]]
}