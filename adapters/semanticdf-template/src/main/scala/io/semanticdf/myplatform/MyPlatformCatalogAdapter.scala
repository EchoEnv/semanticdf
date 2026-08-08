package io.semanticdf.myplatform

import io.semanticdf.core.catalog.{
  CatalogAdapter,
  CatalogEntity,
  CatalogEntry,
  CatalogError,
  CatalogFilter,
  CatalogIdentity,
  CatalogRef,
  PublishMode,
  PublishResult,
}

/** Implements the engine-portable [[CatalogAdapter]] contract against
  * MyPlatform's `POST /api/tables` + `PATCH /api/tables/:id` API.
  *
  * Mirrors `io.semanticdf.hera.HeraCatalogAdapter` (PR #425) and
  * `io.semanticdf.hivemetastore.HiveMetastoreCatalogAdapter` (PR #423).
  *
  * ==CAS mechanism==
  *
  * MyPlatform tables carry a `version` field (the same pattern as
  * Hera's `optLock` and UC/HMS' `semanticdf_version`). We use it for
  * CompareAndSet:
  *
  *   1. Read current `version` from `getTableMeta`
  *   2. Compare against `expectedVersion`
  *   3. If match → call `updateTable` with `expectedVersion` in the
  *      body (server-side validates and returns `version + 1`)
  *   4. If mismatch → `Left(MyPlatformError.Conflict)` → mapped to
  *      `CatalogError.Conflict`
  *
  * ==Error handling==
  *
  * Per `docs/design/error-handling-style.md`:
  *
  *   - All public methods return `Either[CatalogError, X]` (typed ADT)
  *   - The 3 publish modes each have SPECIFIC failure paths
  *     (CreateOnly / Upsert / CompareAndSet) — NOT throw
  *   - Programmer errors (empty catalog name) at the boundary
  *     throw `IllegalArgumentException` (NOT `Either`)
  *
  * ==Boundary contract==
  *
  * Zero Spark imports. */
final class MyPlatformCatalogAdapter(
    client:  MyPlatformClient,
    val   catalog: String,
) extends CatalogAdapter {

  override def publish(
      identity: CatalogIdentity,
      doc:      Any,
      as:       CatalogEntity,
      mode:     PublishMode,
  ): Either[CatalogError, PublishResult] = {
    // The ManifestDocument is `Any` for v1 (per the v0.3.0
    // CatalogAdapter design). Use doc.toString as the synthetic
    // digest — the real digest lands with Manifest v2 (PR 6).
    val newDigest = if (doc == null) "doc-placeholder" else doc.toString

    // Per error-handling-style.md: programmer errors at boundary
    // throw IllegalArgumentException. The caller is misusing the
    // adapter; this is a setup bug.
    if (catalog.isEmpty) throw new IllegalArgumentException(
      "MyPlatformCatalogAdapter.publish: catalog name must not be empty"
    )

    // Per the chaining rule: 1 step (the pre-flight read), so
    // we use `match` directly. The result feeds a 3-step pattern
    // match below (decide mode + commit).
    val currentResult: Either[CatalogError, Option[MyPlatformTableMeta]] =
      client.getTableMeta(identity.name, catalog) match {
        case Right(meta) => Right(Some(meta))
        case Left(MyPlatformError.NotFound(_)) => Right(None)  // absent → Right(None), NOT error
        case Left(other) => Left(myPlatformToCatalogError(other))
      }

    currentResult.flatMap { currentMeta =>
      (currentMeta, mode) match {
        case (None, PublishMode.CreateOnly) =>
          createAndReturn(identity, newDigest)
        case (None, PublishMode.Upsert) =>
          createAndReturn(identity, newDigest)
        case (None, PublishMode.CompareAndSet(_)) =>
          // Per the CAS contract: nothing at identity → digest can't match.
          Right(PublishResult.Conflict(reason = "no entity at identity"))
        case (Some(existing), PublishMode.CreateOnly) =>
          // Already exists → Conflict with current ref.
          Right(PublishResult.Conflict(
            reason  = "already exists",
            current = Some(metaToRef(identity, existing)),
          ))
        case (Some(existing), PublishMode.Upsert) =>
          val prevRef = metaToRef(identity, existing)
          commit(identity, newDigest, existing.version).map { newMeta =>
            val curRef = metaToRef(identity, newMeta)
            PublishResult.Updated(prevRef, curRef)
          }
        case (Some(existing), PublishMode.CompareAndSet(expectedDigest)) =>
          if (existing.version.toString == expectedDigest) {
            val prevRef = metaToRef(identity, existing)
            commit(identity, newDigest, existing.version).map { newMeta =>
              val curRef = metaToRef(identity, newMeta)
              PublishResult.Updated(prevRef, curRef)
            }
          } else {
            Right(PublishResult.Conflict(
              reason  = "digest mismatch",
              current = Some(metaToRef(identity, existing)),
            ))
          }
      }
    }
  }

  /** Create a new table for a fresh identity. Returns `Inserted`. */
  private def createAndReturn(
      identity: CatalogIdentity,
      digest:   String,
  ): Either[CatalogError, PublishResult] = {
    val initialMeta = MyPlatformTableMeta(
      table = identity.name,
      realmId = catalog,
      version = 1L,
      active = true,
    )
    client.createTable(identity.name, catalog, initialMeta).map { meta =>
      val ref = CatalogRef(
        catalog   = catalog,
        namespace = identity.namespace,
        name      = identity.name,
        version   = meta.version.toInt,
        digest    = digest,
      )
      PublishResult.Inserted(ref)
    }.left.map(myPlatformToCatalogError)
  }

  /** Update the table's version for an in-place commit. */
  private def commit(
      identity:      CatalogIdentity,
      digest:        String,
      currentVersion: Long,
  ): Either[CatalogError, MyPlatformTableMeta] = {
    val updatedMeta = MyPlatformTableMeta(
      table = identity.name,
      realmId = catalog,
      version = currentVersion + 1L,
      active = true,
    )
    client.updateTable(identity.name, catalog, updatedMeta, currentVersion)
      .left.map(myPlatformToCatalogError)
  }

  override def discover(
      ref: CatalogRef,
  ): Either[CatalogError, Option[CatalogAdapter#ManifestDocument]] = {
    if (catalog.isEmpty) throw new IllegalArgumentException(
      "MyPlatformCatalogAdapter.discover: catalog name must not be empty"
    )
    // Per error-handling-style.md: 1 step (the read) → direct match.
    client.getTableMeta(ref.name, catalog) match {
      case Right(meta) =>
        // Per DE re-review N1: compare full ref to stored values.
        if (meta.version.toInt == ref.version) Right(Some(ref.digest))
        else Right(None)
      case Left(MyPlatformError.NotFound(_)) =>
        Right(None)  // absent → Right(None), NOT error
      case Left(other) =>
        Left(myPlatformToCatalogError(other))
    }
  }

  override def list(
      filter: CatalogFilter,
  ): Either[CatalogError, List[CatalogEntry]] = {
    val effectiveCatalog = filter.catalog.getOrElse(catalog)
    val prefix = filter.namePrefix.getOrElse("")
    // Per the chaining rule: 1-step read → use match directly.
    client.listTables(effectiveCatalog, prefix) match {
      case Right(names) =>
        // Convert table names to CatalogEntry list. Version is 0
        // to indicate "unknown" (no batch endpoint for metadata).
        Right(names.map { name =>
          val ref = CatalogRef(
            catalog   = effectiveCatalog,
            namespace = "",
            name      = name,
            version   = 0,
            digest    = "unknown",
          )
          CatalogEntry(ref, CatalogEntity.Model)
        })
      case Left(other) =>
        Left(myPlatformToCatalogError(other))
    }
  }

  // -- Helpers --

  /** Convert a [[MyPlatformTableMeta]] to a [[CatalogRef]]. */
  private def metaToRef(
      identity: CatalogIdentity,
      meta:     MyPlatformTableMeta,
  ): CatalogRef = CatalogRef(
    catalog   = catalog,
    namespace = identity.namespace,
    name      = identity.name,
    version   = meta.version.toInt,
    digest    = s"version:${meta.version}",
  )

  /** Map a [[MyPlatformError]] to a [[CatalogError]] case.
    *
    * Per error-handling-style.md "Hard bans": SPECIFIC failure modes,
    * not a generic `ServerError`. */
  private def myPlatformToCatalogError(err: MyPlatformError): CatalogError = err match {
    case MyPlatformError.Unauthorized(r)      => CatalogError.Unauthorized(reason = r)
    case MyPlatformError.Forbidden(r)         => CatalogError.Unauthorized(reason = r)
    case MyPlatformError.NotFound(r)          => CatalogError.Conflict(reason = r)
    case MyPlatformError.AlreadyExists(r)     => CatalogError.Conflict(reason = r)
    case MyPlatformError.Conflict(r)          => CatalogError.Conflict(reason = r)
    case MyPlatformError.BadRequest(r)        => CatalogError.MalformedManifest(reason = r)
    case MyPlatformError.NetworkError(r)      => CatalogError.Network(reason = r)
    case MyPlatformError.MalformedResponse(r) => CatalogError.MalformedManifest(reason = r)
  }
}

object MyPlatformCatalogAdapter {

  /** Smart constructor. */
  def apply(
      client:  MyPlatformClient,
      catalog: String,
  ): MyPlatformCatalogAdapter = new MyPlatformCatalogAdapter(client, catalog)
}