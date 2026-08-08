package io.semanticdf.hera

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

/** v0.3.1: implements the engine-portable [[CatalogAdapter]] contract
  * against Hera's TableManage API.
  *
  * ==CAS mechanism (Hera-specific)==
  *
  * Per the UC/HMS adapter pattern (PRs #424 / #423): the CAS story
  * uses a "version field" carried in the table metadata. UC uses
  * `properties["semanticdf_version"]`; HMS uses `parameters["semanticdf_version"]`.
  * **Hera uses `optLock`** — the table's version field that increments
  * on each update (per `docs/api/tablemanage.md`).
  *
  * Per error-handling-style.md "Converter return types": we map the
  * engine-specific `optLock` (Long) into the engine-portable
  * `CatalogRef.version` (Int, per the v0.3.0 contract). The
  * `semanticdf_digest` slot is carried in `CatalogRef.digest` (String)
  * — same pattern as UC/HMS.
  *
  * ==Why the manifest content lives in `properties` / `parameters`==
  *
  * Same answer as UC/HMS: the manifest's `kind` / `digest` / `version`
  * metadata is carried in Hera's `optLock` (version) + a synthetic
  * digest derived from the doc content. The `ManifestDocument` type
  * is `Any` (per [[CatalogAdapter]] design — real type lands in PR 6
  * "Manifest v2 + dual reader").
  *
  * ==Error handling==
  *
  * Per `docs/design/error-handling-style.md`:
  *
  *   - All public methods return `Either[CatalogError, X]` (typed ADT).
  *   - The 3 publish modes (CreateOnly / Upsert / CompareAndSet) each
  *     have SPECIFIC failure paths:
  *     - CreateOnly on existing → `PublishResult.Conflict` (NOT throw).
  *     - CompareAndSet digest mismatch → `PublishResult.Conflict`
  *       with `current` ref.
  *     - CompareAndSet on absent → `PublishResult.Conflict` with no current.
  *   - For-comprehension for 3+ step operations (read state + decide +
  *     commit); `match` for 1-step.
  *
  * ==Boundary contract==
  *
  * Zero Spark imports. */
final class HeraCatalogAdapter(
    client:  HeraClient,
    val   catalog: String,
) extends CatalogAdapter {

  override def publish(
      identity: CatalogIdentity,
      doc:      Any,
      as:       CatalogEntity,
      mode:     PublishMode,
  ): Either[CatalogError, PublishResult] = {
    // The `ManifestDocument` is `Any` for v1 (per the v0.3.0
    // CatalogAdapter design). Use doc.toString as the synthetic
    // digest — the real digest lands with Manifest v2 (PR 6).
    // Per error-handling-style.md "Programmer error" rule: throw
    // IllegalArgumentException for impossible inputs.
    val newDigest = if (doc == null) "doc-placeholder" else doc.toString

    // realmId from the catalog name. For v1, the realm's id is
    // encoded in the catalog name as "realm_<id>". v0.4.0 will add
    // a richer catalog → realm resolver.
    val realmIdOpt = parseRealmId(catalog)
    if (realmIdOpt.isEmpty) {
      // Per the standard: programmer error at the boundary
      // (the catalog name is the wrong format). Caller bug.
      throw new IllegalArgumentException(
        s"HeraCatalogAdapter.publish: catalog name '$catalog' is not in 'realm_<id>' format"
      )
    }
    val realmId = realmIdOpt.get

    // Pre-flight: read current state. Per the chaining rule:
    // 1 step (the read), so we use `match` directly. The result
    // feeds a 3-step pattern match below (decide mode + commit),
    // which IS a candidate for for-comprehension, but the inner
    // pattern match is clearer inline.
    val currentResult: Either[CatalogError, Option[HeraTableMeta]] =
      client.getTableMeta(identity.name, realmId) match {
        case Right(meta) => Right(Some(meta))
        case Left(HeraClientError.NotFound(reason)) => Right(None)
        case Left(other) => Left(heraToCatalogError(other, "getTableMeta"))
      }

    currentResult.flatMap { currentMeta =>
      (currentMeta, mode) match {
        case (None, PublishMode.CreateOnly) =>
          createAndReturn(identity, newDigest, realmId)
        case (None, PublishMode.Upsert) =>
          createAndReturn(identity, newDigest, realmId)
        case (None, PublishMode.CompareAndSet(_)) =>
          // Per the CAS contract: nothing at identity → digest
          // can't match. Return Conflict with no current.
          Right(PublishResult.Conflict(reason = "no entity at identity"))
        case (Some(existing), PublishMode.CreateOnly) =>
          // Already exists → Conflict with current ref.
          Right(PublishResult.Conflict(
            reason  = "already exists",
            current = Some(metaToRef(identity, existing)),
          ))
        case (Some(existing), PublishMode.Upsert) =>
          val prevRef = metaToRef(identity, existing)
          commit(identity, newDigest, existing.optLock, realmId).map { newMeta =>
            val curRef = metaToRef(identity, newMeta).copy(version = (existing.optLock + 1).toInt)
            PublishResult.Updated(prevRef, curRef)
          }
        case (Some(existing), PublishMode.CompareAndSet(expectedDigest)) =>
          if (existing.optLock.toString == expectedDigest.toLongOption.map(_.toString).getOrElse("")) {
            // Note: per the v0.3.0 CatalogAdapter CAS contract,
            // `expectedDigest` is the entity digest (String), not
            // the optLock (Long). The comparison here is a placeholder
            // — a future PR will reconcile this with the real
            // manifest v2 digest format.
            // For v1, we treat optLock as the digest proxy.
            val prevRef = metaToRef(identity, existing)
            commit(identity, newDigest, existing.optLock, realmId).map { newMeta =>
              val curRef = metaToRef(identity, newMeta).copy(version = (existing.optLock + 1).toInt)
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
      realmId:  Long,
  ): Either[CatalogError, PublishResult] = {
    client.createTableFromSql(
      tableName = identity.name,
      dataType  = "non-time series",  // default; callers can override via a richer API later
      sql       = s"SELECT 1 AS semanticdf_marker WHERE '$digest' = '$digest'",
      realmId   = realmId,
    ).map { meta =>
      val ref = CatalogRef(
        catalog   = catalog,
        namespace = identity.namespace,
        name      = identity.name,
        version   = meta.optLock.toInt,
        digest    = digest,
      )
      PublishResult.Inserted(ref)
    }.left.map { err => heraToCatalogError(err, "createTableFromSql") }
  }

  /** Update the table's optLock for an in-place commit. */
  private def commit(
      identity:    CatalogIdentity,
      digest:      String,
      currentOptLock: Long,
      realmId:     Long,
  ): Either[CatalogError, HeraTableMeta] = {
    client.updateTableSource(
      tableName       = identity.name,
      path            = s"semanticdf://$digest",  // synthetic path encoding the digest
      expectedOptLock = currentOptLock,
      realmId         = realmId,
    ).left.map { err => heraToCatalogError(err, "updateTableSource") }
  }

  override def discover(
      ref: CatalogRef,
  ): Either[CatalogError, Option[CatalogAdapter#ManifestDocument]] = {
    val realmIdOpt = parseRealmId(catalog)
    if (realmIdOpt.isEmpty) {
      throw new IllegalArgumentException(
        s"HeraCatalogAdapter.discover: catalog name '$catalog' is not in 'realm_<id>' format"
      )
    }
    val realmId = realmIdOpt.get
    // Per error-handling-style.md: 1 step (the read) + nested match
    // on the result → use `match` directly (not for-comprehension).
    client.getTableMeta(ref.name, realmId) match {
      case Right(meta) =>
        // Per DE re-review N1: compare full ref to stored values.
        // Same-version + same-digest → exact match; otherwise stale.
        if (meta.optLock.toInt == ref.version && ref.digest == deriveDigest(ref.version)) {
          Right(Some(ref.digest))
        } else {
          Right(None)
        }
      case Left(HeraClientError.NotFound(reason)) =>
        // Not-found at the boundary → absent (Right(None) per the
        // ResolvedSource pattern).
        Right(None)
      case Left(other) =>
        Left(heraToCatalogError(other, "getTableMeta"))
    }
  }

  override def list(
      filter: CatalogFilter,
  ): Either[CatalogError, List[CatalogEntry]] = {
    val effectiveCatalog = filter.catalog.getOrElse(catalog)
    val realmIdOpt = parseRealmId(effectiveCatalog)
    if (realmIdOpt.isEmpty) {
      // No realm-id resolution possible → empty list (caller should
      // pass a catalog in 'realm_<id>' format).
      return Right(Nil)
    }
    val realmId = realmIdOpt.get
    val prefix = filter.namePrefix.getOrElse("")
    // Per the chaining rule: 2 sequential steps → match or flatMap.
    // Here we use flatMap to thread the error.
    // Per error-handling-style.md: 1-step read → use match.
    // The FakeHeraClient implements listTables from its in-memory
    // store; HttpHeraClient returns Nil (documented limitation).
    client.listTables(realmId, prefix) match {
      case Right(names) =>
        // Convert table names to CatalogEntry list. For v1 we
        // can't query each table's metadata (no batch endpoint),
        // so we synthesize a minimal entry per name. The version
        // is set to 0 to indicate "unknown" — callers that need
        // the real version should call getTableMeta individually.
        Right(names.map { name =>
          val ref = CatalogRef(
            catalog   = effectiveCatalog,
            namespace = "",  // v1: not exposed via listTables
            name      = name,
            version   = 0,
            digest    = "unknown",
          )
          CatalogEntry(ref, CatalogEntity.Model)
        })
      case Left(err) =>
        Left(heraToCatalogError(err, "listTables"))
    }
    // Per the standard, the v1 implementation returns Nil for list;
    // v0.4.0 will add a real list-tables endpoint via
    // RealmManage's `moduleZeusList` or a new TableManage list call.
  }

  // -- Helpers --

  /** Parse the realm id from a catalog name like "realm_42". Returns
    * `None` if the name doesn't match the expected format.
    *
    * Per error-handling-style.md: programmer errors (empty catalog
    * name) at the boundary throw `IllegalArgumentException`. Runtime
    * "wrong format" returns `None` (caller decides what to do). */
  private def parseRealmId(catalogName: String): Option[Long] = {
    if (catalogName.isEmpty) throw new IllegalArgumentException("HeraCatalogAdapter.parseRealmId: catalog must not be empty")
    val prefix = "realm_"
    if (catalogName.startsWith(prefix)) {
      catalogName.stripPrefix(prefix).toLongOption
    } else {
      None
    }
  }

  /** Convert a [[HeraTableMeta]] to a [[CatalogRef]] using the current
    * identity's namespace + the meta's optLock as the version. */
  private def metaToRef(
      identity: CatalogIdentity,
      meta:     HeraTableMeta,
  ): CatalogRef = CatalogRef(
    catalog   = catalog,
    namespace = identity.namespace,
    name      = identity.name,
    version   = meta.optLock.toInt,
    digest    = s"optLock:${meta.optLock}",  // synthetic; replaced in PR 6
  )

  /** Synthetic digest for v1 (will be replaced by the real digest
    * from Manifest v2 in PR 6). Per the v0.3.0 CatalogAdapter
    * contract: the digest is a String; the exact format is
    * implementation-defined for v1. */
  private def deriveDigest(version: Int): String = s"optLock:$version"

  /** Map a [[HeraClientError]] to a [[CatalogError]] case.
    *
    * Per error-handling-style.md "Hard bans": SPECIFIC failure modes,
    * not a generic `ServerError`. The mapping:
    *
    *   - `Unauthorized`      → `Unauthorized`
    *   - `Forbidden`         → `Unauthorized` (also auth-side)
    *   - `NoPermission`      → `Unauthorized` (same semantic)
    *   - `NotFound`          → `Conflict` (the catalog entry isn't
    *                            there for the call; consumers
    *                            should already have handled the
    *                            "absent" case via Option)
    *   - `AlreadyExists`     → `Conflict` (semantic: the entry
    *                            already exists — CreateOnly should
    *                            have caught this earlier, but if it
    *                            leaks through, treat as conflict)
    *   - `Conflict`          → `Conflict`
    *   - `QueryFailed`       → `Unsupported` (Hera query syntax issue,
    *                            not a catalog concern)
    *   - `EngineError`       → `Network` (engine broken)
    *   - `BadRequest`        → `MalformedManifest`
    *   - `NetworkError`      → `Network`
    *   - `MalformedResponse` → `MalformedManifest` */
  private def heraToCatalogError(
      err:    HeraClientError,
      action: String,
  ): CatalogError = err match {
    case HeraClientError.Unauthorized(r)        => CatalogError.Unauthorized(reason = s"$action: $r")
    case HeraClientError.Forbidden(r)           => CatalogError.Unauthorized(reason = s"$action: $r")
    case HeraClientError.NoPermission(r)        => CatalogError.Unauthorized(reason = s"$action: $r")
    case HeraClientError.NotFound(r)            => CatalogError.Conflict(reason = s"$action: $r")
    case HeraClientError.AlreadyExists(r)       => CatalogError.Conflict(reason = s"$action: $r")
    case HeraClientError.Conflict(r)            => CatalogError.Conflict(reason = s"$action: $r")
    case HeraClientError.QueryFailed(r)         => CatalogError.Unsupported(reason = s"$action: $r")
    case HeraClientError.EngineError(r)         => CatalogError.Network(reason = s"$action: $r")
    case HeraClientError.BadRequest(r)          => CatalogError.MalformedManifest(reason = s"$action: $r")
    case HeraClientError.NetworkError(r)        => CatalogError.Network(reason = s"$action: $r")
    case HeraClientError.MalformedResponse(r)   => CatalogError.MalformedManifest(reason = s"$action: $r")
  }
}

object HeraCatalogAdapter {

  /** Smart constructor. */
  def apply(
      client:  HeraClient,
      catalog: String,
  ): HeraCatalogAdapter = new HeraCatalogAdapter(client, catalog)
}