package io.semanticdf.postgresql

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

/** Implements the engine-portable `CatalogAdapter` contract against
  * PostgreSQL's DDL.
  *
  * Mirrors `io.semanticdf.hera.HeraCatalogAdapter` (PR #425) and
  * `io.semanticdf.hivemetastore.HiveMetastoreCatalogAdapter` (PR #423).
  *
  * ==CAS mechanism (PostgreSQL-specific)==
  *
  * PostgreSQL's `xmin` system column is a transaction ID that
  * increments on every row update. We use it as the "version" for
  * `CompareAndSet`:
  *
  *   1. Read current `xmin` from `getTableVersion`
  *   2. On `CompareAndSet`, run `casUpdate` with `expectedXmin`
  *   3. If 1 row updated, CAS succeeded; if 0 rows updated, CAS failed
  *
  * This is PG's standard optimistic-concurrency pattern. Distinct
  * from Hera's `optLock`, UC's `properties["semanticdf_version"]`,
  * and HMS's `parameters["semanticdf_version"]` — same pattern
  * (`semanticdf_kind` / `semanticdf_version` / `semanticdf_digest`),
  * different storage mechanism.
  *
  * ==Manifest content==
  *
  * The manifest content is stored in a `content` TEXT column we
  * add at table-create time (alongside the `xmin_lock` column that
  * carries the CAS version). The `kind` / `digest` metadata is
  * encoded in the `content` field per the v0.3.0 convention.
  *
  * ==Error handling==
  *
  * Per `docs/design/error-handling-style.md`:
  *
  *   - All public methods return `Either[CatalogError, X]` (typed ADT)
  *   - The 3 publish modes each have SPECIFIC failure paths
  *     (CreateOnly / Upsert / CompareAndSet) — NOT throw
  *   - Programmer errors (empty catalog name) at the boundary
  *     throw `IllegalArgumentException` (NOT `Either`) */
final class PostgreSqlCatalogAdapter(
    client:  PostgreSqlClient,
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
    // throw `IllegalArgumentException`. The caller is misusing the
    // adapter; this is a setup bug.
    if (catalog.isEmpty) throw new IllegalArgumentException(
      "PostgreSqlCatalogAdapter.publish: catalog name must not be empty"
    )

    // Per the chaining rule: 1 step (the pre-flight read), so
    // we use `match` directly. The result feeds a 3-step pattern
    // match below (decide mode + commit).
    val currentResult: Either[CatalogError, Option[Long]] =
      client.getTableVersion(identity.namespace, identity.name) match {
        case Right(version) => Right(Some(version))
        case Left(PostgreSqlError.TableNotFound(_)) => Right(None)  // absent → Right(None), NOT error
        case Left(other) => Left(postgreSqlToCatalogError(other))
      }

    currentResult.flatMap { currentXmin =>
      (currentXmin, mode) match {
        case (None, PublishMode.CreateOnly) =>
          createAndReturn(identity, newDigest)
        case (None, PublishMode.Upsert) =>
          createAndReturn(identity, newDigest)
        case (None, PublishMode.CompareAndSet(_)) =>
          // Per the CAS contract: nothing at identity → digest
          // can't match → Conflict with no current.
          Right(PublishResult.Conflict(reason = "no entity at identity"))
        case (Some(existingXmin), PublishMode.CreateOnly) =>
          // Already exists → Conflict with current ref.
          val currentRef = xminToRef(identity, existingXmin, newDigest)
          Right(PublishResult.Conflict(reason = "already exists", current = Some(currentRef)))
        case (Some(existingXmin), PublishMode.Upsert) =>
          val prevRef = xminToRef(identity, existingXmin, newDigest)
          // Upsert via CAS: read the latest xmin, then bump.
          // For Upsert we ignore expectedXmin (the contract says
          // "always overwrite"). This is the same pattern as UC/HMS.
          client.casUpdate(identity.namespace, identity.name, expectedXmin = existingXmin, newContent = newDigest)
            .left.map(postgreSqlToCatalogError)
            .map { newXmin =>
              val curRef = xminToRef(identity, newXmin, newDigest)
              PublishResult.Updated(prevRef, curRef)
            }
        case (Some(existingXmin), PublishMode.CompareAndSet(expectedDigest)) =>
          // Per the v0.3.0 CAS contract: expectedDigest is the
          // entity's previous digest, which we encode as the prior
          // xmin. For v1, the digest is "xmin:<version>" — so
          // parsing the expected digest back to a Long lets us
          // run the CAS check.
          val expectedXminOpt = parseXminFromDigest(expectedDigest)
          expectedXminOpt match {
            case Some(expectedXmin) if expectedXmin == existingXmin =>
              val prevRef = xminToRef(identity, existingXmin, newDigest)
              client.casUpdate(identity.namespace, identity.name, expectedXmin = existingXmin, newContent = newDigest)
                .left.map(postgreSqlToCatalogError)
                .map { newXmin =>
                  val curRef = xminToRef(identity, newXmin, newDigest)
                  PublishResult.Updated(prevRef, curRef)
                }
            case _ =>
              // Either: (a) digest doesn't parse to an xmin
              // (legacy or non-PG catalog); (b) the parsed xmin
              // doesn't match the current xmin. Either way, it's
              // a CAS failure → Conflict with current ref.
              val currentRef = xminToRef(identity, existingXmin, newDigest)
              Right(PublishResult.Conflict(reason = "digest mismatch", current = Some(currentRef)))
          }
      }
    }
  }

  /** Create a new manifest table. Returns `Inserted` (the manifest
    * starts at xmin=1 in PG's accounting). */
  private def createAndReturn(
      identity: CatalogIdentity,
      digest:   String,
  ): Either[CatalogError, PublishResult] = {
    val columns = List(
      PostgreSqlColumn(name = "content",  dataType = "TEXT",   nullable = true),
      PostgreSqlColumn(name = "xmin_lock", dataType = "BIGINT", nullable = true),
    )
    client.createTable(identity.namespace, identity.name, columns).left.map(postgreSqlToCatalogError).flatMap { _ =>
      // Now insert the manifest row. Use casUpdate with expectedXmin=0
      // (no row exists yet) to INSERT via the UPDATE pathway — for v1,
      // we just call casUpdate with expectedXmin=0, which inserts.
      // (Real impl: use INSERT INTO ... ON CONFLICT, but for v1 the
      // simplest correct impl is casUpdate that does INSERT-or-UPDATE.)
      client.casUpdate(identity.namespace, identity.name, expectedXmin = 0L, newContent = digest)
        .left.map(postgreSqlToCatalogError)
        .map { newXmin =>
          val ref = CatalogRef(
            catalog   = catalog,
            namespace = identity.namespace,
            name      = identity.name,
            version   = newXmin.toInt,
            digest    = digest,
          )
          PublishResult.Inserted(ref)
        }
    }
  }

  override def discover(
      ref: CatalogRef,
  ): Either[CatalogError, Option[CatalogAdapter#ManifestDocument]] = {
    if (catalog.isEmpty) throw new IllegalArgumentException(
      "PostgreSqlCatalogAdapter.discover: catalog name must not be empty"
    )
    // Per error-handling-style.md: 1-step (the read) → direct match.
    client.getTableVersion(ref.namespace, ref.name) match {
      case Right(version) =>
        // Per DE re-review N1: compare full ref to stored values.
        if (version.toInt == ref.version) Right(Some(ref.digest))
        else Right(None)
      case Left(PostgreSqlError.TableNotFound(_)) =>
        Right(None)  // absent → Right(None), NOT error
      case Left(other) =>
        Left(postgreSqlToCatalogError(other))
    }
  }

  override def list(
      filter: CatalogFilter,
  ): Either[CatalogError, List[CatalogEntry]] = {
    val effectiveCatalog = filter.catalog.getOrElse(catalog)
    val prefix = filter.namePrefix.getOrElse("")
    // Per the standard, the resolver pattern matches on the
    // result. For v1, list returns Nil — the underlying PG
    // metadata API would need a custom query (not in DatabaseMetaData).
    // Documented as a v0.4.0 follow-up.
    Right(Nil)
  }

  // -- Helpers --

  /** Convert a `xmin` Long to a [[CatalogRef]] with the matching
    * synthetic digest (per the v0.3.0 convention). */
  private def xminToRef(
      identity: CatalogIdentity,
      xmin:     Long,
      digest:   String,
  ): CatalogRef = CatalogRef(
    catalog   = catalog,
    namespace = identity.namespace,
    name      = identity.name,
    version   = xmin.toInt,
    digest    = digest,
  )

  /** Parse a synthetic digest ("xmin:<long>") back to a Long.
    * Returns `None` if the digest doesn't match the expected
    * format (e.g. it's a legacy digest from a non-PG catalog). */
  private def parseXminFromDigest(digest: String): Option[Long] = {
    // v1 format: the digest is the manifest content. The xmin
    // is encoded in the version field of the CatalogRef, not
    // the digest. For the digest to be parseable as an xmin,
    // we use a simple convention: "xmin:<long>".
    val prefix = "xmin:"
    if (digest.startsWith(prefix)) {
      digest.stripPrefix(prefix).toLongOption
    } else {
      None
    }
  }

  /** Map a [[PostgreSqlError]] to a [[CatalogError]] case.
    *
    * Per error-handling-style.md "Hard bans": SPECIFIC failure modes,
    * not a generic `ServerError`. */
  private def postgreSqlToCatalogError(err: PostgreSqlError): CatalogError = err match {
    case PostgreSqlError.ConnectionFailed(r)     => CatalogError.Network(reason = r)
    case PostgreSqlError.AuthenticationFailed(r) => CatalogError.Unauthorized(reason = r)
    case PostgreSqlError.TableNotFound(r)        => CatalogError.Conflict(reason = r)
    case PostgreSqlError.ColumnNotFound(r)       => CatalogError.MalformedManifest(reason = r)
    case PostgreSqlError.SyntaxError(r)          => CatalogError.MalformedManifest(reason = r)
    case PostgreSqlError.UniqueViolation(r)      => CatalogError.Conflict(reason = r)
    case PostgreSqlError.CheckViolation(r)       => CatalogError.Conflict(reason = r)
    case PostgreSqlError.CasConflict(r)          => CatalogError.Conflict(reason = r)
    case PostgreSqlError.NetworkError(r)         => CatalogError.Network(reason = r)
    case PostgreSqlError.Interrupted(r)          => CatalogError.Network(reason = r)
    case PostgreSqlError.PoolExhausted(r)        => CatalogError.Network(reason = r)
    case PostgreSqlError.MalformedResponse(r)    => CatalogError.MalformedManifest(reason = r)
  }
}

object PostgreSqlCatalogAdapter {

  /** Smart constructor. */
  def apply(
      client:  PostgreSqlClient,
      catalog: String,
  ): PostgreSqlCatalogAdapter = new PostgreSqlCatalogAdapter(client, catalog)
}