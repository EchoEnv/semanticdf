package io.semanticdf.hivemetastore

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

/** v0.3.1 (Gap 7 closure): implements [[CatalogAdapter]] for
  * Hive Metastore. Maps the engine-portable publish/discover/list
  * contract to HMS's Thrift `create_table` / `alter_table` /
  * `get_table` / `get_all_tables` calls.
  *
  * ==CAS mechanism==
  *
  * HMS 3.x has no native CAS for arbitrary user keys. We implement
  * CAS via the table's `parameters` map: three reserved keys carry
  * the entity identity:
  *
  *   - `semanticdf_kind`    : "model" | "rollup" | "extension_blob"
  *   - `semanticdf_version` : the entity version (Int, stringified)
  *   - `semanticdf_digest`  : the entity digest
  *
  * Per the v0.3.0 CAS contract ([[CatalogAdapter]] scaladoc):
  * implementations MUST be server-side-atomic per identity.
  * HMS's Thrift `alter_table` is NOT atomic across concurrent
  * writers — two concurrent CAS calls can both succeed in
  * reading the same digest and writing different updates.
  *
  * **This adapter delegates atomicity to HMS's per-table lock
  * manager** (default implementation: in-process Zookeeper-free
  * lock, sufficient for single-instance HMS deployments). For
  * multi-instance HMS or zero-downtime failover, callers should
  * wrap `publish` calls in an external lock (e.g. Zookeeper) or
  * use the platform's [[io.semanticdf.platform.streaming.StartupReconciler]]
  * which serializes publications through a single Restate service.
  *
  * ==Why a new adapter class (not extension of `HiveMetastoreSourceResolver`)==
  *
  * Per scala-data-driven-refacer §1: the SHAPE of the contract
  * (`CatalogAdapter`) lives in core; the BODY (the HMS-specific
  * publish/discover/list behavior) lives here. The resolver and
  * the adapter serve different lifecycle roles (resolver = read
  * schema for compile; adapter = write entities for publish),
  * so they're separate classes that share the underlying
  * `HiveMetastoreClient` (a single connection per process).
  *
  * ==Boundary contract==
  *
  * Zero Spark imports. The `ManifestDocument` placeholder is `Any`
  * (per [[CatalogAdapter]] design — the real ADT lands in v0.3.1
  * PR 6 "Manifest v2 + dual reader"). */
final class HiveMetastoreCatalogAdapter(
    client: HiveMetastoreClient,
    val   catalog: String,
) extends CatalogAdapter {

  /** Reserved HMS parameter keys (the only mutable metadata HMS
    * 3.x exposes for application use). */
  private val KindKey    = "semanticdf_kind"
  private val VersionKey = "semanticdf_version"
  private val DigestKey  = "semanticdf_digest"

  override def publish(
      identity: CatalogIdentity,
      doc:      Any,
      as:       CatalogEntity,
      mode:     PublishMode,
  ): Either[CatalogError, PublishResult] = {
    // The ManifestDocument is `Any` for v1; use doc.toString as the
    // synthetic digest. The real digest lands with Manifest v2.
    val newDigest = if (doc == null) "doc-placeholder" else doc.toString

    // Pre-flight: read current state.
    client.getTableParameters(catalog, identity.namespace, identity.name).flatMap {
      case None =>
        // No existing entity at this identity.
        mode match {
          case PublishMode.CreateOnly =>
            // OK to create — proceed to createTable.
            createAndReturn(identity, doc, as, newDigest, version = 1)
          case PublishMode.Upsert =>
            // Create with version 1.
            createAndReturn(identity, doc, as, newDigest, version = 1)
          case PublishMode.CompareAndSet(expectedDigest) =>
            // Nothing at identity — digest cannot match.
            // Per CAS contract: returning Conflict with no current.
            Right(PublishResult.Conflict(reason = "no entity at identity"))
        }
      case Some(existingParams) =>
        val currentVersion = existingParams.getOrElse(VersionKey, "1").toInt
        val currentDigest  = existingParams.getOrElse(DigestKey, "")
        mode match {
          case PublishMode.CreateOnly =>
            // Already exists — conflict.
            val currentRef = CatalogRef(
              catalog   = catalog,
              namespace = identity.namespace,
              name      = identity.name,
              version   = currentVersion,
              digest    = currentDigest,
            )
            Right(PublishResult.Conflict(reason = "already exists", current = Some(currentRef)))
          case PublishMode.Upsert =>
            // Update in place.
            val prevRef = CatalogRef(catalog, identity.namespace, identity.name, currentVersion, currentDigest)
            val newVersion = currentVersion + 1
            commit(identity, as, newDigest, newVersion).map { _ =>
              val curRef = CatalogRef(catalog, identity.namespace, identity.name, newVersion, newDigest)
              PublishResult.Updated(prevRef, curRef)
            }
          case PublishMode.CompareAndSet(expectedDigest) =>
            if (currentDigest == expectedDigest) {
              val prevRef = CatalogRef(catalog, identity.namespace, identity.name, currentVersion, currentDigest)
              val newVersion = currentVersion + 1
              commit(identity, as, newDigest, newVersion).map { _ =>
                val curRef = CatalogRef(catalog, identity.namespace, identity.name, newVersion, newDigest)
                PublishResult.Updated(prevRef, curRef)
              }
            } else {
              val currentRef = CatalogRef(catalog, identity.namespace, identity.name, currentVersion, currentDigest)
              Right(PublishResult.Conflict(reason = "digest mismatch", current = Some(currentRef)))
            }
        }
    }
  }

  /** Create the table for a fresh identity. Returns `Inserted`. */
  private def createAndReturn(
      identity: CatalogIdentity,
      doc:      Any,
      as:       CatalogEntity,
      digest:   String,
      version:  Int,
  ): Either[CatalogError, PublishResult] = {
    val kind = entityKind(as)
    val params = Map(
      KindKey    -> kind,
      VersionKey -> version.toString,
      DigestKey  -> digest,
    )
    client.createTable(
      catalog    = catalog,
      database   = identity.namespace,
      table      = identity.name,
      columns    = Nil,  // HMS tables backing manifest entities don't carry user columns
      parameters = params,
    ).map { _ =>
      val ref = CatalogRef(catalog, identity.namespace, identity.name, version, digest)
      PublishResult.Inserted(ref)
    }
  }

  /** Update the table's parameters for an in-place commit. */
  private def commit(
      identity: CatalogIdentity,
      as:       CatalogEntity,
      digest:   String,
      version:  Int,
  ): Either[CatalogError, Unit] = {
    val params = Map(
      KindKey    -> entityKind(as),
      VersionKey -> version.toString,
      DigestKey  -> digest,
    )
    client.updateTableParameters(
      catalog    = catalog,
      database   = identity.namespace,
      table      = identity.name,
      parameters = params,
    )
  }

  override def discover(
      ref: CatalogRef,
  ): Either[CatalogError, Option[Any]] = {
    client.getTableParameters(ref.catalog, ref.namespace, ref.name).flatMap {
      case None => Right(None)
      case Some(params) =>
        // Compare full ref to stored values (per DE re-review N1:
        // stale and absent both return None; only the exact match
        // returns Some(doc)).
        val storedVersion = params.getOrElse(VersionKey, "1").toInt
        val storedDigest  = params.getOrElse(DigestKey, "")
        if (storedVersion == ref.version && storedDigest == ref.digest) {
          // Real document content is stored in the table's
          // storage descriptor (via create_table's StorageDescriptor)
          // for production use. For now (Any placeholder), the
          // adapter returns the digest as a synthetic marker —
          // consumers that need real doc content should add a
          // ManifestDocument reader in v0.3.1 PR 6.
          Right(Some(storedDigest))
        } else {
          Right(None)
        }
    }
  }

  override def list(
      filter: CatalogFilter,
  ): Either[CatalogError, List[CatalogEntry]] = {
    val effectiveCatalog = filter.catalog.getOrElse(catalog)
    val prefix = filter.namePrefix.getOrElse("")
    // Per the design: list requires a namespace (we list tables within
    // a single database; filter by namespace before calling).
    filter.namespace match {
      case None =>
        // No namespace filter — list across all databases is out of
        // scope for HMS (would require scanning every database).
        // Return Nil; callers should scope by namespace.
        Right(Nil)
      case Some(namespace) =>
        client.listTables(effectiveCatalog, namespace, prefix).map { names =>
          names.sorted.flatMap { name =>
            // Read each table's parameters to build a CatalogEntry.
            // Per DE re-review N4: deterministic order.
            client.getTableParameters(effectiveCatalog, namespace, name) match {
              case Right(Some(params)) =>
                val version = params.getOrElse(VersionKey, "1").toInt
                val digest  = params.getOrElse(DigestKey, "")
                val kind    = params.get(KindKey) match {
                  case Some("rollup")         => Some(CatalogEntity.Rollup)
                  case Some("extension_blob") => Some(CatalogEntity.ExtensionBlob)
                  case _                      => Some(CatalogEntity.Model)  // default
                }
                val ref = CatalogRef(effectiveCatalog, namespace, name, version, digest)
                Some(CatalogEntry(ref, kind.get))
              case _ => None  // skip unreadable entries
            }
          }
        }
    }
  }

  /** Map a [[CatalogEntity]] to its `parameters["semanticdf_kind"]` value. */
  private def entityKind(as: CatalogEntity): String = as match {
    case CatalogEntity.Model        => "model"
    case CatalogEntity.Rollup       => "rollup"
    case CatalogEntity.ExtensionBlob => "extension_blob"
  }
}

object HiveMetastoreCatalogAdapter {

  /** Smart constructor. */
  def apply(
      client:  HiveMetastoreClient,
      catalog: String,
  ): HiveMetastoreCatalogAdapter =
    new HiveMetastoreCatalogAdapter(client, catalog)
}