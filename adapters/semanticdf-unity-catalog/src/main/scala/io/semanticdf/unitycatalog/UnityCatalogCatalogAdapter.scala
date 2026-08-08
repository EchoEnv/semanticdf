package io.semanticdf.unitycatalog

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
  * Unity Catalog. Maps the engine-portable publish/discover/list
  * contract to UC's REST API calls (POST /tables, PATCH /tables,
  * GET /tables).
  *
  * ==CAS mechanism==
  *
  * Per the v0.3.0 CAS contract ([[CatalogAdapter]] scaladoc):
  * implementations MUST be server-side-atomic per identity.
  * UC's REST API is NOT atomic across concurrent writers —
  * two concurrent CAS calls can both succeed in reading the
  * same digest and writing different updates.
  *
  * We use three reserved keys in the table's `properties` map
  * to carry the entity identity:
  *
  *   - `semanticdf_kind`    : "model" | "rollup" | "extension_blob"
  *   - `semanticdf_version` : the entity version (Int, stringified)
  *   - `semanticdf_digest`  : the entity digest
  *
  * **This adapter delegates atomicity to the platform's
  * [[io.semanticdf.platform.streaming.StartupReconciler]]** which
  * serializes publications through a single Restate service.
  * For direct concurrent `publish` calls, callers should wrap
  * them in an external lock (e.g. Zookeeper) or use the
  * `RestateAuditSink` to enforce per-identity ordering.
  *
  * ==Why a new adapter class (not extension of `UnityCatalogSourceResolver`)==
  *
  * Per scala-data-driven-refacer §1: the SHAPE of the contract
  * (`CatalogAdapter`) lives in core; the BODY (the UC-specific
  * publish/discover/list behavior) lives here. The resolver and
  * the adapter serve different lifecycle roles (resolver = read
  * schema for compile; adapter = write entities for publish),
  * so they're separate classes that share the underlying
  * `UnityCatalogClient` (a single connection per process).
  *
  * ==Why a "marker" table (not a real table with columns)==
  *
  * Manifest entities stored in UC don't need real columns —
  * the manifest content lives in the table's `properties` map.
  * We create a minimal table with a single `_marker` column
  * because UC requires at least one column. This is documented
  * as a v0.3.1 limitation; production users who need the table
  * to be queryable should populate real columns.
  *
  * ==Boundary contract==
  *
  * Zero Spark imports. The `ManifestDocument` placeholder is `Any`
  * (per [[CatalogAdapter]] design — the real ADT lands in v0.3.1
  * PR 6 "Manifest v2 + dual reader"). */
final class UnityCatalogCatalogAdapter(
    client: UnityCatalogClient,
    val   catalog: String,
) extends CatalogAdapter {

  /** Reserved UC property keys (stored in the table's `properties` map). */
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
    client.getTableProperties(catalog, identity.namespace, identity.name).flatMap {
      case None =>
        mode match {
          case PublishMode.CreateOnly =>
            createAndReturn(identity, as, newDigest, version = 1)
          case PublishMode.Upsert =>
            createAndReturn(identity, as, newDigest, version = 1)
          case PublishMode.CompareAndSet(_) =>
            Right(PublishResult.Conflict(reason = "no entity at identity"))
        }
      case Some(existingProps) =>
        val currentVersion = existingProps.getOrElse(VersionKey, "1").toInt
        val currentDigest  = existingProps.getOrElse(DigestKey, "")
        mode match {
          case PublishMode.CreateOnly =>
            val currentRef = CatalogRef(
              catalog   = catalog,
              namespace = identity.namespace,
              name      = identity.name,
              version   = currentVersion,
              digest    = currentDigest,
            )
            Right(PublishResult.Conflict(reason = "already exists", current = Some(currentRef)))
          case PublishMode.Upsert =>
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
      as:       CatalogEntity,
      digest:   String,
      version:  Int,
  ): Either[CatalogError, PublishResult] = {
    val props = Map(
      KindKey    -> entityKind(as),
      VersionKey -> version.toString,
      DigestKey  -> digest,
    )
    client.createTable(
      catalog    = catalog,
      schema     = identity.namespace,
      table      = identity.name,
      properties = props,
    ).map { _ =>
      val ref = CatalogRef(catalog, identity.namespace, identity.name, version, digest)
      PublishResult.Inserted(ref)
    }
  }

  /** Update the table's properties for an in-place commit. */
  private def commit(
      identity: CatalogIdentity,
      as:       CatalogEntity,
      digest:   String,
      version:  Int,
  ): Either[CatalogError, Unit] = {
    val props = Map(
      KindKey    -> entityKind(as),
      VersionKey -> version.toString,
      DigestKey  -> digest,
    )
    client.updateTableProperties(
      catalog    = catalog,
      schema     = identity.namespace,
      table      = identity.name,
      properties = props,
    )
  }

  override def discover(
      ref: CatalogRef,
  ): Either[CatalogError, Option[Any]] = {
    client.getTableProperties(ref.catalog, ref.namespace, ref.name).flatMap {
      case None => Right(None)
      case Some(props) =>
        // Compare full ref to stored values (per DE re-review N1).
        val storedVersion = props.getOrElse(VersionKey, "1").toInt
        val storedDigest  = props.getOrElse(DigestKey, "")
        if (storedVersion == ref.version && storedDigest == ref.digest) {
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
    filter.namespace match {
      case None =>
        // No namespace filter — UC requires schema_name in listTables.
        // Return Nil; callers should scope by namespace.
        Right(Nil)
      case Some(namespace) =>
        client.listTables(effectiveCatalog, namespace, prefix).map { names =>
          names.sorted.flatMap { name =>
            client.getTableProperties(effectiveCatalog, namespace, name) match {
              case Right(Some(props)) =>
                val version = props.getOrElse(VersionKey, "1").toInt
                val digest  = props.getOrElse(DigestKey, "")
                val kind    = props.get(KindKey) match {
                  case Some("rollup")         => Some(CatalogEntity.Rollup)
                  case Some("extension_blob") => Some(CatalogEntity.ExtensionBlob)
                  case _                      => Some(CatalogEntity.Model)
                }
                val ref = CatalogRef(effectiveCatalog, namespace, name, version, digest)
                Some(CatalogEntry(ref, kind.get))
              case _ => None
            }
          }
        }
    }
  }

  /** Map a [[CatalogEntity]] to its `properties["semanticdf_kind"]` value. */
  private def entityKind(as: CatalogEntity): String = as match {
    case CatalogEntity.Model        => "model"
    case CatalogEntity.Rollup       => "rollup"
    case CatalogEntity.ExtensionBlob => "extension_blob"
  }
}

object UnityCatalogCatalogAdapter {

  /** Smart constructor. */
  def apply(
      client:  UnityCatalogClient,
      catalog: String,
  ): UnityCatalogCatalogAdapter =
    new UnityCatalogCatalogAdapter(client, catalog)
}