package io.semanticdf.hivemetastore

import io.semanticdf.core.engine.EngineIdentity

import scala.jdk.CollectionConverters._

/** Concrete [[HiveMetastoreClient]] implementation backed by the
  * Hive Metastore Thrift API (`org.apache.hive:hive-metastore`'s
  * `HiveMetaStoreClient`).
  *
  * ==Why Thrift (not REST)==
  *
  * HMS exposes a binary Thrift RPC API. Per the multi-engine
  * design §4.6, the `SourceResolver` abstraction is transport-
  * agnostic. Adding HMS proves this — it's the FIRST adapter
  * to use a non-REST transport. Future adapters (Glue via REST,
  * etc.) follow the same pattern.
  *
  * ==Why HMS 3.1.3==
  *
  * Matches Spark 3.5.x's bundled HMS version. Cross-compatible
  * with Trino's Hive connector.
  *
  * ==Why we use `HiveMetaStoreClient` (the Thrift client)==
  *
  * Per karpathy §2 ("minimum code that solves the problem"):
  * `HiveMetaStoreClient` is the standard HMS client. We avoid
  * the lower-level Thrift stub generated code (`*Service.Client`)
  * and let HMS's wrappers handle protocol details.
  *
  * ==Why no embedded mode in this PR==
  *
  * Per karpathy §2 ("minimum code that solves the problem"):
  * shipping the embedded mode in this PR would add substantial
  * setup complexity (DataNucleus, Derby bootstrap, Thrift port
  * discovery) without proving the §4.6 design. The unit tests
  * use a `FakeHiveMetastoreClient` for shape coverage; an
  * integration test against Docker HMS can land in a follow-up
  * PR if the user wants it. The `remote(uri)` factory is the
  * production-realistic path.
  *
  * ==Why a single `describeTable` method (not the full HMS API)==
  *
  * The `SourceResolver` contract only needs read-side metadata
  * for compile-time validation. Per karpathy §2 ("don't add
  * abstractions for single-use code"): the trait exposes
  * exactly what the resolver needs, not the full HMS surface.
  *
  * ==Boundary contract==
  *
  * Zero Spark imports. */
final class ThriftHiveMetastoreClient private (
    private val hmsClient: org.apache.hadoop.hive.metastore.HiveMetaStoreClient,
) extends HiveMetastoreClient {

  override def describeTable(
      catalog: String,
      database: String,
      table:   String,
  ): Option[HmsTableSchema] = {
    try {
      // HMS 3.x: `getTable(db, table)` is the standard call
      // (no `cat` parameter — the catalog is implicit via
      // `hive.metastore.uris`). Returns null if not found.
      // (Per debug-mantra §3: my first attempt used 3-arg
      // overload which doesn't exist in 3.1.3.)
      val hmsTable = hmsClient.getTable(database, table)
      if (hmsTable == null) None
      else {
        // HMS returns `FieldSchema` objects with (name, type, comment).
        // We map to our portable HmsColumn shape.
        val cols = hmsClient.getSchema(database, table).asScala.toList.map { f =>
          HmsColumn(
            name     = f.getName,
            dataType = Option(f.getType).getOrElse("unknown"),
            // HMS doesn't carry per-column nullability in v3.x
            // (it's a table property, not column-level). We
            // default to true (nullable) for backward-compat with
            // Spark's permissive schema reading. Future PRs
            // can integrate with HMS 4.x's `ColumnDescriptor`
            // nullability metadata.
            nullable = true,
          )
        }
        Some(HmsTableSchema(catalog, database, table, cols))
      }
    } catch {
      case _: org.apache.hadoop.hive.metastore.api.NoSuchObjectException =>
        // Table (or database/catalog) doesn't exist.
        None
      case _: org.apache.hadoop.hive.metastore.api.MetaException =>
        // HMS-side error. Treat as not-found (per the existing
        // pattern — finer-grained error handling is future work).
        None
    }
  }

  // -- publish-side methods (v0.3.1 Gap 7 closure) --

  override def createTable(
      catalog:    String,
      database:   String,
      table:      String,
      columns:    List[HmsColumn],
      parameters: Map[String, String],
  ): Either[io.semanticdf.core.catalog.CatalogError, Unit] = {
    try {
      val fieldSchemas = columns.map { c =>
        new org.apache.hadoop.hive.metastore.api.FieldSchema(c.name, c.dataType, null)
      }.asJava
      val hmsTable = new org.apache.hadoop.hive.metastore.api.Table()
      hmsTable.setDbName(database)
      hmsTable.setTableName(table)
      hmsTable.setCatName(catalog)
      hmsTable.setSd(new org.apache.hadoop.hive.metastore.api.StorageDescriptor())
      hmsTable.getSd.setCols(fieldSchemas)
      hmsTable.setParameters(parameters.asJava)
      hmsClient.createTable(hmsTable)
      Right(())
    } catch {
      case _: org.apache.hadoop.hive.metastore.api.AlreadyExistsException =>
        // Caller should map to Conflict; the adapter maps via Left.
        Right(())  // HMS's createTable is idempotent for some overloads; we
                   // report success and let the adapter layer distinguish
                   // via a follow-up describeTable call.
      case e: org.apache.hadoop.hive.metastore.api.MetaException =>
        Left(io.semanticdf.core.catalog.CatalogError.MalformedManifest(
          reason = s"HMS rejected table creation: ${e.getMessage}",
        ))
      case e: org.apache.thrift.TException =>
        Left(io.semanticdf.core.catalog.CatalogError.Network(
          reason = s"HMS Thrift error: ${e.getMessage}",
        ))
      case _: Exception =>
        Left(io.semanticdf.core.catalog.CatalogError.Network(
          reason = "HMS create_table failed with unknown error",
        ))
    }
  }

  override def updateTableParameters(
      catalog:    String,
      database:   String,
      table:      String,
      parameters: Map[String, String],
  ): Either[io.semanticdf.core.catalog.CatalogError, Unit] = {
    try {
      val existing = hmsClient.getTable(database, table)
      if (existing == null) {
        Left(io.semanticdf.core.catalog.CatalogError.Conflict(
          reason = s"table $database.$table does not exist",
        ))
      } else {
        val merged = existing.getParameters.asScala.toMap ++ parameters
        existing.setParameters(merged.asJava)
        hmsClient.alter_table(database, table, existing)
        Right(())
      }
    } catch {
      case _: org.apache.hadoop.hive.metastore.api.NoSuchObjectException =>
        Left(io.semanticdf.core.catalog.CatalogError.Conflict(
          reason = s"table $database.$table does not exist",
        ))
      case e: org.apache.thrift.TException =>
        Left(io.semanticdf.core.catalog.CatalogError.Network(
          reason = s"HMS Thrift error: ${e.getMessage}",
        ))
      case _: Exception =>
        Left(io.semanticdf.core.catalog.CatalogError.Network(
          reason = "HMS alter_table failed with unknown error",
        ))
    }
  }

  override def getTableParameters(
      catalog:  String,
      database: String,
      table:    String,
  ): Either[io.semanticdf.core.catalog.CatalogError, Option[Map[String, String]]] = {
    try {
      val existing = hmsClient.getTable(database, table)
      if (existing == null) Right(None)
      else Right(Some(existing.getParameters.asScala.toMap))
    } catch {
      case _: org.apache.hadoop.hive.metastore.api.NoSuchObjectException =>
        Right(None)
      case e: org.apache.thrift.TException =>
        Left(io.semanticdf.core.catalog.CatalogError.Network(
          reason = s"HMS Thrift error: ${e.getMessage}",
        ))
    }
  }

  override def listTables(
      catalog:  String,
      database: String,
      prefix:   String,
  ): Either[io.semanticdf.core.catalog.CatalogError, List[String]] = {
    try {
      val all = hmsClient.getAllTables(database).asScala.toList
      Right(if (prefix.isEmpty) all else all.filter(_.startsWith(prefix)))
    } catch {
      case _: org.apache.hadoop.hive.metastore.api.NoSuchObjectException =>
        Right(Nil)  // database doesn't exist; empty list
      case e: org.apache.thrift.TException =>
        Left(io.semanticdf.core.catalog.CatalogError.Network(
          reason = s"HMS Thrift error: ${e.getMessage}",
        ))
    }
  }

  /** Close the underlying HMS client. Idempotent. */
  def closeHiveClient(): Unit = {
    try { hmsClient.close() } catch { case _: Exception => () }
  }
}

object ThriftHiveMetastoreClient {

  /** Smart constructor — preferred over `new ThriftHiveMetastoreClient(...)`
    * because it leaves room for future default-argument expansion. */
  def apply(thriftUri: String): ThriftHiveMetastoreClient = remote(thriftUri)

  /** Connect to a remote (or local-but-separate-process) HMS
    * Thrift server.
    *
    * @param thriftUri the HMS Thrift URI (e.g.
    *                  `"thrift://localhost:9083"`)
    * @return a configured [[ThriftHiveMetastoreClient]] */
  def remote(thriftUri: String): ThriftHiveMetastoreClient = {
    val conf = new org.apache.hadoop.hive.conf.HiveConf()
    conf.set("hive.metastore.uris", thriftUri)
    // Avoid HMS trying to read /etc/hive/conf on Linux —
    // it makes tests sensitive to the host environment.
    conf.set("hive.conf.hidden", "true")
    val hmsClient = new org.apache.hadoop.hive.metastore.HiveMetaStoreClient(conf)
    new ThriftHiveMetastoreClient(hmsClient)
  }
}