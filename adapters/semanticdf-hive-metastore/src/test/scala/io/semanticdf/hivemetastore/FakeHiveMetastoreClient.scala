package io.semanticdf.hivemetastore

import io.semanticdf.core.catalog.CatalogError

import scala.jdk.CollectionConverters._

/** Test fixture: a hand-driven [[HiveMetastoreClient]] that
  * returns scripted responses.
  *
  * Mirrors `FakeUnityCatalogClient` (in the UC adapter, PR #394).
  * Tests register a `(catalog, database, table) -> Option[HmsTableSchema]`
  * lookup; unregistered lookups return `None` (mapped to
  * `ResolvedSource.NotFound` by the resolver).
  *
  * v0.3.1 (Gap 7 closure): extended with publish-side behavior
  * (`createTable`, `updateTableParameters`, `getTableParameters`,
  * `listTables`) so [[HiveMetastoreCatalogAdapter]] tests can run
  * end-to-end against the fake without a real HMS server. */
final class FakeHiveMetastoreClient(
    initialTables: Map[(String, String, String), List[HmsColumn]],
) extends HiveMetastoreClient {

  /** The mutable table store. HMS tables carry (columns, parameters).
    * Tests can populate via the `apply` factory or directly. */
  private val tables: scala.collection.mutable.Map[(String, String, String), (List[HmsColumn], Map[String, String])] =
    scala.collection.mutable.Map.from(
      initialTables.view.mapValues(cols => (cols, Map.empty[String, String]))
    )

  /** The set of (catalog, database, table) tuples this fake has
    * been asked about. Useful for assertions like "did the
    * resolver call describe with the right args?". */
  val called: scala.collection.mutable.Set[(String, String, String)] =
    scala.collection.mutable.Set.empty

  /** The set of (catalog, database, table, parameters) tuples
    * this fake has been asked to update. */
  val parameterUpdates: scala.collection.mutable.ListBuffer[((String, String, String), Map[String, String])] =
    scala.collection.mutable.ListBuffer.empty

  override def describeTable(
      catalog: String,
      database: String,
      table:   String,
  ): Option[HmsTableSchema] = {
    called += ((catalog, database, table))
    tables.get((catalog, database, table)).map { case (cols, _) =>
      HmsTableSchema(catalog, database, table, cols)
    }
  }

  override def createTable(
      catalog:    String,
      database:   String,
      table:      String,
      columns:    List[HmsColumn],
      parameters: Map[String, String],
  ): Either[CatalogError, Unit] = {
    if (tables.contains((catalog, database, table))) {
      // Already exists — return success (HMS create_table is
      // idempotent; the adapter uses describeTable to detect
      // duplicates for CreateOnly mode).
      Right(())
    } else {
      tables += ((catalog, database, table) -> (columns, parameters))
      Right(())
    }
  }

  override def updateTableParameters(
      catalog:    String,
      database:   String,
      table:      String,
      parameters: Map[String, String],
  ): Either[CatalogError, Unit] = {
    tables.get((catalog, database, table)) match {
      case Some((cols, existingParams)) =>
        tables += ((catalog, database, table) -> (cols, existingParams ++ parameters))
        parameterUpdates += (((catalog, database, table), parameters))
        Right(())
      case None =>
        Left(CatalogError.Conflict(reason = s"table $database.$table does not exist"))
    }
  }

  override def getTableParameters(
      catalog:  String,
      database: String,
      table:    String,
  ): Either[CatalogError, Option[Map[String, String]]] = {
    tables.get((catalog, database, table)) match {
      case Some((_, params)) => Right(Some(params))
      case None              => Right(None)
    }
  }

  override def listTables(
      catalog:  String,
      database: String,
      prefix:   String,
  ): Either[CatalogError, List[String]] = {
    val matching = tables.keys.collect {
      case (cat, db, tbl) if cat == catalog && db == database && tbl.startsWith(prefix) => tbl
    }.toList.sorted  // deterministic order per DE re-review N4
    Right(matching)
  }

  // -- inspection helpers (for tests) --

  /** Snapshot the current parameters for a given table. */
  def currentParameters(catalog: String, database: String, table: String): Map[String, String] =
    tables.get((catalog, database, table)).map(_._2).getOrElse(Map.empty)
}

object FakeHiveMetastoreClient {

  /** Empty fake — every lookup returns `None`. Note: this is a
    * `def` (not `val`) so each test gets a fresh, un-polluted fake.
    * Tests that mutate the fake (publish-side, Gap 7) require
    * fresh state per test. */
  def empty: FakeHiveMetastoreClient = new FakeHiveMetastoreClient(Map.empty)

  /** Build a fake with the given list of tables. */
  def apply(tables: (String, String, String, List[HmsColumn])*): FakeHiveMetastoreClient = {
    val m = tables.map { case (cat, db, tbl, cols) =>
      (cat, db, tbl) -> cols
    }.toMap
    new FakeHiveMetastoreClient(m)
  }
}