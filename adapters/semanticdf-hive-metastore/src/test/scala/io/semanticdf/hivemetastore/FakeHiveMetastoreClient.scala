package io.semanticdf.hivemetastore

/** Test fixture: a hand-driven [[HiveMetastoreClient]] that
  * returns scripted responses.
  *
  * Mirrors `FakeUnityCatalogClient` (in the UC adapter, PR #394).
  * Tests register a `(catalog, database, table) -> Option[HmsTableSchema]`
  * lookup; unregistered lookups return `None` (mapped to
  * `ResolvedSource.NotFound` by the resolver). */
final class FakeHiveMetastoreClient(
    tables: Map[(String, String, String), HmsTableSchema],
) extends HiveMetastoreClient {

  /** The set of (catalog, database, table) tuples this fake has
    * been asked about. Useful for assertions like "did the
    * resolver call describe with the right args?". */
  val called: scala.collection.mutable.Set[(String, String, String)] =
    scala.collection.mutable.Set.empty

  override def describeTable(
      catalog: String,
      database: String,
      table:   String,
  ): Option[HmsTableSchema] = {
    called += ((catalog, database, table))
    tables.get((catalog, database, table))
  }
}

object FakeHiveMetastoreClient {

  /** Empty fake — every lookup returns `None`. */
  val empty: FakeHiveMetastoreClient = new FakeHiveMetastoreClient(Map.empty)

  /** Build a fake with the given list of tables. */
  def apply(tables: (String, String, String, List[HmsColumn])*): FakeHiveMetastoreClient = {
    val m = tables.map { case (cat, db, tbl, cols) =>
      (cat, db, tbl) -> HmsTableSchema(cat, db, tbl, cols)
    }.toMap
    new FakeHiveMetastoreClient(m)
  }
}