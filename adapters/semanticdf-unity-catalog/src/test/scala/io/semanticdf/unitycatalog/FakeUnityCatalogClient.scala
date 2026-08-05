package io.semanticdf.unitycatalog

/** Test fixture: a hand-driven [[UnityCatalogClient]] that
  * returns scripted responses.
  *
  * Mirrors the `FakeTrinoClient` pattern (per the existing
  * `TrinoSourceResolver` test fixture). Tests register a
  * `(catalog, schema, table) -> Option[UcTableSchema]` lookup;
  * unregistered lookups return `None` (mapped to
  * `ResolvedSource.NotFound` by the resolver).
  *
  * ==Why a Map (not a behavior mock)==
  *
  * Per scala-data-driven-refactor §1 ("data is data, behavior
  * lives elsewhere"): the fake is a data table — it answers
  * a deterministic question (does this table exist?) with a
  * deterministic answer. No stubs, no spies, no method-call
  * recording. Tests inspect the resolver's behavior, not the
  * client's behavior.
  */
final class FakeUnityCatalogClient(
    tables: Map[(String, String, String), UcTableSchema],
) extends UnityCatalogClient {

  /** The set of (catalog, schema, table) tuples this fake has
    * been asked about. Useful for assertions like "did the
    * resolver call describe with the right args?". */
  val called: scala.collection.mutable.Set[(String, String, String)] =
    scala.collection.mutable.Set.empty

  override def describeTable(
      catalog: String,
      schema:  String,
      table:   String,
  ): Option[UcTableSchema] = {
    called += ((catalog, schema, table))
    tables.get((catalog, schema, table))
  }
}

object FakeUnityCatalogClient {

  /** Empty fake — every lookup returns `None`. */
  val empty: FakeUnityCatalogClient = new FakeUnityCatalogClient(Map.empty)

  /** Build a fake with the given list of tables. */
  def apply(tables: (String, String, String, List[UcColumn])*): FakeUnityCatalogClient = {
    val m = tables.map { case (cat, sch, tbl, cols) =>
      (cat, sch, tbl) -> UcTableSchema(cat, sch, tbl, cols)
    }.toMap
    new FakeUnityCatalogClient(m)
  }
}