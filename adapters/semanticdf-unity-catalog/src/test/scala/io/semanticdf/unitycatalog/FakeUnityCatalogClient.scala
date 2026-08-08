package io.semanticdf.unitycatalog

import io.semanticdf.core.catalog.CatalogError

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
  *
  * v0.3.1 (Gap 7 closure): extended with publish-side behavior
  * (`createTable`, `updateTableProperties`, `getTableProperties`,
  * `listTables`) so [[UnityCatalogCatalogAdapter]] tests can run
  * end-to-end against the fake without a real UC server. */
final class FakeUnityCatalogClient(
    initialTables: Map[(String, String, String), List[UcColumn]],
) extends UnityCatalogClient {

  /** The mutable table store: (catalog, schema, table) → (columns, properties).
    * Tests can populate via the `apply` factory or directly. */
  private val tables: scala.collection.mutable.Map[(String, String, String), (List[UcColumn], Map[String, String])] =
    scala.collection.mutable.Map.from(
      initialTables.view.mapValues(cols => (cols, Map.empty[String, String]))
    )

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
    tables.get((catalog, schema, table)).map { case (cols, _) =>
      UcTableSchema(catalog, schema, table, cols)
    }
  }

  override def createTable(
      catalog:    String,
      schema:     String,
      table:      String,
      properties: Map[String, String],
  ): Either[CatalogError, Unit] = {
    if (tables.contains((catalog, schema, table))) {
      // Already exists — return success (adapter detects via
      // describeTable for CreateOnly mode).
      Right(())
    } else {
      tables += ((catalog, schema, table) -> (Nil, properties))
      Right(())
    }
  }

  override def updateTableProperties(
      catalog:    String,
      schema:     String,
      table:      String,
      properties: Map[String, String],
  ): Either[CatalogError, Unit] = {
    tables.get((catalog, schema, table)) match {
      case Some((cols, existingProps)) =>
        tables += ((catalog, schema, table) -> (cols, existingProps ++ properties))
        Right(())
      case None =>
        Left(CatalogError.Conflict(reason = s"table $catalog.$schema.$table does not exist"))
    }
  }

  override def getTableProperties(
      catalog: String,
      schema:  String,
      table:   String,
  ): Either[CatalogError, Option[Map[String, String]]] = {
    tables.get((catalog, schema, table)) match {
      case Some((_, props)) => Right(Some(props))
      case None             => Right(None)
    }
  }

  override def listTables(
      catalog: String,
      schema:  String,
      prefix:  String,
  ): Either[CatalogError, List[String]] = {
    val matching = tables.keys.collect {
      case (cat, sch, tbl) if cat == catalog && sch == schema && tbl.startsWith(prefix) => tbl
    }.toList.sorted  // deterministic order per DE re-review N4
    Right(matching)
  }

  // -- inspection helpers (for tests) --

  /** Snapshot the current properties for a given table. */
  def currentProperties(catalog: String, schema: String, table: String): Map[String, String] =
    tables.get((catalog, schema, table)).map(_._2).getOrElse(Map.empty)
}

object FakeUnityCatalogClient {

  /** Empty fake — every lookup returns `None`. Note: this is a
    * `def` (not `val`) so each test gets a fresh, un-polluted fake.
    * Tests that mutate the fake (publish-side, Gap 7) require
    * fresh state per test. */
  def empty: FakeUnityCatalogClient = new FakeUnityCatalogClient(Map.empty)

  /** Build a fake with the given list of tables. */
  def apply(tables: (String, String, String, List[UcColumn])*): FakeUnityCatalogClient = {
    val m = tables.map { case (cat, sch, tbl, cols) =>
      (cat, sch, tbl) -> cols
    }.toMap
    new FakeUnityCatalogClient(m)
  }
}