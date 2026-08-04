package io.semanticdf.trino

/** A `FakeTrinoClient` for testing `TrinoSourceResolver` without
  * a real Trino cluster.
  *
  * Holds canned responses keyed by `(catalog, schema, table)`.
  * Tests pre-populate the responses before calling the resolver.
  *
  * Top-level (not nested) so it doesn't capture the enclosing
  * test instance — necessary for Serializable round-trip
  * verification.
  */
final case class FakeTrinoClient(
    describeResponses: Map[(String, String, String), TrinoTableSchema] = Map.empty,
    rowCountResponses: Map[(String, String, String), Long]               = Map.empty,
) extends TrinoClient {

  override def describeTable(
      catalog: String,
      schema:  String,
      table:   String,
  ): Option[TrinoTableSchema] =
    describeResponses.get((catalog, schema, table))

  override def getTableRowCount(
      catalog: String,
      schema:  String,
      table:   String,
  ): Option[Long] =
    rowCountResponses.get((catalog, schema, table))
}

object FakeTrinoClient {

  /** A FakeTrinoClient with one canned describeTable response. */
  def withDescribe(
      catalog: String,
      schema:  String,
      table:   String,
      columns: TrinoTableSchema,
  ): FakeTrinoClient =
    FakeTrinoClient(
      describeResponses = Map((catalog, schema, table) -> columns),
    )
}