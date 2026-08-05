package io.semanticdf.trino

import io.semanticdf.core.expr.LiteralValue

/** A `TrinoConnection` test fixture that exercises the
  * `TrinoResultDecoder` end-to-end.
  *
  * Unlike `FakeTrinoConnection` (which directly returns a
  * pre-built `TrinoResult`), this fixture stores raw rows +
  * column names and uses `TrinoResultDecoder.decode(...)` to
  * produce the `TrinoResult`. This proves that:
  *
  *   - The decoder is correctly integrated into the connection
  *     flow (compile → execute → decode → TrinoResult)
  *   - The decoder handles the actual JDBC translation path
  *   - The full pipeline works end-to-end (this is the path
  *     a real `JdbcTrinoConnection` will follow)
  *
  * Per scala-data-driven-refactor §1 ("data is data, behavior
  * lives elsewhere"): this fixture is engine-specific behavior
  * (a test fixture for the Trino adapter), not data. It lives
  * in the test source.
  *
  * ==Why a separate fixture (not extending FakeTrinoConnection)==
  *
  * The two fixtures have different purposes:
  *   - `FakeTrinoConnection`: tests that need pre-built
  *     `TrinoResult`s (the engine contract test)
  *   - `DecodingFakeTrinoConnection`: tests that need to verify
  *     the decoder integration (this file)
  *
  * Top-level case class (NOT nested) so it doesn't capture the
  * enclosing test instance — necessary for clean test setup.
  */
final case class DecodingFakeTrinoConnection(
    columns: List[String],
    rawRows: List[List[Any]],
) extends TrinoConnection {

  override def prepareStatement(
      sql:        String,
      parameters: List[LiteralValue],
  ): TrinoResult =
    // Use the decoder — same path a real JdbcTrinoConnection
    // would take when translating a ResultSet into TrinoResult.
    TrinoResultDecoder.decode(columns = columns, rawRows = rawRows)

  override def close(): Unit = {
    // no-op for the fake — nothing to release
  }
}

object DecodingFakeTrinoConnection {

  /** Build a `DecodingFakeTrinoConnection` for the given columns +
    * raw rows. The columns are the SQL result-set columns;
    * rawRows are the raw values that the decoder will translate. */
  def of(
      columns: List[String],
      rawRows: List[List[Any]],
  ): DecodingFakeTrinoConnection =
    DecodingFakeTrinoConnection(columns = columns, rawRows = rawRows)
}