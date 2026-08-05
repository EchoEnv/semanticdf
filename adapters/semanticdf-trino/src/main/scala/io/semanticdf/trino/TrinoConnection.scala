package io.semanticdf.trino

import io.semanticdf.core.expr.LiteralValue

/** Engine-specific Trino JDBC connection boundary trait —
  * Phase 2 follow-up to PRs #367 (TrinoClient) + #371 (ParameterizedSql).
  *
  * `TrinoConnection` is the boundary between the engine-portable
  * `Engine.execute(plan, ctx)` contract (in `core.engine`) and
  * the engine-specific Trino JDBC execution. The `TrinoEngine`
  * depends on this trait (not on a concrete JDBC connection), so
  * tests can inject a fake implementation.
  *
  * ==Why a trait (vs. a concrete Trino JDBC connection)==
  *
  * Per scala-data-driven-refactor §1 ("data is data, behavior lives
  * elsewhere"): the BEHAVIOR (calling Trino, parsing responses)
  * is engine-specific. The CONTRACT (the methods the engine
  * executor needs) is in this trait — a small abstraction that's
  * justified by testability needs.
  *
  * ==Why a separate trait from `TrinoClient` (PR #367)==
  *
  * `TrinoClient` is the SOURCE RESOLUTION boundary — it answers
  * "what's the schema of this table?". `TrinoConnection` is the
  * EXECUTION boundary — it answers "run this SQL and return rows".
  * Two distinct concerns; two distinct traits.
  *
  * ==Why `extends Serializable`==
  *
  * The engine may cross serialization boundaries (cluster mode,
  * MCP wire format, future Restate journal). The connection is
  * typically driver-local (per the design's "closure-bypass"
  * pattern), so in practice this trait's instances are NOT
  * serialized — but the trait signature matches the engine's
  * general Serializable contract.
  *
  * ==Why `prepareStatement(sql, parameters)` returns `TrinoResult`==
  *
  * `TrinoResult` is the engine-specific query result shape (rows
  * + columns). The engine adapter translates `TrinoResult` rows
  * to portable `LiteralValue`s (via `TrinoResultDecoder`, a
  * future PR). For v1, the result is returned as-is; the decoder
  * is a follow-up.
  *
  * ==Boundary contract==
  *
  * Zero Spark imports (this is the Trino adapter — no Spark
  * dependencies). Verifiable by:
  * `grep -r 'org.apache.spark' adapters/semanticdf-trino/src/main/scala/io/semanticdf/trino/TrinoConnection.scala`
  */
trait TrinoConnection extends Serializable {

  /** Prepare a parameterized statement for execution.
    *
    * The implementation MUST:
    *   1. Call the JDBC `prepareStatement(sql)`
    *   2. For each parameter at index `i+1`, call the appropriate
    *      `setXxx(i+1, value)` based on the `LiteralValue` case
    *      (e.g. `LiteralValue.StringValue(s)` → `setString`,
    *      `LiteralValue.IntValue(n)` → `setInt`, etc.)
    *   3. Call `executeQuery()` (SELECTs only — the compiler
    *      produces SELECT statements only)
    *   4. Wrap the result in `TrinoResult` and return it
    *
    * @param sql        the SQL with `?` placeholders
    * @param parameters the ordered parameter values to bind
    * @return the query result */
  def prepareStatement(
      sql:        String,
      parameters: List[LiteralValue],
  ): TrinoResult

  /** Close the connection (releases any JDBC resources). The
    * engine calls this after `execute()` returns, in `finally`
    * style. */
  def close(): Unit
}

/** A Trino query result — the rows + columns of a SELECT.
  *
  * Per scala-data-driven-refactor §1: pure data — no behavior.
  * The columns list is `List[String]` (column names); the rows
  * are `List[List[LiteralValue]]` (each cell is a portable
  * value).
  *
  * The engine adapter translates the JDBC `ResultSet` to this
  * shape. The translation is straightforward:
  *   - column name → `ResultSetMetaData.getColumnName(i)`
  *   - cell value → type-switch on the JDBC type → `LiteralValue`
  *
  * For v1, this is the engine's output shape. A future PR will
  * add `ResultDecoder` for richer translation (temporal precision,
  * decimal scale, null handling).
  *
  * ==Boundary contract==
  *
  * Zero Spark imports.
  */
final case class TrinoResult(
    columns: List[String],
    rows:    List[List[LiteralValue]],
) extends Product with Serializable {

  /** Convenience: number of rows. */
  def rowCount: Int = rows.size

  /** Convenience: get a single cell at (row, col). Returns `None`
    * if the indices are out of bounds. */
  def cell(rowIdx: Int, colIdx: Int): Option[LiteralValue] =
    rows.lift(rowIdx).flatMap(_.lift(colIdx))
}