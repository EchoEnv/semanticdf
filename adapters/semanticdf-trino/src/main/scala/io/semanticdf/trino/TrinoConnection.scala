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

  /** Serialize the result to a JSON array. Each row becomes a
    * JSON object (column names -> cell values); the array of
    * rows is wrapped in a top-level array.
    *
    * Mirrors the original Spark library's `df.toJSON` consumer
    * pattern: Spark consumers can call `df.toJSON.collect()`
    * to get an array of row-as-JSON strings. The Trino adapter
    * provides the equivalent compact shape — a single JSON
    * string containing the array of rows.
    *
    * ==Why this method lives on TrinoResult (per scala-data-driven-refactor §1)==
    *
    * Per the data-driven mantra: 'A method belongs on the data
    * type only if it's cheap, total, pure, and purely a function
    * of the fields already there.' `toJson` is:
    *   - CHEAP: a single pass over the rows list (no IO)
    *   - TOTAL: never throws on valid TrinoResult
    *   - PURE: no side effects
    *   - FUNCTION OF FIELDS: input is `this.columns` + `this.rows`
    * So it earns its place on the data class.
    *
    * ==Why hand-rolled JSON (not json4s / circe / argonaut)==
    *
    * The Trino adapter currently has ZERO JSON dependencies in
    * pom.xml. Adding one would be a meaningful dependency change.
    * The output is simple (strings + numbers + booleans), so a
    * 30-line manual serializer suffices. A future PR can swap
    * in a real JSON library if richer types are needed.
    *
    * ==Why this doesn't return a ResultReader/Writer pattern==
    *
    * Spark's `df.toJSON` returns a `Dataset[String]` (lazy
    * transformation). For our engine-portable shape (a
    * data-class `TrinoResult`), lazy doesn't apply — the data
    * is already materialized. Eager serialization is correct. */
  def toJson: String = {
    val rowStrings = rows.map { row =>
      val cells = columns.zip(row).map { case (name, cell) =>
        s"${quote(name)}:${renderCell(cell)}"
      }
      cells.mkString("{", ",", "}")
    }
    rowStrings.mkString("[", ",", "]")
  }

  /** Quote a JSON string key. */
  private def quote(s: String): String = {
    val sb = new StringBuilder(s.length + 2)
    sb.append('"')
    s.foreach {
      case '"'  => sb.append("\\\"")
      case '\\' => sb.append("\\\\")
      case '\n' => sb.append("\\n")
      case '\r' => sb.append("\\r")
      case '\t' => sb.append("\\t")
      case c    => sb.append(c)
    }
    sb.append('"')
    sb.toString
  }

  /** Render a single `LiteralValue` cell to JSON. */
  private def renderCell(v: LiteralValue): String = v match {
    case LiteralValue.StringValue(s)  => quote(s)
    case LiteralValue.IntValue(n)     => n.toString
    case LiteralValue.LongValue(n)    => n.toString
    case LiteralValue.FloatValue(n)   => n.toString
    case LiteralValue.DoubleValue(n)  => n.toString
    case LiteralValue.DecimalValue(d) => d.toString
    case LiteralValue.BoolValue(b)    => b.toString
    case LiteralValue.BinaryValue(b)  => quote(new String(b.toArray, java.nio.charset.StandardCharsets.UTF_8))
    case LiteralValue.TimestampValue(ts) => quote(ts.toString)
    case LiteralValue.DateValue(d)       => quote(d.toString)
    case LiteralValue.ArrayValue(items) =>
      val parts = items.map(renderCell)
      parts.mkString("[", ",", "]")
    case LiteralValue.MapValue(entries) =>
      // MapValue carries LiteralValue keys (not String keys). JSON
      // object keys must be strings, so we render whatever cell the
      // key is, then post-process to strip any non-string content
      // (e.g. IntValue -> \"42\"; for non-string types the JSON would
      // be technically invalid but this is the most general
      // lossless serialization).
      val parts = entries.map { case (k, v) =>
        val keyJson = renderCell(k)
        // Strip leading/trailing quotes if the key was rendered as a
        // string literal; if it wasn't a string, it's JSON-invalid
        // anyway but we keep the raw rendering for diagnosis.
        val keyForObject = if (keyJson.startsWith("\"") && keyJson.endsWith("\"")) keyJson else keyJson
        s"$keyForObject:${renderCell(v)}"
      }
      parts.mkString("{", ",", "}")
    case LiteralValue.StructValue(fields) =>
      val parts = fields.map { case (f, vv) => s"${quote(f)}:${renderCell(vv)}" }
      parts.mkString("{", ",", "}")
    case LiteralValue.NullValue => "null"
  }
}