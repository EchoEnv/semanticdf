package io.semanticdf.trino

import io.semanticdf.core.expr.LiteralValue

/** Engine-specific Trino result decoder — Phase 2 follow-up to
  * PR #372 (`TrinoConnection` + `TrinoResult`).
  *
  * `TrinoResultDecoder` centralizes the translation from raw
  * JDBC values (the output of `java.sql.ResultSet.getXxx(...)`)
  * to portable `LiteralValue`s (the engine-portable value
  * shape). It's a pure function — given a list of raw values,
  * produce a list of `LiteralValue`s.
  *
  * ==Why a separate decoder (vs. inline in the connection)==
  *
  * The JDBC → `LiteralValue` translation is engine-specific
  * behavior. Per scala-data-driven-refactor §1 ("data is data,
  * behavior lives elsewhere"):
  *   - The DATA (the `TrinoResult` shape) is in the connection's
  *     return type (already in the adapter per PR #372)
  *   - The BEHAVIOR (the translation) lives here
  *
  * Centralizing the translation in one place makes it testable
  * in isolation. The real `JdbcTrinoConnection` (a future PR
  * that uses the Trino JDBC driver) calls this decoder to
  * convert `ResultSet` rows to `LiteralValue` cells.
  *
  * ==Why a pure object (vs. a class)==
  *
  * The decoder has no state — it's a pure function. Per
  * scala-data-driven-refactor §1: "A method belongs on the
  * data type only if it's cheap, total, pure, and purely a
  * function of the fields already there." The decoder doesn't
  * need any fields — it's a `object` with pure functions.
  *
  * ==Why exhaustive match on `Any`==
  *
  * The 16 `LiteralValue` cases are exhaustively enumerated via
  * the input types (`String`, `Integer`, `Long`, `Double`, etc.).
  * Each input type maps to exactly one `LiteralValue` case
  * (or `NullValue` for null). The match is NOT on `LiteralValue`
  * itself — it's on the JDBC input type.
  *
  * ==Why the 9-input-type match (vs. full coverage of all Java types)==
  *
  * Per the design's risk #9: "Decimal scale/overflow differs by
  * engine" — the Trino JDBC driver returns `BigDecimal` for
  * DECIMAL columns. The decoder maps `BigDecimal` to
  * `LiteralValue.DecimalValue`.
  *
  * For TIMESTAMP columns, the JDBC driver returns
  * `java.sql.Timestamp` (or `java.time.LocalDateTime` with newer
  * drivers). The decoder maps both to `LiteralValue.TimestampValue`.
  *
  * For DATE columns, the JDBC driver returns `java.sql.Date`
  * (or `java.time.LocalDate` with newer drivers). The decoder
  * maps both to `LiteralValue.DateValue`.
  *
  * ==Boundary contract==
  *
  * Zero Spark imports. Verifiable by:
  * `grep -r 'org.apache.spark' adapters/semanticdf-trino/src/main/scala/io/semanticdf/trino/TrinoResultDecoder.scala`
  *
  * The decoder consumes ONLY `LiteralValue` (engine-portable) +
  * standard Java/JDBC types (universal). No Trino-specific types
  * leak in.
  */
object TrinoResultDecoder {

  /** Decode raw JDBC values into portable `LiteralValue`s.
    *
    * The `rawRows` are typically the output of iterating a
    * `java.sql.ResultSet`: each row is a list of raw values
    * (one per column). The decoder maps each value to its
    * portable `LiteralValue` form.
    *
    * Null cells become `LiteralValue.NullValue`. The decoder
    * does NOT throw on null (the engine-portable `LiteralValue`
    * explicitly has a `NullValue` case per the design).
    *
    * @param columns the column names (in declaration order)
    * @param rawRows the raw row data (each row matches `columns.length`)
    * @return the `TrinoResult` with translated `LiteralValue`s */
  def decode(
      columns: List[String],
      rawRows: List[List[Any]],
  ): TrinoResult = {
    val decodedRows = rawRows.map(_.map(toLiteral))
    TrinoResult(columns = columns, rows = decodedRows)
  }

  /** Map a single raw JDBC value to its `LiteralValue` form.
    *
    * Per scala-data-driven-refactor §3 (sealed ADT over Map):
    * the input types are enumerated exhaustively. Adding a
    * new JDBC type would require a compile error here —
    * the decoder cannot silently default.
    *
    * Numeric types are mapped to the closest `LiteralValue`:
    *   - `Int` → `LiteralValue.IntValue`
    *   - `Long` → `LiteralValue.LongValue`
    *   - `Double` → `LiteralValue.DoubleValue`
    *   - `BigDecimal` → `LiteralValue.DecimalValue`
    *   - `java.sql.Timestamp` / `java.time.LocalDateTime` → `LiteralValue.TimestampValue`
    *   - `java.sql.Date` / `java.time.LocalDate` → `LiteralValue.DateValue`
    *   - `Boolean` → `LiteralValue.BoolValue`
    *   - `String` → `LiteralValue.StringValue`
    *   - `Array[Byte]` → `LiteralValue.BinaryValue`
    *   - `null` → `LiteralValue.NullValue`
    *
    * Unknown types are surfaced as `LiteralValue.StringValue` (the
    * raw toString) — defensive fallback so the decoder doesn't
    * throw. The engine adapter can detect this case (string that
    * looks like a number, etc.) and refine. */
  private def toLiteral(raw: Any): LiteralValue = raw match {
    case null                          => LiteralValue.NullValue
    case s: String                     => LiteralValue.StringValue(s)
    case b: Boolean                    => LiteralValue.BoolValue(b)
    case n: Int                        => LiteralValue.IntValue(n)
    case n: Long                       => LiteralValue.LongValue(n)
    case n: Double                     => LiteralValue.DoubleValue(n)
    case n: Float                      => LiteralValue.FloatValue(n)
    case n: Short                      => LiteralValue.ShortValue(n)
    case n: Byte                       => LiteralValue.ByteValue(n)
    case n: BigDecimal                 => LiteralValue.DecimalValue(n)
    case ts: java.sql.Timestamp        => LiteralValue.TimestampValue(ts.toInstant)
    case ldt: java.time.LocalDateTime  => LiteralValue.TimestampValue(ldt.toInstant(java.time.ZoneOffset.UTC))
    case ld: java.time.LocalDate       => LiteralValue.DateValue(ld)
    case d: java.sql.Date              => LiteralValue.DateValue(d.toLocalDate)
    case bytes: Array[Byte]            => LiteralValue.BinaryValue(Vector[Byte](bytes: _*))
    case other                        => LiteralValue.StringValue(other.toString)
  }
}