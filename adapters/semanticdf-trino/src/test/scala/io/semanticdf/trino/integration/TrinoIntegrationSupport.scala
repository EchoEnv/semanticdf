package io.semanticdf.trino.integration

import io.semanticdf.core.expr.LiteralValue
import io.semanticdf.trino.{TrinoConnection, TrinoEngine, TrinoResult}

/** Wires a `TrinoEngine` against a **real Trino cluster** via the
  * `trino-jdbc` driver on the classpath.
  *
  * This is the test-side analog of the (future) `JdbcTrinoConnection`
  * — production would hold an `io.trino.jdbc.TrinoConnection` (NOT
  * `io.semanticdf.trino.TrinoConnection`, the engine boundary trait).
  * For now, the integration tests are the only consumer of this
  * wiring. A future PR will extract the production version.
  *
  * ==Why this support class lives in tests, not main==
  *
  * Per scala-data-driven-refactor §1: "behavior lives elsewhere."
  * The real-JDBC wiring is *test / dev setup*, not engine code.
  * Production users will eventually get their own connection
  * factory (e.g. `JdbcTrinoConnection` in the main source).
  *
  * ==Why it returns a `TrinoEngine`==
  *
  * Test consumers (TrinoIntegrationSpec) want to exercise the full
  * `compile → execute → result` flow, not raw JDBC. Going through
  * the engine lets the test detect any regression in compile or
  * execute (not just in the bootstrap).
  *
  * ==Why a fresh engine per call==
  *
  * Per the standing pattern: tests are independent. Each call
  * produces a new engine — no shared mutable state, no leaked
  * connection. The cost is ~1ms per test. */
object TrinoIntegrationSupport {

  /** Build a TrinoEngine wired to a real Trino cluster at
    * `trinoUrl`. Each `execute()` opens a fresh JDBC connection
    * (closed via `finally`). */
  def engineWithConnection(trinoUrl: String): TrinoEngine = {
    val connectionFactory: () => TrinoConnection = () =>
      new JdbcTrinoConnection(trinoUrl)
    new TrinoEngine().withConnectionFactory(connectionFactory)
  }

  /** A `TrinoConnection` backed by the real Trino JDBC driver.
    *
    * Maps the engine-portable contract:
    *   - `prepareStatement(sql, parameters)` → JDBC `prepareStatement`,
    *     bind each `LiteralValue`, call `executeQuery()`, return
    *     `TrinoResult`.
    *   - `close()` → close the underlying JDBC connection.
    *
    * Per Trino 435's MEMORY connector, schemas must be explicitly
    * created. We pin the connection to schema `semanticdf` via
    * the URL fragment (`jdbc:trino://...?schema=semanticdf`).
    * If the schema doesn't exist at bootstrap time, the fixture
    * `CREATE SCHEMA`s it (idempotent).
    */
  final class JdbcTrinoConnection(trinoUrl: String) extends TrinoConnection {
    private val conn: java.sql.Connection = {
      Class.forName("io.trino.jdbc.TrinoDriver")
      // Trino JDBC 408 doesn't support `?schema=` URL parameter.
      // We qualify each table with `catalog.schema.table` instead.
      java.sql.DriverManager.getConnection(
        s"$trinoUrl/memory", "test", null,
      )
    }

    override def prepareStatement(
        sql:        String,
        parameters: List[LiteralValue],
    ): TrinoResult = {
      val stmt = conn.prepareStatement(sql)
      try {
        // Bind each parameter at its 1-based index.
        parameters.zipWithIndex.foreach { case (param, i) =>
          bindLiteral(stmt, i + 1, param)
        }
        val rs = stmt.executeQuery()
        try {
          readResultSet(rs)
        } finally {
          rs.close()
        }
      } finally {
        stmt.close()
      }
    }

    override def close(): Unit = {
      try conn.close() catch { case _: java.sql.SQLException => () }
    }

    /** Map a portable `LiteralValue` to the JDBC bind call. This
      * is the test-side mirror of the production bridge. Each
      * JDBC type is bound via the matching `setXxx` call. */
    private def bindLiteral(
        stmt:  java.sql.PreparedStatement,
        index: Int,
        value: LiteralValue,
    ): Unit = value match {
      case LiteralValue.StringValue(s)   => stmt.setString(index, s)
      case LiteralValue.BoolValue(b)     => stmt.setBoolean(index, b)
      case LiteralValue.IntValue(n)      => stmt.setInt(index, n)
      case LiteralValue.LongValue(n)     => stmt.setLong(index, n)
      case LiteralValue.FloatValue(n)    => stmt.setFloat(index, n)
      case LiteralValue.DoubleValue(n)   => stmt.setDouble(index, n)
      case LiteralValue.DecimalValue(d)  => stmt.setBigDecimal(index, d.bigDecimal)
      case LiteralValue.BinaryValue(b)   => stmt.setBytes(index, b.toArray)
      case LiteralValue.TimestampValue(ts) =>
        stmt.setTimestamp(index, java.sql.Timestamp.from(ts))
      case LiteralValue.DateValue(d)     =>
        stmt.setDate(index, java.sql.Date.valueOf(d.toString))
      case LiteralValue.ArrayValue(values) =>
        stmt.setString(index, values.mkString("[", ",", "]"))
      case LiteralValue.MapValue(values) =>
        val rendered = values
          .map { case (k, v) => s"${k.toString}:${v.toString}" }
          .mkString("{", ",", "}")
        stmt.setString(index, rendered)
      case LiteralValue.StructValue(fields) =>
        val rendered = fields
          .map { case (name, v) => s""""$name":${v.toString}""" }
          .mkString("{", ",", "}")
        stmt.setString(index, rendered)
      case LiteralValue.NullValue => stmt.setNull(index, java.sql.Types.NULL)
    }

    /** Read a JDBC ResultSet into a `TrinoResult`. The decoder
      * here uses the engine-portable `LiteralValue` subtypes. */
    private def readResultSet(rs: java.sql.ResultSet): TrinoResult = {
      val meta     = rs.getMetaData
      val ncols    = meta.getColumnCount
      val columns  = (1 to ncols).map(meta.getColumnName).toList
      val rows     = scala.collection.mutable.ListBuffer.empty[List[LiteralValue]]
      while (rs.next()) {
        val row = (1 to ncols).map { i => readCell(rs, i, meta.getColumnType(i)) }.toList
        rows += row
      }
      TrinoResult(columns = columns, rows = rows.toList)
    }

    /** Map a single JDBC cell to its portable `LiteralValue`. */
    private def readCell(
        rs:    java.sql.ResultSet,
        index: Int,
        sqlType: Int,
    ): LiteralValue = {
      val raw = rs.getObject(index)
      if (raw == null) LiteralValue.NullValue
      else sqlType match {
        case java.sql.Types.VARCHAR | java.sql.Types.CHAR | java.sql.Types.LONGVARCHAR =>
          LiteralValue.StringValue(raw.toString)
        case java.sql.Types.BOOLEAN => LiteralValue.BoolValue(raw.asInstanceOf[Boolean])
        case java.sql.Types.INTEGER => LiteralValue.IntValue(raw.asInstanceOf[Int])
        case java.sql.Types.BIGINT  => LiteralValue.LongValue(raw.asInstanceOf[Long])
        case java.sql.Types.FLOAT   => LiteralValue.FloatValue(raw.asInstanceOf[Float])
        case java.sql.Types.DOUBLE  => LiteralValue.DoubleValue(raw.asInstanceOf[Double])
        case java.sql.Types.DECIMAL | java.sql.Types.NUMERIC =>
          LiteralValue.DecimalValue(raw.asInstanceOf[java.math.BigDecimal])
        case java.sql.Types.TIMESTAMP =>
          LiteralValue.TimestampValue(raw.asInstanceOf[java.sql.Timestamp].toInstant)
        case java.sql.Types.DATE =>
          // Convert java.sql.Date -> LocalDate via toLocalDate
          LiteralValue.DateValue(
            raw.asInstanceOf[java.sql.Date].toLocalDate
          )
        case _ => LiteralValue.StringValue(raw.toString)  // defensive fallback
      }
    }
  }
}
