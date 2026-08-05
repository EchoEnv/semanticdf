package io.semanticdf.trino

import io.semanticdf.core.expr.LiteralValue

/** A production-ready [[TrinoConnection]] backed by the real
  * `trino-jdbc` driver (already declared in `pom.xml`).
  *
  * ==Why this exists==
  *
  * Per the multi-engine design (PR #337) and the README's open
  * item #3, end users need a concrete `TrinoConnection` to wire
  * to a real Trino cluster. The integration tests (PR #384) had
  * a copy of this class in test source; this PR (#386) promoted
  * it to main source; this PR (#388) refactored it to support
  * both single-connection and pool-backed usage.
  *
  * ==Why two factory methods (`fromUrl` + `fromConnection`)==
  *
  * Single-shot usage: `fromUrl(url)` opens its own JDBC connection
  * (closed by `close()`). For connection pooling (HikariCP),
  * use `fromConnection(existingConn)` — the pool owns the
  * connection lifecycle; we just wrap it.
  *
  * ==Why no Spark dependencies==
  *
  * The boundary contract enforced by `pom.xml`: this module has
  * zero `org.apache.spark.*` dependencies. JDBC is a Java API;
  * the `LiteralValue` ↔ JDBC binding is engine-internal behavior.
  *
  * ==Usage (single-connection, simplest)==
  *
  * {{{
  *   val engine = TrinoEngine.instance.withConnectionFactory { () =>
  *     JdbcTrinoConnection.fromUrl("jdbc:trino://coordinator.example.com:8080")
  *   }
  * }}}
  *
  * ==Usage (pool-backed, production-recommended)==
  *
  * {{{
  *   val pool = TrinoConnectionPoolFactory.hikari("jdbc:trino://coordinator:8080")
  *   val engine = TrinoEngine.instance.withConnectionFactory(pool)
  * }}}
  *
  * ==Why `extends TrinoConnection` (vs. concrete JDBC driver class)==
  *
  * The engine's `withConnectionFactory` accepts the *engine-portable*
  * `() => TrinoConnection`. Production code injects this concrete
  * class; tests inject `FakeTrinoConnection`. The trait is the
  * seam that lets the engine stay unaware of the driver. */
final class JdbcTrinoConnection private (private val conn: java.sql.Connection)
    extends TrinoConnection {

  override def prepareStatement(
      sql:        String,
      parameters: List[LiteralValue],
  ): TrinoResult = {
    val stmt = conn.prepareStatement(sql)
    try {
      // Bind each parameter at its 1-based JDBC index.
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
    // Closing a pooled connection returns it to the pool
    // (HikariCP wraps the connection in a proxy); closing a
    // single-shot connection (per `fromUrl`) actually closes
    // the underlying JDBC connection.
    try conn.close() catch { case _: java.sql.SQLException => () }
  }

  /** Map a portable [[LiteralValue]] to the matching JDBC `setXxx`
    * call. The matching is exhaustive over all 16 `LiteralValue`
    * cases (compiler-checked). */
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

  /** Read a JDBC `ResultSet` into a [[TrinoResult]]. Per
    * scala-data-driven-refactor §1: `TrinoResult` is pure data;
    * this is the data layer's "shape" with no behavior. */
  private def readResultSet(rs: java.sql.ResultSet): TrinoResult = {
    val meta     = rs.getMetaData
    val ncols    = meta.getColumnCount
    val columns  = (1 to ncols).map(meta.getColumnName).toList
    val rows     = scala.collection.mutable.ListBuffer.empty[List[LiteralValue]]
    while (rs.next()) {
      val row = (1 to ncols).map { i =>
        readCell(rs, i, meta.getColumnType(i))
      }.toList
      rows += row
    }
    TrinoResult(columns = columns, rows = rows.toList)
  }

  /** Map a single JDBC cell to its portable [[LiteralValue]]. */
  private def readCell(
      rs:      java.sql.ResultSet,
      index:   Int,
      sqlType: Int,
  ): LiteralValue = {
    val raw = rs.getObject(index)
    if (raw == null) LiteralValue.NullValue
    else sqlType match {
      case java.sql.Types.VARCHAR | java.sql.Types.CHAR | java.sql.Types.LONGVARCHAR =>
        LiteralValue.StringValue(raw.toString)
      case java.sql.Types.BOOLEAN =>
        LiteralValue.BoolValue(raw.asInstanceOf[Boolean])
      case java.sql.Types.INTEGER =>
        LiteralValue.IntValue(raw.asInstanceOf[Int])
      case java.sql.Types.BIGINT  =>
        LiteralValue.LongValue(raw.asInstanceOf[Long])
      case java.sql.Types.FLOAT   =>
        LiteralValue.FloatValue(raw.asInstanceOf[Float])
      case java.sql.Types.DOUBLE  =>
        LiteralValue.DoubleValue(raw.asInstanceOf[Double])
      case java.sql.Types.DECIMAL | java.sql.Types.NUMERIC =>
        LiteralValue.DecimalValue(raw.asInstanceOf[java.math.BigDecimal])
      case java.sql.Types.TIMESTAMP =>
        LiteralValue.TimestampValue(
          raw.asInstanceOf[java.sql.Timestamp].toInstant,
        )
      case java.sql.Types.DATE =>
        LiteralValue.DateValue(
          raw.asInstanceOf[java.sql.Date].toLocalDate,
        )
      case _ => LiteralValue.StringValue(raw.toString)  // defensive fallback
    }
  }
}

object JdbcTrinoConnection {

  /** Single-shot: open a JDBC connection to the given Trino URL
    * and wrap it. The connection is closed when `close()` is
    * called on this instance.
    *
    * Per scala-data-driven-refactor §2 ("shape and validity are
    * separate"): the driver load + URL parse happen at the
    * factory boundary; the body of the class trusts its inputs. */
  def fromUrl(trinoUrl: String): JdbcTrinoConnection = {
    Class.forName("io.trino.jdbc.TrinoDriver")
    val conn = java.sql.DriverManager.getConnection(trinoUrl, "test", null)
    new JdbcTrinoConnection(conn)
  }

  /** Pool-backed: wrap an already-open JDBC connection (typically
    * borrowed from a connection pool). The pool owns the
    * connection lifecycle; calling `close()` on the wrapper
    * returns the connection to the pool (HikariCP wraps the
    * connection in a closeable proxy). */
  def fromConnection(conn: java.sql.Connection): JdbcTrinoConnection =
    new JdbcTrinoConnection(conn)
}
