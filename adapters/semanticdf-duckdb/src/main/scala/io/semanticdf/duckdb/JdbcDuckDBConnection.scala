package io.semanticdf.duckdb

import io.semanticdf.core.expr.LiteralValue

import java.math.BigDecimal
import java.sql.{Connection => JdbcConn, DriverManager, PreparedStatement, ResultSet, ResultSetMetaData, Timestamp, Types}
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.util.UUID

/** Concrete [[DuckDBConnection]] implementation backed by the
  * DuckDB JDBC driver (`org.duckdb:duckdb_jdbc`).
  *
  * ==Why JDBC (not the in-process API)==
  *
  * Per karpathy §2 ("minimum code that solves the problem"):
  * the standard JDBC driver is sufficient for DuckDB — the
  * `org.duckdb:duckdb_jdbc` artifact exposes both the embedded
  * engine (URL `jdbc:duckdb:`) and the server protocol (URL
  * `jdbc:duckdb://host:port/...`). Same code path, same
  * `PreparedStatement` semantics. Adding the in-process API
  * as a second impl would be premature.
  *
  * ==Why two factories (fromUrl + fromConnection)==
  *
  * Mirrors `JdbcTrinoConnection.fromUrl` / `.fromConnection`
  * (PR #389): one-shot URL-based + pool-friendly
  * connection-passing. Production users wire the pool via
  * `DuckDBConnectionPoolFactory.hikari(...)` which calls
  * `fromConnection(conn)` on each borrowed connection.
  *
  * ==Why a hard-coded timeout / no retry==
  *
  * Per the user's "monitor memory, disk first" constraint: a
  * hung JDBC call would leak the connection. We rely on the
  * DuckDB driver for query timeout (set via session options if
  * needed) and the engine's `finally` close to bound the
  * lifetime. No retry layer here — the engine's caller decides
  * retry policy (matches the Trino adapter).
  *
  * ==Why no auth header (yet)==
  *
  * DuckDB has no HTTP-style auth — the JDBC URL is the entire
  * auth surface (file path or server URL + optional credential
  * file via `JASYPT` extensions, not standard). Embedded mode
  * has no auth. Production server deployments use network ACLs
  * or DuckDB's Enterprise auth; both are out of scope for v1.
  *
  * ==Boundary contract==
  *
  * Zero Spark imports. Verifiable by:
  * `grep -r 'org.apache.spark' adapters/semanticdf-duckdb/src/main/scala/io/semanticdf/duckdb/JdbcDuckDBConnection.scala`
  */
final class JdbcDuckDBConnection private (
    private val jdbcConn: JdbcConn,
) extends DuckDBConnection {

  override def prepareStatement(
      sql:        String,
      parameters: Seq[Any] = Nil,
  ): DuckDBResult = {
    val ps: PreparedStatement = jdbcConn.prepareStatement(sql)
    try {
      // Bind parameters positionally (1-indexed JDBC convention).
      parameters.zipWithIndex.foreach { case (value, i) =>
        bindParameter(ps, i + 1, value)
      }
      val rs:        ResultSet          = ps.executeQuery()
      val meta:      ResultSetMetaData  = rs.getMetaData
      val columnCount = meta.getColumnCount
      val columns = (1 to columnCount).map(meta.getColumnLabel).toList
      val rows = scala.collection.mutable.ListBuffer.empty[List[LiteralValue]]
      while (rs.next()) {
        val row = (1 to columnCount).map { i =>
          JdbcDuckDBConnection.readCell(rs, i, meta.getColumnType(i))
        }.toList
        rows += row
      }
      DuckDBResult(columns, rows.toList)
    } finally {
      ps.close()
    }
  }

  /** Bind a Scala value to a JDBC `?` placeholder at `idx`
    * (1-indexed). Handles the common scalar types and falls
    * back to `setObject` for anything else.
    *
    * ==Why explicit per-type binding==
    *
    * DuckDB is strict about type coercion (unlike Trino, which
    * is more forgiving). `setString` for an INT column throws
    * "Conversion Error". Per karpathy §3 ("touch only what you
    * must"): explicit mapping is the minimum code that makes
    * DuckDB happy. */
  private def bindParameter(ps: PreparedStatement, idx: Int, value: Any): Unit = value match {
    case null                       => ps.setNull(idx, Types.NULL)
    case None                      => ps.setNull(idx, Types.NULL)
    case Some(v)                   => bindParameter(ps, idx, v)
    case s: String                  => ps.setString(idx, s)
    case b: Boolean                 => ps.setBoolean(idx, b)
    case i: Int                     => ps.setInt(idx, i)
    case l: Long                    => ps.setLong(idx, l)
    case f: Float                   => ps.setFloat(idx, f)
    case d: Double                  => ps.setDouble(idx, d)
    case bd: BigDecimal             => ps.setBigDecimal(idx, bd)
    case ts: Timestamp              => ps.setTimestamp(idx, ts)
    case ldt: LocalDateTime         => ps.setTimestamp(idx, Timestamp.valueOf(ldt))
    case ld: LocalDate              => ps.setDate(idx, java.sql.Date.valueOf(ld))
    case lt: LocalTime              => ps.setTime(idx, java.sql.Time.valueOf(lt))
    case other                      => ps.setObject(idx, other)
  }

  /** Close the underlying JDBC connection.
    *
    * For connections opened via `fromUrl`, this terminates the
    * underlying conn.
    *
    * For connections wrapped via `fromConnection` (typical pool
    * use case — HikariCP, etc.), the pool owns the connection
    * lifecycle: the wrapped conn is a Hikari proxy whose
    * `close()` returns it to the pool rather than terminating
    * it. This matches the `JdbcTrinoConnection` contract
    * (`trino/src/main/scala/io/semanticdf/trino/JdbcTrinoConnection.scala:75-81`).
    *
    * Per H1 revert (2026-08-11): an earlier flag-based fix
    * (`ownsConnection`) was reverted because making
    * `close()` a no-op for pooled connections leaked them —
    * the pool's "release-on-close" contract was being bypassed.
    * Closing the proxy via the standard `Connection.close()`
    * contract is the correct pool-aware behavior. */
  override def close(): Unit = {
    try { jdbcConn.close() } catch { case _: java.sql.SQLException => () }
  }
}

object JdbcDuckDBConnection {

  /** Smart constructor — preferred over `new JdbcDuckDBConnection(...)`
    * because it lets future default-argument expansion (e.g.
    * `queryTimeout: Duration = 30s`) not break call sites. */
  def apply(url: String): JdbcDuckDBConnection = fromUrl(url)

  /** Build a connection from a JDBC URL. Examples:
    *
    *   - In-memory: `"jdbc:duckdb:"`
    *   - File-based: `"jdbc:duckdb:/tmp/orders.db"`
    *   - Server:    `"jdbc:duckdb://host:port/mydb"`
    *
    * The connection is auto-commit (`true`) — the engine
    * doesn't need transactional semantics for SELECT queries.
    * For DDL (CREATE TABLE in test setup), explicit commit is
    * handled by the caller via the same connection's
    * `connection.commit()`. */
  def fromUrl(url: String): JdbcDuckDBConnection = {
    // DuckDB's JDBC driver auto-registers via ServiceLoader.
    // We still call Class.forName to fail fast if the driver
    // isn't on the classpath.
    //
    // Per debug-mantra §3 ("falsify"): DuckDB's JDBC driver
    // does NOT support the standard `loginTimeout` property
    // (it rejects unknown options at startup). The property
    // was removed in PR #396 — we use a plain URL with no
    // Properties instead. JDBC timeout (if needed) can be
    // set via DuckDB session options in a future PR.
    Class.forName("org.duckdb.DuckDBDriver")
    val conn = DriverManager.getConnection(url)
    new JdbcDuckDBConnection(conn)
  }

  /** Build a connection from a pre-existing JDBC connection
    * (typical pool use case — HikariCP, etc.). The pool owns
    * the connection lifecycle; calling `close()` on the wrapper
    * returns the connection to the pool (HikariCP wraps the
    * connection in a closeable proxy whose `close()` releases
    * rather than terminates).
    *
    * This matches the `JdbcTrinoConnection.fromConnection` contract. */
  def fromConnection(conn: JdbcConn): JdbcDuckDBConnection =
    new JdbcDuckDBConnection(conn)

  /** Read a single cell from a ResultSet, mapping the JDBC
    * native type to the portable `LiteralValue` sealed ADT.
    *
    * ==Why a per-type read helper==
    *
    * DuckDB returns rich types (DECIMAL, TIMESTAMP, ARRAY,
    * STRUCT, MAP, JSON). Each maps to a different
    * `LiteralValue` case. Per scala-data-driven-refacer §1:
    * the mapping happens at the engine boundary (here), the
    * consumer sees portable `LiteralValue` only. */
  private[duckdb] def readCell(rs: ResultSet, idx: Int, sqlType: Int): LiteralValue = {
    val v = rs.getObject(idx)
    if (v == null) LiteralValue.NullValue
    else sqlType match {
      case Types.BOOLEAN | Types.BIT                  => LiteralValue.BoolValue(v.asInstanceOf[Boolean])
      case Types.TINYINT | Types.SMALLINT | Types.INTEGER
         | Types.BIGINT                              => LiteralValue.LongValue(v.asInstanceOf[Long])
      case Types.REAL | Types.FLOAT | Types.DOUBLE    => LiteralValue.DoubleValue(v.asInstanceOf[Double])
      case Types.DECIMAL | Types.NUMERIC              => LiteralValue.DecimalValue(v.asInstanceOf[BigDecimal])
      case Types.VARCHAR | Types.CHAR | Types.LONGVARCHAR
         | Types.NVARCHAR | Types.NCHAR | Types.LONGNVARCHAR
                                                   => LiteralValue.StringValue(v.toString)
      case Types.TIMESTAMP | Types.TIMESTAMP_WITH_TIMEZONE
                                                   => LiteralValue.TimestampValue(v.asInstanceOf[Timestamp].toInstant)
      case Types.DATE                                 => LiteralValue.DateValue(v.asInstanceOf[java.sql.Date].toLocalDate)
      case Types.TIME | Types.TIME_WITH_TIMEZONE      => LiteralValue.StringValue(v.toString)
      case Types.BINARY | Types.VARBINARY | Types.LONGVARBINARY
                                                   => LiteralValue.StringValue(java.util.Base64.getEncoder.encodeToString(v.asInstanceOf[Array[Byte]]))
      case other =>
        // ARRAY, STRUCT, MAP, JSON, UUID, etc. — DuckDB-specific
        // complex types. We serialize via toString for now; a
        // future PR can add per-type handling (e.g.
        // LiteralValue.ArrayValue).
        LiteralValue.StringValue(v.toString)
    }
  }
}