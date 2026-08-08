package io.semanticdf.postgresql

import java.sql.{Connection, DriverManager, ResultSet, SQLException}

import io.semanticdf.core.engine.ResolvedSchema

/** Concrete [[PostgreSqlClient]] implementation backed by the standard
  * JDBC API + the PostgreSQL JDBC driver (`org.postgresql:postgresql`).
  *
  * Mirrors `io.semanticdf.trino.JdbcTrinoConnection` (the production
  * JDBC impl for Trino) in shape.
  *
  * ==Why the standard JDBC API (not jOOQ / Spring JDBC / etc.)==
  *
  * Per karpathy §2 ("minimum code that solves the problem"): the
  * JDK's `java.sql` package is sufficient for the operations we need
  * (execute, describe, create, drop, update). Adding jOOQ would
  * pull a new dependency for no real benefit on a v1 adapter.
  *
  * ==Error handling==
  *
  * Per `docs/design/error-handling-style.md`:
  *
  *   - NO `catch { case _: Exception => ... }` (catch-all banned).
  *   - Catch SPECIFIC exception types: `SQLException` (with
  *     SQLState inspection for typed cases), `IOException`,
  *     `InterruptedException`.
  *   - Map JDBC SQLStates to SPECIFIC `PostgreSqlError` cases
  *     (no generic `ServerError`).
  *
  * ==CAS via `xmin`==
  *
  * PostgreSQL's `xmin` is a system column (transaction ID) that
  * increments on every row update. We use it as the version for
  * CompareAndSet: the catalog adapter reads `xmin` for the
  * manifest row, then on `CompareAndSet` runs an UPDATE with a
  * WHERE clause `xmin = expected`. If 1 row updated, CAS
  * succeeded; if 0 rows updated, CAS failed (→ `CasConflict`).
  *
  * ==Boundary contract==
  *
  * Zero Spark imports. */
final class JdbcPostgreSqlClient(
    jdbcUrl:    String,
    user:       String,
    password:   String,
) extends PostgreSqlClient {

  // Per error-handling-style.md: programmer errors at boundary
  // throw `IllegalArgumentException`. The caller is misusing the
  // adapter; this is a setup bug.
  if (jdbcUrl.isEmpty)  throw new IllegalArgumentException("JdbcPostgreSqlClient: jdbcUrl must not be empty")
  if (user.isEmpty)     throw new IllegalArgumentException("JdbcPostgreSqlClient: user must not be empty")
  if (password.isEmpty) throw new IllegalArgumentException("JdbcPostgreSqlClient: password must not be empty (use empty-password JDBC URLs for trust auth)")

  // The connection is opened lazily on first query. Per the JDBC
  // contract, the driver registers itself via the ServiceLoader
  // when the class is loaded. We force the load to surface
  // ClassNotFoundException at construction time (not on first query).
  Class.forName("org.postgresql.Driver")

  private var connection: Connection = _

  /** Get or create the connection. Per error-handling-style.md:
    * catch SPECIFIC exception types. */
  private def getConnection(): Either[PostgreSqlError, Connection] = {
    if (connection != null && !connection.isClosed) Right(connection)
    else {
      try {
        connection = DriverManager.getConnection(jdbcUrl, user, password)
        Right(connection)
      } catch {
        case e: SQLException =>
          Left(sqlExceptionToError(e, "connect"))
      }
    }
  }

  override def executeQuery(
      sql:    String,
      params: Seq[Any] = Seq.empty,
  ): Either[PostgreSqlError, PostgreSqlResult] = {
    // Per error-handling-style.md: 3+ step operation (connect +
    // prepare + execute + extract) → use for-comprehension.
    getConnection().flatMap { conn =>
      try {
        val ps = conn.prepareStatement(sql)
        try {
          // Bind parameters (1-indexed in JDBC).
          params.zipWithIndex.foreach { case (v, i) =>
            ps.setObject(i + 1, v)
          }
          val rs = ps.executeQuery()
          try {
            val meta     = rs.getMetaData
            val colNames = (1 to meta.getColumnCount).map(meta.getColumnName).toList
            val colTypes = (1 to meta.getColumnCount).map(meta.getColumnTypeName).toList
            val cols     = colNames.zip(colTypes).map { case (n, t) =>
              PostgreSqlColumn(name = n, dataType = t, nullable = true)
            }
            // Per scala-data-driven-refacer §1: pure data, no
            // behavior. Build rows as Map[String, Any] (same pattern
            // as the HeraResultEncoder).
            val rows = scala.collection.mutable.ListBuffer.empty[Map[String, Any]]
            while (rs.next()) {
              val row = scala.collection.mutable.LinkedHashMap.empty[String, Any]
              cols.foreach { c =>
                row += (c.name -> rs.getObject(c.name))
              }
              rows += row.toMap
            }
            Right(PostgreSqlResult(cols, rows.toList))
          } finally rs.close()
        } catch {
          case e: SQLException =>
            Left(sqlExceptionToError(e, "executeQuery"))
        } finally ps.close()
      } catch {
        case e: SQLException =>
          Left(sqlExceptionToError(e, "executeQuery"))
      }
    }
  }

  override def describeTable(
      schema: String,
      table:  String,
  ): Either[PostgreSqlError, ResolvedSchema] = {
    // Per the standard, the `DatabaseMetaData` API is the JDBC-
    // standard way to read schema. Works on both real PG and H2-PG.
    getConnection().flatMap { conn =>
      try {
        val meta = conn.getMetaData
        val rs = meta.getColumns(null, schema, table, null)
        try {
          val cols = scala.collection.mutable.LinkedHashMap.empty[String, String]
          while (rs.next()) {
            val name = rs.getString("COLUMN_NAME")
            val typ  = rs.getString("TYPE_NAME")
            if (name != null && typ != null) cols += (name -> typ)
          }
          if (cols.isEmpty) {
            // Per the standard: "may not exist" at the boundary →
            // typed `Left(TableNotFound)`, not `Option` (this is a
            // computation, not a pure lookup).
            Left(PostgreSqlError.TableNotFound(reason = s"table '$schema.$table' not found"))
          } else {
            Right(ResolvedSchema(cols.toMap))
          }
        } finally rs.close()
      } catch {
        case e: SQLException =>
          Left(sqlExceptionToError(e, "describeTable"))
      }
    }
  }

  override def createTable(
      schema:  String,
      table:   String,
      columns: List[PostgreSqlColumn],
  ): Either[PostgreSqlError, Unit] = {
    // Per the standard: 1-step (the execute) → direct match.
    val columnDefs = columns.map { c =>
      s""""${c.name}" ${pgTypeForCreate(c.dataType)}${if (c.nullable) "" else " NOT NULL"}""""
    }.mkString(", ")
    val sql = s"""CREATE TABLE "$schema"."$table" ($columnDefs, "xmin_lock" BIGINT)"""
    executeQuery(sql).map(_ => ())
  }

  override def dropTable(
      schema: String,
      table:  String,
  ): Either[PostgreSqlError, Unit] = {
    val sql = s"""DROP TABLE IF EXISTS "$schema"."$table" """
    executeQuery(sql).map(_ => ())
  }

  override def getTableVersion(
      schema: String,
      table:  String,
  ): Either[PostgreSqlError, Long] = {
    // Per the standard: 1-step (the read) → direct match.
    val sql = s"""SELECT xmin FROM "$schema"."$table" WHERE xmin_lock = 1 LIMIT 1"""
    executeQuery(sql).flatMap { result =>
      result.rows.headOption match {
        case Some(row) =>
          row.get("xmin") match {
            case Some(n: Long) => Right(n)
            case Some(n: Int)  => Right(n.toLong)
            case Some(n: Number) => Right(n.longValue)
            case other => Left(PostgreSqlError.MalformedResponse(reason = s"xmin is not a number: $other"))
          }
        case None =>
          // No row with xmin_lock = 1 means the table is empty or
          // has no manifest row. Treat as TableNotFound for the
          // catalog adapter's CAS purposes.
          Left(PostgreSqlError.TableNotFound(reason = s"no manifest row in '$schema.$table'"))
      }
    }
  }

  override def casUpdate(
      schema:         String,
      table:          String,
      expectedXmin:   Long,
      newContent:     String,
  ): Either[PostgreSqlError, Long] = {
    // PG's standard optimistic-concurrency pattern: UPDATE with
    // a WHERE clause that includes the expected xmin. If 0 rows
    // are affected, the CAS failed (someone else updated the row).
    val sql = s"""UPDATE "$schema"."$table" SET "xmin_lock" = ?, "content" = ? WHERE xmin = ?"""
    getConnection().flatMap { conn =>
      try {
        val ps = conn.prepareStatement(sql)
        try {
          ps.setLong(1, expectedXmin + 1)  // we bump to the next version
          ps.setString(2, newContent)
          ps.setLong(3, expectedXmin)
          val affected = ps.executeUpdate()
          if (affected == 0) {
            Left(PostgreSqlError.CasConflict(reason = s"xmin $expectedXmin no longer current in '$schema.$table'"))
          } else {
            Right(expectedXmin + 1)
          }
        } catch {
          case e: SQLException =>
            Left(sqlExceptionToError(e, "casUpdate"))
        } finally ps.close()
      } catch {
        case e: SQLException =>
          Left(sqlExceptionToError(e, "casUpdate"))
      }
    }
  }

  override def close(): Unit = {
    if (connection != null) {
      try connection.close() catch {
        case _: SQLException => ()  // close-time errors are not actionable
      }
      connection = null
    }
  }

  // -- SQLState → typed error mapping --

  /** Map a `SQLException` to a SPECIFIC `PostgreSqlError` case.
    *
    * Per error-handling-style.md "Hard bans": SPECIFIC failure modes
    * (no generic `ServerError`). The PG SQLState classes are:
    *   08 — connection exception
    *   23 — integrity constraint violation
    *   28 — authorization exception
    *   42 — syntax error or access rule violation
    */
  private[postgresql] def sqlExceptionToError(
      e:      SQLException,
      action: String,
  ): PostgreSqlError = {
    val state = Option(e.getSQLState).getOrElse("")
    state match {
      case s if s.startsWith("08") =>
        PostgreSqlError.ConnectionFailed(reason = s"$action: ${e.getMessage}")
      case "28000" =>
        PostgreSqlError.AuthenticationFailed(reason = s"$action: ${e.getMessage}")
      case "42P01" =>
        PostgreSqlError.TableNotFound(reason = s"$action: ${e.getMessage}")
      case "42703" =>
        PostgreSqlError.ColumnNotFound(reason = s"$action: ${e.getMessage}")
      case s if s.startsWith("42") =>
        PostgreSqlError.SyntaxError(reason = s"$action: ${e.getMessage}")
      case "23505" =>
        PostgreSqlError.UniqueViolation(reason = s"$action: ${e.getMessage}")
      case "23514" =>
        PostgreSqlError.CheckViolation(reason = s"$action: ${e.getMessage}")
      case _ =>
        // Unknown SQLState — fall back to MalformedResponse per
        // the standard's "treat unknown as MalformedResponse so
        // callers see the actual status code" rule (per the
        // existing pattern in HttpHeraClient.sendJson).
        PostgreSqlError.MalformedResponse(reason = s"$action: SQLState=$state msg=${e.getMessage}")
    }
  }

  /** Map a portable type name (e.g. "integer", "text", "numeric")
    * to a PG DDL type name. For v1, we trust the input type
    * verbatim (the resolver produces a known set; we don't
    * translate at this layer). Future PRs can add the type
    * mapping here. */
  private[postgresql] def pgTypeForCreate(t: String): String = t
}

object JdbcPostgreSqlClient {

  /** Smart constructor. Per the standard, throws
    * `IllegalArgumentException` for programmer errors (empty URL,
    * empty user, empty password). Runtime errors (connection
    * failure) surface on the first `executeQuery` call. */
  def apply(
      jdbcUrl:  String,
      user:     String,
      password: String,
  ): JdbcPostgreSqlClient = new JdbcPostgreSqlClient(jdbcUrl, user, password)

  /** The standard H2-in-PostgreSQL-mode URL for unit tests.
    *
    * Per the existing pattern (per `docs/agents/embedding-data-platforms.md`
    * §JDBC): unit tests use H2 in PG compatibility mode. No real
    * PG needed. */
  def h2TestUrl(uniqueDb: String): String =
    s"jdbc:h2:mem:$uniqueDb;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1"
}