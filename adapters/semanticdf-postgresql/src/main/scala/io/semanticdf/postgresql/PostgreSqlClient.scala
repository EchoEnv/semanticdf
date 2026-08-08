package io.semanticdf.postgresql

import io.semanticdf.core.engine.{ResolvedSchema, ResolvedSource}

/** Boundary trait for PostgreSQL data-plane operations.
  *
  * Mirrors `io.semanticdf.trino.TrinoConnection` (in spirit) and
  * `io.semanticdf.hera.HeraClient` (in return type pattern).
  *
  * ==Why a trait (vs. a concrete JDBC client)==
  *
  * Per scala-data-driven-refacer §1 ("data is data, behavior lives
  * elsewhere"): the SHAPE of the contract is here; the BODY (the
  * actual JDBC calls) is in the concrete impl. Tests inject a fake
  * implementation via this trait.
  *
  * ==Why all data-plane methods return `Either[PostgreSqlError, X]`==
  *
  * Per `docs/design/error-handling-style.md`: public APIs must return
  * `Either[L, X]` where `L` is a sealed ADT. [[PostgreSqlError]] is
  * the ADT; it has 12 SPECIFIC failure cases (no catch-all
  * `ServerError`).
  *
  * ==Why no `connect` (vs. Trino's pattern)==
  *
  * Per the user's preference "use the same API as the original Spark
  * library" + karpathy §2 (minimum code that solves the problem):
  * we DON'T expose a connection-establishing method. The concrete
  * impl takes a `DataSource` (or single `Connection`) in its
  * constructor; tests can inject either. Adding a `connect(url)`
  * method is what every other adapter has done and what we should
  * NOT do for v1 (defer to a richer factory if needed).
  *
  * ==Boundary contract==
  *
  * Zero Spark imports. */
trait PostgreSqlClient extends Serializable {

  /** Execute a SQL query against PostgreSQL. Returns the rows + columns.
    *
    * Per error-handling-style.md: returns `Either[PostgreSqlError, X]`.
    * Failures can be: SyntaxError (42601), TableNotFound (42P01),
    * ColumnNotFound (42703), or transport failure.
    *
    * @param sql    the SQL string to execute
    * @param params the parameter values to bind (in order) */
  def executeQuery(
      sql:    String,
      params: Seq[Any] = Seq.empty,
  ): Either[PostgreSqlError, PostgreSqlResult]

  /** Describe a table's schema. Returns a portable [[ResolvedSchema]]
    * (name -> type-string map; per the existing UC pattern).
    *
    * Returns `Left(PostgreSqlError.TableNotFound)` if the table
    * doesn't exist. */
  def describeTable(
      schema: String,
      table:  String,
  ): Either[PostgreSqlError, ResolvedSchema]

  /** Create a new table with the given columns. Returns
    * `Left(PostgreSqlError.UniqueViolation)` if the table already
    * exists. */
  def createTable(
      schema:  String,
      table:   String,
      columns: List[PostgreSqlColumn],
  ): Either[PostgreSqlError, Unit]

  /** Drop a table. Returns `Left(PostgreSqlError.TableNotFound)` if
    * the table doesn't exist. */
  def dropTable(
      schema: String,
      table:  String,
  ): Either[PostgreSqlError, Unit]

  /** Get a table's current `xmin` (PostgreSQL's row-version system
    * column; transaction ID that increments on every update).
    *
    * Used by the catalog adapter for CAS: the current `xmin` is
    * the "version" we compare against on `CompareAndSet` publish.
    *
    * Returns `Left(PostgreSqlError.TableNotFound)` if the table
    * doesn't exist. */
  def getTableVersion(
      schema: String,
      table:  String,
  ): Either[PostgreSqlError, Long]

  /** Update a table's content with CAS via the `xmin` system column.
    *
    * The implementation runs `UPDATE ... SET xmin_lock = ? WHERE
    * xmin = ?` (a marker column we add on create). The WHERE
    * clause either matches 1 row (CAS success) or 0 rows (CAS
    * failure → `CasConflict`). This is PG's standard optimistic-
    * concurrency pattern.
    *
    * @return `Right(newXmin)` on success; `Left(CasConflict)` on
    *         version mismatch. */
  def casUpdate(
      schema:         String,
      table:          String,
      expectedXmin:   Long,
      newContent:     String,
  ): Either[PostgreSqlError, Long]

  /** Close the underlying connection / pool. Idempotent. */
  def close(): Unit = ()
}

/** Engine-portable description of a PostgreSQL query result.
  *
  * Per scala-data-driven-refacer §1: pure data, `Product with Serializable`. */
final case class PostgreSqlResult(
    columns: List[PostgreSqlColumn],
    rows:    List[Map[String, Any]],
) extends Product with Serializable

/** Engine-portable description of a PostgreSQL column.
  *
  * `dataType` is the PG type-name string (e.g. `"integer"`, `"text"`,
  * `"numeric"`); the resolver maps it to a portable type via the
  * [[pgTypeToPortable]] function in the resolver. */
final case class PostgreSqlColumn(
    name:     String,
    dataType: String,
    nullable: Boolean,
) extends Product with Serializable