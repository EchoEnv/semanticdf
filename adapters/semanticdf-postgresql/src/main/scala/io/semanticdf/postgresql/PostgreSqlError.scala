package io.semanticdf.postgresql

/** Engine-specific typed error ADT for PostgreSQL operations.
  *
  * Mirrors the pattern from `io.semanticdf.hera.HeraClientError`
  * (PR #425) and `io.semanticdf.core.catalog.CatalogError`.
  *
  * ==Why a sealed ADT (vs `Either[String, X]` or `Either[Throwable, X]`)==
  *
  * Per `docs/design/error-handling-style.md`: public APIs MUST return
  * `Either[L, X]` where `L` is a sealed ADT — not a string or a
  * `Throwable`. The compiler enforces exhaustive pattern-matching on
  * the failure side.
  *
  * Each case maps to a SPECIFIC failure mode (per the standard's
  * "Hard bans" section — no catch-all `Exception`):
  *
  *   - `ConnectionFailed`     : JDBC connect failure (SQLException 08001)
  *   - `AuthenticationFailed` : JDBC auth failure (SQLException 28000)
  *   - `TableNotFound`        : JDBC SQLException 42P01 (undefined_table)
  *   - `ColumnNotFound`       : JDBC SQLException 42703 (undefined_column)
  *   - `SyntaxError`          : JDBC SQLException 42601 (syntax_error)
  *   - `UniqueViolation`      : JDBC SQLException 23505 (unique_violation)
  *   - `CheckViolation`       : JDBC SQLException 23514 (check_violation)
  *   - `CasConflict`          : row count mismatch on optimistic update
  *   - `NetworkError`         : transport failure (IOException)
  *   - `Interrupted`          : timeout / interrupt
  *   - `PoolExhausted`        : connection pool exhausted
  *   - `MalformedResponse`    : unexpected ResultSet shape
  *
  * ==Why `extends Product with Serializable`==
  *
  * Per scala-data-driven-refacer §1 ("data is data, behavior lives
  * elsewhere"): every case is a pure data value. Per the distributed-
  * serialization reference: this ADT may be referenced from a Restate
  * service boundary; `Product with Serializable` is the cheapest
  * sound default. */
sealed trait PostgreSqlError extends Product with Serializable

object PostgreSqlError {

  /** JDBC connect failure (SQLState class 08). */
  final case class ConnectionFailed(reason: String) extends PostgreSqlError

  /** JDBC authentication failure (SQLState 28000). */
  final case class AuthenticationFailed(reason: String) extends PostgreSqlError

  /** Table doesn't exist (PG SQLState 42P01). */
  final case class TableNotFound(reason: String) extends PostgreSqlError

  /** Column doesn't exist (PG SQLState 42703). */
  final case class ColumnNotFound(reason: String) extends PostgreSqlError

  /** SQL syntax error (PG SQLState 42601). */
  final case class SyntaxError(reason: String) extends PostgreSqlError

  /** Unique constraint violation (PG SQLState 23505). */
  final case class UniqueViolation(reason: String) extends PostgreSqlError

  /** Check constraint violation (PG SQLState 23514). */
  final case class CheckViolation(reason: String) extends PostgreSqlError

  /** CAS failure: the WHERE xmin = expected matched 0 rows.
    * Per the v0.3.0 catalog CAS contract: returns
    * `CatalogError.StaleConflict` (caller can retry from new state). */
  final case class CasConflict(reason: String) extends PostgreSqlError

  /** Transport-level failure (IOException). */
  final case class NetworkError(reason: String) extends PostgreSqlError

  /** Interrupted / timeout. */
  final case class Interrupted(reason: String) extends PostgreSqlError

  /** Connection pool exhausted. */
  final case class PoolExhausted(reason: String) extends PostgreSqlError

  /** Unexpected ResultSet shape (parse error). */
  final case class MalformedResponse(reason: String) extends PostgreSqlError
}