package io.semanticdf.postgresql

import io.semanticdf.core.engine.{EngineIdentity, ResolvedSource, SourceResolver}
import io.semanticdf.core.model.SourceRef

/** Implements the engine-portable `SourceResolver` contract against
  * PostgreSQL's `DatabaseMetaData.getColumns` (called via the
  * [[PostgreSqlClient.describeTable]] boundary).
  *
  * Mirrors `io.semanticdf.hera.HeraSourceResolver` (PR #425) in
  * return-type pattern (returns `ResolvedSource` sealed ADT directly,
  * not `Either`).
  *
  * ==Why return `ResolvedSource` (NOT `Either[ResolvedSource, X]`)==
  *
  * Per `docs/design/error-handling-style.md`: the `ResolvedSource`
  * sealed ADT (Scan / NotFound / AuthFailed / Incompatible) IS the
  * failure mode. Pattern-matching on the result enforces exhaustive
  * handling of all 4 cases at the call site.
  *
  * Mapping per [[PostgreSqlError]] → `ResolvedSource`:
  *   - `ConnectionFailed` / `AuthenticationFailed` → `AuthFailed`
  *     (we can't even reach the DB, so it's an auth/network issue)
  *   - `TableNotFound`                      → `NotFound`
  *   - other                                → `Incompatible` */
final class PostgreSqlSourceResolver(
    client:   PostgreSqlClient,
    database: String,
) extends SourceResolver {

  override def resolve(
      source:   SourceRef,
      identity: EngineIdentity,
  ): ResolvedSource = source match {
    case SourceRef.ByName(catalog, namespace, table) =>
      val schema = namespace.getOrElse("public")
      // Per error-handling-style.md: 1-step (the read) → direct
      // match (NOT for-comprehension).
      client.describeTable(schema, table) match {
        case Right(resolvedSchema) =>
          ResolvedSource.Scan(source = source, schema = resolvedSchema)
        case Left(PostgreSqlError.TableNotFound(reason)) =>
          ResolvedSource.NotFound(source = source, reason = reason)
        case Left(PostgreSqlError.ConnectionFailed(reason)) =>
          ResolvedSource.AuthFailed(source = source, reason = reason)
        case Left(PostgreSqlError.AuthenticationFailed(reason)) =>
          ResolvedSource.AuthFailed(source = source, reason = reason)
        case Left(other: PostgreSqlError) =>
          ResolvedSource.Incompatible(
            source = source,
            reason = s"${other.getClass.getSimpleName}: ${reasonOf(other)}",
          )
      }

    case SourceRef.ByPath(_, path, _) =>
      // Per the design: path-based sources aren't supported by
      // PostgreSQL (no equivalent of "describe a file"). Map to
      // Incompatible, NOT NotFound or AuthFailed.
      ResolvedSource.Incompatible(
        source = source,
        reason = s"PostgreSQL resolver does not support path-based sources (path=$path); use SourceRef.ByName",
      )

    case SourceRef.ByProvider(provider) =>
      ResolvedSource.Incompatible(
        source = source,
        reason = s"PostgreSQL resolver does not support provider-based sources (provider=$provider); use SourceRef.ByName",
      )
  }

  /** Extract the `reason` from a [[PostgreSqlError]] without losing
    * the type info. Per error-handling-style.md "Internal helper
    * rule": ONE call site (the `match` above), caller uses value
    * immediately → plain function returning `String`, NOT `Either`. */
  private def reasonOf(e: PostgreSqlError): String = e match {
    case PostgreSqlError.ConnectionFailed(r)     => r
    case PostgreSqlError.AuthenticationFailed(r) => r
    case PostgreSqlError.TableNotFound(r)        => r
    case PostgreSqlError.ColumnNotFound(r)       => r
    case PostgreSqlError.SyntaxError(r)          => r
    case PostgreSqlError.UniqueViolation(r)      => r
    case PostgreSqlError.CheckViolation(r)       => r
    case PostgreSqlError.CasConflict(r)          => r
    case PostgreSqlError.NetworkError(r)         => r
    case PostgreSqlError.Interrupted(r)          => r
    case PostgreSqlError.PoolExhausted(r)        => r
    case PostgreSqlError.MalformedResponse(r)    => r
  }
}

object PostgreSqlSourceResolver {

  /** Smart constructor. */
  def apply(
      client:   PostgreSqlClient,
      database: String,
  ): PostgreSqlSourceResolver = new PostgreSqlSourceResolver(client, database)
}