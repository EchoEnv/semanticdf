package io.semanticdf.myplatform

import io.semanticdf.core.engine.{EngineIdentity, ResolvedSource}
import io.semanticdf.core.model.SourceRef

/** Implements the engine-portable `SourceResolver` contract against
  * MyPlatform's `GET /api/tables/:name` endpoint.
  *
  * Mirrors `io.semanticdf.hera.HeraSourceResolver` (PR #425).
  *
  * ==Why return `ResolvedSource` (NOT `Either[ResolvedSource, X]`)==
  *
  * Per `docs/design/error-handling-style.md`: the `ResolvedSource`
  * sealed ADT (Scan / NotFound / AuthFailed / Incompatible) IS the
  * failure mode. Pattern-matching on the result enforces exhaustive
  * handling of all 4 cases at the call site. Wrapping in
  * `Either` would lose that property.
  *
  * Mapping per [[MyPlatformError]] → `ResolvedSource`:
  *   - `Unauthorized` / `Forbidden` → `AuthFailed`
  *   - `NotFound`              → `NotFound`
  *   - other                   → `Incompatible` */
final class MyPlatformSourceResolver(
    client: MyPlatformClient,
) extends SourceResolver {

  override def resolve(
      source:    SourceRef,
      identity:  EngineIdentity,
  ): ResolvedSource = source match {
    case SourceRef.ByName(catalog, namespace, table) =>
      val realmId = client.resolveRealmId(catalog.getOrElse("")).getOrElse("")
      if (realmId.isEmpty) {
        ResolvedSource.Incompatible(
          source = source,
          reason = s"catalog '${catalog.getOrElse("")}' does not map to any known MyPlatform realm",
        )
      } else {
        // Per error-handling-style.md: 1-step (the read) → direct
        // match (NOT for-comprehension).
        client.describeTable(table, realmId) match {
          case Right(schema) =>
            ResolvedSource.Scan(source = source, schema = schema)
          case Left(MyPlatformError.NotFound(reason)) =>
            ResolvedSource.NotFound(source = source, reason = reason)
          case Left(MyPlatformError.Unauthorized(reason)) =>
            ResolvedSource.AuthFailed(source = source, reason = reason)
          case Left(MyPlatformError.Forbidden(reason)) =>
            ResolvedSource.AuthFailed(source = source, reason = reason)
          case Left(other: MyPlatformError) =>
            ResolvedSource.Incompatible(
              source = source,
              reason = s"${other.getClass.getSimpleName}: ${reasonOf(other)}",
            )
        }
      }

    case SourceRef.ByPath(_, path, _) =>
      // Per the design: path-based sources aren't supported here.
      ResolvedSource.Incompatible(
        source = source,
        reason = s"MyPlatform source resolver does not support path-based sources (path=$path); use SourceRef.ByName",
      )

    case SourceRef.ByProvider(provider) =>
      ResolvedSource.Incompatible(
        source = source,
        reason = s"MyPlatform source resolver does not support provider-based sources (provider=$provider); use SourceRef.ByName",
      )
  }

  /** Extract the `reason` string from any [[MyPlatformError]] case.
    * Per error-handling-style.md "Internal helper rule": ONE call
    * site, caller uses value immediately → plain function returning
    * String, NOT `Either`. */
  private def reasonOf(e: MyPlatformError): String = e match {
    case MyPlatformError.Unauthorized(r)      => r
    case MyPlatformError.Forbidden(r)         => r
    case MyPlatformError.NotFound(r)          => r
    case MyPlatformError.AlreadyExists(r)     => r
    case MyPlatformError.Conflict(r)          => r
    case MyPlatformError.BadRequest(r)        => r
    case MyPlatformError.NetworkError(r)      => r
    case MyPlatformError.MalformedResponse(r) => r
  }
}

object MyPlatformSourceResolver {

  /** Smart constructor. */
  def apply(client: MyPlatformClient): MyPlatformSourceResolver = new MyPlatformSourceResolver(client)
}