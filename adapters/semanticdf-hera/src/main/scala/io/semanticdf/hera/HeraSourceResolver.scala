package io.semanticdf.hera

import io.semanticdf.core.engine.{EngineIdentity, ResolvedSchema, ResolvedSource, SourceResolver}
import io.semanticdf.core.model.SourceRef

/** v0.3.1: implements the engine-portable [[SourceResolver]] contract
  * against Hera's `POST /private/explore/describe/table` endpoint.
  *
  * ==Why a separate class (vs. extension of `HeraClient`)==
  *
  * Per scala-data-driven-refacer §1: the SHAPE of the contract
  * (`SourceResolver`) lives in core; the BODY (Hera-specific
  * resolution behavior) lives here. The resolver and the client
  * serve different lifecycle roles (resolver = read schema for
  * compile; client = all data-plane ops), so they're separate
  * classes that share the underlying [[HeraClient]] (a single
  * connection per process).
  *
  * ==Why return `ResolvedSource` (NOT `Either[ResolvedSource, X]`)==
  *
  * Per `docs/design/error-handling-style.md`: the `ResolvedSource`
  * sealed ADT (Scan / NotFound / AuthFailed / Incompatible) IS the
  * failure mode — pattern-matching on the result enforces exhaustive
  * handling of all four cases at the call site. Wrapping in
  * `Either` would lose that property.
  *
  * Mapping per [[HeraClientError]] → `ResolvedSource`:
  *   - `Unauthorized`      → `AuthFailed` (HTTP 401 = auth token bad)
  *   - `Forbidden`         → `AuthFailed` (HTTP 403 = scope issue)
  *   - `NoPermission`      → `AuthFailed` (Hera 602 = same semantic)
  *   - `NotFound`          → `NotFound` (HTTP 404 = table doesn't exist)
  *   - `QueryFailed`       → `Incompatible` (Hera 600 = server-side query issue, not "doesn't exist")
  *   - `EngineError`       → `Incompatible` (Hera 521 = broken engine, surface as "incompatible with engine")
  *   - `Conflict`          → `Incompatible` (CAS failure on a metadata lookup is engine-side weirdness)
  *   - `AlreadyExists`     → `Incompatible` (impossible on describe; server bug)
  *   - `BadRequest`        → `Incompatible` (Hera rejected the request shape)
  *   - `NetworkError`      → `Incompatible` (transport failure → caller retries, not "not found")
  *   - `MalformedResponse` → `Incompatible` (parse failure → engine bug) */
final class HeraSourceResolver(
    client: HeraClient,
) extends SourceResolver {

  override def resolve(
      source:    SourceRef,
      identity:  EngineIdentity,
  ): ResolvedSource = source match {
    case SourceRef.ByName(catalog, namespace, table) =>
      // Per the multi-engine design §4.3.2: we use the catalog +
      // namespace + table to look up the schema. For Hera, the
      // catalog is the realm (per user "realm is the top layer
      // that separates catalogs/engines/etc.").
      //
      // Per the user constraint: realm_id is required for the Hera
      // call. The portable `SourceRef.ByName.catalog` field carries
      // the catalog name; we look up the realm_id via the client's
      // realm listing.
      val realmId = resolveRealmId(catalog.getOrElse(""))
      if (realmId < 0) {
        ResolvedSource.Incompatible(
          source = source,
          reason = s"catalog '${catalog.getOrElse("")}' does not correspond to any known Hera realm",
        )
      } else {
        // Per error-handling-style.md: 3+ sequential steps → for-
        // comprehension. Here it's 1 step (the client call); use
        // match directly per the chaining rule.
        client.describeTable(table, realmId) match {
          case Right(schema) =>
            ResolvedSource.Scan(source = source, schema = schema)
          case Left(HeraClientError.NotFound(reason)) =>
            ResolvedSource.NotFound(source = source, reason = reason)
          case Left(HeraClientError.Unauthorized(reason)) =>
            ResolvedSource.AuthFailed(source = source, reason = reason)
          case Left(HeraClientError.Forbidden(reason)) =>
            ResolvedSource.AuthFailed(source = source, reason = reason)
          case Left(HeraClientError.NoPermission(reason)) =>
            ResolvedSource.AuthFailed(source = source, reason = reason)
          case Left(other: HeraClientError) =>
            ResolvedSource.Incompatible(source = source, reason = s"${other.getClass.getSimpleName}: ${otherReason(other)}")
        }
      }

    case SourceRef.ByPath(_, path, _) =>
      // Per the design: path-based sources aren't supported by
      // Hera's describe-table API (which only takes a table name).
      // Per error-handling-style.md: this is an INCOMPATIBLE source
      // shape, NOT a not-found or auth-failed. Map to Incompatible.
      ResolvedSource.Incompatible(
        source = source,
        reason = s"Hera source resolver does not support path-based sources (path=$path); use SourceRef.ByName",
      )

    case SourceRef.ByProvider(provider) =>
      ResolvedSource.Incompatible(
        source = source,
        reason = s"Hera source resolver does not support provider-based sources (provider=$provider); use SourceRef.ByName",
      )
  }

  /** Look up the realm id for a given catalog name.
    *
    * Returns `-1` if no realm with that name exists.
    *
    * Per error-handling-style.md "Internal helper rule": this is
    * a single-call-site helper (the `match` above is the only
    * caller). Per the rule: "ONE call site, caller does `match`
    * on it right away → plain function, return `X` (not `Either[L, X]`)".
    * So this returns `Long`, not `Either[... , Long]`. */
  private def resolveRealmId(catalogName: String): Long = {
    if (catalogName.isEmpty) return -1L
    client.listRealms() match {
      case Right(realms) =>
        realms.find(_.name == catalogName).map(_.id).getOrElse(-1L)
      case Left(_) =>
        // Transport failure → caller will surface Incompatible
        // upstream; here we return -1 as "unknown."
        -1L
    }
  }

  /** Extract the `reason` string from any [[HeraClientError]] case
    * without losing the type info. The cases all have a `reason: String`
    * field per the ADT design; we just match on them. */
  private def otherReason(e: HeraClientError): String = e match {
    case HeraClientError.Unauthorized(r)        => r
    case HeraClientError.Forbidden(r)           => r
    case HeraClientError.NoPermission(r)        => r
    case HeraClientError.NotFound(r)            => r
    case HeraClientError.AlreadyExists(r)       => r
    case HeraClientError.Conflict(r)            => r
    case HeraClientError.QueryFailed(r)         => r
    case HeraClientError.EngineError(r)         => r
    case HeraClientError.BadRequest(r)          => r
    case HeraClientError.NetworkError(r)        => r
    case HeraClientError.MalformedResponse(r)   => r
  }
}

object HeraSourceResolver {

  /** Smart constructor. */
  def apply(client: HeraClient): HeraSourceResolver = new HeraSourceResolver(client)
}