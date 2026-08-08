package io.semanticdf.myplatform

/** Engine-specific typed error ADT for MyPlatform data-plane operations.
  *
  * Mirrors [[io.semanticdf.hera.HeraClientError]] and the engine-portable
  * `EngineError` / `CatalogError` ADTs in `semanticdf-core`.
  *
  * ==Why a sealed ADT (vs `Either[String, X]` or `Either[Throwable, X]`)==
  *
  * Per `docs/design/error-handling-style.md`: public APIs MUST return
  * `Either[L, X]` where `L` is a sealed ADT — not a string or a
  * `Throwable`. The compiler enforces exhaustive pattern-matching on
  * the failure side; `String`/`Throwable` would lose that guarantee.
  *
  * Each case maps to a SPECIFIC failure mode (per the standard's
  * "Hard bans" section — no catch-all `Exception`, no generic
  * `ServerError`):
  *
  *   - `Unauthorized`      : HTTP 401 — auth token missing/invalid
  *   - `Forbidden`         : HTTP 403 — auth token valid but lacks scope
  *   - `NotFound`          : HTTP 404 — entity doesn't exist
  *   - `AlreadyExists`     : HTTP 400 with "already exists" — duplicate
  *   - `Conflict`          : HTTP 409 — concurrent modification (CAS)
  *   - `BadRequest`        : HTTP 400 — generic (malformed request)
  *   - `NetworkError`      : transport failure (IOException / Interrupted)
  *   - `MalformedResponse` : 2xx but body shape unexpected
  *
  * ==Why all `extends Product with Serializable`==
  *
  * Per scala-data-driven-refacer §1 ("data is data, behavior lives
  * elsewhere"): every case is a pure data value. Per the distributed-
  * serialization reference: this ADT may be referenced from a Restate
  * service boundary; `Product with Serializable` is the cheapest
  * sound default. */
sealed trait MyPlatformError extends Product with Serializable

object MyPlatformError {

  /** HTTP 401 — auth token missing/invalid. */
  final case class Unauthorized(reason: String) extends MyPlatformError

  /** HTTP 403 — auth token valid but lacks scope. */
  final case class Forbidden(reason: String) extends MyPlatformError

  /** HTTP 404 — the requested entity doesn't exist. */
  final case class NotFound(reason: String) extends MyPlatformError

  /** HTTP 400 "Already Exists" — duplicate on create. Distinct from
    * `Conflict` (which is for CAS failures on update). */
  final case class AlreadyExists(reason: String) extends MyPlatformError

  /** HTTP 409 — concurrent modification (CAS failure). Mapped to
    * `CatalogError.StaleConflict` by the catalog adapter. */
  final case class Conflict(reason: String) extends MyPlatformError

  /** HTTP 400 with no specific reason. */
  final case class BadRequest(reason: String) extends MyPlatformError

  /** Transport failure: `IOException` (TCP/DNS) or `InterruptedException`
    * (timeout). Per error-handling-style.md "Worked example": map
    * specific JDK exception types to this single failure case. */
  final case class NetworkError(reason: String) extends MyPlatformError

  /** 2xx response but body shape we expected wasn't there. */
  final case class MalformedResponse(reason: String) extends MyPlatformError
}