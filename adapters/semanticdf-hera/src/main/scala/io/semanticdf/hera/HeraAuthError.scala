package io.semanticdf.hera

/** Engine-specific typed error ADT for Hera authentication
  * (OAuth2 login + refresh-token). Mirrors the pattern used by
  * `EngineError` (engine-portable, PR #412-#418) and `CatalogError`
  * (catalog-portable, PR #410/#423/#424).
  *
  * ==Why a sealed ADT (vs `Either[String, X]` or `Either[Throwable, X]`)==
  *
  * Per `docs/design/error-handling-style.md` (PR #418 + #420), public
  * APIs MUST return `Either[L, X]` where `L` is a sealed ADT — not a
  * string or a `Throwable`. The compiler then enforces exhaustive
  * pattern-matching on the failure side; `String`/`Throwable` would
  * lose that guarantee.
  *
  * Each case maps to a SPECIFIC failure mode (per the standard's
  * "Hard bans" section — no catch-all `Exception`):
  *
  *   - `InvalidCredentials` : wrong username/password (HTTP 401 from `/auth/login`)
  *   - `TokenExpired`       : refresh token also rejected (HTTP 401 from `/auth/refresh_token`)
  *   - `NetworkError`       : transport-level failure (JDK `IOException` / `InterruptedException`)
  *   - `MalformedResponse`  : 2xx response we couldn't parse (programmer error in our parser)
  *
  * ==Why all `extends Product with Serializable`==
  *
  * Per scala-data-driven-refacer §1 ("data is data, behavior lives
  * elsewhere"): every case is a pure data value. Per the distributed-
  * serialization reference: this ADT may be referenced from a Restate
  * service boundary (if the platform's `StartupReconciler` ever
  * stores auth state); `Product with Serializable` is the cheapest
  * sound default. */
sealed trait HeraAuthError extends Product with Serializable

object HeraAuthError {

  /** Login credentials were rejected (HTTP 401 from `/auth/login`).
    * Distinct from `TokenExpired` so callers can surface "wrong
    * password" vs "session expired" differently. */
  final case class InvalidCredentials(reason: String) extends HeraAuthError

  /** Refresh token also rejected (HTTP 401 from `/auth/refresh_token`).
    * Caller should treat this as terminal — re-authentication needed. */
  final case class TokenExpired(reason: String) extends HeraAuthError

  /** Transport-level failure: `IOException` (TCP/DNS) or
    * `InterruptedException` (timeout). Per error-handling-style.md
    * "Worked example": we map the specific JDK exception types to
    * this single failure case (no catch-all `Exception`). */
  final case class NetworkError(reason: String) extends HeraAuthError

  /** The server returned 2xx but the body shape we expected wasn't
    * there (e.g. `oauth.access_token` missing). Indicates a server
    * upgrade or our parser being out of sync — not a transient
    * user-facing failure. */
  final case class MalformedResponse(reason: String) extends HeraAuthError
}