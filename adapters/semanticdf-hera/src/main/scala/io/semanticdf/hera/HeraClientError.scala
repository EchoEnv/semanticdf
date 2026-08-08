package io.semanticdf.hera

/** Engine-specific typed error ADT for Hera data-plane operations
  * (query, describe, TableManage, RealmManage, etc.). Mirrors the
  * pattern used by `EngineError` (PR #412-#418) and `CatalogError`
  * (PR #410/#423/#424).
  *
  * ==Why a sealed ADT (vs `Either[String, X]` or `Either[Throwable, X]`)==
  *
  * Per `docs/design/error-handling-style.md` (PR #418 + #420), public
  * APIs MUST return `Either[L, X]` where `L` is a sealed ADT — not a
  * string or a `Throwable`. The compiler enforces exhaustive
  * pattern-matching; `String`/`Throwable` would lose that guarantee.
  *
  * Each case maps to a SPECIFIC failure mode (per the standard's
  * "Hard bans" section — no catch-all `Exception`):
  *
  *   - `Unauthorized`      : HTTP 401 — auth token missing/invalid
  *   - `Forbidden`         : HTTP 403 — auth token valid but lacks scope
  *   - `NoPermission`      : Hera's custom HTTP 602 — same semantic as Forbidden,
  *                            kept as a distinct case so callers can log
  *                            the Hera-specific code
  *   - `NotFound`          : HTTP 404 — table/realm/etc. doesn't exist
  *   - `AlreadyExists`     : HTTP 400 "Already Exists" — duplicate on create
  *   - `Conflict`          : HTTP 409 — concurrent modification (CAS failure)
  *   - `QueryFailed`       : HTTP 600 — Hera-specific "query failed" code
  *   - `EngineError`       : HTTP 521 — Hera-specific "engine error" code
  *   - `BadRequest`        : HTTP 400 — generic (when no specific reason)
  *   - `NetworkError`      : transport-level failure (JDK `IOException` /
  *                            `InterruptedException`)
  *   - `MalformedResponse` : 2xx but body shape we expected wasn't there
  *
  * ==Why many HTTP-status-specific cases (vs `ServerError`)==
  *
  * Per scala-chaos-testing §2 ("silence is a symptom"): a generic
  * `ServerError(String)` case loses the specific failure mode.
  * `EngineError.QueryRuntimeFailed` (added in PR #418) exists
  * specifically so callers can distinguish "connection failed" from
  * "query runtime error." We follow the same principle here —
  * 11 specific cases, not 1 generic case. */
sealed trait HeraClientError extends Product with Serializable

object HeraClientError {

  /** HTTP 401 — auth token missing/invalid. */
  final case class Unauthorized(reason: String) extends HeraClientError

  /** HTTP 403 — auth token valid but lacks scope. */
  final case class Forbidden(reason: String) extends HeraClientError

  /** Hera's custom HTTP 602 — same semantic as Forbidden, kept distinct
    * so callers can log the Hera-specific status code. Per the
    * standard's "Hard bans": do NOT fold this into `Forbidden`. */
  final case class NoPermission(reason: String) extends HeraClientError

  /** HTTP 404 — the requested entity (table / realm / etc.) doesn't exist. */
  final case class NotFound(reason: String) extends HeraClientError

  /** The specified realm_id doesn't exist on this Hera instance.
    * Distinct from `NotFound` so callers can surface "wrong tenant"
    * separately from "table doesn't exist" — they usually need to
    * handle them differently (refresh realm list vs. refresh table
    * list).
    *
    * Per user domain knowledge: realm is the top-level tenant
    * separator; an invalid realmId is a configuration error that
    * should NOT be retried. */
  final case class RealmNotFound(reason: String) extends HeraClientError

  /** The specified zeus_id doesn't exist (or isn't available for
    * this realm). Distinct from `RealmNotFound` because the recovery
    * is different: caller can list the realm's available Zeus engines
    * via `oauthUser.moduleZeusList[]` and try a different one.
    *
    * Per user domain knowledge: "zeus is hera engine for execution";
    * an invalid zeusId is a routing error — the realm exists but the
    * requested execution engine doesn't. */
  final case class ZeusNotFound(reason: String) extends HeraClientError

  /** HTTP 400 with "Already Exists" body — duplicate on create. Distinct
    * from `Conflict` (which is for CAS failures on update). */
  final case class AlreadyExists(reason: String) extends HeraClientError

  /** HTTP 409 — concurrent modification (CAS failure). Mapped to
    * `CatalogError.StaleConflict` by [[HeraCatalogAdapter]]. */
  final case class Conflict(reason: String) extends HeraClientError

  /** Hera's custom HTTP 600 — query / command failed. Distinct from
    * `EngineError` (HTTP 521) so callers can distinguish "command
    * failed" from "engine broken." */
  final case class QueryFailed(reason: String) extends HeraClientError

  /** Hera's custom HTTP 521 — engine error. */
  final case class EngineError(reason: String) extends HeraClientError

  /** HTTP 400 with no specific reason (e.g. "Not Support"). */
  final case class BadRequest(reason: String) extends HeraClientError

  /** Transport-level failure: `IOException` (TCP/DNS) or
    * `InterruptedException` (timeout). Per error-handling-style.md
    * "Worked example": map specific JDK exception types to this
    * single failure case (no catch-all `Exception`). */
  final case class NetworkError(reason: String) extends HeraClientError

  /** The server returned 2xx but the body shape we expected wasn't
    * there. Indicates a server upgrade or our parser being out of
    * sync. */
  final case class MalformedResponse(reason: String) extends HeraClientError
}