package io.semanticdf.hera

/** Engine-portable view of an authenticated Hera session.
  *
  * Carries the `access_token` + `refresh_token` pair returned by
  * `POST /auth/login`. Per scala-data-driven-refacer §1: pure data
  * (no behavior); `extends Product with Serializable` per the
  * distributed-serialization reference (the token may be cached in
  * a Restate service boundary or held in a session registry).
  *
  * @param accessToken  the bearer token to send in `Authorization: <token>`
  * @param refreshToken the refresh token to send to `/auth/refresh_token`
  *                     when `accessToken` expires
  * @param expiresAt    the wall-clock instant when `accessToken` expires;
  *                     per error-handling-style.md, we carry this as a
  *                     primitive (Long/Instant) not as an exception
  * @param realmId      the default realm selected at login (Hera carries
  *                     `oauthUser.selRealmId` in the login response)
  * @param defaultZeusId the default Zeus engine id for query execution
  *                     within this realm. Hera carries
  *                     `oauthUser.moduleZeusList[]` with a `default: true`
  *                     entry; we extract that id at login. `None` if the
  *                     realm has no Zeus configured (query would fail
  *                     server-side in that case — caller decides). */
final case class HeraToken(
    accessToken:   String,
    refreshToken:  String,
    expiresAt:     java.time.Instant,
    realmId:       Long,
    defaultZeusId: Option[Long] = None,
) extends Product with Serializable

/** Boundary trait for Hera authentication (OAuth2 with refresh tokens).
  *
  * Per scala-data-driven-refacer §1 ("data is data, behavior lives
  * elsewhere"): the SHAPE of the contract is here; the BODY (the
  * actual HTTP calls to `/auth/login` and `/auth/refresh_token`)
  * lives in the concrete impl (e.g. `HttpHeraAuth`).
  *
  * ==Why a trait (vs. a concrete HTTP client)==
  *
  * Testability — `FakeHeraAuth` can return scripted tokens without
  * an HTTP server. Mirrors `UnityCatalogClient` /
  * `HiveMetastoreClient` (PRs #394, #398, #410, #423, #424).
  *
  * ==Why all methods return `Either[HeraAuthError, X]`==
  *
  * Per `docs/design/error-handling-style.md`: public APIs must return
  * `Either[L, X]` where `L` is a sealed ADT (not a string or a
  * `Throwable`). The compiler enforces exhaustive pattern-matching
  * on the failure side; `String`/`Throwable` would lose that
  * guarantee. [[HeraAuthError]] is the sealed ADT.
  *
  * ==Why `login` and `refreshToken` are separate methods==
  *
  * Per the OAuth2 contract: `login` requires username + password
  * (one-time); `refreshToken` requires only the refresh token
  * (per-session). Collapsing them into one method would require
  * "magic arg" handling that's harder to type-check.
  *
  * ==Boundary contract==
  *
  * Zero Spark imports. Verifiable by:
  * `grep -r 'org.apache.spark' adapters/semanticdf-hera/src/main/scala/io/semanticdf/hera/HeraAuth.scala` */
trait HeraAuth extends Serializable {

  /** Authenticate with username/password + tenant id; returns an
    * access token + refresh token pair.
    *
    * Per error-handling-style.md "Hard bans": programmer errors
    * (empty username, empty password) throw `IllegalArgumentException`
    * at the boundary — they are not data. Runtime errors
    * (HTTP 401, network failure) are returned as `Left(HeraAuthError)`.
    *
    * @throws IllegalArgumentException if `username.isEmpty` /
    *                                  `password.isEmpty` (programmer error)
    * @return `Right(HeraToken)` on success; `Left(HeraAuthError)` on
    *         any auth-side failure */
  def login(
      username: String,
      password: String,
      tenantId: Long,
  ): Either[HeraAuthError, HeraToken]

  /** Exchange a refresh token for a new access token + refresh token
    * pair. Used when the access token has expired.
    *
    * Per error-handling-style.md: throws `IllegalArgumentException`
    * for programmer errors (empty token); returns
    * `Left(HeraAuthError.TokenExpired)` when the refresh token is
    * rejected (which means re-authentication is needed).
    *
    * @throws IllegalArgumentException if `refreshToken.isEmpty`
    * @return `Right(HeraToken)` on success; `Left(HeraAuthError)` on
    *         any auth-side failure */
  def refreshToken(
      username:    String,
      refreshToken: String,
      tenantId:     Long,
  ): Either[HeraAuthError, HeraToken]
}

object HeraAuth {

  /** Smart constructor for [[HeraToken]]. Per error-handling-style.md
    * §2 (shape vs validity are separate): validates inputs at the
    * boundary (no empty fields) and throws `IllegalArgumentException`
    * for programmer errors. This is the SINGLE validation point —
    * downstream consumers trust the type. */
  def makeToken(
      accessToken:   String,
      refreshToken:  String,
      expiresAt:     java.time.Instant,
      realmId:       Long,
      defaultZeusId: Option[Long] = None,
  ): HeraToken = {
    if (accessToken.isEmpty)  throw new IllegalArgumentException("HeraToken.accessToken must not be empty")
    if (refreshToken.isEmpty) throw new IllegalArgumentException("HeraToken.refreshToken must not be empty")
    if (expiresAt == null)    throw new IllegalArgumentException("HeraToken.expiresAt must not be null")
    HeraToken(accessToken, refreshToken, expiresAt, realmId, defaultZeusId)
  }
}