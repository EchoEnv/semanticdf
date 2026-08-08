package io.semanticdf.hera

import java.time.Instant

/** Test fixture: a hand-driven [[HeraAuth]] that returns scripted
  * tokens.
  *
  * Mirrors `FakeUnityCatalogClient` pattern (PR #424): tests pre-
  * populate the fake's state; the fake returns it deterministically.
  *
  * ==Why a separate class from `FakeHeraClient`==
  *
  * Per scala-data-driven-refacer §1: auth and data-plane are
  * separate concerns. Splitting the fakes lets each test exercise
  * one layer in isolation. */
final class FakeHeraAuth(
    tokensByLogin:  Map[(String, String, Long), HeraToken] = Map.empty,
    tokensByRefresh: Map[(String, String, Long), HeraToken] = Map.empty,
) extends HeraAuth {

  override def login(
      username: String,
      password: String,
      tenantId: Long,
  ): Either[HeraAuthError, HeraToken] = {
    tokensByLogin.get((username, password, tenantId)) match {
      case Some(t) => Right(t)
      case None    => Left(HeraAuthError.InvalidCredentials(reason = s"no scripted response for ($username, $password, $tenantId)"))
    }
  }

  override def refreshToken(
      username:    String,
      refreshToken: String,
      tenantId:     Long,
  ): Either[HeraAuthError, HeraToken] = {
    tokensByRefresh.get((username, refreshToken, tenantId)) match {
      case Some(t) => Right(t)
      case None    => Left(HeraAuthError.TokenExpired(reason = s"no scripted response for refresh($username, $refreshToken, $tenantId)"))
    }
  }
}

object FakeHeraAuth {

  /** Empty fake — every call returns the relevant error. */
  def empty: FakeHeraAuth = new FakeHeraAuth()

  /** Build a fake that returns a single token for a single (user, pass, tenant). */
  def withLogin(
      username: String,
      password: String,
      tenantId: Long,
      token:    HeraToken,
  ): FakeHeraAuth = new FakeHeraAuth(
    tokensByLogin = Map((username, password, tenantId) -> token),
  )
}