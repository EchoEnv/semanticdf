package io.semanticdf.hera

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.time.Instant

/** v0.3.1: tests for [[HeraAuth]] trait + [[FakeHeraAuth]] test
  * fixture. Real HttpHeraAuth tests deferred to v0.4.0 (per the
  * existing UC/HMS pattern: integration tests against a real
  * server, not the unit tests).
  *
  * Per `docs/design/error-handling-style.md`:
  *   - Typed `Either[HeraAuthError, X]` for all failure paths
  *   - No exceptions for "not implemented" / "wrong mode"
  *   - Programmer errors (empty username/password) at the boundary
  *     throw `IllegalArgumentException` */
class HeraAuthSpec extends AnyFunSuite with Matchers {

  private val sampleToken = HeraAuth.makeToken(
    accessToken  = "test-access",
    refreshToken = "test-refresh",
    expiresAt    = Instant.now().plusSeconds(3600),
    realmId      = 1L,
  )

  // -- login --

  test("login returns Right(token) when scripted response matches") {
    val auth = FakeHeraAuth.withLogin(
      username = "alice",
      password = "secret",
      tenantId = 1L,
      token    = sampleToken,
    )
    val result = auth.login("alice", "secret", 1L)
    result shouldBe Right(sampleToken)
  }

  test("login returns Left(InvalidCredentials) when no scripted response matches") {
    val auth = FakeHeraAuth.empty
    val result = auth.login("alice", "wrong", 1L)
    result match {
      case Left(HeraAuthError.InvalidCredentials(reason)) => reason should include ("alice")
      case other => fail(s"expected InvalidCredentials, got $other")
    }
  }

  // -- refreshToken --

  test("refreshToken returns Right(token) when scripted response matches") {
    val auth = new FakeHeraAuth(Map.empty, Map(("alice", "old-refresh", 1L) -> sampleToken))
    val result = auth.refreshToken("alice", "old-refresh", 1L)
    result shouldBe Right(sampleToken)
  }

  test("refreshToken returns Left(TokenExpired) when refresh token rejected") {
    val auth = FakeHeraAuth.empty
    val result = auth.refreshToken("alice", "old-refresh", 1L)
    result match {
      case Left(HeraAuthError.TokenExpired(reason)) => reason should include ("alice")
      case other => fail(s"expected TokenExpired, got $other")
    }
  }

  // -- boundary validation (HttpHeraAuth behavior) --

  // Note: HttpHeraAuth is the real impl that throws at the boundary;
  // FakeHeraAuth is permissive (test doubles don't validate). We
  // test the boundary check via the smart constructor [[HeraAuth.makeToken]].
  test("HeraAuth.makeToken throws IllegalArgumentException for empty accessToken (programmer error)") {
    intercept[IllegalArgumentException] {
      HeraAuth.makeToken("", "refreshToken", Instant.now().plusSeconds(3600), 1L)
    }
  }

  test("HeraAuth.makeToken throws IllegalArgumentException for empty refreshToken") {
    intercept[IllegalArgumentException] {
      HeraAuth.makeToken("accessToken", "", Instant.now().plusSeconds(3600), 1L)
    }
  }

  test("HeraAuth.makeToken throws IllegalArgumentException for null expiresAt") {
    intercept[IllegalArgumentException] {
      HeraAuth.makeToken("accessToken", "refreshToken", null, 1L)
    }
  }
}