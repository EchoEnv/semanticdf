package io.semanticdf.hera

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** v0.3.1: regression tests for [[HttpHeraAuth]]'s parseAuthResponse
  * helper after the `Either[String, _]` cleanup (the parse helper
  * now returns `Either[HeraAuthError, HeraToken]` directly).
  *
  * Per docs/design/error-handling-style.md "Converter return types":
  * the helper returns `Either[L, X]` DIRECTLY so the type info is
  * preserved across the boundary. These tests pin that contract:
  *   - Missing `access_token` / `refresh_token` fields surface as
  *     `HeraAuthError.MalformedResponse` (a SPECIFIC case, NOT a String).
  *   - The reason string is carried inside `MalformedResponse.reason`
  *     (NOT as a separate String at the call site).
  *
  * Per scala-spark-batch-bugs §1: assert the actual HeraAuthError
  * case class (not just "returned Left").
  *
  * Per karpathy §4 (verifiable goals): this is the verification step
  * for the parse-helper refactor. */
class HeraAuthParseSpec extends AnyFunSuite with Matchers {

  // -- missing access_token --

  test("parseAuthResponse: missing access_token → MalformedResponse") {
    val json =
      """{"oauth":{"refresh_token":"rt","expires_in":"3600"},
        | "oauthUser":{"selRealmId":1}}""".stripMargin
    val result = HttpHeraAuth.parseAuthResponse(json)
    result shouldBe Left(HeraAuthError.MalformedResponse(reason = "missing oauth.access_token"))
  }

  // -- missing refresh_token --

  test("parseAuthResponse: missing refresh_token → MalformedResponse") {
    val json =
      """{"oauth":{"access_token":"at","expires_in":"3600"},
        | "oauthUser":{"selRealmId":1}}""".stripMargin
    val result = HttpHeraAuth.parseAuthResponse(json)
    result shouldBe Left(HeraAuthError.MalformedResponse(reason = "missing oauth.refresh_token"))
  }

  // -- well-formed JSON → Right(HeraToken) --

  test("parseAuthResponse: well-formed JSON → Right(HeraToken)") {
    val json =
      """{"oauth":{"access_token":"at-123","refresh_token":"rt-456","expires_in":"3600"},
        | "oauthUser":{"selRealmId":42}}""".stripMargin
    val result = HttpHeraAuth.parseAuthResponse(json)
    result.isRight shouldBe true
    val token = result.toOption.get
    token.accessToken shouldBe "at-123"
    token.refreshToken shouldBe "rt-456"
    token.realmId shouldBe 42L
    token.defaultZeusId shouldBe None  // no moduleZeusList → None
  }

  test("parseAuthResponse: well-formed JSON with default Zeus → Right with defaultZeusId set") {
    val json =
      """{"oauth":{"access_token":"at","refresh_token":"rt","expires_in":"3600"},
        | "oauthUser":{"selRealmId":1,
        |              "moduleZeusList":[{"id":7,"name":"spark-1","default":false},
        |                              {"id":8,"name":"spark-2","default":true}]}}""".stripMargin
    val result = HttpHeraAuth.parseAuthResponse(json)
    result.isRight shouldBe true
    result.toOption.get.defaultZeusId shouldBe Some(8L)
  }
}
