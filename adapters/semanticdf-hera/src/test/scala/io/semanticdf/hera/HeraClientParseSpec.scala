package io.semanticdf.hera

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** v0.3.1: regression tests for [[HttpHeraClient]]'s parse helpers
  * after the `Either[String, _]` cleanup (the parse helpers now
  * return `Either[HeraClientError, X]` directly).
  *
  * Per docs/design/error-handling-style.md "Converter return types":
  * the helper returns `Either[L, X]` DIRECTLY so the type info is
  * preserved across the boundary. These tests pin that contract:
  *   - Missing/malformed JSON surfaces as `HeraClientError.MalformedResponse`
  *     (a SPECIFIC case, NOT a String).
  *   - The reason string is carried inside `MalformedResponse.reason`
  *     (NOT as a separate String at the call site).
  *
  * Per scala-spark-batch-bugs §1: assert the actual HeraClientError
  * case class (not just "returned Left").
  *
  * Per karpathy §4 (verifiable goals): this is the verification
  * step for the parse-helper refactor. */
class HeraClientParseSpec extends AnyFunSuite with Matchers {

  // -- parseQueryResult --

  test("parseQueryResult: missing fields array → MalformedResponse") {
    val json = """{"rows":[],"queryTime":100}"""
    val result = HttpHeraClient.parseQueryResult(json)
    result shouldBe Left(HeraClientError.MalformedResponse(reason = "missing fields array"))
  }

  test("parseQueryResult: missing rows array → MalformedResponse") {
    val json = """{"fields":[],"queryTime":100}"""
    val result = HttpHeraClient.parseQueryResult(json)
    result shouldBe Left(HeraClientError.MalformedResponse(reason = "missing rows array"))
  }

  test("parseQueryResult: well-formed JSON → Right with parsed fields") {
    val json =
      """{"fields":[{"name":"carrier","dataType":"string","nullable":false}],
        | "rows":[{"carrier":"AA"}],
        | "queryTime":100}""".stripMargin
    val result = HttpHeraClient.parseQueryResult(json)
    result.isRight shouldBe true
    result.toOption.get.fields.map(_.name) shouldBe List("carrier")
    result.toOption.get.rows.map(_("carrier")) shouldBe List("AA")
  }

  // -- parseDescribeTable --

  test("parseDescribeTable: missing fields array → MalformedResponse") {
    val json = """{"rows":[{"col_name":"carrier","data_type":"string"}]}"""
    val result = HttpHeraClient.parseDescribeTable(json)
    result shouldBe Left(HeraClientError.MalformedResponse(reason = "missing fields array"))
  }

  test("parseDescribeTable: missing rows array → MalformedResponse") {
    val json = """{"fields":[]}"""
    val result = HttpHeraClient.parseDescribeTable(json)
    result shouldBe Left(HeraClientError.MalformedResponse(reason = "missing rows array"))
  }

  test("parseDescribeTable: well-formed JSON → Right with ResolvedSchema") {
    val json =
      """{"fields":[],
        | "rows":[{"col_name":"carrier","data_type":"string"},
        |         {"col_name":"flight_count","data_type":"long"}]}""".stripMargin
    val result = HttpHeraClient.parseDescribeTable(json)
    result.isRight shouldBe true
    result.toOption.get.fields shouldBe Map("carrier" -> "string", "flight_count" -> "long")
  }

  // -- parseRealm (single-realm) --

  test("parseRealm: missing id field → MalformedResponse (per legacy 'could not parse realm')") {
    val json = """{"name":"finance","active":true}"""
    val result = HttpHeraClient.parseRealm(json)
    result shouldBe Left(HeraClientError.MalformedResponse(reason = "could not parse realm"))
  }

  test("parseRealm: well-formed JSON → Right(HeraRealm)") {
    val json = """{"id":42,"name":"finance","description":"Finance realm","active":true}"""
    val result = HttpHeraClient.parseRealm(json)
    result shouldBe Right(HeraRealm(id = 42L, name = "finance",
      description = Some("Finance realm"), active = true))
  }

  // -- parseRealmList (always Right in current impl) --

  test("parseRealmList: well-formed JSON → Right(List) of parsed realms") {
    val json =
      """[{"id":1,"name":"finance","active":true},
        | {"id":2,"name":"marketing","active":true}]""".stripMargin
    val result = HttpHeraClient.parseRealmList(json)
    result shouldBe Right(List(
      HeraRealm(id = 1L, name = "finance", description = None, active = true),
      HeraRealm(id = 2L, name = "marketing", description = None, active = true),
    ))
  }
}
