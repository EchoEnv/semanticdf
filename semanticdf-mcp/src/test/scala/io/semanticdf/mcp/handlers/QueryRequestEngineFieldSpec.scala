package io.semanticdf.mcp.handlers

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.{HashMap, Map => JMap}

/** Tests for the 'engine' field on 'QueryRequest' (added in
  * PR 5b of the 12-PR triage plan).
  *
  * Per design \u00a76.4 + the round-3 DE review's "MCP engine
  * registry" finding: the 12th 'queryToolSchema' property is
  * the 'engine' field that routes the query through the
  * 'MCPEngineRegistry'. This spec pins the parsing behavior:
  *   1. Missing 'engine' field \u2192 defaults to "" (backward-compat
  *      with pre-PR-5b callers)
  *   2. Present 'engine' field \u2192 stored on the request */
class QueryRequestEngineFieldSpec extends AnyFunSuite with Matchers {

  test("parseRequest defaults 'engine' to empty string when the field is absent") {
    val args: JMap[String, AnyRef] = new HashMap[String, AnyRef]()
    args.put("model", "orders")
    args.put("measures", java.util.Arrays.asList("amount"))
    val req = Query.parseRequest(args)
    req.engine shouldBe ""
  }

  test("parseRequest captures 'engine' when the field is present") {
    val args: JMap[String, AnyRef] = new HashMap[String, AnyRef]()
    args.put("model", "orders")
    args.put("measures", java.util.Arrays.asList("amount"))
    args.put("engine", "trino")
    val req = Query.parseRequest(args)
    req.engine shouldBe "trino"
  }

  test("QueryRequest default 'engine' is empty string (backward-compat)") {
    val req = QueryRequest(
      model    = "orders",
      measures = Seq("amount"),
    )
    req.engine shouldBe ""
  }

  test("QueryRequest explicit 'engine' is preserved") {
    val req = QueryRequest(
      model    = "orders",
      measures = Seq("amount"),
      engine   = "spark",
    )
    req.engine shouldBe "spark"
  }
}

// -- engine-default flip (v0.3.1 partial migration) --
// Per the user audit (post-v0.3.1): "the default path was the legacy
// path for too long, hiding the engine-portable path. Flipping the
// default makes the new path the standard." The actual routing logic
// lives in Query.handle; the spec below pins the contract.
