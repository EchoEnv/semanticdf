package io.semanticdf.mcp

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** v0.3.1: regression tests for [[Main.parseArgs]] after the
  * `Either[String, _]` cleanup (the parse function now returns
  * `Either[McpParseError, Config]` directly).
  *
  * Per docs/design/error-handling-style.md "Converter return types":
  * the helper returns `Either[L, X]` DIRECTLY so the type info is
  * preserved across the boundary. These tests pin that contract:
  *   - Every parse failure surfaces as a SPECIFIC `McpParseError`
  *     case (NOT a generic String).
  *   - The reason details are carried INSIDE the case class
  *     (NOT as a separate String at the call site).
  *   - Exhaustiveness: 6 cases cover all parse failure modes.
  *
  * Per scala-spark-batch-bugs §1: assert the actual McpParseError
  * case class (not just "returned Left").
  *
  * Per karpathy §4 (verifiable goals): this is the verification
  * step for the parseArgs refactor.
  *
  * Visibility: `parseArgs` is `private[mcp]` (package-private) so
  * tests in the same package can call it directly. This is the
  * smallest visibility change needed to enable direct testing
  * (per scala-impact-analysis: blast radius = test surface only). */
class MainParseArgsSpec extends AnyFunSuite with Matchers {

  // -- happy path --

  test("parseArgs: all required flags → Right(Config)") {
    val result = Main.parseArgs(Seq(
      "--models",     "/tmp/models",
      "--data",       "/tmp/data.yaml",
      "--okf-bundle", "/tmp/okf",
      "--transport",  "rest",
      "--rest-port",  "8999",
    ))
    result.isRight shouldBe true
    val c = result.toOption.get
    c.modelsDir    shouldBe "/tmp/models"
    c.dataConfig   shouldBe "/tmp/data.yaml"
    c.okfBundleDir shouldBe "/tmp/okf"
    c.transport    shouldBe "rest"
    c.restPort     shouldBe 8999
    c.remote       shouldBe None
  }

  test("parseArgs: defaults applied when minimal flags given") {
    // No flags at all → transport defaults to "stdio", restPort to 8080.
    // But the required-arg check fires first → Left(MissingRequiredArgument).
    val result = Main.parseArgs(Seq.empty[String])
    result shouldBe Left(Main.McpParseError.MissingRequiredArgument(flag = "--models", usage = "<dir>"))
  }

  test("parseArgs: --remote sc://... is accepted") {
    val result = Main.parseArgs(Seq(
      "--models",     "/tmp/models",
      "--data",       "/tmp/data.yaml",
      "--okf-bundle", "/tmp/okf",
      "--remote",     "sc://cluster:15002",
    ))
    result.isRight shouldBe true
    result.toOption.get.remote shouldBe Some("sc://cluster:15002")
  }

  // -- MissingFlagValue (flag present without value) --

  test("parseArgs: --models (no value) → MissingFlagValue(\"--models\")") {
    // Put the empty-valued flag at the END of args so the
    // `:: Nil` pattern can match. The parser treats a flag
    // followed by another flag as "flag has that flag as its
    // value" (preserved legacy behavior); `--models <EOF>`
    // is the canonical "no value" case.
    val result = Main.parseArgs(Seq(
      "--data",       "/tmp/data.yaml",
      "--okf-bundle", "/tmp/okf",
      "--models",
    ))
    result shouldBe Left(Main.McpParseError.MissingFlagValue(flag = "--models"))
  }

  test("parseArgs: --rest-port (no value) → MissingFlagValue(\"--rest-port\")") {
    val result = Main.parseArgs(Seq(
      "--models",     "/tmp/models",
      "--data",       "/tmp/data.yaml",
      "--okf-bundle", "/tmp/okf",
      "--rest-port",
    ))
    result shouldBe Left(Main.McpParseError.MissingFlagValue(flag = "--rest-port"))
  }

  // -- MissingRequiredArgument (post-parse required check) --

  test("parseArgs: empty modelsDir → MissingRequiredArgument(\"--models\")") {
    val result = Main.parseArgs(Seq(
      "--data",       "/tmp/data.yaml",
      "--okf-bundle", "/tmp/okf",
    ))
    result shouldBe Left(Main.McpParseError.MissingRequiredArgument(flag = "--models", usage = "<dir>"))
  }

  // -- InvalidIntRange --

  test("parseArgs: --rest-port out of range → InvalidIntRange") {
    val result = Main.parseArgs(Seq(
      "--models",     "/tmp/models",
      "--data",       "/tmp/data.yaml",
      "--okf-bundle", "/tmp/okf",
      "--rest-port",  "99999",
    ))
    result shouldBe Left(Main.McpParseError.InvalidIntRange(
      flag = "--rest-port", value = "99999", min = 1, max = 65535
    ))
  }

  test("parseArgs: --rest-port not an integer → InvalidIntRange") {
    val result = Main.parseArgs(Seq(
      "--models",     "/tmp/models",
      "--data",       "/tmp/data.yaml",
      "--okf-bundle", "/tmp/okf",
      "--rest-port",  "not-a-number",
    ))
    result shouldBe Left(Main.McpParseError.InvalidIntRange(
      flag = "--rest-port", value = "not-a-number", min = 1, max = 65535
    ))
  }

  // -- InvalidScheme --

  test("parseArgs: --remote without sc:// scheme → InvalidScheme") {
    val result = Main.parseArgs(Seq(
      "--models",     "/tmp/models",
      "--data",       "/tmp/data.yaml",
      "--okf-bundle", "/tmp/okf",
      "--remote",     "http://cluster:15002",
    ))
    result shouldBe Left(Main.McpParseError.InvalidScheme(
      flag = "--remote", value = "http://cluster:15002", expectedScheme = "sc://"
    ))
  }

  // -- InvalidEnumValue --

  test("parseArgs: --transport with invalid value → InvalidEnumValue") {
    val result = Main.parseArgs(Seq(
      "--models",     "/tmp/models",
      "--data",       "/tmp/data.yaml",
      "--okf-bundle", "/tmp/okf",
      "--transport",  "websocket",
    ))
    result shouldBe Left(Main.McpParseError.InvalidEnumValue(
      flag = "--transport",
      value = "websocket",
      allowedValues = Set("stdio", "rest"),
    ))
  }

  // -- UnknownArgument --

  test("parseArgs: unknown flag → UnknownArgument") {
    val result = Main.parseArgs(Seq(
      "--models",     "/tmp/models",
      "--data",       "/tmp/data.yaml",
      "--okf-bundle", "/tmp/okf",
      "--bogus",      "value",
    ))
    result shouldBe Left(Main.McpParseError.UnknownArgument(arg = "--bogus"))
  }
}
