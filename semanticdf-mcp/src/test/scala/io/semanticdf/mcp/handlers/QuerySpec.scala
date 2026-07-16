package io.semanticdf.mcp.handlers

import io.semanticdf.{Dimension, Measure, toSemanticTable}
import io.semanticdf.mcp.{DataConfig, ErrorDetail, ErrorEnvelope, Models}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{count, lit}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._
import org.scalatest.{BeforeAndAfterAll, Suite}

/** Tests for the `query` tool handler.
  *
  * Focus: `RESULT_TOO_LARGE` enforcement — the safety cap that rejects
  * queries that omit `limit` and return more rows than `MCP_MAX_ROWS`
  * (default 10000, injectable via the `Query(spark, maxRows = ...)`
  * constructor parameter for testing).
  *
  * Per `mcp-contract.md` §7: if the request omits `limit` and the result
  * exceeds the cap, the server must reject with `RESULT_TOO_LARGE` and
  * include `suggested_limit` in `error.details`. If the request includes
  * `limit`, the result is trusted (the user asked for that many rows). */
class QuerySpec extends AnyFunSuite with BeforeAndAfterAll {

  private val spark: SparkSession = {
    val s = SparkSession.builder()
      .master("local[2]")
      .appName("query-spec")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.ansi.enabled", "false")
      .getOrCreate()
    s
  }

  override protected def afterAll(): Unit = {
    if (spark != null) spark.stop()
  }

  // ---------------------------------------------------------------------------
  // Fixtures
  // ---------------------------------------------------------------------------

  /** 6 rows across 3 distinct carriers → 3 grouped rows. */
  private def flightsDf = {
    import spark.implicits._
    Seq(
      ("AA", 1), ("AA", 1),
      ("UA", 1), ("UA", 1), ("UA", 1),
      ("DL", 1),
    ).toDF("carrier", "dummy")
  }

  private def registry: Models = {
    val table = toSemanticTable(flightsDf, name = Some("flights"))
      .withDimensions(Dimension("carrier", t => t("carrier")))
      .withMeasures(Measure("flight_count", t => count(lit(1))))
    new Models(Map("flights" -> table), DataConfig(Map.empty))
  }

  private def baseRequest = QueryRequest(
    model      = "flights",
    measures   = Seq("flight_count"),
    dimensions = Some(Seq("carrier")),
  )

  // ---------------------------------------------------------------------------
  // RESULT_TOO_LARGE — the safety cap
  // ---------------------------------------------------------------------------

  test("RESULT_TOO_LARGE: query without limit and > maxRows throws ResultTooLarge") {
    // 3 grouped rows (one per carrier) > maxRows=2 → rejection.
    val query = new Query(spark, maxRows = 2)
    val ex = intercept[QueryErrors.ResultTooLarge] {
      query.handle(registry, baseRequest)
    }
    assert(ex.rowCount == 3, s"expected 3 grouped rows, got ${ex.rowCount}")
    assert(ex.limit == 2)
    assert(ex.getMessage.contains("RESULT_TOO_LARGE"))
    assert(ex.getMessage.contains("3"))
    assert(ex.getMessage.contains("2"))
  }

  test("RESULT_TOO_LARGE: query WITH limit is trusted (returns data, not error)") {
    // maxRows=2 but limit=5 — user explicitly asked for more, so we return.
    val query = new Query(spark, maxRows = 2)
    val req = baseRequest.copy(limit = Some(5))
    val env = query.handle(registry, req)
    assert(env.status == "ok")
    assert(env.data.row_count == 3)
    assert(env.data.rows.length == 3)
  }

  test("RESULT_TOO_LARGE: query within maxRows succeeds (no false positive)") {
    // 3 grouped rows < maxRows=10 → succeeds normally.
    val query = new Query(spark, maxRows = 10)
    val env = query.handle(registry, baseRequest)
    assert(env.status == "ok")
    assert(env.data.row_count == 3)
  }

  test("RESULT_TOO_LARGE: the error envelope carries RESULT_TOO_LARGE code + suggested_limit") {
    // Build the error envelope the same way runWithError does and verify
    // its shape directly (no mapper dependency — the SDK's Jackson
    // serialisation is its own concern).
    val query = new Query(spark, maxRows = 2)
    val ex = intercept[QueryErrors.ResultTooLarge] {
      query.handle(registry, baseRequest)
    }
    val env = ErrorEnvelope(
      status = "error",
      error = ErrorDetail(
        code = "RESULT_TOO_LARGE",
        message = ex.getMessage,
        hint = Some(s"Add \"limit\": ${ex.limit} to your request, or narrow your filters with \"where\"."),
        details = Map("suggested_limit" -> ex.limit.toString),
      ),
    )
    assert(env.status == "error")
    assert(env.error.code == "RESULT_TOO_LARGE")
    assert(env.error.details.get("suggested_limit") == Some("2"))
    assert(env.error.hint.isDefined)
    assert(env.error.message.contains("3"))
    assert(env.error.message.contains("2"))
  }

  test("maxRowsFromEnv returns a positive integer (env-aware default)") {
    // CI may or may not have MCP_MAX_ROWS set; either way the result must
    // be a sensible positive integer.
    val parsed = Query.maxRowsFromEnv()
    assert(parsed > 0, s"maxRowsFromEnv must be positive, got $parsed")
    // Verify it matches the env or falls back to the documented default.
    val expected = sys.env.get("MCP_MAX_ROWS")
      .flatMap(v => scala.util.Try(v.toInt).toOption)
      .filter(_ > 0)
      .getOrElse(10000)
    parsed shouldBe expected
  }
}