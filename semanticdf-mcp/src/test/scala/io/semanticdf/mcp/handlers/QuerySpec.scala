package io.semanticdf.mcp.handlers

import io.semanticdf.{Dimension, Measure, Predicate, toSemanticTable}
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
class QuerySpec extends AnyFunSuite with io.semanticdf.mcp.SparkFixture {

  // (SparkSession comes from the shared SparkFixture trait; no
  // per-spec construction/stop. This avoids the "Cannot call methods on
  // a stopped SparkContext" error that hit sibling specs when each spec
  // used to manage its own session lifecycle.)

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

  /** Build a registry with one flights model and a chosen status. */
  private def registryWithStatus(s: io.semanticdf.ModelStatus): Models = {
    val table = toSemanticTable(flightsDf, name = Some("flights"))
      .withDimensions(Dimension("carrier", t => t("carrier")))
      .withMeasures(Measure("flight_count", t => count(lit(1))))
      .status(s)
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

  // ---------------------------------------------------------------------------
  // QUERY_TIMEOUT — the deadline enforcement
  // ---------------------------------------------------------------------------
  //
  // These tests exercise Query.withTimeout directly (a package-private helper
  // extracted from handle()). Using Thread.sleep instead of a real Spark
  // query makes the timeout deterministic — no wall-clock flakiness, no
  // dependency on query execution speed. The integration (handle → timeout)
  // is exercised by the production path, which calls withTimeout.

  test("QUERY_TIMEOUT: withTimeout throws QueryTimeout when body exceeds deadline") {
    val ex = intercept[QueryErrors.QueryTimeout] {
      Query.withTimeout(spark, timeoutMs = 1, groupId = "test-group", "test") {
        Thread.sleep(100)  // 100ms body, 1ms deadline → must timeout
        42
      }
    }
    assert(ex.timeoutMs == 1)
    assert(ex.getMessage.contains("QUERY_TIMEOUT"))
    assert(ex.getMessage.contains("1"))
  }

  test("QUERY_TIMEOUT: withTimeout returns the body's value when within deadline") {
    val result = Query.withTimeout(spark, timeoutMs = 5000, groupId = "test-group", "test") {
      42
    }
    assert(result == 42)
  }

  test("QUERY_TIMEOUT: withTimeout with timeoutMs=0 disables the deadline") {
    // No timeout enforced — a fast body returns its value.
    val result = Query.withTimeout(spark, timeoutMs = 0, groupId = "test-group", "test") {
      Thread.sleep(10)  // 10ms body, no deadline → completes normally
      "done"
    }
    assert(result == "done")
  }

  test("QUERY_TIMEOUT: the error envelope carries QUERY_TIMEOUT code + timeout_ms") {
    // Mirror the runWithError catch logic for QueryTimeout.
    val ex = QueryErrors.QueryTimeout(timeoutMs = 2500)
    val env = ErrorEnvelope(
      status = "error",
      error = ErrorDetail(
        code = "QUERY_TIMEOUT",
        message = ex.getMessage,
        hint = Some("Add a narrower \"where\" or \"limit\" clause, or raise MCP_QUERY_TIMEOUT_MS."),
        details = Map("timeout_ms" -> ex.timeoutMs.toString),
      ),
    )
    assert(env.status == "error")
    assert(env.error.code == "QUERY_TIMEOUT")
    assert(env.error.details.get("timeout_ms") == Some("2500"))
    assert(env.error.hint.isDefined)
    assert(env.error.message.contains("2500"))
  }

  test("QUERY_TIMEOUT: timeoutMsFromEnv returns a non-negative value") {
    // CI may or may not have MCP_QUERY_TIMEOUT_MS set; either way the
    // result must be a sensible non-negative long. A value of 0 is valid
    // (means "disabled").
    val parsed = Query.timeoutMsFromEnv()
    assert(parsed >= 0, s"timeoutMsFromEnv must be non-negative, got $parsed")
  }

  // ---------------------------------------------------------------------------
  // AMBIGUOUS_MEASURE / AMBIGUOUS_DIMENSION — case-insensitive name collisions
  // ---------------------------------------------------------------------------
  //
  // After a join, the merged model exposes both the left model's dimensions
  // (bare names) and the right model's dimensions (aliased as `alias.col`).
  // If a query name matches BOTH a bare and an aliased field
  // case-insensitively, it's ambiguous — the agent needs to pick one
  // explicitly. The MCP layer catches this BEFORE handing off to the
  // library (whose resolution is case-sensitive exact-match, and would
  // silently pick one).

  private def joinedRegistry: Models = {
    import spark.implicits._
    // Two models with a MIXED-CASE field name (left has "Name", right
    // has "name"). The library's case-sensitive collision check sees them
    // as different fields, so the join succeeds. But case-insensitive
    // resolution makes a query for "name" match BOTH.
    val flights = toSemanticTable(
      Seq(("AA", "flight-A", 100.0), ("UA", "flight-U", 200.0)).toDF("carrier", "Name", "distance"),
      name = Some("flights"),
    ).withDimensions(
      Dimension("carrier", t => t("carrier")),
      Dimension("Name",    t => t("Name")),
    ).withMeasures(
      Measure("flight_count", t => org.apache.spark.sql.functions.count(lit(1))),
    )
    val carriers = toSemanticTable(
      Seq(("AA", "American"), ("UA", "United")).toDF("carrier", "name"),
      name = Some("carriers"),
    ).withDimensions(
      Dimension("carrier", t => t("carrier")),
      Dimension("name", t => t("name")),
    )
    val joined = flights.join_one(
      carriers,
      (l, r) => l("carrier") === r("carrier"),
    )
    new Models(Map("flights" -> joined), DataConfig(Map.empty))
  }

  test("AMBIGUOUS: query name matching case-insensitive duplicates throws AmbiguousDimension") {
    // 'name' (lowercase) case-insensitively matches BOTH 'Name' (left)
    // and 'name' (right) → two dimensions share a case-insensitive name.
    val query = new Query(spark, maxRows = 10000)
    val req = baseRequest.copy(
      model      = "flights",
      dimensions = Some(Seq("name")),
      measures   = Seq("flight_count"),
    )
    val ex = intercept[QueryErrors.AmbiguousDimension] {
      query.handle(joinedRegistry, req)
    }
    assert(ex.name == "name")
    assert(ex.candidates.toSet == Set("Name", "name"))
  }

  test("AMBIGUOUS: unique-name query succeeds (single match — no false positive)") {
    // 'carrier' matches exactly one field. Neither 'Name' nor 'name'
    // case-insensitively matches 'carrier', so no ambiguity.
    val query = new Query(spark, maxRows = 10000)
    val req = baseRequest.copy(
      model      = "flights",
      dimensions = Some(Seq("carrier")),
      measures   = Seq("flight_count"),
    )
    val env = query.handle(joinedRegistry, req)
    assert(env.status == "ok")
  }

  test("AMBIGUOUS: checkAmbiguity throws when the same name appears as both dim and measure") {
    import spark.implicits._
    val ambigTable = toSemanticTable(
      Seq(("AA", 1.0), ("UA", 2.0)).toDF("carrier", "score"),
      name = Some("flights"),
    ).withDimensions(
      Dimension("carrier", t => t("carrier")),
    ).withMeasures(
      Measure("score", t => org.apache.spark.sql.functions.sum(t("score"))),
    )
    // Add a dimension with a case-insensitive duplicate name.
    val withDup = ambigTable.withDimensions(
      Dimension("SCORE", t => t("score")),
    )
    val req = QueryRequest(
      model      = "flights",
      measures   = Seq("score"),
      dimensions = Some(Seq("SCORE")),
    )
    val ex = intercept[QueryErrors.AmbiguousDimension] {
      Query.checkAmbiguity(req, withDup)
    }
    assert(ex.candidates.toSet == Set("score", "SCORE"))
  }

  // ===========================================================================
  // Regression: order_by over the REST transport (PR #54 Jackson Scala module)
  // ===========================================================================
  //
  // PR #54 registered Jackson's DefaultScalaModule via `JsonSupport.scalaMapper()`.
  // That change made the MCP server's REST JSON output correctly serialize
  // generic case classes like `Envelope[T]` — but it also changed how the
  // server *deserializes* incoming requests. Untyped nested JSON objects
  // (inside arrays) now arrive as Scala `Map2` instead of `java.util.LinkedHashMap`.
  //
  // `OrderByParser.parse` only matched `java.util.Map[_, _]`, so requests
  // with `order_by` started failing with:
  //   "order_by entry must be a JSON object, got Map2"
  //
  // Caught by the new `sdf` CLI consumer (which sends real JSON over HTTP).
  // No existing test exercised this path: the SDK adapter builds args as
  // `java.util.Map` directly, bypassing Jackson. The fix needs to accept
  // BOTH `java.util.Map` (legacy / SDK-direct callers) and Scala `Map`
  // (Jackson-with-Scala-module callers).

  test("parseRequest: order_by works through the Jackson-with-Scala-module path (REST)") {
    // Replicate the EXACT deserialization the RestServer does:
    //   mapper.readValue(body, classOf[java.util.Map[String, Object]])
    // where `mapper` is the Scala-module-registered JsonSupport mapper.
    val mapper = io.semanticdf.mcp.JsonSupport.scalaMapper()
    val body =
      """{"model":"flights","measures":["c"],"order_by":[{"field":"carrier","direction":"asc"}]}"""
    val args = mapper.readValue(body, classOf[java.util.Map[String, Object]])

    val req = Query.parseRequest(args)

    assert(req.order_by == Seq(OrderBy("carrier", "asc")),
      s"expected parsed order_by, got: ${req.order_by}")
  }

  test("parseRequest: order_by with desc direction works through the Scala-module path") {
    val mapper = io.semanticdf.mcp.JsonSupport.scalaMapper()
    val body =
      """{"model":"flights","measures":["c"],"order_by":[{"field":"x","direction":"desc"}]}"""
    val args = mapper.readValue(body, classOf[java.util.Map[String, Object]])
    val req = Query.parseRequest(args)
    assert(req.order_by == Seq(OrderBy("x", "desc")))
  }

  test("parseRequest: multiple order_by entries work through the Scala-module path") {
    val mapper = io.semanticdf.mcp.JsonSupport.scalaMapper()
    val body =
      """{"model":"flights","measures":["c"],"order_by":[
        |{"field":"a","direction":"asc"},
        |{"field":"b","direction":"desc"}
        |]}""".stripMargin
    val args = mapper.readValue(body, classOf[java.util.Map[String, Object]])
    val req = Query.parseRequest(args)
    assert(req.order_by == Seq(OrderBy("a", "asc"), OrderBy("b", "desc")))
  }

  test("OrderByParser.parse: accepts java.util.Map (legacy SDK-direct callers)") {
    // Backward compat: SDK adapter still constructs java.util.Map directly.
    val m: java.util.Map[String, Any] = new java.util.HashMap[String, Any]()
    m.put("field", "carrier"); m.put("direction", "asc")
    val ob = OrderByParser.parse(m)
    assert(ob == OrderBy("carrier", "asc"))
  }

  test("OrderByParser.parse: accepts Scala Map (Jackson-with-Scala-module callers)") {
    // The regression: Jackson's Scala module deserialises nested objects
    // as Scala Map2, not java.util.LinkedHashMap.
    val m: Map[String, Any] = Map("field" -> "carrier", "direction" -> "desc")
    val ob = OrderByParser.parse(m)
    assert(ob == OrderBy("carrier", "desc"))
  }

  // ---------------------------------------------------------------------------
  // Lifecycle warnings — query/explain surface model lifecycle on the envelope
  // ---------------------------------------------------------------------------

  test("query-warns-deprecated-model: query against Deprecated model carries warning") {
    val reg = registryWithStatus(io.semanticdf.ModelStatus.Deprecated)
    val env = new Query(spark).handle(reg, baseRequest)
    env.warnings shouldBe List("model 'flights' is deprecated")
  }

  test("query-warns-draft-model: query against Draft model carries draft-specific warning") {
    val reg = registryWithStatus(io.semanticdf.ModelStatus.Draft)
    val env = new Query(spark).handle(reg, baseRequest)
    env.warnings shouldBe List("model 'flights' is in draft; shape may change")
  }

  test("query-does-not-warn-published: query against Published model returns warnings == Nil") {
    val env = new Query(spark).handle(registryWithStatus(io.semanticdf.ModelStatus.Published), baseRequest)
    env.warnings shouldBe Nil
  }

  test("query-explains-warns-deprecated: explain() follows the same warning policy as handle()") {
    val reg = registryWithStatus(io.semanticdf.ModelStatus.Deprecated)
    val env = new Query(spark).explain(reg, baseRequest)
    env.warnings shouldBe List("model 'flights' is deprecated")
  }

  test("error-envelope-codes-unchanged: closed error-code list unaffected by lifecycle") {
    // Regression: lifecycle states produce success envelopes with warnings,
    // never error envelopes. Verify the model-not-found path still uses
    // the existing error code (not a new MODEL_DEPRECATED etc.).
    val ex = intercept[io.semanticdf.mcp.ModelNotFound] {
      new Query(spark).handle(registry, baseRequest.copy(model = "ghost"))
    }
    ex.getMessage should include("ghost")
  }

  // ---------------------------------------------------------------------------
  // ast_where: structured predicate on the query wire
  // ---------------------------------------------------------------------------

  test("ast_where: parses and runs a simple leaf AST") {
    val ast: java.util.Map[String, Any] = new java.util.HashMap[String, Any]()
    ast.put("op",    "eq")
    ast.put("left",  "carrier")
    ast.put("right", "AA")
    val req = baseRequest.copy(
      measures = Seq("flight_count"),
      ast_where = Some(ast),
    )
    val env = new Query(spark).handle(registry, req)
    assert(env.status == "ok", s"expected ok, got ${env.status}: ${env.data}")
    // 2 rows of AA out of 6.
    val data = env.data.asInstanceOf[Query.Data]
    assert(data.row_count == 1, s"expected 1 row (1 group: AA), got ${data.row_count}")
  }

  test("ast_where: nested AND/OR produces the right row count") {
    // distance: int; carrier: string. Build a 2-dim table to make this meaningful.
    val table = toSemanticTable(
      spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(
          ("AA", 100), ("AA", 600),
          ("UA", 200), ("UA", 800),
          ("DL", 50),
        ))
      ).toDF("carrier", "distance"),
      name = Some("flights"))
      .withDimensions(Dimension("carrier", t => t("carrier")))
      .withMeasures(Measure("flight_count", t => count(lit(1))))
    val reg = new Models(Map("flights" -> table), DataConfig(Map.empty))
    // (carrier=AA OR carrier=DL) AND distance > 400
    val ast: java.util.Map[String, Any] = new java.util.HashMap[String, Any]()
    ast.put("op", "and")
    val left: java.util.Map[String, Any] = new java.util.HashMap[String, Any]()
    left.put("op", "or")
    val a: java.util.Map[String, Any] = new java.util.HashMap[String, Any]()
    a.put("op", "eq"); a.put("left", "carrier"); a.put("right", "AA")
    val b: java.util.Map[String, Any] = new java.util.HashMap[String, Any]()
    b.put("op", "eq"); b.put("left", "carrier"); b.put("right", "DL")
    left.put("left", a); left.put("right", b)
    val right: java.util.Map[String, Any] = new java.util.HashMap[String, Any]()
    right.put("op", "gt"); right.put("left", "distance"); right.put("right", 400)
    ast.put("left", left); ast.put("right", right)
    val env = new Query(spark).handle(reg, baseRequest.copy(measures = Seq("flight_count"), ast_where = Some(ast)))
    // Rows where (carrier IN [AA, DL]) AND distance > 400: only the AA row with distance 600.
    val data = env.data.asInstanceOf[Query.Data]
    assert(data.row_count == 1, s"expected 1, got ${data.row_count}: ${data.rows}")
  }

  test("ast_where AND flat where: both applied, AND-combined by server") {
    // flightsDf has carrier + dummy columns. Two predicates both on `carrier`:
    // AST says carrier=AA; flat says carrier=UA. Server must AND-combine:
    // no row matches both, so result is 0 rows.
    val ast: java.util.Map[String, Any] = new java.util.HashMap[String, Any]()
    ast.put("op", "eq"); ast.put("left", "carrier"); ast.put("right", "AA")
    val flat: java.util.Map[String, Any] = new java.util.HashMap[String, Any]()
    flat.put("type", "eq"); flat.put("field", "carrier"); flat.put("value", "UA")
    val req = baseRequest.copy(
      measures = Seq("flight_count"),
      ast_where = Some(ast),
      where = Some(Seq(flat)),
    )
    val env = new Query(spark).handle(registry, req)
    val data = env.data.asInstanceOf[Query.Data]
    // No row matches both AA AND UA -> 0 grouped rows.
    assert(env.status == "ok")
    assert(data.row_count == 0, s"expected 0 rows (AA AND UA is unsatisfiable), got ${data.row_count}")
  }

  test("mergedWhere: returns None when both fields are empty") {
    assert(Query.mergedWhere(QueryRequest("f", Seq("m"))) == None)
  }

  test("mergedWhere: returns AST when only AST is present") {
    val ast: java.util.Map[String, Any] = new java.util.HashMap[String, Any]()
    ast.put("op", "eq"); ast.put("left", "carrier"); ast.put("right", "AA")
    val req = QueryRequest("f", Seq("m"), ast_where = Some(ast))
    val merged = Query.mergedWhere(req)
    assert(merged.isDefined)
    assert(merged.get == Predicate.Compare.Eq("carrier", "AA"))
  }

  test("mergedWhere: AND-combines AST and flat when both present") {
    val ast: java.util.Map[String, Any] = new java.util.HashMap[String, Any]()
    ast.put("op", "gt"); ast.put("left", "distance"); ast.put("right", 100)
    val flat: java.util.Map[String, Any] = new java.util.HashMap[String, Any]()
    flat.put("type", "eq"); flat.put("field", "carrier"); flat.put("value", "AA")
    val req = QueryRequest("f", Seq("m"), ast_where = Some(ast), where = Some(Seq(flat)))
    val merged = Query.mergedWhere(req).get
    assert(merged.isInstanceOf[Predicate.And])
  }

  test("parseRequest: ast_where round-trips from JSON") {
    val mapper = io.semanticdf.mcp.JsonSupport.scalaMapper()
    val body =
      """{"model":"flights","measures":["c"],"ast_where":{"op":"eq","left":"carrier","right":"AA"}}"""
    val args = mapper.readValue(body, classOf[java.util.Map[String, Object]])
    val req = Query.parseRequest(args)
    assert(req.ast_where.isDefined, s"expected ast_where to be set, got ${req.ast_where}")
    val parsed = AstPredicates.parse(req.ast_where.get)
    assert(parsed == Predicate.Compare.Eq("carrier", "AA"))
  }
}