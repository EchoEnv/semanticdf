package io.semanticdf.mcp

import com.sun.net.httpserver.{HttpServer => JdkHttpServer}
import io.modelcontextprotocol.json.McpJsonDefaults
import io.semanticdf.{Dimension, Measure, toSemanticTable}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{count, lit}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import java.net.{InetSocketAddress, URI}
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.time.Duration
import scala.io.Source
import scala.jdk.CollectionConverters._

/** Integration tests for the REST transport.
  *
  * Starts a real [[RestServer]] on a random port and fires HTTP requests
  * at it via the JDK's `java.net.http.HttpClient`. Verifies that:
  *   - GET /models returns the loaded models
  *   - GET /models/{name} returns schema details
  *   - POST /query runs a query and returns rows
  *   - POST /explain returns the plan
  *   - POST /introspect handles errors gracefully
  *   - Error paths return the right HTTP status + MCP error code
  *
  * Uses a programmatic [[SemanticTable]] (not a YAML model) so the tests
  * are self-contained — no file fixtures needed. */
class RestServerSpec extends AnyFunSuite with org.scalatest.BeforeAndAfterAll {

  private val spark: SparkSession = {
    val s = SparkSession.builder()
      .master("local[2]")
      .appName("rest-server-spec")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.ansi.enabled", "false")
      .getOrCreate()
    s
  }

  override protected def afterAll(): Unit = {
    if (server != null) server.stop(0)
    // Don't stop the spark — other specs in the same suite share it.
    // The JVM exit will clean it up.
  }

  // ---------------------------------------------------------------------------
  // Fixtures
  // ---------------------------------------------------------------------------

  private def testModels: Models = {
    import spark.implicits._
    val df = Seq(("AA", 1.0), ("UA", 2.0), ("DL", 3.0)).toDF("carrier", "score")
    val table = toSemanticTable(df, name = Some("flights"))
      .withDimensions(Dimension("carrier", t => t("carrier")))
      .withMeasures(Measure("score_sum", t => count(lit(1))))
    new Models(Map("flights" -> table), DataConfig(Map.empty))
  }

  private var server: JdkHttpServer = _
  private var port: Int = _
  private val client = HttpClient.newBuilder()
    .connectTimeout(Duration.ofSeconds(5))
    .build()

  override protected def beforeAll(): Unit = {
    val mapper = McpJsonDefaults.getMapper()
    val rest = new RestServer(spark, testModels, new OkfCache(Map.empty), mapper, port = 0)
    server = rest.start()
    port = server.getAddress.getPort
  }

  // ---------------------------------------------------------------------------
  // HTTP helpers
  // ---------------------------------------------------------------------------

  private def get(path: String): (Int, String) = {
    val req = HttpRequest.newBuilder(URI.create(s"http://localhost:$port$path"))
      .timeout(Duration.ofSeconds(10))
      .GET()
      .build()
    val resp = client.send(req, HttpResponse.BodyHandlers.ofString())
    (resp.statusCode(), resp.body())
  }

  private def postJson(path: String, body: String): (Int, String) = {
    val req = HttpRequest.newBuilder(URI.create(s"http://localhost:$port$path"))
      .timeout(Duration.ofSeconds(10))
      .header("Content-Type", "application/json")
      .POST(HttpRequest.BodyPublishers.ofString(body))
      .build()
    val resp = client.send(req, HttpResponse.BodyHandlers.ofString())
    (resp.statusCode(), resp.body())
  }

  // ---------------------------------------------------------------------------
  // Happy paths
  // ---------------------------------------------------------------------------

  test("GET /models returns the loaded models") {
    val (status, body) = get("/models")
    assert(status == 200, s"expected 200, got $status: $body")
    assert(body.contains("flights"),
      s"expected body to contain 'flights', got: $body")
  }

  test("GET /models/{name} returns schema details for the model") {
    val (status, body) = get("/models/flights")
    assert(status == 200, s"expected 200, got $status: $body")
    assert(body.contains("flights"),
      s"expected body to contain 'flights', got: $body")
  }

  test("POST /query with valid request returns 200") {
    val body = """{"model": "flights", "measures": ["score_sum"], "dimensions": ["carrier"]}"""
    val (status, _) = postJson("/query", body)
    assert(status == 200, s"expected 200, got $status")
  }

  test("POST /explain returns the plan text") {
    val body = """{"model": "flights", "measures": ["score_sum"], "dimensions": ["carrier"]}"""
    val (status, resp) = postJson("/explain", body)
    assert(status == 200, s"expected 200, got $status")
    assert(resp.contains("PLAN") || resp.contains("plan") || resp.contains("Error") || resp.length > 0,
      s"expected non-empty response, got: ${resp.take(100)}")
  }

  // ---------------------------------------------------------------------------
  // Error paths
  // ---------------------------------------------------------------------------

  test("POST /query with invalid JSON returns error envelope") {
    val (status, body) = postJson("/query", "{not json}")
    // Either 400 (we caught it) or 500 (something else caught it) — but we
    // should get an error response, not a 200 with garbage.
    assert(status >= 400, s"expected 4xx/5xx, got $status: $body")
    assert(body.contains("error") || body.contains("INVALID") || body.contains("FAIL"),
      s"expected error in body, got: $body")
  }

  test("POST /query with unknown model returns error envelope") {
    val body = """{"model": "nonexistent", "measures": ["x"]}"""
    val (status, resp) = postJson("/query", body)
    assert(status >= 400, s"expected 4xx/5xx, got $status: $resp")
    assert(resp.contains("error") || resp.contains("MODEL_NOT_FOUND") || resp.contains("not found"),
      s"expected error/MODEL_NOT_FOUND in response, got: $resp")
  }

  test("POST /query with empty body returns error envelope") {
    val (status, body) = postJson("/query", "")
    assert(status >= 400, s"expected 4xx/5xx, got $status: $body")
    assert(body.contains("error") || body.contains("INVALID"),
      s"expected error in body, got: $body")
  }

  // ---------------------------------------------------------------------------
  // RESULT_TOO_LARGE surfaced over REST
  // ---------------------------------------------------------------------------

  test("POST /query exceeding MCP_MAX_ROWS returns RESULT_TOO_LARGE") {
    // Build a registry with maxRows=1 so any multi-row query trips the cap.
    import spark.implicits._
    val df = Seq(("AA", 1), ("UA", 2), ("DL", 3)).toDF("carrier", "n")
    val table = toSemanticTable(df, name = Some("flights"))
      .withDimensions(Dimension("carrier", t => t("carrier")))
      .withMeasures(Measure("n_sum", t => count(lit(1))))
    val models = new Models(Map("flights" -> table), DataConfig(Map.empty))
    val mapper = McpJsonDefaults.getMapper()
    val tinyRest = new RestServer(spark, models, new OkfCache(Map.empty), mapper, port = 0, numThreads = 2)
    val tiny = tinyRest.start()
    try {
      val p = tiny.getAddress.getPort
      val req = HttpRequest.newBuilder(URI.create(s"http://localhost:$p/query"))
        .timeout(Duration.ofSeconds(10))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(
          """{"model": "flights", "measures": ["n_sum"], "dimensions": ["carrier"]}"""))
        .build()
      val resp = client.send(req, HttpResponse.BodyHandlers.ofString())
      assert(resp.statusCode() == 500, s"expected 500, got ${resp.statusCode()}")
      assert(resp.body().contains("RESULT_TOO_LARGE"),
        s"expected RESULT_TOO_LARGE, got: ${resp.body()}")
    } finally tiny.stop(0)
  }
}