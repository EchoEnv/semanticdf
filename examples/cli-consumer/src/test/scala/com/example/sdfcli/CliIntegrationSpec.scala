package com.example.sdfcli

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets

import scala.collection.mutable
import scala.io.Source
import scala.jdk.CollectionConverters._

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}

import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

/** Integration tests for `sdf` — the CLI's only test surface.
  *
  * Approach: spin up an in-process `com.sun.net.httpserver.HttpServer` on a
  * random port, register handlers that respond with realistic MCP server JSON,
  * point `Main.run` at it via `--url`, capture stdout/stderr via `Console`,
  * and assert on exit code + rendered output.
  *
  * Why real HTTP, not mocks: `sdf` is documented as the REST contract's
  * regression witness. Stubbing the HTTP layer would prove nothing about
  * the wire format. JDK's `HttpServer` is in the `jdk.httpserver` module
  * — zero extra deps.
  *
  * Fixture JSON shapes mirror what `RestServer` (the actual MCP server)
  * produces; if the server shape drifts, these tests catch it.
  */
class CliIntegrationSpec
  extends AnyFunSpec
  with Matchers
  with BeforeAndAfterAll
  with BeforeAndAfterEach {

  private var server: HttpServer = _
  private var baseUrl: String = _
  /** Path -> response (status, body). The handler reads this on each call. */
  private val responses: mutable.Map[String, (Int, String)] = mutable.Map.empty
  /** Path -> last received body (so tests can assert on what the CLI sent). */
  private val received: mutable.Map[String, String] = mutable.Map.empty

  override def beforeAll(): Unit = {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    server.createContext("/", new RoutingHandler)
    server.start()
    val port = server.getAddress.getPort
    baseUrl = s"http://127.0.0.1:$port"
    // Print to the underlying System.err so we see it even with Console.withErr wrapping.
    java.lang.System.err.println(s"[CliIntegrationSpec] server bound at $baseUrl")
  }

  override def afterAll(): Unit = {
    if (server != null) server.stop(0)
  }

  override def beforeEach(): Unit = resetFixture()

  /** Reset the fixture state between tests. */
  private def resetFixture(): Unit = {
    responses.clear()
    received.clear()
  }

  /** Program the mock server: path -> (status, JSON body). */
  private def respondWith(path: String, status: Int, body: String): Unit = {
    responses(path) = (status, body)
  }

  /** Wrap `Main.run` with stdout + stderr capture, return (exit, out, err).
    *
    * Scala 2.13's `println` goes to `Console.out` (NOT `System.out`), and
    * `Console.withOut` only swaps `Console.out`. So we must:
    *   - `System.setOut`/`System.setErr` for the CLI's `System.err.println`
    *     calls (lifecycle warnings, error envelopes).
    *   - `Console.withOut`/`Console.withErr` for the CLI's `println` calls
    *     (table rendering, status line, etc.).
    * Both routes write into the same ByteArrayOutputStream so the
    * returned `out`/`err` strings aggregate everything. */
  private def runCli(args: List[String]): (Int, String, String) = {
    val outBuf = new java.io.ByteArrayOutputStream
    val errBuf = new java.io.ByteArrayOutputStream
    val sysOut = System.out
    val sysErr = System.err
    val outStream = new java.io.PrintStream(outBuf, true, "UTF-8")
    val errStream = new java.io.PrintStream(errBuf, true, "UTF-8")
    System.setOut(outStream)
    System.setErr(errStream)
    val exit =
      try Console.withOut(outStream) {
        Console.withErr(errStream) {
          Main.run(args)
        }
      } finally {
        outStream.flush()
        errStream.flush()
        System.setOut(sysOut)
        System.setErr(sysErr)
      }
    (exit, outBuf.toString("UTF-8"), errBuf.toString("UTF-8"))
  }

  private def args(cmd: String, more: String*): List[String] =
    cmd :: List("--url", baseUrl) ++ more.toList

  // ============================================================================
  // Routing handler — wires each request to its programmed response
  // ============================================================================

  private class RoutingHandler extends HttpHandler {
    override def handle(exch: HttpExchange): Unit = {
      val path = exch.getRequestURI.getPath
      received(path) =
        Option(exch.getRequestBody).map { in =>
          Source.fromInputStream(in, "UTF-8").getLines.mkString
        }.getOrElse("")
      responses.get(path) match {
        case Some((status, body)) =>
          val bytes = body.getBytes(StandardCharsets.UTF_8)
          exch.sendResponseHeaders(status, bytes.length.toLong)
          exch.getResponseBody.write(bytes)
        case None =>
          // No response programmed — return empty 404. The CLI treats
          // empty body as "no models loaded" or similar; tests that
          // expect a 404 body should program a response.
          exch.sendResponseHeaders(404, -1L)
      }
      exch.close()
    }
  }

  // ============================================================================
  // 1. `list` command
  // ============================================================================

  describe("`list` command") {

    it("renders a table with MODEL, STATUS, DESCRIPTION columns") {
      val dbg = s"DEBUG: baseUrl=$baseUrl\nDEBUG: args=${args("list")}\nDEBUG: server.port=${server.getAddress.getPort}"
      respondWith("/models", 200, """{
        |  "status": "ok",
        |  "data": {
        |    "models": [
        |      {"name": "flights",  "description": "Flight facts",     "status": "published"},
        |      {"name": "carriers", "description": "Carrier lookup",   "status": "deprecated"}
        |    ]
        |  },
        |  "warnings": [],
        |  "meta": {}
        |}""".stripMargin)

      val (exit, out, err) = runCli(args("list"))
      exit shouldBe 0
      err shouldBe ""
      out should include("MODEL")
      out should include("STATUS")
      out should include("DESCRIPTION")
      out should include("flights")
      out should include("carriers")
      out should include("published")
      out should include("deprecated")
    }

    it("prints WARN: lines for each non-Published model") {
      respondWith("/models", 200, """{
        |  "status": "ok",
        |  "data": {"models": [
        |    {"name": "flights",  "description": "Flight facts",   "status": "published"},
        |    {"name": "legacy",   "description": "Legacy stuff",   "status": "deprecated"},
        |    {"name": "draft_m",  "description": "In progress",    "status": "draft"}
        |  ]},
        |  "warnings": [
        |    "model 'legacy' is deprecated",
        |    "model 'draft_m' is in draft; shape may change"
        |  ],
        |  "meta": {}
        |}""".stripMargin)

      val (exit, out, err) = runCli(args("list"))
      exit shouldBe 0
      out should include("flights")
      err should include("WARN: model 'legacy' is deprecated")
      err should include("WARN: model 'draft_m' is in draft; shape may change")
    }

    it("prints `(no models loaded)` for an empty registry") {
      respondWith("/models", 200, """{
        |  "status": "ok", "data": {"models": []}, "warnings": [], "meta": {}
        |}""".stripMargin)

      val (exit, out, _) = runCli(args("list"))
      exit shouldBe 0
      out should include("(no models loaded)")
    }

    it("`--json` prints the raw envelope to stdout, no WARN: lines on stderr") {
      respondWith("/models", 200, """{
        |  "status": "ok", "data": {"models": [{"name": "x", "status": "published"}]},
        |  "warnings": ["model 'x' is deprecated"], "meta": {}
        |}""".stripMargin)

      val (exit, out, err) = runCli(args("list", "--json"))
      exit shouldBe 0
      // --json emits the raw envelope to stdout. Test for substring presence
      // without embedded double-quotes (those are awkward to escape).
      out should include("deprecated")  // comes from warnings[0]
      out should include("\"status\"")
      // --json does NOT emit WARN: lines (warnings are for the human-readable
      // path; the raw JSON output is what consumers parse).
      err shouldBe ""
    }
  }

  // ============================================================================
  // 2. `describe` command
  // ============================================================================

  describe("`describe` command") {

    it("renders Model / Version / Status lines + sections") {
      respondWith("/models/flights", 200, """{
        |  "status": "ok",
        |  "data": {
        |    "model": "flights", "version": 3, "status": "published",
        |    "source_table": "flights_csv",
        |    "filters": [],
        |    "dimensions": [{"name": "carrier", "expr": "carrier"}],
        |    "measures": [{"name": "flight_count", "kind": "base", "expr": "count(1)"}],
        |    "joins": []
        |  },
        |  "warnings": [],
        |  "meta": {}
        |}""".stripMargin)

      val (exit, out, err) = runCli(args("describe", "flights"))
      exit shouldBe 0
      err shouldBe ""
      out should include("Model:        flights")
      out should include("Version:      3")
      out should include("Status:       published")
      out should include("Source table: flights_csv")
      out should include("carrier")
      out should include("flight_count")
    }

    it("prints WARN: line on stderr when model is Deprecated, Status line shows deprecated") {
      respondWith("/models/legacy_flights", 200, """{
        |  "status": "ok",
        |  "data": {
        |    "model": "legacy_flights", "version": 1, "status": "deprecated",
        |    "source_table": "legacy_csv",
        |    "filters": [], "dimensions": [], "measures": [], "joins": []
        |  },
        |  "warnings": ["model 'legacy_flights' is deprecated"],
        |  "meta": {}
        |}""".stripMargin)

      val (exit, out, err) = runCli(args("describe", "legacy_flights"))
      exit shouldBe 0
      err should include("WARN: model 'legacy_flights' is deprecated")
      out should include("Status:       deprecated")
    }

    it("returns exit 1 + error on stderr for MODEL_NOT_FOUND") {
      respondWith("/models/ghost", 404, """{
        |  "status": "error",
        |  "error": {"code": "MODEL_NOT_FOUND", "message": "no model named 'ghost'"}
        |}""".stripMargin)

      val (exit, _, err) = runCli(args("describe", "ghost"))
      exit shouldBe 1
      err should include("MODEL_NOT_FOUND")
      err should include("ghost")
    }

    it("`describe` with no argument returns exit 2 (usage error)") {
      val (exit, _, err) = runCli(args("describe"))
      exit shouldBe 2
      err should include("Usage")
    }
  }

  // ============================================================================
  // 3. `query` and `explain` commands
  // ============================================================================

  describe("`query` and `explain` commands") {

    it("query: renders a table and row_count footer") {
      respondWith("/query", 200, """{
        |  "status": "ok",
        |  "data": {
        |    "columns": [{"name": "carrier"}, {"name": "flight_count"}],
        |    "rows": [
        |      ["AA", 100], ["UA", 200], ["DL", 300]
        |    ],
        |    "row_count": 3,
        |    "truncated": false
        |  },
        |  "warnings": [],
        |  "meta": {"elapsed_ms": 42, "model": "flights"}
        |}""".stripMargin)

      val (exit, out, err) = runCli(args("query", "flights",
        "-m", "flight_count", "-d", "carrier", "--limit", "10"))
      exit shouldBe 0
      err shouldBe ""
      out should include("carrier")
      out should include("flight_count")
      out should include("AA")
      out should include("UA")
      out should include("DL")
      out should include("3 rows")
    }

    it("query: POSTs a body with model/dimensions/measures/limit to /query") {
      respondWith("/query", 200, """{
        |  "status": "ok",
        |  "data": {"columns": [], "rows": [], "row_count": 0, "truncated": false},
        |  "warnings": [], "meta": {}
        |}""".stripMargin)

      runCli(args("query", "flights",
        "-d", "carrier", "-m", "flight_count", "-o", "flight_count:desc",
        "--limit", "5"))
      val sent = received("/query")
      sent should include("\"model\":\"flights\"")
      sent should include("\"dimensions\":[\"carrier\"]")
      sent should include("\"measures\":[\"flight_count\"]")
      sent should include("\"limit\":5")
      sent should include("\"direction\":\"desc\"")
    }

    it("query: surfaces WARN: line on stderr when model is Deprecated") {
      respondWith("/query", 200, """{
        |  "status": "ok",
        |  "data": {"columns": [], "rows": [], "row_count": 0, "truncated": false},
        |  "warnings": ["model 'legacy' is deprecated"],
        |  "meta": {}
        |}""".stripMargin)

      val (exit, _, err) = runCli(args("query", "legacy", "-m", "flight_count"))
      exit shouldBe 0
      err should include("WARN: model 'legacy' is deprecated")
    }

    it("query: returns exit 1 for RESULT_TOO_LARGE") {
      respondWith("/query", 400, """{
        |  "status": "error",
        |  "error": {
        |    "code": "RESULT_TOO_LARGE",
        |    "message": "result exceeds maxRows=100 (got 250); add a `limit` parameter",
        |    "details": {"row_count": "250", "max_rows": "100", "suggested_limit": "100"}
        |  }
        |}""".stripMargin)

      val (exit, _, err) = runCli(args("query", "flights", "-m", "flight_count"))
      exit shouldBe 1
      err should include("RESULT_TOO_LARGE")
    }

    it("explain: prints the plan text + WARN: line on stderr") {
      respondWith("/explain", 200, """{
        |  "status": "ok",
        |  "data": "PLAN SUMMARY\n  table: flights\n  group by: carrier\n  compute: flight_count\n",
        |  "warnings": ["model 'flights' is deprecated"],
        |  "meta": {"model": "flights"}
        |}""".stripMargin)

      val (exit, out, err) = runCli(args("explain", "flights",
        "-d", "carrier", "-m", "flight_count"))
      exit shouldBe 0
      out should include("PLAN SUMMARY")
      out should include("group by: carrier")
      err should include("WARN: model 'flights' is deprecated")
    }

    it("query with no model returns exit 2 (usage error)") {
      // Note: a query with no `-m` is actually allowed (the CLI builds an
      // empty-measures query). The missing-model case is the real usage
      // error.
      val (exit, _, err) = runCli(args("query"))
      exit shouldBe 2
      err should include("Usage")
    }
  }

  // ============================================================================
  // 4. Exit codes and global flags
  // ============================================================================

  describe("exit codes and global flags") {

    it("`--help` returns exit 0") {
      val (exit, _, _) = runCli(args("--help"))
      exit shouldBe 0
    }

    it("unknown subcommand returns exit 2") {
      val (exit, _, err) = runCli(args("nope"))
      exit shouldBe 2
      err.toLowerCase should include("unknown")
    }

    it("transport error (server down) returns exit 3") {
      // Point at a port nothing is listening on. `--url` must come AFTER
      // the subcommand for the CLI's existing dispatch to extract it.
      val (exit, _, _) = runCli(args("list", "--url", "http://127.0.0.1:1"))
      exit shouldBe 3
    }

    it("`--json` for query prints the raw envelope") {
      respondWith("/query", 200, """{
        |  "status": "ok",
        |  "data": {"columns": [], "rows": [], "row_count": 0, "truncated": false},
        |  "warnings": [], "meta": {}
        |}""".stripMargin)

      val (exit, out, err) = runCli(args("query", "flights",
        "-m", "flight_count", "--json"))
      exit shouldBe 0
      out should include("\"status\": \"ok\"")
      // --json does not print WARN lines (raw envelope is the source of truth)
      err shouldBe ""
    }
  }
}