package com.example.sdfcli

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.time.Duration
import scala.annotation.tailrec
import scala.jdk.CollectionConverters._

/** `sdf` — a command-line client for the semanticdf REST API.
  *
  * A pure HTTP+JSON client. No Spark, no model loading — it talks to a
  * running semanticdf REST server (started via
  * `mvn exec:java -Dexec.mainClass=io.semanticdf.mcp.Main -- --transport rest ...`
  * on the `semanticdf-mcp` module).
  *
  * == Subcommands ==
  *
  * {{{
  *   sdf list                            list available models
  *   sdf describe <model>                show a model's dimensions/measures/filters
  *   sdf query <model> [options]         run a semantic query, print a table
  *   sdf explain <model> [options]       show the semantic plan (no execution)
  * }}}
  *
  * == Global options ==
  *
  *   --url <base>     server base URL (default $SDF_URL or http://localhost:8080)
  *   --json           print the raw JSON response instead of pretty output
  *   -h, --help       show usage
  *
  * == Query/explain options ==
  *
  *   -d, --dim <name>        dimension (repeatable)
  *   -m, --measure <name>    measure (repeatable)
  *   -o, --order <f:dir>     order by field, dir = asc|desc (repeatable)
  *   --limit <n>             row limit
  *
  * Run via the bin/sdf wrapper, or directly:
  *   mvn -q exec:java -Dexec.mainClass=com.example.sdfcli.Main -Dexec.args="list --url http://localhost:8080"
  */
object Main {

  def main(args: Array[String]): Unit = {
    val exit = run(args.toList)
    sys.exit(exit)
  }

  /** Pure (testable) entry point — returns an exit code instead of calling sys.exit.
    *
    * Wraps each subcommand call in a TransportFailure catch so transport
    * errors (connection refused, timeout) return exit code 3 cleanly,
    * even when the JVM is being driven by a test harness. */
  def run(args: List[String]): Int = args match {
    case Nil | ("-h" :: _) | ("--help" :: _) | ("help" :: _) =>
      printUsage(); 0
    case ("-v" :: _) | ("--version" :: _) =>
      println("sdf 0.1.17 (semanticdf CLI client)"); 0
    case ("list" :: rest)       => withGlobalConfig(rest) { (cfg, rem) => safeRun { cmdList(cfg); 0 } }
    case ("describe" :: rest)   => withGlobalConfig(rest) { (cfg, rem) => safeRun(cmdDescribe(cfg, rem)) }
    case ("query" :: rest)      => withGlobalConfig(rest) { (cfg, rem) => safeRun(cmdQuery(cfg, rem, explain = false)) }
    case ("explain" :: rest)    => withGlobalConfig(rest) { (cfg, rem) => safeRun(cmdQuery(cfg, rem, explain = true)) }
    case other :: _ =>
      System.err.println(s"sdf: unknown command '$other'. Run 'sdf --help'."); 2
  }

  /** Run a subcommand with TransportFailure caught — returns 3 on transport
    * error. The detail message is already printed to stderr by [[Client.send]],
    * so this is silent on the success path. */
  private def safeRun(f: => Int): Int =
    try f
    catch { case _: Client.TransportFailure => 3 }

  // ---------------------------------------------------------------------------
  // Global config: --url and --json can appear anywhere; strip them first so
  // each subcommand handler only sees its own flags.
  // ---------------------------------------------------------------------------

  private case class Config(baseUrl: String, json: Boolean)

  // -------------------------------------------------------------------
  // Typed CLI parse errors (per docs/design/error-handling-style.md).
  //
  // Per the standard's hard bans:
  //   - No `Either[String, X]` in any new code path.
  //   - All sealed error ADTs use SPECIFIC cases (no generic `ParseError`).
  //
  // Each case carries the data needed to format a stable, programmatic
  // human-readable message via `.message`. We don't format eagerly (no
  // s-strings at construction) because the caller may want to log or
  // surface the case structurally before showing it.
  // -------------------------------------------------------------------
  private sealed trait CliParseError extends Product with Serializable {
    /** Stable human-readable message for stderr / logs. */
    def message: String
  }
  private object CliParseError {
    final case class MissingFlagValue(flag: String) extends CliParseError {
      val message: String = s"$flag requires a value"
    }
    final case class MissingModel(usage: String) extends CliParseError {
      val message: String = s"missing <model>. $usage"
    }
    final case class InvalidOrderFormat(value: String) extends CliParseError {
      val message: String = s"--order must be <field:asc|desc>, got '$value'"
    }
    final case class InvalidLimit(value: String) extends CliParseError {
      val message: String = s"--limit must be a non-negative integer, got '$value'"
    }
    final case class UnknownFlag(flag: String) extends CliParseError {
      val message: String = s"unknown flag: $flag"
    }
    final case class UnexpectedPositional(value: String, existingModel: String)
        extends CliParseError {
      val message: String =
        s"unexpected argument: $value (model already given as $existingModel)"
    }
  }

  /** Pull `--url` / `--json` out of the arg list (they can appear anywhere),
    * then hand the remaining args (and the resolved config) to the
    * subcommand handler. Implemented as a single two-pass walk in
    * [[extractGlobals]]. */
  private def withGlobalConfig(args: List[String])(f: (Config, List[String]) => Int): Int =
    extractGlobals(args) match {
      case Left(err) => System.err.println(s"sdf: ${err.message}"); 2
      case Right((cfg, rem)) => f(cfg, rem)
    }

  /** Two-pass extraction: walk the whole arg list, pulling out --url/--json
    * wherever they appear, leaving everything else in declaration order.
    *
    * Per docs/design/error-handling-style.md hard ban #1: returns
    * `Either[CliParseError, _]` (the existing ADT from PR #433) — no
    * `Either[String, X]`. The single failure mode (`--url` without a
    * value) maps to `CliParseError.MissingFlagValue(flag = "--url")`,
    * reusing the case the `QueryArgs` parser already uses. */
  private def extractGlobals(args: List[String]): Either[CliParseError, (Config, List[String])] = {
    @tailrec def loop(
        in: List[String],
        url: Option[String],
        json: Boolean,
        kept: List[String],
    ): Either[CliParseError, (Config, List[String])] = in match {
      case Nil =>
        Right((Config(url.getOrElse(defaultUrl), json), kept.reverse))
      case ("--url" :: u :: rest) => loop(rest, Some(u), json, kept)
      case ("--url" :: Nil)       => Left(CliParseError.MissingFlagValue(flag = "--url"))
      case ("--json" :: rest)     => loop(rest, url, json = true, kept)
      case other :: rest          => loop(rest, url, json, other :: kept)
    }
    loop(args, None, json = false, Nil)
  }

  /** Plain Jackson mapper (no Scala module) — the CLI only reads JSON via
    * the tree model and writes small string/int values, so a vanilla
    * ObjectMapper is all it needs. Keeps the client dependency-free beyond
    * jackson-databind. */
  private val mapper = new ObjectMapper()

  private def defaultUrl: String =
    sys.env.getOrElse("SDF_URL", "http://localhost:8080")

  // ---------------------------------------------------------------------------
  // Lifecycle warnings — rendered to stderr so they don't pollute --json
  // ---------------------------------------------------------------------------

  /** Print lifecycle warnings to stderr. Wire-stable strings from the MCP
    * server (lifecycle surfacing contract). Format: one `WARN: <message>` line per
    * warning. Printed before the command's main output so the human
    * reader sees them in context. */
  private def printWarnings(warnings: List[String]): Unit = warnings.foreach { w =>
    System.err.println(s"WARN: $w")
  }

  // ---------------------------------------------------------------------------
  // Commands
  // ---------------------------------------------------------------------------

  private def cmdList(cfg: Config): Unit = {
    val resp = Client.get(cfg, "/models")
    if (cfg.json) { println(resp.body); return }
    val root = resp.parseJson
    if (root.errorPath(cfg)) return
    printWarnings(root.warningsPath)
    val models = root.dataPath.field("models").elemList
    if (models.isEmpty) { println("(no models loaded)"); return }
    val rows = models.map { m =>
      val name = m.field("name").text
      val desc = m.field("description").text
      val status = m.field("status").text
      List(name, desc, status)
    }
    println(Table.render(List("MODEL", "STATUS", "DESCRIPTION"), rows))
  }

  private def cmdDescribe(cfg: Config, args: List[String]): Int = args match {
    case Nil =>
      System.err.println("sdf describe: missing <model>. Usage: sdf describe <model>"); 2
    case model :: Nil =>
      val resp = Client.get(cfg, s"/models/$model")
      if (cfg.json) { println(resp.body); return 0 }
      val root = resp.parseJson
      if (root.errorPath(cfg)) return 1
      printWarnings(root.warningsPath)
      printDescribe(root.dataPath)
      0
    case _ =>
      System.err.println("sdf describe: too many arguments. Usage: sdf describe <model>"); 2
  }

  private def printDescribe(d: JsonNode): Unit = {
    println(s"Model:        ${d.field("model").text}")
    println(s"Version:      ${d.field("version").text}")
    val status = d.field("status").textOption
    status.foreach(s => println(s"Status:       $s"))
    val src = d.field("source_table").textOption
    src.foreach(s => println(s"Source table: $s"))
    println()

    def section(title: String, field: String, cols: List[String], extract: JsonNode => List[String]): Unit = {
      val items = d.field(field).elemList
      if (items.nonEmpty) {
        println(s"$title:")
        val rows = items.map(extract)
        println(Table.render(cols, rows))
        println()
      }
    }

    section("Filters",     "filters",     List("NAME", "EXPR"),
      m => List(m.field("name").text, maskExpr(m.field("expr").text)))
    section("Dimensions",  "dimensions",  List("NAME", "EXPR"),
      m => List(m.field("name").text, maskExpr(m.field("expr").text)))
    section("Measures",    "measures",    List("NAME", "KIND", "EXPR"),
      m => List(m.field("name").text, m.field("kind").text, maskExpr(m.field("expr").text)))

    val joins = d.field("joins").elemList
    if (joins.nonEmpty) {
      println("Joins:")
      val rows = joins.map { j =>
        List(j.field("name").text, j.field("left").text, j.field("right").text,
          j.field("keys").elemList.map(_.text).mkString(", "))
      }
      println(Table.render(List("NAME", "LEFT", "RIGHT", "KEYS"), rows))
    }
  }

  private def cmdQuery(cfg: Config, args: List[String], explain: Boolean): Int = {
    QueryArgs.parse(args) match {
      case Left(err) => System.err.println(s"sdf: ${err.message}"); 2
      case Right(qa) =>
        val body = qa.toJson
        val endpoint = if (explain) "/explain" else "/query"
        val resp = Client.postJson(cfg, endpoint, body)
        if (cfg.json) { println(resp.body); return 0 }
        val root = resp.parseJson
        if (root.errorPath(cfg)) return 1
        printWarnings(root.warningsPath)
        if (explain) {
          // /explain returns Envelope[String] — the plan text is `data`.
          println(root.dataPath.text)
        } else {
          printQueryResult(root.dataPath)
        }
        0
    }
  }

  private def printQueryResult(d: JsonNode): Unit = {
    val cols = d.field("columns").elemList.map(_.field("name").text)
    val rows = d.field("rows").elemList.map { r =>
      r.elemList.map(cellToString).toList
    }
    val count = d.field("row_count").text.toInt
    val truncated = d.field("truncated").text.toBoolean
    println(Table.render(cols, rows))
    println(s"\n$count row${if (count == 1) "" else "s"}${if (truncated) " (TRUNCATED)" else ""}")
  }

  private def cellToString(n: JsonNode): String =
    if (n.isNull) "NULL"
    else if (n.isNumber) n.asText()
    else if (n.isBoolean) n.asText()
    else n.asText()

  /** Mask opaque lambda `toString` addresses for graceful degradation against
    * older server versions that don't carry the `exprString` field. Newer
    * servers (≥ PR feat/describe-model-expr-string) emit the original YAML
    * expression string verbatim, so this is a no-op in the common case —
    * we keep it for safety. e.g. `io.semanticdf.adapters.YamlLoader$$$Lambda$...`
    * is human-unreadable; masking it keeps the table legible when run
    * against a pre-PR server. */
  private def maskExpr(s: String): String =
    if (s != null && (s.contains("$") && s.contains("Lambda"))) "<inline fn>"
    else if (s != null && s.contains("@") && s.matches(".*@[0-9a-fA-F]+")) "<inline fn>"
    else s

  // ---------------------------------------------------------------------------
  // Query/explain flag parsing
  // ---------------------------------------------------------------------------

  private case class QueryArgs(
      model: String,
      dims: List[String],
      measures: List[String],
      order: List[(String, String)],
      limit: Option[Int],
      /**
       * Engine routing hint (per docs/design/multi-engine-design.md §6.4,
       * landed in PR #431). The server interprets this:
       *   - absent   -> server decides routing (default, backward compat)
       *   - empty    -> legacy `Models` + `SemanticTable` path
       *   - non-empty -> route through the `MCPEngineRegistry`
       *
       * The CLI omits the field when empty (per karpathy §2: minimum code).
       * If a user passes `--engine ""` literally, they get the same
       * behavior as omitting the field — which is the right default.
       * Users who need to *force* the legacy path with the field present
       * can pass a non-empty sentinel (e.g. `--engine legacy`).
       */
      engine: String = "",
  ) {
    /** Build the JSON request body for /query and /explain. */
    def toJson: String = {
      val sb = new StringBuilder
      sb.append('{').append("\"model\":").append(mapper.writeValueAsString(model))
      if (engine.nonEmpty) sb.append(",\"engine\":").append(mapper.writeValueAsString(engine))
      if (measures.nonEmpty) sb.append(",\"measures\":").append(mapper.writeValueAsString(measures.toArray))
      if (dims.nonEmpty)     sb.append(",\"dimensions\":").append(mapper.writeValueAsString(dims.toArray))
      if (order.nonEmpty) {
        val arr = order.map { case (f, d) => s"""{"field":${mapper.writeValueAsString(f)},"direction":"$d"}""" }
        sb.append(",\"order_by\":[").append(arr.mkString(",")).append("]")
      }
      limit.foreach(n => sb.append(",\"limit\":").append(n))
      sb.append('}').toString
    }
  }

  private object QueryArgs {
    def parse(args: List[String]): Either[CliParseError, QueryArgs] = {
      @tailrec def loop(
          in: List[String],
          model: Option[String],
          dims: List[String],
          measures: List[String],
          order: List[(String, String)],
          limit: Option[Int],
          engine: String,
      ): Either[CliParseError, QueryArgs] = in match {
        case Nil =>
          model match {
            case Some(m) =>
              Right(QueryArgs(
                model    = m,
                dims     = dims.reverse,
                measures = measures.reverse,
                order    = order.reverse,
                limit    = limit,
                engine   = engine,
              ))
            case None =>
              Left(CliParseError.MissingModel(
                usage = "Usage: sdf query <model> -d <dim> -m <measure>"
              ))
          }
        case ("-d" | "--dim") :: v :: rest => loop(rest, model, v :: dims, measures, order, limit, engine)
        case ("-d" | "--dim") :: Nil => Left(CliParseError.MissingFlagValue(flag = "--dim"))
        case ("-m" | "--measure") :: v :: rest => loop(rest, model, dims, v :: measures, order, limit, engine)
        case ("-m" | "--measure") :: Nil => Left(CliParseError.MissingFlagValue(flag = "--measure"))
        case ("-o" | "--order") :: v :: rest =>
          v.split(":", 2) match {
            case Array(f, d) if d == "asc" || d == "desc" =>
              loop(rest, model, dims, measures, (f, d) :: order, limit, engine)
            case Array(f) =>
              loop(rest, model, dims, measures, (f, "asc") :: order, limit, engine)
            case _ => Left(CliParseError.InvalidOrderFormat(value = v))
          }
        case ("-o" | "--order") :: Nil => Left(CliParseError.MissingFlagValue(flag = "--order"))
        case "--limit" :: v :: rest =>
          v.toIntOption match {
            case Some(n) if n >= 0 => loop(rest, model, dims, measures, order, Some(n), engine)
            case _ => Left(CliParseError.InvalidLimit(value = v))
          }
        case "--limit" :: Nil => Left(CliParseError.MissingFlagValue(flag = "--limit"))
        // PR #432 (v0.3.1 Step 2): expose the MCP server's engine-routing
        // field on the CLI. Omitted by default (server decides routing per
        // PR #431); when present, server routes through MCPEngineRegistry
        // if configured.
        case ("--engine") :: v :: rest => loop(rest, model, dims, measures, order, limit, engine = v)
        case ("--engine") :: Nil => Left(CliParseError.MissingFlagValue(flag = "--engine"))
        case flag :: _ if flag.startsWith("-") => Left(CliParseError.UnknownFlag(flag = flag))
        case v :: rest => model match {
          case Some(existing) =>
            Left(CliParseError.UnexpectedPositional(value = v, existingModel = existing))
          case None => loop(rest, Some(v), dims, measures, order, limit, engine)
        }
      }
      loop(args, None, Nil, Nil, Nil, None, engine = "")
    }
  }

  // ---------------------------------------------------------------------------
  // HTTP client + JSON response wrapper
  // ---------------------------------------------------------------------------

  private object Client {
    private val http = HttpClient.newBuilder()
      .connectTimeout(Duration.ofSeconds(5))
      .build()

    case class Response(status: Int, body: String) {
      /** Parse the body as a JSON tree. Throws on malformed JSON — callers
        * should have already validated via errorPath or know the shape. */
      def parseJson: JsonRoot = {
        val node = mapper.readTree(body)
        JsonRoot(node)
      }
    }

    def get(cfg: Config, path: String): Response = {
      val req = HttpRequest.newBuilder(uri(cfg, path))
        .timeout(Duration.ofSeconds(30))
        .GET().build()
      send(req)
    }

    def postJson(cfg: Config, path: String, body: String): Response = {
      val req = HttpRequest.newBuilder(uri(cfg, path))
        .timeout(Duration.ofSeconds(30))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(body)).build()
      send(req)
    }

    private def uri(cfg: Config, path: String): URI =
      URI.create(cfg.baseUrl.replaceAll("/+$", "") + path)

    private def send(req: HttpRequest): Response =
      try {
        val resp = http.send(req, HttpResponse.BodyHandlers.ofString())
        Response(resp.statusCode(), resp.body())
      } catch {
        case e: java.net.ConnectException =>
          // Print to stderr and rethrow as a typed exception. The caller
          // (Main.run) catches it and returns exit code 3. We don't call
          // sys.exit here because that would kill the test JVM when
          // CliIntegrationSpec exercises Main.run directly.
          System.err.println(s"sdf: could not connect to ${req.uri} (is the server running?)")
          throw new TransportFailure(req.uri.toString, e)
        case e: Exception =>
          System.err.println(s"sdf: request failed: ${e.getClass.getSimpleName}: ${e.getMessage}")
          throw new TransportFailure(req.uri.toString, e)
      }

    /** Raised by [[send]] on any HTTP transport error (connect refused,
      * timeout, malformed response). [[Main.run]] catches this and returns
      * exit code 3 so the CLI's behaviour is testable end-to-end without
      * the JVM dying via `sys.exit`. */
    class TransportFailure(uri: String, cause: Throwable)
      extends RuntimeException(s"transport failure for $uri", cause)
  }

  /** Thin wrapper over a Jackson JsonNode tree to make the response-walking
    * code readable. All accessors are lenient — missing fields render as
    * empty rather than throwing, since the server is the source of truth
    * for the shape and a partial render beats a crash. */
  private final case class JsonRoot(node: JsonNode) {
    def dataPath: JsonNode =
      if (node.has("data") && !node.get("data").isNull) node.get("data")
      else com.fasterxml.jackson.databind.node.NullNode.getInstance()

    /** If the envelope is an error, print it and return true. */
    def errorPath(cfg: Config): Boolean = {
      val status = node.path("status").asText("")
      if (status == "error" || node.has("error")) {
        val err = node.get("error")
        val code = err.path("code").asText("UNKNOWN")
        val msg = err.path("message").asText("(no message)")
        if (cfg.json) println(body) else System.err.println(s"sdf: $code: $msg")
        true
      } else false
    }

    /** Lifecycle warnings carried on the envelope. The MCP server emits
      * these when a tool touched a Deprecated or Draft model;
      * the field is additive for tolerant JSON clients and absent on older
      * server versions, in which case this returns Nil. */
    def warningsPath: List[String] = {
      val arr = node.path("warnings")
      if (arr == null || !arr.isArray) Nil
      else arr.iterator.asScala.toList.map(_.asText(""))
    }

    def body: String = node.toString
  }

  // ---------------------------------------------------------------------------
  // JSON node helpers (implicit-class style, kept local to avoid polluting
  // the Jackson namespace project-wide)
  // ---------------------------------------------------------------------------

  private implicit class JsonNodeOps(private val node: JsonNode) extends AnyVal {
    def field(name: String): JsonNode =
      if (node != null && node.has(name) && !node.get(name).isNull) node.get(name)
      else com.fasterxml.jackson.databind.node.NullNode.getInstance()
    def elemList: List[JsonNode] =
      if (node != null && node.isArray) node.iterator.asScala.toList else Nil
    def text: String = if (node == null || node.isNull) "" else node.asText("")
    def textOption: Option[String] =
      if (node == null || node.isNull) None else Some(node.asText(""))
  }

  // ---------------------------------------------------------------------------
  // Minimal table renderer (no deps)
  // ---------------------------------------------------------------------------

  private object Table {
    def render(headers: List[String], rows: List[List[String]]): String = {
      val all = headers :: rows
      val widths = headers.indices.map { i =>
        all.map(row => if (i < row.length) row(i).length else 0).max
      }
      val fmt = widths.map(w => s"%-${w}s").mkString("  ")
      val sep = widths.map("-" * _).mkString("  ")
      val headerLine = fmt.format(headers: _*)
      val dataLines = rows.map(r => fmt.format(padTo(r, headers.size): _*))
      (headerLine :: sep :: dataLines).mkString("\n")
    }
    private def padTo(row: List[String], n: Int): List[String] =
      row ++ List.fill(n - row.length)("")
  }

  // ---------------------------------------------------------------------------
  // Usage
  // ---------------------------------------------------------------------------

  private def printUsage(): Unit = {
    println(
      """sdf — a command-line client for the semanticdf REST API.
        |
        |usage: sdf <command> [options]
        |
        |commands:
        |  list                            list available models
        |  describe <model>                show a model's dimensions / measures / filters / joins
        |  query <model> [opts]            run a semantic query, print a table
        |  explain <model> [opts]          show the semantic plan (no execution)
        |
        |query/explain options:
        |  -d, --dim <name>                dimension (repeatable)
        |  -m, --measure <name>            measure (repeatable)
        |  -o, --order <field:asc|desc>    order by field (repeatable; asc default)
        |  --limit <n>                     row limit
        |
        |global options:
        |  --url <base>                    server URL (default $SDF_URL or http://localhost:8080)
        |  --json                          print raw JSON response
        |  -h, --help                      show this help
        |  -v, --version                   print version
        |
        |examples:
        |  sdf list
        |  sdf describe flights
        |  sdf query flights -d carrier -m flight_count -o carrier:asc --limit 10
        |  sdf explain flights -d carrier -m flight_count
        |""".stripMargin)
  }
}
