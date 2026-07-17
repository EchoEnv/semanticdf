package io.semanticdf.mcp

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer => JdkHttpServer}
import io.modelcontextprotocol.json.McpJsonMapper
import io.semanticdf.mcp.handlers.{DescribeModel, Introspect, ListModels, Query}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import java.net.InetSocketAddress
import java.util.concurrent.Executors
import scala.io.Source
import scala.util.control.NonFatal

/** Tier 3: HTTP REST transport for the semanticdf catalog.
  *
  * Wraps the same handler logic the MCP stdio transport uses
  * ([[Query]], [[Introspect]], [[ListModels]], [[DescribeModel]]) and
  * exposes them as JSON-over-HTTP endpoints. Built on the JDK's
  * built-in `com.sun.net.httpserver.HttpServer` — zero new dependencies.
  *
  * == Endpoints ==
  *
  * {{{
  *   GET    /models                  → ListModels
  *   GET    /models/{name}           → DescribeModel (with optional ?include_okf=true)
  *   POST   /query                   → Query
  *   POST   /explain                 → Query.explain (same body shape as /query)
  *   POST   /introspect              → Introspect
  * }}}
  *
  * All endpoints return JSON of the same `Envelope[T]` shape the MCP
  * tools produce. Errors carry `{"status":"error", "error": {"code": ..., "message": ...}}`.
  *
  * == Why JDK HttpServer, not Cask or Akka HTTP ==
  *
  * Zero new deps, ~200 lines of routing code, fine for the modest
  * traffic a semantic layer sees. The handler logic is decoupled from
  * the transport — swap in http4s or Akka HTTP later if traffic grows.
  *
  * == Security ==
  *
  * No auth — matches the stdio transport's "trusted environment"
  * assumption. For production, front this with a reverse proxy (nginx,
  * Caddy, etc.) that enforces TLS + an auth header. */
final class RestServer(
    spark: SparkSession,
    models: Models,
    okf: OkfCache,
    mapper: McpJsonMapper = JsonSupport.scalaMapper(),
    port: Int = 8080,
    numThreads: Int = 4,
) {
  RestServer.log.info(s"semanticdf-rest: starting on port $port")
  private val queryHandler = new Query(spark)
  private val introspectHandler = new Introspect(spark)
  private val listHandler = new ListModels()
  private val describeHandler = new DescribeModel()

  /** Start the HTTP server. Returns the server so the caller can stop it. */
  def start(): JdkHttpServer = {
    val server = JdkHttpServer.create(new InetSocketAddress(port), 0)
    server.createContext("/models",     new ModelsRoute(this))
    server.createContext("/query",      new QueryRoute(this))
    server.createContext("/explain",    new ExplainRoute(this))
    server.createContext("/introspect", new IntrospectRoute(this))
    server.setExecutor(Executors.newFixedThreadPool(numThreads))
    server.start()
    RestServer.log.info(s"semanticdf-rest listening on http://localhost:${server.getAddress.getPort}")
    server
  }

  // ---------------------------------------------------------------------------
  // Route handlers
  // ---------------------------------------------------------------------------
  //
  // HTTP status codes:
  //   200 — success (Envelope.status == "ok")
  //   400 — invalid JSON body (INVALID_REQUEST)
  //   404 — model not found (MODEL_NOT_FOUND)
  //   405 — wrong HTTP method
  //   500 — domain error (RESULT_TOO_LARGE, QUERY_TIMEOUT, AMBIGUOUS_*, EXECUTION_ERROR)
  //
  // MCP error CODE is preserved in `error.code` so clients can distinguish.

  private final class ModelsRoute(server: RestServer) extends io.semanticdf.mcp.RestServer.Route(server) {
    override def serve(exchange: HttpExchange): Unit = {
      if (!server.isGet(exchange)) return server.methodNotAllowed(exchange, "GET")
      val path = exchange.getRequestURI.getPath
      val tail = path.stripPrefix("/models").stripPrefix("/")
      if (tail.isEmpty) {
        server.respondOk(exchange, listHandler.handle(models))
      } else {
        val includeOkf = Option(exchange.getRequestURI.getQuery)
          .exists(_.split('&').exists(_.startsWith("include_okf=")))
        try {
          server.respondOk(exchange, describeHandler.handle(models, okf, tail, includeOkf))
        } catch {
          case e: ModelNotFound =>
            server.respondError(exchange, 404, "MODEL_NOT_FOUND", e.getMessage)
        }
      }
    }
  }

  private final class QueryRoute(server: RestServer) extends io.semanticdf.mcp.RestServer.Route(server) {
    override def serve(exchange: HttpExchange): Unit = {
      if (!server.isPost(exchange)) return server.methodNotAllowed(exchange, "POST")
      server.withJsonBody(exchange) { args =>
        val req = Query.parseRequest(args)
        try {
          server.respondOk(exchange, queryHandler.handle(models, req))
        } catch {
          case e: io.semanticdf.mcp.handlers.QueryErrors.ResultTooLarge =>
            server.respondError(exchange, 500, "RESULT_TOO_LARGE", e.getMessage,
              details = Map("suggested_limit" -> e.limit.toString))
          case e: io.semanticdf.mcp.handlers.QueryErrors.QueryTimeout =>
            server.respondError(exchange, 500, "QUERY_TIMEOUT", e.getMessage,
              details = Map("timeout_ms" -> e.timeoutMs.toString))
          case e: io.semanticdf.mcp.handlers.QueryErrors.AmbiguousDimension =>
            server.respondError(exchange, 500, "AMBIGUOUS_DIMENSION", e.getMessage,
              details = Map("candidates" -> e.candidates.mkString(",")))
          case e: io.semanticdf.mcp.handlers.QueryErrors.AmbiguousMeasure =>
            server.respondError(exchange, 500, "AMBIGUOUS_MEASURE", e.getMessage,
              details = Map("candidates" -> e.candidates.mkString(",")))
          case e: ModelNotFound =>
            server.respondError(exchange, 404, "MODEL_NOT_FOUND", e.getMessage)
          case e: IllegalArgumentException =>
            server.respondError(exchange, 500, "EXECUTION_ERROR", e.getMessage)
        }
      }
    }
  }

  private final class ExplainRoute(server: RestServer) extends io.semanticdf.mcp.RestServer.Route(server) {
    override def serve(exchange: HttpExchange): Unit = {
      if (!server.isPost(exchange)) return server.methodNotAllowed(exchange, "POST")
      server.withJsonBody(exchange) { args =>
        val req = Query.parseRequest(args)
        server.respondOk(exchange, queryHandler.explain(models, req))
      }
    }
  }

  private final class IntrospectRoute(server: RestServer) extends io.semanticdf.mcp.RestServer.Route(server) {
    override def serve(exchange: HttpExchange): Unit = {
      if (!server.isPost(exchange)) return server.methodNotAllowed(exchange, "POST")
      server.withJsonBody(exchange) { args =>
        val req = Introspect.parseRequest(args)
        server.respondOk(exchange, introspectHandler.handle(
          models, req.table, req.format, req.path, req.model_name, req.read_options,
        ))
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private def isGet(exchange: HttpExchange): Boolean =
    exchange.getRequestMethod == "GET"
  private def isPost(exchange: HttpExchange): Boolean =
    exchange.getRequestMethod == "POST"

  private def methodNotAllowed(exchange: HttpExchange, required: String): Unit = {
    respondError(exchange, 405, "METHOD_NOT_ALLOWED",
      s"this endpoint requires $required, got ${exchange.getRequestMethod}",
      details = Map("allowed" -> required))
  }

  private def withJsonBody(exchange: HttpExchange)(f: java.util.Map[String, Object] => Unit): Unit = {
    val body = try Source.fromInputStream(exchange.getRequestBody, "UTF-8").mkString
    catch { case e: java.io.IOException =>
      respondError(exchange, 400, "INVALID_REQUEST", s"could not read body: ${e.getMessage}")
      return
    }
    val args: java.util.Map[String, Object] = try {
      mapper.readValue(body, classOf[java.util.Map[String, Object]])
    } catch {
      case e: Exception =>
        respondError(exchange, 400, "INVALID_REQUEST", s"invalid JSON: ${e.getMessage}")
        return
    }
    f(args)
  }

  private def respondOk(exchange: HttpExchange, env: Envelope[_]): Unit = {
    val json = mapper.writeValueAsString(env)
    val bytes = json.getBytes("UTF-8")
    exchange.getResponseHeaders.set("Content-Type", "application/json; charset=utf-8")
    exchange.sendResponseHeaders(200, bytes.length.toLong)
    exchange.getResponseBody.write(bytes)
    exchange.getResponseBody.close()
  }

  private def respondError(
      exchange: HttpExchange,
      httpStatus: Int,
      code: String,
      message: String,
      details: Map[String, String] = Map.empty,
  ): Unit = {
    val env = ErrorEnvelope(
      status = "error",
      error = ErrorDetail(code = code, message = message, details = details),
    )
    val json = mapper.writeValueAsString(env)
    val bytes = json.getBytes("UTF-8")
    exchange.getResponseHeaders.set("Content-Type", "application/json; charset=utf-8")
    exchange.sendResponseHeaders(httpStatus, bytes.length.toLong)
    exchange.getResponseBody.write(bytes)
    exchange.getResponseBody.close()
  }

}

object RestServer {
  private val log = LoggerFactory.getLogger(classOf[RestServer])

  /** Base trait for all routes. Wraps JDK's HttpHandler with a try/catch
    * that returns 500 on unhandled errors. Subclasses implement `serve`
    * (not `handle`) to avoid the name clash. Lives in the companion
    * object so the inner route classes can reference it as
    * `RestServer.Route(this)`. */
  abstract class Route(server: RestServer) extends HttpHandler {
    final override def handle(exchange: HttpExchange): Unit = {
      try serve(exchange)
      catch {
        case NonFatal(e) =>
          RestServer.log.error(s"unhandled error in ${exchange.getRequestURI.getPath}", e)
          server.respondError(exchange, 500, "INTERNAL_ERROR",
            e.getClass.getSimpleName + ": " + e.getMessage)
      }
    }
    def serve(exchange: HttpExchange): Unit
  }
}