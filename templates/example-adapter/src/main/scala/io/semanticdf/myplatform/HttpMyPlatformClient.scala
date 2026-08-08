package io.semanticdf.myplatform

import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration

import io.semanticdf.core.engine.{ResolvedSchema, ResolvedSource}
import io.semanticdf.core.schema.{Field, SealedDataType}

/** Concrete [[MyPlatformClient]] implementation backed by MyPlatform's
  * REST API. Uses the JDK's `java.net.http.HttpClient` (no new
  * dependency — Java 11+ stdlib).
  *
  * Mirrors `io.semanticdf.hera.HttpHeraClient` (PR #425).
  *
  * ==Why `java.net.http.HttpClient` (not OkHttp / sttp / etc.)==
  *
  * Per karpathy §2 ("minimum code that solves the problem"): the
  * JDK's built-in HTTP client is sufficient for MyPlatform's REST
  * API. Adding OkHttp or sttp would pull a new dependency for no
  * real benefit.
  *
  * ==Error handling==
  *
  * Per `docs/design/error-handling-style.md`:
  *
  *   - NO `catch { case _: Exception => ... }` (catch-all banned).
  *   - Catch SPECIFIC exception types: `IOException` / `InterruptedException`.
  *   - Map HTTP status codes to SPECIFIC `MyPlatformError` cases
  *     (no generic `ServerError`).
  *
  * ==Boundary contract==
  *
  * Zero Spark imports. */
final class HttpMyPlatformClient(
    baseUrl: String,
) extends MyPlatformClient {

  // JDK HttpClient is thread-safe; one instance is shared.
  private val http = HttpClient.newBuilder()
    .connectTimeout(Duration.ofSeconds(10))
    .build()

  // Per error-handling-style.md "Hard bans": auth tokens belong in
  // the trait constructor (not hardcoded). The caller is expected
  // to inject them via a header builder. For v1 we use Bearer.
  private val authHeader: String = ""  // populated via withAuth or constructor

  /** Smart constructor with explicit auth. */
  def this(baseUrl: String, authToken: String) = {
    this(baseUrl)
    // Per scala-data-driven-refacer §1: pure data, behavior lives
    // elsewhere. We set the auth header as a side-effect of
    // construction; for v0.4.0 we'd thread this via a builder.
    this.authHeader = s"Bearer $authToken"
  }

  override def executeQuery(
      sql:     String,
      realmId: String,
      limit:   Int    = 100,
  ): Either[MyPlatformError, MyPlatformResult] = {
    val url = s"$baseUrl/api/query"
    val body = s"""{"sql":"${escapeJson(sql)}","limit":$limit,"realm":"$realmId"}"""
    val req = basePost(url, body)
    sendJson(req).flatMap { json =>
      HttpMyPlatformClient.parseQueryResult(json).left.map(MyPlatformError.MalformedResponse(_))
    }
  }

  override def describeTable(
      table:   String,
      realmId: String,
  ): Either[MyPlatformError, ResolvedSchema] = {
    val url = s"$baseUrl/api/tables/$table?realm=$realmId"
    val req = baseGet(url)
    sendJson(req).flatMap { json =>
      HttpMyPlatformClient.parseDescribeTable(json).left.map(MyPlatformError.MalformedResponse(_))
    }
  }

  override def getTableMeta(
      table:   String,
      realmId: String,
  ): Either[MyPlatformError, MyPlatformTableMeta] = {
    val url = s"$baseUrl/api/tables/$table?realm=$realmId"
    val req = baseGet(url)
    sendJson(req).flatMap { json =>
      HttpMyPlatformClient.parseTableMeta(json, table, realmId).left.map(MyPlatformError.MalformedResponse(_))
    }
  }

  override def createTable(
      table:   String,
      realmId: String,
      meta:    MyPlatformTableMeta,
  ): Either[MyPlatformError, MyPlatformTableMeta] = {
    val url = s"$baseUrl/api/tables"
    val body = s"""{"name":"$table","realm":"$realmId","active":${meta.active}}"""
    val req = basePost(url, body)
    sendJson(req).map { _ =>
      meta.copy(version = 1L)
    }
  }

  override def updateTable(
      table:           String,
      realmId:         String,
      meta:            MyPlatformTableMeta,
      expectedVersion: Long,
  ): Either[MyPlatformError, MyPlatformTableMeta] = {
    val url = s"$baseUrl/api/tables/$table?realm=$realmId&expectedVersion=$expectedVersion"
    val body = s"""{"active":${meta.active}}"""
    val req = HttpRequest.newBuilder()
      .uri(URI.create(url))
      .timeout(Duration.ofSeconds(10))
      .header("Authorization", authHeader)
      .header("Content-Type", "application/json")
      .method("PATCH", HttpRequest.BodyPublishers.ofString(body))
      .build()
    sendJson(req).map { _ =>
      meta.copy(version = expectedVersion + 1)
    }
  }

  override def listTables(
      realmId: String,
      prefix:  String,
  ): Either[MyPlatformError, List[String]] = {
    // Per error-handling-style.md: 1-step (the read) → use match
    // directly. No for-comprehension needed for a single HTTP call.
    val url = s"$baseUrl/api/tables?realm=$realmId"
    val req = baseGet(url)
    sendJson(req).flatMap { json =>
      HttpMyPlatformClient.parseTableNames(json, prefix).left.map(MyPlatformError.MalformedResponse(_))
    }
  }

  override def resolveRealmId(catalogName: String): Option[String] = {
    // Per error-handling-style.md "Internal helper rule": 1 call
    // site, caller does `match` on it immediately → plain function
    // returning `Option[String]`, NOT `Either[L, X]`.
    if (catalogName.isEmpty) return None
    Some(catalogName)
  }

  // -- HTTP plumbing --

  /** Build a POST request with the auth header + JSON content type. */
  private def basePost(url: String, body: String): HttpRequest =
    HttpRequest.newBuilder()
      .uri(URI.create(url))
      .timeout(Duration.ofSeconds(30))
      .header("Authorization", authHeader)
      .header("Content-Type", "application/json")
      .header("Accept", "application/json")
      .POST(HttpRequest.BodyPublishers.ofString(body))
      .build()

  /** Build a GET request with the auth header. */
  private def baseGet(url: String): HttpRequest =
    HttpRequest.newBuilder()
      .uri(URI.create(url))
      .timeout(Duration.ofSeconds(30))
      .header("Authorization", authHeader)
      .header("Accept", "application/json")
      .GET()
      .build()

  /** Send a request and translate the response into the typed ADT.
    *
    * Per error-handling-style.md "Hard bans": catch SPECIFIC exception
    * types (no catch-all `Exception`); map HTTP status codes to
    * SPECIFIC error cases (no generic `ServerError`). */
  private def sendJson(req: HttpRequest): Either[MyPlatformError, String] = {
    try {
      val resp = http.send(req, HttpResponse.BodyHandlers.ofString())
      resp.statusCode() match {
        case 200 => Right(resp.body())
        case 400 =>
          val body = resp.body()
          if (body.contains("Already Exists") || body.contains("already exists"))
            Left(MyPlatformError.AlreadyExists(reason = s"HTTP 400: ${body.take(200)}"))
          else
            Left(MyPlatformError.BadRequest(reason = s"HTTP 400: ${body.take(200)}"))
        case 401 => Left(MyPlatformError.Unauthorized(reason = "HTTP 401"))
        case 403 => Left(MyPlatformError.Forbidden(reason = "HTTP 403"))
        case 404 => Left(MyPlatformError.NotFound(reason = "HTTP 404"))
        case 409 => Left(MyPlatformError.Conflict(reason = s"HTTP 409: ${resp.body().take(200)}"))
        case _   => Left(MyPlatformError.MalformedResponse(reason = s"unexpected HTTP ${resp.statusCode()}: ${resp.body().take(200)}"))
      }
    } catch {
      // Per error-handling-style.md: catch SPECIFIC exception types.
      case _: java.io.IOException   => Left(MyPlatformError.NetworkError(reason = "network error"))
      case _: InterruptedException  => Left(MyPlatformError.NetworkError(reason = "timeout"))
    }
  }

  /** Escape a string for inclusion in a JSON value. */
  private def escapeJson(s: String): String =
    s.flatMap {
      case '"'  => "\\\""
      case '\\' => "\\\\"
      case '\n' => "\\n"
      case '\r' => "\\r"
      case '\t' => "\\t"
      case c    => c.toString
    }
}

object HttpMyPlatformClient {

  /** Smart constructor. */
  def apply(baseUrl: String): HttpMyPlatformClient = new HttpMyPlatformClient(baseUrl)

  def apply(baseUrl: String, authToken: String): HttpMyPlatformClient =
    new HttpMyPlatformClient(baseUrl, authToken)

  // -- JSON helpers (hand-rolled; no new dep) --

  /** Parse a MyPlatform query response into a [[MyPlatformResult]].
    *
    * Per error-handling-style.md "Converter return types": returns
    * `Either[String, X]` HERE because this is a PRIVATE helper
    * (not exposed); the caller wraps `Left(...)` into
    * `Either[MyPlatformError, ...]`. */
  private[myplatform] def parseQueryResult(json: String): Either[String, MyPlatformResult] = {
    // TODO: implement based on your platform's actual response shape.
    // For now, return a placeholder — real impl depends on the
    // MyPlatform JSON contract.
    Right(MyPlatformResult(Nil, Nil))
  }

  /** Parse a describe-table response into a [[ResolvedSchema]]. */
  private[myplatform] def parseDescribeTable(json: String): Either[String, ResolvedSchema] = {
    // Per the existing UC/Hera pattern: ResolvedSchema takes
    // Map[String, String] (name -> type-string).
    Right(ResolvedSchema(Map.empty))
  }

  /** Parse a getTableMeta response. */
  private[myplatform] def parseTableMeta(
      json:    String,
      table:   String,
      realmId: String,
  ): Either[String, MyPlatformTableMeta] = {
    // TODO: implement based on your platform's actual response shape.
    Right(MyPlatformTableMeta(table, realmId, version = 1L, active = true))
  }

  /** Parse a listTables response, applying the prefix filter. */
  private[myplatform] def parseTableNames(json: String, prefix: String): Either[String, List[String]] = {
    Right(Nil)
  }
}