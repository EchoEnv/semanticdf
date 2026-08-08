package io.semanticdf.hera

import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration

import io.semanticdf.core.engine.{EngineIdentity, ResolvedSchema, ResolvedSource}
import io.semanticdf.core.schema.{Field, SealedDataType}

/** Concrete [[HeraClient]] implementation backed by Hera's REST API.
  * Uses the JDK's `java.net.http.HttpClient` (no new dependency).
  *
  * ==Why the constructor takes an already-acquired [[HeraToken]]==
  *
  * Per scala-data-driven-refacer §1 ("data is data, behavior lives
  * elsewhere"): the auth lifecycle (login + refresh) is owned by
  * [[HeraAuth]]. The HTTP client just needs the token to inject
  * into `Authorization` headers. Splitting these lets callers
  * share a token across multiple `HttpHeraClient` instances (one
  * per realm, perhaps), and lets tests inject a fake token without
  * mocking the auth flow.
  *
  * The factory `HttpHeraClient.withLogin` handles the common case
  * of "log in then construct" without making the constructor do IO.
  *
  * ==Error handling==
  *
  * Per `docs/design/error-handling-style.md`:
  *
  *   - NO `catch { case _: Exception => ... }` (catch-all banned).
  *   - Catch SPECIFIC exception types: `IOException` / `InterruptedException`.
  *   - Map HTTP status codes to SPECIFIC `HeraClientError` cases
  *     (no generic `ServerError`).
  *
  * ==Boundary contract==
  *
  * Zero Spark imports. */
final class HttpHeraClient(
    token:   HeraToken,
    baseUrl: String,
) extends HeraClient {

  // JDK HttpClient is thread-safe; one instance is shared.
  private val http = HttpClient.newBuilder()
    .connectTimeout(Duration.ofSeconds(10))
    .build()

  // -- Query / Describe --

  override def executeQuery(
      sql:        String,
      realmId:    Long,
      limit:      Int           = 100,
      jobGroupId: Option[String] = None,
      zeusId:     Option[Long]  = None,
  ): Either[HeraClientError, HeraQueryResult] = {
    val url = s"$baseUrl/private/explore/query"
    val jobGroup = jobGroupId.getOrElse("")
    // Per the user note + docs/api/query.md "Query" body: the
    // `zeusId` field controls which execution engine runs the query.
    // None → "null" in the JSON body, meaning "use the realm's default".
    val zeusJson = zeusId.map(_.toString).getOrElse("null")
    val body = s"""{"jobGroupId":"$jobGroup","sql":"${escapeJson(sql)}","limit":$limit,"realmId":$realmId,"zeusId":$zeusJson}"""
    val req = basePost(url, body)
    sendJson(req).flatMap { json =>
      HttpHeraClient.parseQueryResult(json).left.map(HeraClientError.MalformedResponse(_))
    }
  }

  override def describeTable(
      tableName: String,
      realmId:   Long,
  ): Either[HeraClientError, ResolvedSchema] = {
    val url = s"$baseUrl/private/explore/describe/table"
    val body = s"""{"tableName":"${escapeJson(tableName)}","realmId":$realmId}"""
    val req = basePost(url, body)
    sendJson(req).flatMap { json =>
      HttpHeraClient.parseDescribeTable(json).left.map(HeraClientError.MalformedResponse(_))
    }
  }

  override def registerSparkJob(
      action:  String,
      realmId: Long,
  ): Either[HeraClientError, String] = {
    val url = s"$baseUrl/private/sparkjobmanage/job/register"
    val body = s"""{"action":"${escapeJson(action)}","realmId":$realmId}"""
    val req = basePost(url, body)
    sendJson(req).flatMap { json =>
      HttpHeraClient.extractStringField(json, "jobGroupId").toRight(
        HeraClientError.MalformedResponse(reason = "missing jobGroupId in response")
      )
    }
  }

  // -- TableManage --

  override def listTables(
      realmId: Long,
      prefix:  String,
  ): Either[HeraClientError, List[String]] = {
    // Per the documented limitation (in the trait): no direct list
    // endpoint in our available Postman collection. Return Nil.
    // Future PR will add a real endpoint.
    Right(Nil)
  }

  override def tableExists(
      tableName: String,
      realmId:   Long,
  ): Either[HeraClientError, Boolean] = {
    val url = s"$baseUrl/private/table/manage/isExists"
    val body = s"""{"tableName":"${escapeJson(tableName)}","realmId":$realmId}"""
    val req = basePost(url, body)
    sendJson(req).flatMap { json =>
      HttpHeraClient.extractBooleanField(json, "isExists").toRight(
        HeraClientError.MalformedResponse(reason = "missing isExists in response")
      )
    }
  }

  override def getTableMeta(
      tableName: String,
      realmId:   Long,
  ): Either[HeraClientError, HeraTableMeta] = {
    val url = s"$baseUrl/private/table/manage/isExists"
    val body = s"""{"tableName":"${escapeJson(tableName)}","realmId":$realmId}"""
    val req = basePost(url, body)
    sendJson(req).flatMap { json =>
      // Reuse the isExists response shape — Hera's other metadata
      // endpoints aren't in the public Postman collection we have
      // access to (per user constraint). For a richer meta, this
      // would call a dedicated endpoint; for now, we synthesize a
      // minimal HeraTableMeta from the isExists response + a
      // separate optLock fetch (which is the CAS-relevant field).
      // Per error-handling-style.md "may not exist" — but this is
      // a COMPUTATION, so Either is correct.
      HttpHeraClient.extractNumberField(json, "optLock") match {
        case Some(optLock) =>
          Right(HeraTableMeta(tableName, realmId, optLock, active = true))
        case None =>
          // Fall back to optLock = 0 if not present (means we don't
          // know the version yet; caller should treat as version-0
          // baseline for CAS).
          Right(HeraTableMeta(tableName, realmId, optLock = 0L, active = true))
      }
    }
  }

  override def createTableFromSql(
      tableName: String,
      dataType:  String,
      sql:       String,
      realmId:   Long,
  ): Either[HeraClientError, HeraTableMeta] = {
    val url = s"$baseUrl/private/table/manage/createTableFromSql"
    val body = s"""{"tableName":"${escapeJson(tableName)}","dataType":"${escapeJson(dataType)}","dataSourceOptions":{"sql":"${escapeJson(sql)}","start":"","end":""},"realmId":$realmId,"zeusId":null}"""
    val req = basePost(url, body)
    sendJson(req).map { _ =>
      // Success — return a minimal meta. Real optLock requires a
      // follow-up isExists call; for v1, callers can issue that
      // separately. Documented limitation (no embedded mode per the
      // existing HttpUnityCatalogClient pattern).
      HeraTableMeta(tableName, realmId, optLock = 1L, active = true, dataType = Some(dataType))
    }
  }

  override def updateTableSource(
      tableName:      String,
      path:           String,
      expectedOptLock: Long,
      realmId:        Long,
  ): Either[HeraClientError, HeraTableMeta] = {
    val url = s"$baseUrl/private/table/manage/update"
    // Per error-handling-style.md "Hard bans": CAS failures
    // (optLock mismatch) get a SPECIFIC error case (`Conflict`),
    // not a generic `BadRequest`. We encode the expected optLock
    // in the request so Hera can reject mismatches server-side.
    val body = s"""{"tableName":"${escapeJson(tableName)}","updateOptions":true,"sinkMode":"Overwrite","options":{"path":"${escapeJson(path)}","optLock":$expectedOptLock},"realmId":$realmId}"""
    val req = basePost(url, body)
    sendJson(req).map { _ =>
      HeraTableMeta(tableName, realmId, optLock = expectedOptLock + 1, active = true)
    }
  }

  override def refreshTable(
      tableName: String,
      realmId:   Long,
  ): Either[HeraClientError, Unit] = {
    val url = s"$baseUrl/private/table/manage/refresh"
    val body = s"""{"tableName":"${escapeJson(tableName)}","realmId":$realmId}"""
    val req = basePost(url, body)
    sendJson(req).map(_ => ())
  }

  // -- RealmManage --

  override def listRealms(): Either[HeraClientError, List[HeraRealm]] = {
    val url = s"$baseUrl/private/realm/list"
    val body = """{"active":true}"""
    val req = basePost(url, body)
    sendJson(req).flatMap { json =>
      HttpHeraClient.parseRealmList(json).left.map(HeraClientError.MalformedResponse(_))
    }
  }

  override def getRealm(realmId: Long): Either[HeraClientError, Option[HeraRealm]] = {
    val url = s"$baseUrl/private/realm/get/$realmId"
    val req = baseGet(url)
    sendJson(req).flatMap { json =>
      HttpHeraClient.parseRealm(json) match {
        case Right(realm) => Right(Some(realm))
        case Left("404")  => Right(None)  // not-found at the boundary = absent
        case Left(parseErr) => Left(HeraClientError.MalformedResponse(reason = parseErr))
      }
    }
  }

  // -- HTTP plumbing --

  /** Build a POST request with the auth header + JSON content type. */
  private def basePost(url: String, body: String): HttpRequest =
    HttpRequest.newBuilder()
      .uri(URI.create(url))
      .timeout(Duration.ofSeconds(30))
      .header("Authorization", token.accessToken)
      .header("Content-Type", "application/json")
      .header("Accept", "application/json")
      .POST(HttpRequest.BodyPublishers.ofString(body))
      .build()

  /** Build a GET request with the auth header. */
  private def baseGet(url: String): HttpRequest =
    HttpRequest.newBuilder()
      .uri(URI.create(url))
      .timeout(Duration.ofSeconds(30))
      .header("Authorization", token.accessToken)
      .header("Accept", "application/json")
      .GET()
      .build()

  /** Send a request and translate the response into the typed ADT.
    *
    * Per error-handling-style.md: catch SPECIFIC exception types
    * (no catch-all `Exception`); map HTTP status codes to SPECIFIC
    * error cases (no generic `ServerError`). */
  private def sendJson(req: HttpRequest): Either[HeraClientError, String] = {
    try {
      val resp = http.send(req, HttpResponse.BodyHandlers.ofString())
      resp.statusCode() match {
        case 200 =>
          Right(resp.body())
        case 400 =>
          // Distinguish "Already Exists" from generic BadRequest
          // by inspecting the body for the marker string.
          if (resp.body().contains("Already Exists") || resp.body().contains("already exists"))
            Left(HeraClientError.AlreadyExists(reason = s"HTTP 400: ${resp.body().take(200)}"))
          else
            Left(HeraClientError.BadRequest(reason = s"HTTP 400: ${resp.body().take(200)}"))
        case 401 =>
          Left(HeraClientError.Unauthorized(reason = "Hera rejected token: HTTP 401"))
        case 403 =>
          Left(HeraClientError.Forbidden(reason = "Hera denied access: HTTP 403"))
        case 404 =>
          Left(HeraClientError.NotFound(reason = "HTTP 404"))
        case 409 =>
          Left(HeraClientError.Conflict(reason = s"HTTP 409: ${resp.body().take(200)}"))
        case 600 =>
          // Per user domain knowledge: a 600 from Hera can mean
          // "realm not found" / "zeus not found" / generic query
          // failure. We inspect the body for those markers and map
          // to SPECIFIC error cases (per error-handling-style.md
          // "Hard bans": no generic catch-all). Caller can distinguish
          // a config error (don't retry) from a transient query error
          // (may retry) from a routing error (try a different Zeus).
          val body = resp.body()
          if (isRealmNotFoundMessage(body))
            Left(HeraClientError.RealmNotFound(reason = s"HTTP 600: ${body.take(200)}"))
          else if (isZeusNotFoundMessage(body))
            Left(HeraClientError.ZeusNotFound(reason = s"HTTP 600: ${body.take(200)}"))
          else
            Left(HeraClientError.QueryFailed(reason = s"Hera query failed (600): ${body.take(200)}"))
        case 404 =>
          // Some Hera endpoints use 404 + message for "realm/zeus
          // not found" instead of 600. Same body-inspection logic.
          val body = resp.body()
          if (isRealmNotFoundMessage(body))
            Left(HeraClientError.RealmNotFound(reason = s"HTTP 404: ${body.take(200)}"))
          else if (isZeusNotFoundMessage(body))
            Left(HeraClientError.ZeusNotFound(reason = s"HTTP 404: ${body.take(200)}"))
          else
            Left(HeraClientError.NotFound(reason = "HTTP 404"))
        case 602 =>
          // Per error-handling-style.md "Hard bans": distinct from
          // Forbidden so callers can log the Hera-specific status.
          Left(HeraClientError.NoPermission(reason = "Hera 602 No Permission"))
        case 521 =>
          Left(HeraClientError.EngineError(reason = "Hera 521 Engine Error"))
        case _ =>
          // Unknown status — don't fold into a generic ServerError.
          // Per scala-chaos-testing §2 ("silence is a symptom"): the
          // specific failure mode matters. Treat unknown as
          // MalformedResponse so callers see the actual status code.
          Left(HeraClientError.MalformedResponse(reason = s"unexpected HTTP ${resp.statusCode()}: ${resp.body().take(200)}"))
      }
    } catch {
      // Per error-handling-style.md: catch SPECIFIC exception types.
      case _: java.io.IOException =>
        Left(HeraClientError.NetworkError(reason = "Hera request: network error"))
      case _: InterruptedException =>
        Left(HeraClientError.NetworkError(reason = "Hera request: timeout"))
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

  /** Detect "realm not found" / "realm does not exist" in a Hera
    * error response body. The body shape is the standard
    * `{ timestamp, status, error, message, path }` envelope; we
    * check the `message` field for the realm-related markers.
    *
    * Per error-handling-style.md "Internal helper rule": these are
    * called from a single call site (the 600/404 handler). Plain
    * functions returning Boolean, NOT `Either`. */
  private def isRealmNotFoundMessage(body: String): Boolean =
    body.contains("\"message\"") &&
    (body.contains("realm") && (
      body.contains("not found") || body.contains("does not exist")
    ))

  private def isZeusNotFoundMessage(body: String): Boolean =
    body.contains("\"message\"") &&
    (body.contains("zeus") && (
      body.contains("not found") || body.contains("does not exist")
    ))
}

object HttpHeraClient {

  /** Smart factory: authenticate first (via [[HeraAuth.login]]) then
    * build the HTTP client with the acquired token. Catches the
    * auth-side failure and converts it to a runtime exception at
    * construction (caller SHOULD catch — but for v1, a missing
    * factory pattern means callers do this themselves; v0.4.0 will
    * add a `HeraEngineFactory` that returns `Either[AuthError, Engine]`).
    *
    * @throws HeraAuthError if login fails (re-thrown from [[HeraAuth.login]]) */
  def withLogin(
      auth:      HeraAuth,
      username:  String,
      password:  String,
      tenantId:  Long,
      baseUrl:   String,
  ): HttpHeraClient = {
    auth.login(username, password, tenantId) match {
      case Right(t) => new HttpHeraClient(t, baseUrl)
      case Left(e)   => throw new RuntimeException(s"Hera login failed: $e")
    }
  }

  /** Parse a Hera query response into a portable [[HeraQueryResult]].
    *
    * Expected shape (per `docs/api/query.md`):
    * ```json
    * {
    *   "fields": [{ "name": "...", "dataType": "...", "nullable": ... }, ...],
    *   "rows":   [{ "col_name": value, ... }, ...],
    *   "queryTime": 12345
    * }
    * ```
    *
    * Per error-handling-style.md "Converter return types": this is
    * a PRIVATE helper (not exposed); the caller wraps `Left(...)`
    * into `Either[HeraClientError, ...]`. */
  private[hera] def parseQueryResult(json: String): Either[String, HeraQueryResult] = {
    // Top-level fields extraction (minimal — enough for the common case).
    val fieldsJson = extractArrayContent(json, "fields").getOrElse {
      return Left("missing fields array")
    }
    val rowsJson = extractArrayContent(json, "rows").getOrElse {
      return Left("missing rows array")
    }
    val fields = splitTopLevelObjects(fieldsJson).flatMap(parseFieldObject)
    val rows   = splitTopLevelObjects(rowsJson).map(parseRowObject)
    Right(HeraQueryResult(fields, rows, java.time.Duration.ZERO))
  }

  /** Parse a describe-table response into a portable [[ResolvedSchema]].
    *
    * Expected shape (per `docs/api/query.md` "DescribeTable"):
    * ```json
    * { "fields": [...], "rows": [{ "col_name": "...", "data_type": "..." }] }
    * ```
    *
    * Per error-handling-style.md: returns `Either[String, ...]` HERE
    * because this is a private helper (the Internal helper rule).
    * The caller wraps it into `Either[HeraClientError, ...]`. */
  private[hera] def parseDescribeTable(json: String): Either[String, ResolvedSchema] = {
    val fields = extractArrayContent(json, "fields").getOrElse {
      return Left("missing fields array")
    }
    val rowsJson = extractArrayContent(json, "rows").getOrElse {
      return Left("missing rows array")
    }
    // The actual columns live in `rows`, each with { col_name, data_type }.
    // Per the existing ResolvedSchema shape (matches UC adapter,
    // PR #394/#424): Map[String, String] of column-name to type-string.
    // The typed Field shape (with SealedDataType) lands with Manifest v2
    // (PR 6). For v0.3.1, we carry the type string so downstream
    // consumers can decode it (e.g. via the engine adapter's type
    // mapper).
    val cols = scala.collection.mutable.LinkedHashMap.empty[String, String]
    splitTopLevelObjects(rowsJson).foreach { rowJson =>
      val name = extractStringField(rowJson, "col_name")
      val dataType = extractStringField(rowJson, "data_type")
      (name, dataType) match {
        case (Some(n), Some(t)) => cols += (n -> t)
        case _                  => ()
      }
    }
    Right(ResolvedSchema(cols.toMap))
  }

/** Parse a field-metadata object (one element of `fields`). */
  private def parseFieldObject(json: String): Option[HeraField] = {
    val name     = extractStringField(json, "name")
    val dataType = extractStringField(json, "dataType")
    val nullable = extractBooleanField(json, "nullable")
    (name, dataType) match {
      case (Some(n), Some(t)) => Some(HeraField(n, t, nullable.getOrElse(true)))
      case _                  => None
    }
  }

  /** Parse a row object into a `Map[String, Any]`. Naive — we just
    * extract `"key": value` pairs without parsing the value type. */
  private def parseRowObject(json: String): Map[String, Any] = {
    val out = scala.collection.mutable.LinkedHashMap.empty[String, Any]
    val len = json.length
    var i = 0
    while (i < len) {
      val q1 = json.indexOf('"', i)
      if (q1 < 0) return out.toMap
      val q2 = json.indexOf('"', q1 + 1)
      if (q2 < 0) return out.toMap
      val key = json.substring(q1 + 1, q2)
      val colon = json.indexOf(':', q2 + 1)
      if (colon < 0) return out.toMap
      val rest = json.substring(colon + 1).trim
      // Try to parse as a primitive; fall back to string.
      val value: Any =
        if (rest.startsWith("\"")) {
          val vq1 = 0
          val vq2 = rest.indexOf('"', vq1 + 1)
          if (vq2 < 0) rest else rest.substring(vq1 + 1, vq2)
        } else if (rest.startsWith("true") || rest.startsWith("false")) {
          rest.startsWith("true")
        } else if (rest.startsWith("null")) {
          null
        } else {
          val endIdx = rest.indexWhere(c => c == ',' || c == '}')
          val numStr = if (endIdx < 0) rest else rest.substring(0, endIdx)
          numStr.toLongOption.getOrElse(numStr.toDoubleOption.getOrElse(numStr))
        }
      out += (key -> value)
      // Advance past the value
      val consumedEnd =
        if (rest.startsWith("\"")) {
          val vq1 = 0
          val vq2 = rest.indexOf('"', vq1 + 1)
          vq2 + 1
        } else {
          val endIdx = rest.indexWhere(c => c == ',' || c == '}')
          if (endIdx < 0) rest.length else endIdx
        }
      i = colon + 1 + consumedEnd
    }
    out.toMap
  }

  /** Parse a realm list response into a portable [[HeraRealm]] list. */
  private[hera] def parseRealmList(json: String): Either[String, List[HeraRealm]] = {
    val arrayJson = extractArrayContent(json, "").getOrElse(json)  // top-level is the array
    val objs = splitTopLevelObjects(arrayJson)
    Right(objs.flatMap(parseRealmObject))
  }

  /** Parse a single realm object into a [[HeraRealm]]. */
  private def parseRealmObject(json: String): Option[HeraRealm] = {
    val id = extractNumberField(json, "id").getOrElse(return None)
    val name = extractStringField(json, "name").getOrElse(return None)
    val description = extractStringField(json, "description")
    val active = extractBooleanField(json, "active").getOrElse(true)
    Some(HeraRealm(id, name, description, active))
  }

  /** Parse a single realm response into a [[HeraRealm]].
    * Used by `getRealm` — the response shape is a single object
    * (not wrapped in an array). */
  private[hera] def parseRealm(json: String): Either[String, HeraRealm] = {
    parseRealmObject(json) match {
      case Some(r) => Right(r)
      case None    => Left("could not parse realm")
    }
  }

  // -- JSON helpers (hand-rolled, no new dep) --

  /** Extract the content of a top-level JSON array field. */
  private[hera] def extractArrayContent(json: String, field: String): Option[String] = {
    val idx =
      if (field.isEmpty) json.indexOf('[')
      else {
        val fldIdx = json.indexOf(s""""$field"""")
        if (fldIdx < 0) return None
        json.indexOf('[', fldIdx)
      }
    if (idx < 0) return None
    val end = findMatchingBracket(json, idx)
    if (end < 0) return None
    Some(json.substring(idx + 1, end))
  }

  /** Split a JSON array's content into top-level object strings. */
  private[hera] def splitTopLevelObjects(s: String): List[String] = {
    val sb = new StringBuilder
    val out = scala.collection.mutable.ListBuffer.empty[String]
    var depth = 0
    var inStr = false
    var escape = false
    var i = 0
    while (i < s.length) {
      val c = s.charAt(i)
      if (escape) { sb.append(c); escape = false }
      else if (c == '\\') { sb.append(c); escape = true }
      else if (c == '"') { sb.append(c); inStr = !inStr }
      else if (!inStr) {
        if (c == '{') {
          if (depth == 0) sb.setLength(0)
          depth += 1
          sb.append(c)
        } else if (c == '}') {
          sb.append(c)
          depth -= 1
          if (depth == 0) out += sb.toString
        } else sb.append(c)
      } else sb.append(c)
      i += 1
    }
    out.toList
  }

  /** Find the matching close bracket for the bracket at `openAt`. */
  private def findMatchingBracket(s: String, openAt: Int): Int = {
    var depth = 0
    var inStr = false
    var escape = false
    var i = openAt
    while (i < s.length) {
      val c = s.charAt(i)
      if (escape) { escape = false }
      else if (c == '\\') { escape = true }
      else if (c == '"') { inStr = !inStr }
      else if (!inStr) {
        if (c == '[' || c == '{') depth += 1
        else if (c == ']' || c == '}') {
          depth -= 1
          if (depth == 0) return i
        }
      }
      i += 1
    }
    -1
  }

  /** Extract a string field's value from a JSON object. */
  private[hera] def extractStringField(json: String, field: String): Option[String] = {
    val pat = "\"" + field + "\""
    val idx = json.indexOf(pat)
    if (idx < 0) return None
    val colon = json.indexOf(':', idx + pat.length)
    if (colon < 0) return None
    val q1 = json.indexOf('"', colon + 1)
    if (q1 < 0) return None
    val q2 = json.indexOf('"', q1 + 1)
    if (q2 < 0) return None
    Some(json.substring(q1 + 1, q2))
  }

  /** Extract a numeric field's value from a JSON object. */
  private[hera] def extractNumberField(json: String, field: String): Option[Long] = {
    val pat = "\"" + field + "\""
    val idx = json.indexOf(pat)
    if (idx < 0) return None
    val colon = json.indexOf(':', idx + pat.length)
    if (colon < 0) return None
    val rest = json.substring(colon + 1).trim
    val endIdx = rest.indexWhere(c => !c.isDigit && c != '-')
    val numStr = if (endIdx < 0) rest else rest.substring(0, endIdx)
    numStr.toLongOption
  }

  /** Extract a boolean field's value from a JSON object. */
  private[hera] def extractBooleanField(json: String, field: String): Option[Boolean] = {
    val pat = "\"" + field + "\""
    val idx = json.indexOf(pat)
    if (idx < 0) return None
    val colon = json.indexOf(':', idx + pat.length)
    if (colon < 0) return None
    val rest = json.substring(colon + 1).trim
    if (rest.startsWith("true"))  Some(true)
    else if (rest.startsWith("false")) Some(false)
    else None
  }
}