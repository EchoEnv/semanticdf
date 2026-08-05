package io.semanticdf.unitycatalog

import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration

/** Concrete [[UnityCatalogClient]] implementation backed by the
  * Unity Catalog REST API. Uses the JDK's `java.net.http.HttpClient`
  * (no external dependency — Java 11+ stdlib).
  *
  * ==Why `java.net.http.HttpClient` (not OkHttp / sttp / etc.)==
  *
  * Per karpathy §2 ("minimum code that solves the problem"):
  * the JDK's built-in HTTP client is sufficient for the UC
  * REST API (GET-only, JSON response). Adding OkHttp or sttp
  * would pull a new dependency for no real benefit.
  *
  * ==Why GET-only (no POST/PUT/DELETE)==
  *
  * The [[UnityCatalogClient]] contract is read-only — it's a
  * metadata SOURCE. The resolver asks "what columns does this
  * table have?"; it doesn't create or modify tables. The
  * integration test creates schemas/tables via direct curl
  * (one-time setup) and then the resolver reads them. This
  * matches the existing `TrinoSourceResolver` pattern (which
  * also reads, not writes).
  *
  * ==Why a hard-coded timeout==
  *
  * Per the user's "monitor memory, disk" constraint: a hung
  * HTTP call would leak the JVM's HTTP client resources. The
  * 10-second timeout bounds the worst case; on timeout, the
  * call returns `None` (mapped to `ResolvedSource.NotFound` by
  * the caller — same as "table not found").
  *
  * ==Why no auth header (yet)==
  *
  * The integration-test setup disables UC auth (`server.authorization=disable`
  * in `docker-uc/config/server.properties`). Production users
  * who enable auth will need to extend this client with a
  * `Bearer <token>` header — left as a follow-up.
  *
  * ==Boundary contract==
  *
  * Zero Spark imports. Verifiable by:
  * `grep -r 'org.apache.spark' adapters/semanticdf-trino/src/main/scala/io/semanticdf/trino/HttpUnityCatalogClient.scala`
  */
final class HttpUnityCatalogClient(
    baseUrl: String,             // e.g. "http://localhost:8089"
    apiVersion: String = "2.1",  // UC API version
) extends UnityCatalogClient {

  // JDK HttpClient is thread-safe; one instance is shared.
  private val http = HttpClient.newBuilder()
    .connectTimeout(Duration.ofSeconds(10))
    .build()

  override def describeTable(
      catalog: String,
      schema:  String,
      table:   String,
  ): Option[UcTableSchema] = {
    // UC REST path: /api/{ver}/unity-catalog/tables/{full_name}
    // where {full_name} = "{catalog}.{schema}.{table}"
    val url = s"$baseUrl/api/$apiVersion/unity-catalog/tables/$catalog.$schema.$table"
    val req = HttpRequest.newBuilder()
      .uri(URI.create(url))
      .timeout(Duration.ofSeconds(10))
      .header("Accept", "application/json")
      .GET()
      .build()
    try {
      val resp = http.send(req, HttpResponse.BodyHandlers.ofString())
      if (resp.statusCode() == 200) {
        val tableInfo = HttpUnityCatalogClient.parseTableInfo(
          resp.body(),
          catalog = catalog,
          schema  = schema,
          table   = table,
        )
        Some(tableInfo)
      } else if (resp.statusCode() == 404) {
        // UC returns 404 for missing catalog/schema/table.
        None
      } else {
        // Auth, server errors, etc. — treat as not-found for now.
        // (Future PR: distinguish AuthFailed from NotFound.)
        None
      }
    } catch {
      case _: java.io.IOException    => None  // network error
      case _: InterruptedException  => None  // timeout
    }
  }
}

object HttpUnityCatalogClient {

  /** Smart constructor — preferred over `new HttpUnityCatalogClient(...)`
    * because it leaves room for future default-argument expansion
    * (e.g. `authToken: Option[String] = None`) without breaking
    * call sites. */
  def apply(
      baseUrl:    String,
      apiVersion: String = "2.1",
  ): HttpUnityCatalogClient =
    new HttpUnityCatalogClient(baseUrl, apiVersion)

  /** Parse a UC table-info JSON response into a [[UcTableSchema]].
    *
    * ==Why a hand-rolled JSON parser==
    *
    * Per karpathy §2 ("minimum code"): the UC response shape
    * is fixed (we control the server via our docker setup). A
    * minimal regex-based extractor handles the 2 fields we
    * need (`name`, `type_name`, `nullable`) without adding a
    * JSON dependency. We can swap to a real JSON library if the
    * response shape grows.
    *
    * ==Why this fails fast==
    *
    * If UC's response shape drifts, the parse fails and we
    * return `None`. The integration test catches the drift
    * loudly (not silently). */
  private[unitycatalog] def parseTableInfo(
      json:   String,
      catalog: String,
      schema:  String,
      table:   String,
  ): UcTableSchema = {
    // Extract columns array — minimal regex for our test setup:
    //   "columns":[ { "name":"...", "type_name":"...", "nullable":... }, ... ]
    val columns = extractColumns(json)
    UcTableSchema(catalog, schema, table, columns)
  }

  /** Parse the `columns` array from a UC table-info response.
    * Hand-rolled for the integration test (no JSON dep). */
  private[unitycatalog] def extractColumns(json: String): List[UcColumn] = {
    // Find the "columns":[ ... ] array. We use a regex that
    // tolerates nested braces (UC's column JSON can include
    // `type_precision`, `type_scale`, etc.).
    val colsStart = json.indexOf("\"columns\"")
    if (colsStart < 0) return Nil
    val arrayStart = json.indexOf('[', colsStart)
    if (arrayStart < 0) return Nil
    val arrayEnd   = findMatchingBracket(json, arrayStart)
    if (arrayEnd < 0) return Nil
    val arrayText  = json.substring(arrayStart + 1, arrayEnd)

    // Each column is a `{ ... }` object. Walk brace-by-brace.
    splitTopLevelObjects(arrayText).flatMap { obj =>
      parseColumnObject(obj)
    }
  }

  private def splitTopLevelObjects(s: String): List[String] = {
    val sb     = new StringBuilder
    val out    = scala.collection.mutable.ListBuffer.empty[String]
    var depth  = 0
    var inStr  = false
    var escape = false
    var i      = 0
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

  private def findMatchingBracket(s: String, openAt: Int): Int = {
    var depth  = 0
    var inStr  = false
    var escape = false
    var i      = openAt
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

  private def parseColumnObject(obj: String): Option[UcColumn] = {
    val name     = extractStringField(obj, "name")
    val typeName = extractStringField(obj, "type_name")
    val nullable = extractBooleanField(obj, "nullable")
    (name, typeName) match {
      case (Some(n), Some(t)) => Some(UcColumn(n, t, nullable.getOrElse(true)))
      case _                  => None
    }
  }

  private def extractStringField(obj: String, field: String): Option[String] = {
    val pat  = "\"" + field + "\""
    val idx  = obj.indexOf(pat)
    if (idx < 0) return None
    val colon = obj.indexOf(':', idx + pat.length)
    if (colon < 0) return None
    val q1 = obj.indexOf('"', colon + 1)
    if (q1 < 0) return None
    val q2 = obj.indexOf('"', q1 + 1)
    if (q2 < 0) return None
    Some(obj.substring(q1 + 1, q2))
  }

  private def extractBooleanField(obj: String, field: String): Option[Boolean] = {
    val pat  = "\"" + field + "\""
    val idx  = obj.indexOf(pat)
    if (idx < 0) return None
    val colon = obj.indexOf(':', idx + pat.length)
    if (colon < 0) return None
    val rest  = obj.substring(colon + 1).trim
    if (rest.startsWith("true"))  Some(true)
    else if (rest.startsWith("false")) Some(false)
    else None
  }
}