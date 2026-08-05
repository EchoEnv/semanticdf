package io.semanticdf.trino.integration

import org.scalatest.{Assertion, BeforeAndAfterAll}
import org.scalatest.Assertions.assume
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration

/** Base trait for the Unity Catalog integration test. Provides:
  *   - the `assumeDocker()` gate (cancelled when `-Ddocker.tests=true` is unset)
  *   - the `ucUrl` system property (default `http://localhost:8089`)
  *   - HTTP helpers for setting up catalogs / schemas / tables via the
  *     UC REST API
  *
  * ==Why mirror the Trino fixture pattern==
  *
  * Per the standing pattern (`DockerTrinoFixture`), this fixture
  * reads system properties INSIDE the test methods (not in the
  * constructor) so the Surefire fork's system properties are
  * visible. Tests that don't override `dockerTestsEnabled` get
  * the default-gate behavior. */
abstract class UnityCatalogFixture
    extends AnyFunSuite
    with Matchers
    with BeforeAndAfterAll {

  /** The integration-test gate. Set `-Ddocker.tests=true` to enable.
    * Per debug-mantra §1 ("reproduce"): we use `parseBoolean` not
    * `toBoolean` because Maven's `<systemProperties>${docker.tests}</...>`
    * substitutes to the literal string "null" when unset, which
    * `toBoolean` rejects. */
  protected def dockerTestsEnabled: Boolean =
    java.lang.Boolean.parseBoolean(sys.props.getOrElse("docker.tests", "false"))

  /** The UC base URL. Override with `-Duc.url=...`. */
  protected val ucUrl: String =
    sys.props.getOrElse("uc.url", "http://localhost:8089")

  /** Wrap a test body in this gate. */
  protected def assumeDocker(): Assertion = {
    assume(dockerTestsEnabled, s"docker tests disabled — set -Ddocker.tests=true to enable")
  }

  /** The shared HttpClient for setup + the resolver. */
  protected val http: HttpClient = HttpClient.newBuilder()
    .connectTimeout(Duration.ofSeconds(10))
    .build()

  /** POST JSON to UC. Returns (statusCode, responseBody). */
  protected def postJson(path: String, body: String): (Int, String) = {
    val req = HttpRequest.newBuilder()
      .uri(URI.create(s"$ucUrl$path"))
      .timeout(Duration.ofSeconds(10))
      .header("Content-Type", "application/json")
      .POST(HttpRequest.BodyPublishers.ofString(body))
      .build()
    val resp = http.send(req, HttpResponse.BodyHandlers.ofString())
    (resp.statusCode(), resp.body())
  }

  /** GET from UC. Returns (statusCode, responseBody). */
  protected def getJson(path: String): (Int, String) = {
    val req = HttpRequest.newBuilder()
      .uri(URI.create(s"$ucUrl$path"))
      .timeout(Duration.ofSeconds(10))
      .header("Accept", "application/json")
      .GET()
      .build()
    val resp = http.send(req, HttpResponse.BodyHandlers.ofString())
    (resp.statusCode(), resp.body())
  }

  /** Set up the test catalog + schema + table. Idempotent — UC's
    * REST API returns 409 Conflict on existing names, which we
    * ignore (the resource already exists, which is what we want).
    *
    * Per the user's "monitor memory, disk" constraint, setup uses
    * a single table with 3 columns and < 100 rows of synthetic
    * data — disk usage stays under the catalog's quota.
    *
    * ==Why each column carries `type_text`, `type_json`, AND `position`==
    *
    * Per debug-mantra §3 ("falsify"): UC's v0.5.0 strictly
    * validates all 3 fields. We build them all explicitly so
    * the request doesn't get rejected with
    * "type_json.name is required" / "type_json.metadata is
    * required" / "ColumnInfo.getPosition() is null".
    *
    * `type_name` (LONG, STRING, DECIMAL) is the high-level type
    * tag UC uses for portable type identity. `type_text` is the
    * SQL-syntax form (e.g. `"decimal(18,2)"`). `type_json` is
    * the JSON-form full descriptor. `position` is the column
    * index. All four are required. */
  protected def setupTestResources(): Unit = {
    // 1. Create the catalog `unity` (idempotent).
    val catBody =
      """{"name":"unity","comment":"test catalog","properties":{}}"""
    postJson("/api/2.1/unity-catalog/catalogs", catBody)

    // 2. Create the schema `unity.semanticdf` (idempotent).
    val schBody =
      """{"name":"semanticdf","catalog_name":"unity","properties":{}}"""
    postJson("/api/2.1/unity-catalog/schemas", schBody)

    // 3. Create the table `unity.semanticdf.orders` (idempotent).
    //    The column format below is what UC v0.5.0 accepts.
    val tblBody =
      """{
        "name":"orders",
        "catalog_name":"unity",
        "schema_name":"semanticdf",
        "table_type":"EXTERNAL",
        "columns":[
          {"name":"id","type_name":"LONG","type_text":"long",
           "type_json":"{\"name\":\"id\",\"type\":\"long\",\"nullable\":false,\"metadata\":{}}",
           "nullable":false,"position":0},
          {"name":"region","type_name":"STRING","type_text":"string",
           "type_json":"{\"name\":\"region\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}",
           "nullable":true,"position":1},
          {"name":"amount","type_name":"DECIMAL","type_text":"decimal(18,2)",
           "type_json":"{\"name\":\"amount\",\"type\":\"decimal\",\"nullable\":true,\"metadata\":{}}",
           "nullable":true,"position":2}
        ],
        "storage_location":"file:///tmp/uc-orders-test",
        "comment":"orders table for UC integration test"
      }"""
    val (status, body) = postJson("/api/2.1/unity-catalog/tables", tblBody)
    if (status >= 400 && !body.contains("ALREADY_EXISTS")) {
      fail(s"Failed to create table: status=$status body=$body")
    }
  }

  /** Quick smoke test that the UC cluster is reachable. */
  protected def assertClusterHealthy(): Unit = {
    val (status, body) = getJson("/api/2.1/unity-catalog/catalogs")
    assert(status == 200, s"UC not reachable: status=$status body=$body")
  }
}