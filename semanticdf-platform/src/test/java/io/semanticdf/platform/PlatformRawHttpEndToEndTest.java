package io.semanticdf.platform;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import dev.restate.sdk.testing.BindService;
import dev.restate.sdk.testing.RestateTest;
import dev.restate.sdk.testing.RestateURL;

import io.semanticdf.cache.ResultCache;
import io.semanticdf.platform.audit.AuditService;
import io.semanticdf.platform.audit.NoOpAuditEventStore;
import io.semanticdf.platform.catalog.CatalogService;
import io.semanticdf.platform.model.NoOpModelStore;
import io.semanticdf.platform.model.ModelService;
import io.semanticdf.platform.query.QueryService;
import io.semanticdf.platform.streaming.HotReloadingModelRegistry;
import io.semanticdf.platform.streaming.ModelRegistry;
import io.semanticdf.platform.streaming.YamlModelRegistry;

import java.net.URL;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * End-to-end test for the platform's services via RAW HTTP through the
 * Restate TestKit ingress.
 *
 * <p>Unlike {@code ModelServiceHotReloadTest} (which uses the SDK Client),
 * this test uses Java's built-in {@link HttpClient} with plain
 * {@code Content-Type: application/json} POSTs. This is the wire format a
 * browser, a curl invocation, or any REST client sends — NOT the
 * Restate SDK's wire protocol.
 *
 * <p><b>Why this exists:</b> PR #256/257 verified the deployment logic
 * (register+query round-trip) via the SDK Client, which speaks the wire
 * protocol natively. An enduser hitting the platform from a browser sends
 * raw HTTP and expects raw HTTP to work. This test pins that contract at
 * the test-suite level — the same path an enduser exercises in production.
 *
 * <p><b>The keystone test:</b> {@code registerBrandNewModel_thenQueryViaRawHttp}
 * proves the full deployment story:
 * <ol>
 *   <li>Boot-time registry has only a placeholder model (seed.yml).
 *   <li>POST /ModelService/rawtest/register via raw HTTP succeeds.
 *   <li>POST /QueryService/runQuery for that model via raw HTTP returns
 *       real rows. <b>Pre-H3 this would fail with "model not found".</b>
 * </ol>
 *
 * <p>Boots a real Restate runtime via {@code @RestateTest} (Testcontainers,
 * Docker required). Uses the TestKit-allocated ingress URL.
 */
@RestateTest
class PlatformRawHttpEndToEndTest {

  static final SparkSession spark = createSpark();
  static final Path modelsDir;

  static {
    try {
      // YamlModelRegistry.load() requires a non-empty *.yml directory.
      // We seed a placeholder model that we don't query, then mutate the
      // registry via HotReloadingModelRegistry overlay for the actual test.
      // The placeholder ensures YamlModelRegistry's non-empty invariant
      // is satisfied; the keystone test then queries a different model
      // (rawtest) that exists only in the runtime overlay.
      modelsDir = Files.createTempDirectory("raw-http-e2e-models");
      Files.writeString(
          modelsDir.resolve("seed.yml"),
          "seed:\n"
              + "  table: flights_tbl\n"
              + "  description: Placeholder to satisfy YamlModelRegistry non-empty check\n"
              + "  dimensions:\n"
              + "    carrier: carrier\n"
              + "  measures:\n"
              + "    s: \"count(flight_count)\"\n");
      spark
          .createDataFrame(
              java.util.Arrays.asList(
                  org.apache.spark.sql.RowFactory.create("AA", 1L, 100L),
                  org.apache.spark.sql.RowFactory.create("AA", 2L, 200L),
                  org.apache.spark.sql.RowFactory.create("UA", 3L, 300L)),
              new org.apache.spark.sql.types.StructType()
                  .add("carrier", "string")
                  .add("flight_count", "long")
                  .add("total_distance", "long"))
          .createOrReplaceTempView("flights_tbl");
    } catch (Exception e) {
      throw new RuntimeException("could not set up the e2e fixture", e);
    }
  }

  static final ObjectMapper JSON = new ObjectMapper();
  // Standard HttpClient with default JSON content-type. NOT the SDK Client.
  static final HttpClient HTTP = HttpClient.newBuilder()
      .connectTimeout(Duration.ofSeconds(5))
      .build();

  // Production wiring: boot-time registry wrapped in hot-reload decorator.
  static final YamlModelRegistry BOOT_REGISTRY =
      YamlModelRegistry.load(modelsDir.toString(), spark);
  static final ModelRegistry MODELS = new HotReloadingModelRegistry(BOOT_REGISTRY);

  @BindService
  final ModelService modelService =
      new ModelService(new NoOpModelStore(), spark, ResultCache.NoOp(), MODELS);

  @BindService
  final QueryService queryService = QueryService.noOp(MODELS, spark);

  @BindService
  final CatalogService catalogService =
      new CatalogService(new NoOpModelStore());

  @BindService
  final AuditService auditService = new AuditService(new NoOpAuditEventStore());

  private static SparkSession createSpark() {
    return SparkSession.builder()
        .master("local[2]")
        .appName("platform-raw-http-e2e-test")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate();
  }

  @AfterAll
  static void tearDownAll() {
    if (spark != null) {
      spark.stop();
    }
  }

  /**
   * Build a request with the enduser-standard headers:
   *   Content-Type: application/json
   *   Accept: application/json
   *
   * <p>No SDK headers, no Restate-specific headers. Just what a curl /
   * Postman / browser fetch would send.
   */
  private HttpRequest.Builder rawJson(URL url) {
    return HttpRequest.newBuilder()
        .uri(URI.create(url.toString()))
        .timeout(Duration.ofSeconds(60))
        .header("Content-Type", "application/json")
        .header("Accept", "application/json");
  }

  // =====================================================================
  // Keystone test: register a brand-new model via raw HTTP, then query it.
  // This is what the user's "register, query via external ip" demo does.
  // =====================================================================

  /**
   * The keystone regression test for the raw-HTTP deployment path.
   *
   * <p>End-to-end via {@link HttpClient} (NOT the SDK Client):
   * <ol>
   *   <li>Boot-time registry has only a placeholder model (seed).
   *   <li>{@code POST /ModelService/rawtest/register} via raw JSON → 200.
   *   <li>{@code POST /QueryService/runQuery} for that model → 200 with rows.
   *       <b>Pre-H3: would fail with "model not found".</b>
   * </ol>
   */
  @Test
  @Timeout(value = 120)
  void registerBrandNewModel_thenQueryViaRawHttp(@RestateURL URL ingress) throws Exception {
    // STEP 1: register a brand-new model via raw HTTP (no SDK Client).
    String yaml =
        "rawtest:\n"
            + "  table: flights_tbl\n"
            + "  description: Registered via raw HTTP (PR-A keystone)\n"
            + "  dimensions:\n"
            + "    carrier: carrier\n"
            + "  measures:\n"
            + "    num: \"count(flight_count)\"\n"
            + "    total: \"sum(total_distance)\"\n";
    ObjectNode registerBody = JSON.createObjectNode();
    registerBody.put("modelName", "rawtest");
    registerBody.put("yaml", yaml);

    HttpResponse<String> regResp = HTTP.send(
        rawJson(new URL(ingress.toString() + "/ModelService/rawtest/register"))
            .POST(HttpRequest.BodyPublishers.ofString(registerBody.toString()))
            .build(),
        HttpResponse.BodyHandlers.ofString());
    assertEquals(200, regResp.statusCode(),
        "register must return 200; body=" + regResp.body());

    // STEP 2: query the same model via raw HTTP. This is what would fail
    // pre-H3 because the in-memory registry has no 'rawtest' model.
    ObjectNode queryBody = JSON.createObjectNode();
    queryBody.put("modelName", "rawtest");
    queryBody.putArray("measures").add("num").add("total");
    queryBody.putArray("dimensions").add("carrier");
    queryBody.put("where", "");

    HttpResponse<String> queryResp = HTTP.send(
        rawJson(new URL(ingress.toString() + "/QueryService/runQuery"))
            .POST(HttpRequest.BodyPublishers.ofString(queryBody.toString()))
            .build(),
        HttpResponse.BodyHandlers.ofString());
    assertEquals(200, queryResp.statusCode(),
        "runQuery for the just-registered model must return 200; body="
            + queryResp.body());

    JsonNode body = JSON.readTree(queryResp.body());
    JsonNode rows = body.get("rows");
    assertNotNull(rows, "runQuery response must include rows");
    assertEquals(2, rows.size(),
        "expected 2 distinct carriers (AA, UA); got: " + rows);
  }

  // =====================================================================
  // Direct contract tests for each raw-HTTP path
  // =====================================================================

  /** {@code POST /ModelService/{name}/getCurrentVersion} — Void handler. */
  @Test
  @Timeout(value = 120)
  void getCurrentVersion_returnsIntegerOverRawHttp(@RestateURL URL ingress) throws Exception {
    // Register first.
    ObjectNode registerBody = JSON.createObjectNode();
    registerBody.put("modelName", "vtest");
    registerBody.put("yaml",
        "vtest:\n  table: flights_tbl\n  dimensions:\n    carrier: carrier\n"
            + "  measures:\n    c: \"count(flight_count)\"\n");
    HttpResponse<String> regResp = HTTP.send(
        rawJson(new URL(ingress.toString() + "/ModelService/vtest/register"))
            .POST(HttpRequest.BodyPublishers.ofString(registerBody.toString()))
            .build(),
        HttpResponse.BodyHandlers.ofString());
    assertEquals(200, regResp.statusCode());

    // getCurrentVersion is a Void handler — must accept empty body
    // and no Content-Type. The Restate ingress rejects Content-Type
    // when the handler expects Void input.
    HttpResponse<String> verResp = HTTP.send(
        HttpRequest.newBuilder()
            .uri(URI.create(new URL(ingress.toString() + "/ModelService/vtest/getCurrentVersion").toString()))
            .timeout(Duration.ofSeconds(60))
            .POST(HttpRequest.BodyPublishers.noBody())
            .build(),
        HttpResponse.BodyHandlers.ofString());
    assertEquals(200, verResp.statusCode(),
        "getCurrentVersion must return 200; body=" + verResp.body());
    JsonNode ver = JSON.readTree(verResp.body());
    assertTrue(ver.asInt() >= 1,
        "version must be >= 1 after a successful register; got " + ver);
  }

  /** {@code POST /CatalogService/listModels} via raw HTTP. */
  @Test
  @Timeout(value = 120)
  void catalogListModels_returnsArrayOverRawHttp(@RestateURL URL ingress) throws Exception {
    ObjectNode reqBody = JSON.createObjectNode();
    reqBody.put("namespace", "");
    HttpResponse<String> resp = HTTP.send(
        rawJson(new URL(ingress.toString() + "/CatalogService/listModels"))
            .POST(HttpRequest.BodyPublishers.ofString(reqBody.toString()))
            .build(),
        HttpResponse.BodyHandlers.ofString());
    assertEquals(200, resp.statusCode(),
        "listModels must return 200; body=" + resp.body());
    // Returns an array — empty because NoOpModelStore.
    JsonNode arr = JSON.readTree(resp.body());
    assertTrue(arr.isArray(), "expected JSON array, got: " + arr);
  }

  /**
   * {@code POST /AuditService/append} via raw HTTP.
   * Tenant is the VirtualObject key; the handler is exclusive.
   */
  @Test
  @Timeout(value = 120)
  void auditAppend_acceptsJsonOverRawHttp(@RestateURL URL ingress) throws Exception {
    ObjectNode reqBody = JSON.createObjectNode();
    reqBody.put("tenant", "raw-http-tenant");
    reqBody.put("eventType", "platform.e2e");
    reqBody.put("timestamp", System.currentTimeMillis());
    reqBody.put("dedupHash", "raw-http-hash-" + System.nanoTime());
    reqBody.put("payload", "{\"smoke\":\"raw-http\"}");

    HttpResponse<String> resp = HTTP.send(
        rawJson(new URL(ingress.toString() + "/AuditService/raw-http-tenant/append"))
            .POST(HttpRequest.BodyPublishers.ofString(reqBody.toString()))
            .build(),
        HttpResponse.BodyHandlers.ofString());
    assertEquals(200, resp.statusCode(),
        "AuditService.append must return 200; body=" + resp.body());
  }

  // =====================================================================
  // H3-overlay-specific: register two models with the same name, query the
  // second one — proves the overlay's put-replaces semantic.
  // =====================================================================

  /**
   * Second registration of the same model name replaces the first overlay
   * entry. The query for the second registration's measure must succeed.
   */
  @Test
  @Timeout(value = 120)
  void secondRegister_replacesOverlayEntry_queriedViaRawHttp(@RestateURL URL ingress) throws Exception {
    // First registration: measure c.
    ObjectNode body1 = JSON.createObjectNode();
    body1.put("modelName", "replace-test");
    body1.put("yaml",
        "replace-test:\n  table: flights_tbl\n  dimensions:\n    carrier: carrier\n"
            + "  measures:\n    c: \"count(flight_count)\"\n");
    assertEquals(200, HTTP.send(
        rawJson(new URL(ingress.toString() + "/ModelService/replace-test/register"))
            .POST(HttpRequest.BodyPublishers.ofString(body1.toString()))
            .build(),
        HttpResponse.BodyHandlers.ofString()).statusCode());

    // Second registration: different measure (d).
    ObjectNode body2 = JSON.createObjectNode();
    body2.put("modelName", "replace-test");
    body2.put("yaml",
        "replace-test:\n  table: flights_tbl\n  dimensions:\n    carrier: carrier\n"
            + "  measures:\n    d: \"sum(total_distance)\"\n");
    assertEquals(200, HTTP.send(
        rawJson(new URL(ingress.toString() + "/ModelService/replace-test/register"))
            .POST(HttpRequest.BodyPublishers.ofString(body2.toString()))
            .build(),
        HttpResponse.BodyHandlers.ofString()).statusCode());

    // Query the new measure — only succeeds if overlay was replaced.
    ObjectNode queryBody = JSON.createObjectNode();
    queryBody.put("modelName", "replace-test");
    queryBody.putArray("measures").add("d");
    queryBody.putArray("dimensions").add("carrier");
    queryBody.put("where", "");
    HttpResponse<String> qResp = HTTP.send(
        rawJson(new URL(ingress.toString() + "/QueryService/runQuery"))
            .POST(HttpRequest.BodyPublishers.ofString(queryBody.toString()))
            .build(),
        HttpResponse.BodyHandlers.ofString());
    assertEquals(200, qResp.statusCode(),
        "second-register: query for d must succeed (overlay replaced); body="
            + qResp.body());
  }
}
