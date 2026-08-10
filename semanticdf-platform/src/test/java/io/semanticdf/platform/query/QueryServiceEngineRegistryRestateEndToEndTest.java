package io.semanticdf.platform.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.restate.sdk.testing.BindService;
import dev.restate.sdk.testing.RestateTest;
import dev.restate.sdk.testing.RestateURL;
import io.semanticdf.SemanticTable;
import io.semanticdf.cache.ResultCache;
import io.semanticdf.core.engine.MCPEngineRegistry;
import io.semanticdf.platform.streaming.ModelRegistry;
import io.semanticdf.platform.streaming.YamlModelRegistry;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * End-to-end test for the v0.3.1 engine-portable query path through Restate.
 *
 * <p>Per the v0.3.1 Platform migration design doc (PR #443) and the engine-portable
 * seams added in #446:
 * <ol>
 *   <li>{@link QueryService} is wired with an {@link MCPEngineRegistry} (the
 *       4-arg constructor pattern — Spark is the default engine).
 *   <li>The model is registered as a {@code core.Model} via {@code ModelRegistry.getModel}
 *       (the dual-store from PR #445).
 *   <li>{@link QueryService#runQuery} dispatches to the engine-portable path
 *       instead of the legacy {@code SemanticTable} path.
 * </ol>
 *
 * <p>This test is the missing E2E counterpart to {@link QueryServiceRestateEndToEndTest},
 * which covers the legacy path. The two tests together prove the full
 * v0.3.1 query surface via Restate.
 *
 * <p><b>Why this matters:</b> The legacy path and the engine-portable path
 * share zero code at the QueryService level — they're parallel dispatch
 * branches. The legacy path is fully E2E-tested (PR #245 cache-poisoning
 * regression). The engine-portable path was only wired and unit-tested
 * (PR #446). This test closes the gap.
 *
 * <p><b>Boot model:</b> a real Restate runtime via Testcontainers +
 * a real Spark session + a YAML-loaded model that the dual-store
 * exposes as both {@code SemanticTable} (legacy) AND {@code core.Model}
 * (engine-portable).
 *
 * <p>Verifies the full Restate-ingress path:
 *   Restate ingress → QueryService.runQuery → engine-portable dispatch
 *   → MCPEngineRegistry.select("spark") → SparkEngineProvider.query
 *   → DataFrame collection → PortableQueryResult → wire-shape conversion
 *   → 200 OK + JSON.
 */
@RestateTest
class QueryServiceEngineRegistryRestateEndToEndTest {

  static final SparkSession spark = createSpark();
  static final Path modelsDir;
  static final ModelRegistry MODEL_REGISTRY;

  static {
    try {
      modelsDir = Files.createTempDirectory("query-engine-registry-e2e-models");
      Files.writeString(
          modelsDir.resolve("flights.yml"),
          "flights:\n"
              + "  table: flights_tbl\n"
              + "  description: Flights data (3 rows, engine-portable E2E)\n"
              + "  dimensions:\n"
              + "    carrier: carrier\n"
              + "  measures:\n"
              + "    c: \"count(flight_count)\"\n"
              + "    d: \"sum(total_distance)\"\n");
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
    // Load the YAML model AFTER the temp view is registered. The dual-store
    // (PR #445) populates both `Map<String, SemanticTable>` and
    // `Map<String, Model>` so the engine-portable path can find the model
    // via `ModelRegistry.getModel(name)`.
    MODEL_REGISTRY = YamlModelRegistry.load(modelsDir.toString(), spark);
  }

  static final ObjectMapper JSON = new ObjectMapper();
  static final HttpClient HTTP = HttpClient.newBuilder()
      .connectTimeout(Duration.ofSeconds(5))
      .build();

  // Precheck: prove the model is dual-stored (both legacy AND engine-portable).
  static final SemanticTable FLIGHTS_LEGACY = MODEL_REGISTRY.get("flights");
  static final io.semanticdf.core.model.Model FLIGHTS_PORTABLE =
      MODEL_REGISTRY.getModel("flights").orElse(null);

  // Build the engine registry with Spark as the default engine.
  // Same factory as PlatformApplication (PR #446).
  static final MCPEngineRegistry ENGINE_REGISTRY =
      io.semanticdf.spark.PlatformEngineRegistryBuilder.buildSparkDefaultStatic(spark);

  @BindService
  final QueryService queryService =
      new QueryService(MODEL_REGISTRY, spark, ResultCache.NoOp(), ENGINE_REGISTRY);

  private static SparkSession createSpark() {
    return SparkSession.builder()
        .master("local[2]")
        .appName("query-engine-registry-restate-e2e-test")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate();
  }

  /**
   * E2E: real query via the engine-portable path through Restate.
   *
   * <p>Per the design doc, when the engine registry is wired AND the model
   * is registered as a {@code core.Model}, {@code QueryService.runQuery}
   * dispatches to the engine-portable path. This test verifies that
   * path produces the same shape of response as the legacy path.
   */
  @Test
  @Timeout(value = 120)
  void runQuery_viaEngineRegistry_viaRestateIngress_executesAgainstRealSpark(
      @RestateURL java.net.URL ingress) throws Exception {
    // Precheck: the model is registered as a core.Model (otherwise the
    // engine-portable path would silently fall back to legacy).
    assertNotNull(FLIGHTS_PORTABLE,
        "Model must be registered as a core.Model for the engine-portable "
            + "path to activate. YamlModelRegistry's dual-store (PR #445) "
            + "should populate getModel() for any model loaded via YAML.");
    assertNotNull(FLIGHTS_LEGACY,
        "Model must also be registered as a legacy SemanticTable — the "
            + "dual-store covers both representations.");

    String requestBody = JSON.writeValueAsString(
        Map.of("modelName", "flights",
               "measures", List.of("c", "d"),
               "dimensions", List.of("carrier"),
               "where", ""));

    HttpResponse<String> resp = HTTP.send(
        HttpRequest.newBuilder()
            .uri(URI.create(ingress.toString() + "/QueryService/runQuery"))
            .timeout(Duration.ofSeconds(90))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(requestBody))
            .build(),
        HttpResponse.BodyHandlers.ofString());

    assertEquals(200, resp.statusCode(),
        "Restate must return 200; body=" + resp.body());

    JsonNode body = JSON.readTree(resp.body());
    assertEquals("flights", body.get("model").asText());
    assertEquals(2, body.get("rows").size(),
        "two carriers (AA, UA) — proves the engine-portable path produced "
            + "the same row set as the legacy path");

    // Verify the rows by sorting on the carrier column (pos 0).
    var rows = new java.util.ArrayList<JsonNode>();
    body.get("rows").forEach(rows::add);
    rows.sort((a, b) -> a.get(0).asText().compareTo(b.get(0).asText()));
    assertEquals("AA", rows.get(0).get(0).asText(), "first row carrier = AA");
    assertEquals("UA", rows.get(1).get(0).asText(), "second row carrier = UA");

    // Verify the data flow through the engine-portable path: the 'd' measure
    // is sum(total_distance), so AA's d = 100+200=300, UA's d = 300.
    long dAa = readMeasureForCarrier(body, "d", "AA");
    long dUa = readMeasureForCarrier(body, "d", "UA");
    assertEquals(300L, dAa, "AA sum(total_distance) = 100+200 = 300");
    assertEquals(300L, dUa, "UA sum(total_distance) = 300");
  }

  /**
   * Different `where` filters through the engine-portable path produce
   * different rows. Regression for the cache-poisoning fix at the
   * engine-portable level.
   */
  @Test
  @Timeout(value = 120)
  void runQuery_viaEngineRegistry_differentWhereYieldsDifferentRows(
      @RestateURL java.net.URL ingress) throws Exception {
    String bodyAll = JSON.writeValueAsString(
        Map.of("modelName", "flights",
               "measures", List.of("c", "d"),
               "dimensions", List.of("carrier"),
               "where", ""));
    String bodyFiltered = JSON.writeValueAsString(
        Map.of("modelName", "flights",
               "measures", List.of("c", "d"),
               "dimensions", List.of("carrier"),
               "where", "carrier = 'AA'"));

    HttpResponse<String> respAll = HTTP.send(
        HttpRequest.newBuilder()
            .uri(URI.create(ingress.toString() + "/QueryService/runQuery"))
            .timeout(Duration.ofSeconds(90))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(bodyAll))
            .build(),
        HttpResponse.BodyHandlers.ofString());

    HttpResponse<String> respFiltered = HTTP.send(
        HttpRequest.newBuilder()
            .uri(URI.create(ingress.toString() + "/QueryService/runQuery"))
            .timeout(Duration.ofSeconds(90))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(bodyFiltered))
            .build(),
        HttpResponse.BodyHandlers.ofString());

    assertEquals(200, respAll.statusCode(), "Restate must return 200");
    assertEquals(200, respFiltered.statusCode(), "Restate must return 200");

    JsonNode allBody = JSON.readTree(respAll.body());
    JsonNode filteredBody = JSON.readTree(respFiltered.body());

    assertEquals(2, allBody.get("rows").size(),
        "all-rows returns 2 carrier rows (AA, UA) via engine-portable path");
    assertEquals(1, filteredBody.get("rows").size(),
        "filtered (carrier='AA') returns 1 row via engine-portable path");
    assertTrue(allBody.get("rows").size() != filteredBody.get("rows").size(),
        "different WHERE strings produce different row sets via the "
            + "engine-portable path, confirming the cache-poisoning fix "
            + "(PR #245 + #447) holds for the engine-portable surface");
  }

  private static long readMeasureForCarrier(JsonNode body, String measureName, String carrier) {
    var names = body.get("measures");
    int idx = -1;
    for (int i = 0; i < names.size(); i++) {
      if (measureName.equals(names.get(i).asText())) {
        idx = i;
        break;
      }
    }
    if (idx < 0) {
      throw new AssertionError("response measures does not contain '" + measureName
          + "': " + body);
    }
    var rows = body.get("rows");
    for (int i = 0; i < rows.size(); i++) {
      if (carrier.equals(rows.get(i).get(0).asText())) {
        return rows.get(i).get(idx).asLong();
      }
    }
    throw new AssertionError("response rows do not contain carrier '" + carrier + "': " + body);
  }

  @AfterAll
  static void tearDownAll() throws java.io.IOException {
    if (spark != null) {
      spark.stop();
    }
    if (modelsDir != null) {
      try (var walk = Files.walk(modelsDir)) {
        walk.sorted(java.util.Comparator.reverseOrder())
            .forEach(p -> { try { Files.deleteIfExists(p); } catch (Exception ignored) {} });
      }
    }
  }
}
