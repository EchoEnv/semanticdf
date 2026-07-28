package io.semanticdf.platform.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import dev.restate.sdk.testing.BindService;
import dev.restate.sdk.testing.RestateTest;
import dev.restate.sdk.testing.RestateURL;

import io.semanticdf.cache.ResultCache;
import io.semanticdf.platform.query.QueryService;
import io.semanticdf.platform.streaming.HotReloadingModelRegistry;
import io.semanticdf.platform.streaming.ModelRegistry;
import io.semanticdf.platform.streaming.YamlModelRegistry;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * End-to-end regression for the v0.2.3 H3 fix:
 * {@code ModelService.register} must propagate new models into the
 * runtime registry so {@code QueryService.runQuery} can execute them.
 *
 * <p>Pre-H3: {@code QueryService.runQuery} consulted the boot-time
 * {@link YamlModelRegistry} only. A model registered via
 * {@code POST /ModelService/{name}/register} was durable in Postgres
 * and visible via {@code CatalogService}, but {@code QueryService}
 * returned "model not found" because the in-memory registry was stale.
 *
 * <p>Post-H3: {@link HotReloadingModelRegistry} wraps the boot-time
 * registry. {@code ModelService.register} STEP F mutates the overlay.
 * This test proves the round-trip via real Restate ingress.
 *
 * <p>Boots a real Restate runtime via {@code @RestateTest} (Docker
 * required). Issues real HTTP POSTs to {@code /ModelService/...} and
 * {@code /QueryService/...} via the TestKit-allocated ingress URL.
 *
 * <p><b>Test model:</b> the boot-time registry contains a real
 * {@code flights} model so {@code YamlModelRegistry.load} succeeds.
 * The {@code H3 fix} is exercised by registering a brand-new model
 * name ({@code hotwidget}) that's NOT in the boot-time YAML — the
 * follow-up {@code runQuery} for {@code hotwidget} can ONLY succeed
 * if the H3 overlay mutation propagated to the runtime registry.
 */
@RestateTest
class ModelServiceHotReloadTest {

  static final SparkSession spark = createSpark();
  static final Path modelsDir;

  static {
    try {
      modelsDir = Files.createTempDirectory("h3-models");
      // Boot-time YAML — only contains "flights" so YamlModelRegistry.load succeeds.
      Files.writeString(
          modelsDir.resolve("flights.yml"),
          "flights:\n"
              + "  table: flights_tbl\n"
              + "  description: Boot-time fixture\n"
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
      // 'hotwidget' table — the model that will be REGISTERED via
      // ModelService (post-H3) and then queried.
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
          .createOrReplaceTempView("hotwidget_tbl");
    } catch (Exception e) {
      throw new RuntimeException("could not set up the e2e fixture", e);
    }
  }

  static final ObjectMapper JSON = new ObjectMapper();
  static final HttpClient HTTP = HttpClient.newBuilder()
      .connectTimeout(Duration.ofSeconds(5))
      .build();

  // Wrap the boot-time registry in a HotReloadingModelRegistry.
  // This is the production wiring; pre-H3 tests would use
  // YamlModelRegistry.load(...) directly, which is why the bug
  // surfaced in production but not in tests.
  static final YamlModelRegistry BOOT_REGISTRY = YamlModelRegistry.load(modelsDir.toString(), spark);
  static final ModelRegistry MODELS = new HotReloadingModelRegistry(BOOT_REGISTRY);

  @BindService
  final ModelService modelService =
      new ModelService(new io.semanticdf.platform.model.NoOpModelStore(), spark, ResultCache.NoOp(), MODELS);

  @BindService
  final QueryService queryService =
      QueryService.noOp(MODELS, spark);

  private static SparkSession createSpark() {
    return SparkSession.builder()
        .master("local[2]")
        .appName("model-hot-reload-test")
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
   * The keystone regression test for H3:
   *
   * <ol>
   *   <li>Boot-time registry has {@code flights} only.
   *   <li>{@code POST /ModelService/hotwidget/register} persists the
   *       model in Postgres + bumps the journal CURRENT_VERSION +
   *       mutates the HotReloadingModelRegistry overlay (H3).
   *   <li>{@code POST /QueryService/runQuery} for {@code hotwidget}
   *       executes against Spark and returns rows.
   * </ol>
   *
   * <p>Pre-H3, step 3 fails with "model not found" because the
   * in-memory registry has no {@code hotwidget}. Post-H3, step 3
   * returns the aggregated rows.
   */
  @Test
  @Timeout(value = 120)
  void registerThenRunQuery_propagatesToRuntimeRegistry(@RestateURL java.net.URL ingress) throws Exception {
    String yaml =
        "hotwidget:\n"
            + "  table: hotwidget_tbl\n"
            + "  description: H3 hot-reload e2e\n"
            + "  dimensions:\n"
            + "    carrier: carrier\n"
            + "  measures:\n"
            + "    c: \"count(flight_count)\"\n"
            + "    d: \"sum(total_distance)\"\n";

    // STEP 1: register the brand-new model.
    String regBody = JSON.writeValueAsString(
        Map.of("modelName", "hotwidget", "yaml", yaml));
    HttpResponse<String> regResp = HTTP.send(
        HttpRequest.newBuilder()
            .uri(URI.create(ingress.toString() + "/ModelService/hotwidget/register"))
            .timeout(Duration.ofSeconds(60))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(regBody))
            .build(),
        HttpResponse.BodyHandlers.ofString());
    assertEquals(200, regResp.statusCode(),
        "register must return 200; body=" + regResp.body());

    // STEP 2: runQuery — this is what pre-H3 would fail on
    // ("model not found: hotwidget").
    String queryBody = JSON.writeValueAsString(
        Map.of("modelName", "hotwidget",
               "measures", java.util.List.of("c", "d"),
               "dimensions", java.util.List.of("carrier"),
               "where", ""));
    HttpResponse<String> queryResp = HTTP.send(
        HttpRequest.newBuilder()
            .uri(URI.create(ingress.toString() + "/QueryService/runQuery"))
            .timeout(Duration.ofSeconds(90))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(queryBody))
            .build(),
        HttpResponse.BodyHandlers.ofString());
    assertEquals(200, queryResp.statusCode(),
        "runQuery must return 200 post-H3 (pre-H3 it would 404 model-not-found); body="
            + queryResp.body());

    JsonNode body = JSON.readTree(queryResp.body());
    JsonNode rows = body.get("rows");
    assertNotNull(rows, "runQuery response must include rows");
    assertEquals(2, rows.size(),
        "expected 2 distinct carriers (AA, UA); got: " + rows);
  }

  /**
   * Two registrations of the same model name replace the prior overlay
   * entry. Verifies {@code HotReloadingModelRegistry.register} is a put.
   */
  @Test
  @Timeout(value = 120)
  void secondRegister_replacesFirstOverlayEntry(@RestateURL java.net.URL ingress) throws Exception {
    // First registration for "hotwidget2" with measure c.
    String yaml1 =
        "hotwidget2:\n"
            + "  table: hotwidget_tbl\n"
            + "  dimensions:\n"
            + "    carrier: carrier\n"
            + "  measures:\n"
            + "    c: \"count(flight_count)\"\n";
    String body1 = JSON.writeValueAsString(
        Map.of("modelName", "hotwidget2", "yaml", yaml1));
    assertEquals(200, HTTP.send(
        HttpRequest.newBuilder()
            .uri(URI.create(ingress.toString() + "/ModelService/hotwidget2/register"))
            .timeout(Duration.ofSeconds(60))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body1))
            .build(),
        HttpResponse.BodyHandlers.ofString()).statusCode());

    // Query for measure c — should work after first registration.
    String queryBodyC = JSON.writeValueAsString(
        Map.of("modelName", "hotwidget2",
               "measures", java.util.List.of("c"),
               "dimensions", java.util.List.of("carrier"),
               "where", ""));
    HttpResponse<String> qResp1 = HTTP.send(
        HttpRequest.newBuilder()
            .uri(URI.create(ingress.toString() + "/QueryService/runQuery"))
            .timeout(Duration.ofSeconds(90))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(queryBodyC))
            .build(),
        HttpResponse.BodyHandlers.ofString());
    assertEquals(200, qResp1.statusCode(),
        "first registration: query for c must succeed; body=" + qResp1.body());

    // Second registration for "hotwidget2" with a DIFFERENT measure (d).
    String yaml2 =
        "hotwidget2:\n"
            + "  table: hotwidget_tbl\n"
            + "  dimensions:\n"
            + "    carrier: carrier\n"
            + "  measures:\n"
            + "    d: \"sum(total_distance)\"\n";
    String body2 = JSON.writeValueAsString(
        Map.of("modelName", "hotwidget2", "yaml", yaml2));
    assertEquals(200, HTTP.send(
        HttpRequest.newBuilder()
            .uri(URI.create(ingress.toString() + "/ModelService/hotwidget2/register"))
            .timeout(Duration.ofSeconds(60))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body2))
            .build(),
        HttpResponse.BodyHandlers.ofString()).statusCode());

    // Query for measure d — only available after the second registration.
    String queryBodyD = JSON.writeValueAsString(
        Map.of("modelName", "hotwidget2",
               "measures", java.util.List.of("d"),
               "dimensions", java.util.List.of("carrier"),
               "where", ""));
    HttpResponse<String> qResp2 = HTTP.send(
        HttpRequest.newBuilder()
            .uri(URI.create(ingress.toString() + "/QueryService/runQuery"))
            .timeout(Duration.ofSeconds(90))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(queryBodyD))
            .build(),
        HttpResponse.BodyHandlers.ofString());
    assertEquals(200, qResp2.statusCode(),
        "second registration: query for d must succeed (overlay replaced); body="
            + qResp2.body());
  }
}