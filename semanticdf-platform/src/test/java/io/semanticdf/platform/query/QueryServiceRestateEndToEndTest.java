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

/**
 * The keystone v0.2.2 end-to-end smoke: a real query via
 * POST /QueryService/runQuery to QueryService.runQuery through
 * Restate.run("query.execute", ...) against real Spark.
 *
 * Verifies the full Restate-ingress path including the Restate.run
 * journal path. Returns 200 only if every layer works:
 *   Restate ingress -> Restate.run journal -> spark plan ->
 *   cache-miss execution -> wire-shape conversion.
 *
 * Before PR #245 + #248, this test threw InvalidDefinitionException
 * (Cannot construct instance of Row) on journal replay. PR #248
 * fixed the journal payload by introducing RestateCachedRow.
 */
@RestateTest
class QueryServiceRestateEndToEndTest {

  static final SparkSession spark = createSpark();
  static final Path modelsDir;

  static {
    try {
      modelsDir = Files.createTempDirectory("query-restate-e2e-models");
      Files.writeString(
          modelsDir.resolve("flights.yml"),
          "flights:\n"
              + "  table: flights_tbl\n"
              + "  description: Flights data (3 rows, Restate ingress smoke)\n"
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
  }

  static final ObjectMapper JSON = new ObjectMapper();
  static final HttpClient HTTP = HttpClient.newBuilder()
      .connectTimeout(Duration.ofSeconds(5))
      .build();

  static final ModelRegistry MODEL_REGISTRY =
      YamlModelRegistry.load(modelsDir.toString(), spark);

  // Sanity: the model loaded once at class-init time, on this side
  // of the Restate TestKit. (Restate's @BindService field construction
  // sees the same registry.)
  static final SemanticTable FLIGHTS =
      QueryServiceEndToEndTestPrecheck.run();

  @BindService
  final QueryService queryService =
      QueryService.noOp(MODEL_REGISTRY, spark);

  private static SparkSession createSpark() {
    return SparkSession.builder()
        .master("local[2]")
        .appName("query-restate-e2e-test")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate();
  }

  /** Real query through the full Restate ingress path. */
  @org.junit.jupiter.api.Test
  @org.junit.jupiter.api.Timeout(value = 120)
  void runQuery_viaRestateIngress_executesAgainstRealSpark(
      @RestateURL java.net.URL ingress) throws Exception {
    String requestBody = JSON.writeValueAsString(
        Map.of("modelName", "flights",
               "measures", List.of("c", "d"),
               "dimensions", List.of("carrier"),
               "where", ""));

    HttpRequest req =
        HttpRequest.newBuilder()
            .uri(URI.create(ingress.toString() + "/QueryService/runQuery"))
            .timeout(Duration.ofSeconds(90))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(requestBody))
            .build();
    HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());

    assertEquals(200, resp.statusCode(),
        "Restate must return 200; body=" + resp.body());

    JsonNode body = JSON.readTree(resp.body());
    assertEquals("flights", body.get("model").asText());
    assertEquals(2, body.get("rows").size(),
        "two carriers (AA, UA)");

    var rows = new java.util.ArrayList<JsonNode>();
    body.get("rows").forEach(rows::add);
    rows.sort((a, b) -> a.get(0).asText().compareTo(b.get(0).asText()));
    assertEquals("AA", rows.get(0).get(0).asText(), "first row carrier = AA");
    assertEquals("UA", rows.get(1).get(0).asText(), "second row carrier = UA");
  }

  /**
   * Different `where` filters through Restate ingress produce different
   * result rows. Regression for the cache-poisoning fix at the
   * Restate-ingress level.
   *
   * <p>The platform's `where` is applied to the post-aggregation
   * DataFrame, so it references measure or dimension names (not raw
   * source columns). `carrier = 'AA'` selects only the AA rows; the
   * sum(d) for those is 100+200=300, vs all-rows' 100+200+300=600.
   */
  @org.junit.jupiter.api.Test
  @org.junit.jupiter.api.Timeout(value = 120)
  void runQuery_viaRestateIngress_differentWhereYieldsDifferentRows(
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

    assertEquals(200, respAll.statusCode(), "all-rows: body=" + respAll.body());
    assertEquals(200, respFiltered.statusCode(),
        "filtered: body=" + respFiltered.body());

    JsonNode allBody = JSON.readTree(respAll.body());
    JsonNode filteredBody = JSON.readTree(respFiltered.body());

    // Different `where` strings produce different result-row counts
    // (all-rows → 2 carrier rows; filtered (carrier='AA') → 1 row).
    // The measure values within the AA row are identical across the
    // two queries (the filter doesn't change the AA group's
    // aggregate); the differentiator is the ROW SET.
    assertEquals(2, allBody.get("rows").size(),
        "all-rows returns 2 carrier rows (AA, UA)");
    assertEquals(1, filteredBody.get("rows").size(),
        "filtered (carrier='AA') returns 1 row");
    assertTrue(allBody.get("rows").size() != filteredBody.get("rows").size(),
        "different WHERE strings produce different row sets, confirming " +
        "the cache-poisoning fix (PR #245) holds end-to-end through Restate");
  }

  /** Find the 'd' (sum) column index in the response. */
  private static long readDColumn(JsonNode body) {
    var names = body.get("measures");
    int dIdx = -1;
    for (int i = 0; i < names.size(); i++) {
      if ("d".equals(names.get(i).asText())) {
        dIdx = i;
        break;
      }
    }
    if (dIdx < 0) {
      throw new AssertionError("response measures does not contain 'd': "
          + body);
    }
    long max = Long.MIN_VALUE;
    var rows = body.get("rows");
    for (int i = 0; i < rows.size(); i++) {
      JsonNode v = rows.get(i).get(dIdx);
      if (v != null && !v.isNull()) {
        max = Math.max(max, v.asLong());
      }
    }
    return max;
  }

  /** Type precheck: load the YAML model at class-init and assert it
   *  produced a non-null SemanticTable. Surfaces fixture errors with
   *  a clear error message rather than the catch-all NPE later. */
  static final class QueryServiceEndToEndTestPrecheck {
    static SemanticTable run() {
      SemanticTable m = MODEL_REGISTRY.get("flights");
      assertNotNull(m, "YAML registry should have loaded the flights model");
      return m;
    }
  }
}
