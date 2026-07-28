package io.semanticdf.platform.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import dev.restate.sdk.testing.BindService;
import dev.restate.sdk.testing.RestateTest;
import dev.restate.sdk.testing.RestateURL;

import io.semanticdf.cache.ResultCache;
import io.semanticdf.platform.catalog.CatalogService;
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
 * End-to-end smoke for {@code ModelService.register} through the
 * Restate ingress.
 *
 * <p>This test surfaces the bug surfaced by the v0.2.2 DE/architect
 * review (PR #248 follow-up): the previous implementation called
 * {@code Restate.run("model.compile", SemanticTable.class, ...)}
 * which journals a full {@code SemanticTable} (carrying a Spark
 * {@code Dataset.rdd} chain). Jackson cannot reconstruct abstract
 * Spark types on journal replay, so the previous implementation
 * threw {@code InvalidDefinitionException} and entered a retry
 * storm. PR #249 moves the compile and lineage calls out of
 * {@code Restate.run}; only the durable Postgres write remains
 * journal-bounded.
 *
 * <p>Boots a real Restate runtime via {@code @RestateTest}
 * (Testcontainers \u2014 Docker required). The model's YAML is
 * registered via {@code POST /ModelService/{name}/register} over
 * the Restate ingress, then read back via the same path. The
 * register call must return 200 + the persisted {@code
 * ModelDefinition}, and {@code getCurrentVersion} must reflect the
 * new version.
 */
@RestateTest
class ModelServiceEndToEndTest {

  static final SparkSession spark = createSpark();
  static final Path modelsDir;

  static {
    try {
      modelsDir = Files.createTempDirectory("model-e2e-models");
      Files.writeString(
          modelsDir.resolve("flights.yml"),
          "flights:\n"
              + "  table: flights_tbl\n"
              + "  description: Flights data (3 rows, register smoke)\n"
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

  // The startup-time registry is what production Query/Streaming
  // services use today (PR-A/B pre-#249). After #249 lands, runtime
  // registration via ModelService.register should be the source of
  // truth at production, but for the e2e test we use the same
  // pre-loaded registry.
  static final ModelRegistry MODEL_REGISTRY =
      YamlModelRegistry.load(modelsDir.toString(), spark);

  // We use a fresh in-memory ModelStore that this test owns \u2014
  // it doesn't need a real Postgres because the test only verifies
  // the Restate ingress path returns 200 + the persisted record.
  static final InMemoryTestModelStore STORE = new InMemoryTestModelStore();

  @BindService
  final ModelService modelService =
      new ModelService(STORE, spark, ResultCache.NoOp());

  // CatalogService is bound for a follow-up read-back test.
  @BindService
  final CatalogService catalogService =
      new CatalogService(STORE);

  private static SparkSession createSpark() {
    return SparkSession.builder()
        .master("local[2]")
        .appName("model-e2e-test")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate();
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

  /**
   * Keystones: a real registration via Restate ingress returns 200
   * (no InvalidDefinitionException on journal replay). Then the
   * {@code getCurrentVersion} read reflects the new version.
   *
   * <p>Before PR #249, the same HTTP call entered a retry storm
   * because {@code model.compile} journaled a full {@code SemanticTable}.
   * After PR #249, only the persist step (Postgres write) is
   * journal-bounded; compile and lineage are pure and re-run
   * cheaply on replay.
   */
  @Test
  @Timeout(value = 120)
  void register_viaRestateIngress_persistsAndAdvancesVersion(
      @RestateURL java.net.URL ingress) throws Exception,
      java.io.IOException {
    // POST /ModelService/{name}/register
    String body = JSON.writeValueAsString(
        Map.of("modelName", "flights", "yaml", loadYaml()));
    HttpResponse<String> regResp = HTTP.send(
        HttpRequest.newBuilder()
            .uri(URI.create(ingress.toString() + "/ModelService/flights/register"))
            .timeout(Duration.ofSeconds(60))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build(),
        HttpResponse.BodyHandlers.ofString());

    assertEquals(200, regResp.statusCode(),
        "register must return 200; body=" + regResp.body());

    // GET /ModelService/{name}/getCurrentVersion
    HttpResponse<String> verResp = HTTP.send(
        HttpRequest.newBuilder()
            .uri(URI.create(ingress.toString() + "/ModelService/flights/getCurrentVersion"))
            .timeout(Duration.ofSeconds(30))
            .GET()
            .build(),
        HttpResponse.BodyHandlers.ofString());
    assertEquals(200, verResp.statusCode(),
        "getCurrentVersion must return 200; body=" + verResp.body());
    JsonNode ver = JSON.readTree(verResp.body());
    // Service may return the version as a plain integer (newer
    // Restate SDK) or wrapped in a record. Try both.
    int version;
    if (ver.isInt()) {
      version = ver.asInt();
    } else {
      // wrap: {"value": 1} or similar
      version = ver.path("value").asInt();
    }
    assertTrue(version >= 1,
        "version must be at least 1 after a successful register; got "
        + verResp.body());

    // The model store should hold the persisted row.
    ModelStore.ModelDefinition def = STORE.lastWritten;
    assertNotNull(def, "the in-memory test store should have received a write");
    assertEquals("flights", def.modelName());
    assertEquals(version, def.version());
  }

  private static String loadYaml() throws Exception {
    return Files.readString(modelsDir.resolve("flights.yml"));
  }

  /**
   * Lightweight in-memory {@link ModelStore} for the test, replacing
   * the Postgres-backed one. Records the last write so the test can
   * assert the persisted version matches the version reported back
   * by the journal.
   */
  static final class InMemoryTestModelStore implements ModelStore {
    volatile ModelDefinition lastWritten;

    @Override
    public ModelDefinition registerIfAbsent(
        String modelName, int version, String yaml, String manifestHash,
        java.time.Instant registeredAt, String lineageJson) {
      lastWritten = new ModelDefinition(
          modelName, version, yaml, manifestHash, registeredAt, lineageJson);
      return lastWritten;
    }

    @Override
    public java.util.List<ModelDefinition> listAll() {
      return lastWritten == null
          ? java.util.List.of()
          : java.util.List.of(lastWritten);
    }

    @Override
    public ModelDefinition loadByName(String modelName, int version) {
      if (lastWritten != null
          && lastWritten.modelName().equals(modelName)
          && lastWritten.version() == version) {
        return lastWritten;
      }
      return null;
    }

    @Override
    public ModelDefinition loadLatest(String modelName) {
      if (lastWritten != null && lastWritten.modelName().equals(modelName)) {
        return lastWritten;
      }
      return null;
    }

    @Override
    public void ensureSchema() {
      // no-op
    }

    @Override
    public void close() {
      // no-op
    }
  }
}
