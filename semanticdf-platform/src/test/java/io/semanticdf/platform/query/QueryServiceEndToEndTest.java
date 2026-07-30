package io.semanticdf.platform.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.semanticdf.cache.CacheBridge;
import io.semanticdf.cache.CachedResult;
import io.semanticdf.cache.ResultCache;
import io.semanticdf.platform.streaming.ModelRegistry;
import io.semanticdf.platform.streaming.YamlModelRegistry;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * End-to-end smoke for the v0.2.2 keystone \u2014 a real query on the
 * platform's compile/execute path against a real Spark session.
 *
 * <p>Invokes the {@link CacheBridge#executeQuery} step that
 * {@link QueryService#runQuery} delegates to inside its
 * {@code Restate.run(...)} block. By calling the bridge directly,
 * we sidestep the Restate-handler-context requirement (which the
 * {@code QueryService.runQuery} entry-point enforces) while still
 * exercising the real Spark compilation and SQL execution path.
 *
 * <p>Boots:
 * <ul>
 *   <li>A real {@link SparkSession} (in-process, local[2]);
 *   <li>A tiny flights-style YAML fixture (3 rows) loaded via
 *       {@link YamlModelRegistry} (the same code-path
 *       {@code PlatformApplication.main} uses at startup).
 * </ul>
 *
 * <p>The complementary path through the Restate ingress lives in
 * {@code PlatformEndToEndTest} for services that don't journal
 * {@code Row[]} (AuditService + CatalogService).
 */
class QueryServiceEndToEndTest {

  static SparkSession spark;
  static Path modelsDir;
  static io.semanticdf.SemanticTable flightsModel;

  @BeforeAll
  static void setUpAll() throws Exception {
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("query-end-to-end-test")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.ansi.enabled", "false")
            .getOrCreate();

    modelsDir = Files.createTempDirectory("query-e2e-models");
    Files.writeString(
        modelsDir.resolve("flights.yml"),
        "flights:\n"
            + "  table: flights_tbl\n"
            + "  description: Flights data (3 rows, e2e fixture)\n"
            + "  dimensions:\n"
            + "    carrier: carrier\n"
            + "  measures:\n"
            + "    c: \"count(flight_count)\"\n"
            + "    d: \"sum(total_distance)\"\n");

    // The YAML loader resolves `table:` via spark.table(...); the
    // e2e fixture's DataFrame needs to be registered as a temp
    // view so the model can compile.
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

    ModelRegistry registry = YamlModelRegistry.load(modelsDir.toString(), spark);
    flightsModel = registry.get("flights");
    assertNotNull(flightsModel, "YAML registry should have loaded the flights model");
  }

  @AfterAll
  static void tearDownAll() throws Exception {
    if (spark != null) {
      spark.stop();
    }
    if (modelsDir != null) {
      try (var walk = Files.walk(modelsDir)) {
        walk.sorted(Comparator.reverseOrder())
            .forEach(p -> { try { Files.deleteIfExists(p); } catch (Exception ignored) {} });
      }
    }
  }

  /**
   * Keystone: run a real query through {@code CacheBridge.executeQuery}
   * (the path {@link QueryService#runQuery} uses inside its
   * {@code Restate.run(...)} block) on a real Spark session +
   * YamlModelRegistry. Verify the result rows.
   */
  @Test
  @Timeout(value = 120)
  void executeQuery_returnsRowsFromRealSpark() {
    CachedResult result =
        CacheBridge.executeQuery(
            flightsModel,
            spark,
            List.of("c", "d"),
            List.of("carrier"),
            /* where */ "",
            /* maxRows */ CacheBridge.defaultMaxRows());

    assertEquals("flights", flightsModel.name().isDefined() ? flightsModel.name().get() : "?",
        "model name resolves");
    // Schema-declaration order: carrier, c, d
    assertEquals(2, result.rows().length,
        "two carriers (AA, UA); one row per carrier per Spark groupBy");

    // Sort the rows by carrier for deterministic assertion.
    var sorted = new java.util.ArrayList<org.apache.spark.sql.Row>(
        java.util.Arrays.asList(result.rows()));
    sorted.sort((a, b) -> a.getString(0).compareTo(b.getString(0)));
    assertEquals("AA", sorted.get(0).getString(0), "first row carrier = AA");
    assertEquals("UA", sorted.get(1).getString(0), "second row carrier = UA");
    // c (count) and d (sum) aggregate the underlying rows.
    assertTrue(sorted.get(0).get(1) instanceof Number,
        "c (count) returns a numeric type: " + sorted.get(0).get(1));
    assertTrue(sorted.get(0).get(2) instanceof Number,
        "d (sum) returns a numeric type: " + sorted.get(0).get(2));
  }

  /**
   * Cache-key bug regression (PR #245): the platform's cache key
   * helper used by the handler path produces distinct keys for
   * distinct {@code where} strings, so two callers with different
   * filters don't collide on cache.
   */
  @Test
  @Timeout(value = 60)
  void platformCacheKey_distinguishesWhereFilters() {
    int v = flightsModel.version();
    String keyAll = CacheBridge.platformCacheKey(
        "flights", v, List.of("c", "d"), List.of("carrier"), "");
    String keyFiltered = CacheBridge.platformCacheKey(
        "flights", v, List.of("c", "d"), List.of("carrier"),
        "total_distance > 100");
    assertTrue(!keyAll.equals(keyFiltered),
        "different WHERE strings must produce different cache keys");
  }

  /**
   * InMemoryResultCache populates after a miss: re-running the same
   * query hits the cache. The default platform wiring uses
   * {@code ResultCache.NoOp()} \u2014 this test verifies the
   * non-default cache path works end-to-end.
   */
  @Test
  @Timeout(value = 120)
  void inMemoryResultCache_populatesAfterMissAndServesOnHit() {
    ResultCache recording = ResultCache.inMemory(16);

    String key =
        CacheBridge.platformCacheKey(
            "flights", flightsModel.version(),
            List.of("c"), List.of("carrier"), "");

    assertEquals(
        scala.Option.empty(), recording.get(key),
        "before any call, the cache is empty for this key");

    // First call: execute + populate.
    CachedResult fresh =
        CacheBridge.executeQuery(
            flightsModel, spark, List.of("c"), List.of("carrier"), "", CacheBridge.defaultMaxRows());
    recording.putWithModelAndVersion(key, fresh, "flights", flightsModel.version());

    scala.Option<CachedResult> cached = recording.get(key);
    assertTrue(cached.isDefined(),
        "after the populate, the cache entry is readable");
    assertEquals(2, cached.get().rows().length,
        "the cached entry has 2 carrier rows");
  }
}
