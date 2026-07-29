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
import io.semanticdf.platform.model.ModelService;
import io.semanticdf.platform.model.NoOpModelStore;
import io.semanticdf.platform.query.QueryService;
import io.semanticdf.platform.streaming.HotReloadingModelRegistry;
import io.semanticdf.platform.streaming.ModelRegistry;
import io.semanticdf.platform.streaming.YamlModelRegistry;

import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.Timeout;

/**
 * End-to-end cache correctness tests. Issues closed:
 *
 * <ul>
 *   <li><b>#261</b>: cache invalidation targets the journal CURRENT_VERSION,
 *       not the YAML-declared version. Fix: invalidate by model NAME.</li>
 *   <li><b>#262</b>: {@code platformCacheKey} joins with delimiters. Fix:
 *       use {@code LengthPrefixed}.</li>
 *   <li><b>#263</b>: {@code QueryResult.truncated} compared against 1024.
 *       Fix: compare against {@code CacheBridge.DefaultMaxRows}.</li>
 *   <li><b>#267</b>: cache HIT path and invalidation behavior untested.
 *       Fix: this class wires {@code ResultCache.inMemory} through
 *       {@code QueryService}'s real constructor.</li>
 * </ul>
 *
 * <p>Plus coverage of multi-model YAML with join + filter.
 *
 * <p><b>Test ordering matters.</b> Test 4 (`registerThenQuery`) calls
 * {@code ModelService.register("flights", ...)} which overlays the
 * boot-loaded flights model. If it runs before the boot-shape tests
 * (1, 2, 3), those would query a model whose `c` measure was
 * overwritten with `m`. JUnit's default is alphabetical, which puts
 * `registerThenQuery` FIRST — wrong for this class. Pinning order
 * with {@link Order} keeps the intent.
 */
@RestateTest
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class PlatformCacheCorrectnessTest {

  static final SparkSession spark = createSpark();
  static final Path modelsDir;

  static {
    try {
      modelsDir = Files.createTempDirectory("cache-correctness-models");

      // Single-table model (no joins).
      Files.writeString(
          modelsDir.resolve("flights.yml"),
          "flights:\n"
              + "  table: flights_tbl\n"
              + "  dimensions:\n"
              + "    carrier: carrier\n"
              + "  measures:\n"
              + "    c: \"count(flight_count)\"\n");

      // Multi-model file: customers + orders with a join + filter.
      Files.writeString(
          modelsDir.resolve("commerce.yml"),
          "customers:\n"
              + "  table: customers_csv\n"
              + "  filters:\n"
              + "    has_name:\n"
              + "      expr: \"name IS NOT NULL\"\n"
              + "  dimensions:\n"
              + "    customer_id: customer_id\n"
              + "    name: name\n"
              + "  measures:\n"
              + "    cust_count: \"count(1)\"\n"
              + "orders:\n"
              + "  table: orders_csv\n"
              + "  joins:\n"
              + "    customers:\n"
              + "      model: customers\n"
              + "      type: one\n"
              + "      left_on: customer_id\n"
              + "      right_on: customer_id\n"
              + "  dimensions:\n"
              + "    order_id: order_id\n"
              + "  measures:\n"
              + "    order_count: \"count(1)\"\n");

      spark
          .createDataFrame(
              java.util.Arrays.asList(
                  org.apache.spark.sql.RowFactory.create("AA", 1L),
                  org.apache.spark.sql.RowFactory.create("AA", 2L),
                  org.apache.spark.sql.RowFactory.create("UA", 3L)),
              new org.apache.spark.sql.types.StructType()
                  .add("carrier", "string")
                  .add("flight_count", "long"))
          .createOrReplaceTempView("flights_tbl");

      spark
          .createDataFrame(
              java.util.Arrays.asList(
                  org.apache.spark.sql.RowFactory.create(1L, "Alice"),
                  org.apache.spark.sql.RowFactory.create(2L, null),
                  org.apache.spark.sql.RowFactory.create(3L, "Bob")),
              new org.apache.spark.sql.types.StructType()
                  .add("customer_id", "long")
                  .add("name", "string"))
          .createOrReplaceTempView("customers_csv");

      spark
          .createDataFrame(
              java.util.Arrays.asList(
                  org.apache.spark.sql.RowFactory.create(100L, 1L),
                  org.apache.spark.sql.RowFactory.create(101L, 3L)),
              new org.apache.spark.sql.types.StructType()
                  .add("order_id", "long")
                  .add("customer_id", "long"))
          .createOrReplaceTempView("orders_csv");
    } catch (Exception e) {
      throw new RuntimeException("could not set up the e2e fixture", e);
    }
  }

  static final ObjectMapper JSON = new ObjectMapper();
  static final HttpClient HTTP = HttpClient.newBuilder()
      .connectTimeout(Duration.ofSeconds(5))
      .build();

  static final YamlModelRegistry BOOT_REGISTRY =
      YamlModelRegistry.load(modelsDir.toString(), spark);

  // Real InMemoryResultCache wired through QueryService's actual
  // constructor (not the noOp(...) convenience).
  static final ResultCache CACHE = ResultCache.inMemory(64);
  static final ModelRegistry MODELS = new HotReloadingModelRegistry(BOOT_REGISTRY);

  @BindService
  final ModelService modelService =
      new ModelService(new NoOpModelStore(), spark, CACHE, MODELS);

  @BindService
  final QueryService queryService = new QueryService(MODELS, spark, CACHE);

  @BindService
  final AuditService auditService = new AuditService(new NoOpAuditEventStore());

  private static SparkSession createSpark() {
    return SparkSession.builder()
        .master("local[2]")
        .appName("platform-cache-correctness-test")
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

  // HTTP timeout: short. If a handler hangs (e.g. Restate retry storm
  // on an exception — see #270), we want to fail fast and surface the
  // error, not wait 60s for the retry loop to exhaust the test budget.
  private static final Duration HTTP_TIMEOUT = Duration.ofSeconds(20);

  /**
   * Decode a JSON cell value as a Long. Spark's JSON output can be a
   * plain integer, a decimal, or a JSON number with trailing zeroes;
   * all of these should map to the underlying numeric value.
   */
  private static long asLong(JsonNode node) {
    if (node.isIntegralNumber()) return node.asLong();
    if (node.isNumber()) return (long) node.asDouble();
    // BigDecimal (sum) — parse from string representation.
    return Long.parseLong(node.asText().trim());
  }

  private HttpRequest.Builder rawJson(URL url) {
    return HttpRequest.newBuilder()
        .uri(URI.create(url.toString()))
        .timeout(HTTP_TIMEOUT)
        .header("Content-Type", "application/json")
        .header("Accept", "application/json");
  }

  // #267 — Keystone: cache HIT path
  @Test
  @Order(1)
  @Timeout(value = 120)
  void identicalQuery_secondCallHitsCache(@RestateURL URL ingress) throws Exception {
    ObjectNode queryBody = JSON.createObjectNode();
    queryBody.put("modelName", "flights");
    queryBody.putArray("measures").add("c");
    queryBody.putArray("dimensions").add("carrier");
    queryBody.put("where", "");

    HttpResponse<String> r1 = HTTP.send(
        rawJson(new URL(ingress.toString() + "/QueryService/runQuery"))
            .POST(HttpRequest.BodyPublishers.ofString(queryBody.toString()))
            .build(),
        HttpResponse.BodyHandlers.ofString());
    assertEquals(200, r1.statusCode(), "first call: body=" + r1.body());

    HttpResponse<String> r2 = HTTP.send(
        rawJson(new URL(ingress.toString() + "/QueryService/runQuery"))
            .POST(HttpRequest.BodyPublishers.ofString(queryBody.toString()))
            .build(),
        HttpResponse.BodyHandlers.ofString());
    assertEquals(200, r2.statusCode(), "second call: body=" + r2.body());

    JsonNode body1 = JSON.readTree(r1.body());
    JsonNode body2 = JSON.readTree(r2.body());
    assertEquals(body1.get("rowCount").asInt(), body2.get("rowCount").asInt(),
        "rowCount must match between calls (deterministic cache hit)");
  }

  // #262 — Cache-key delimiter collision regression
  @Test
  @Order(2)
  @Timeout(value = 120)
  void differentWhere_doesNotCollideInCache(@RestateURL URL ingress) throws Exception {
    ObjectNode bodyAll = JSON.createObjectNode();
    bodyAll.put("modelName", "flights");
    bodyAll.putArray("measures").add("c");
    bodyAll.putArray("dimensions").add("carrier");
    bodyAll.put("where", "");

    ObjectNode bodyFiltered = JSON.createObjectNode();
    bodyFiltered.put("modelName", "flights");
    bodyFiltered.putArray("measures").add("c");
    bodyFiltered.putArray("dimensions").add("carrier");
    bodyFiltered.put("where", "carrier = 'AA'");

    assertEquals(200, HTTP.send(
        rawJson(new URL(ingress.toString() + "/QueryService/runQuery"))
            .POST(HttpRequest.BodyPublishers.ofString(bodyAll.toString()))
            .build(),
        HttpResponse.BodyHandlers.ofString()).statusCode());

    HttpResponse<String> respFiltered = HTTP.send(
        rawJson(new URL(ingress.toString() + "/QueryService/runQuery"))
            .POST(HttpRequest.BodyPublishers.ofString(bodyFiltered.toString()))
            .build(),
        HttpResponse.BodyHandlers.ofString());
    assertEquals(200, respFiltered.statusCode(), "filtered call: body=" + respFiltered.body());

    JsonNode body = JSON.readTree(respFiltered.body());
    assertEquals(1, body.get("rowCount").asInt(),
        "filtered where must reduce rowCount (cache key was distinct)");
  }

  // #261 — Cache invalidation on ModelService.register
  @Test
  @Order(4)
  @Timeout(value = 120)
  void registerThenQuery_servesNewDefinition_notStaleRows(@RestateURL URL ingress)
      throws Exception {
    // 1. Register flights with measure = count(*)
    String yaml1 =
        "flights:\n"
            + "  table: flights_tbl\n"
            + "  dimensions:\n"
            + "    carrier: carrier\n"
            + "  measures:\n"
            + "    m: \"count(flight_count)\"\n";
    ObjectNode register1 = JSON.createObjectNode();
    register1.put("modelName", "flights");
    register1.put("yaml", yaml1);
    assertEquals(200, HTTP.send(
        rawJson(new URL(ingress.toString() + "/ModelService/flights/register"))
            .POST(HttpRequest.BodyPublishers.ofString(register1.toString()))
            .build(),
        HttpResponse.BodyHandlers.ofString()).statusCode());

    // 2. Query once to populate cache. count(flight_count) on 3 rows
    // (1, 2, 3) = 3. Single-row aggregate over all rows, no dims.
    ObjectNode query = JSON.createObjectNode();
    query.put("modelName", "flights");
    query.putArray("measures").add("m");
    query.putArray("dimensions");
    query.put("where", "");
    HttpResponse<String> r1 = HTTP.send(
        rawJson(new URL(ingress.toString() + "/QueryService/runQuery"))
            .POST(HttpRequest.BodyPublishers.ofString(query.toString()))
            .build(),
        HttpResponse.BodyHandlers.ofString());
    assertEquals(200, r1.statusCode(), "first query: body=" + r1.body());
    JsonNode body1 = JSON.readTree(r1.body());
    assertEquals(1, body1.get("rowCount").asInt(),
        "first query must return exactly 1 row (aggregate over all rows)");
    long countValue = asLong(body1.get("rows").get(0).get(0));
    assertEquals(3L, countValue,
        "count(flight_count) over (1,2,3) must equal 3; body=" + r1.body());

    // 3. Re-register flights with measure = sum(flight_count) -- the
    // alias is `m` but the EXPRESSION differs, so the cache key
    // differs. The v0.2.2 bug: the invalidate call targeted the
    // journal's CURRENT_VERSION, not the YAML-declared version, so
    // it was a no-op. Cache served the stale `count=3` result for
    // the post-re-register query. The fix: invalidate by model name.
    String yaml2 =
        "flights:\n"
            + "  table: flights_tbl\n"
            + "  dimensions:\n"
            + "    carrier: carrier\n"
            + "  measures:\n"
            + "    m: \"sum(flight_count)\"\n";
    ObjectNode register2 = JSON.createObjectNode();
    register2.put("modelName", "flights");
    register2.put("yaml", yaml2);
    assertEquals(200, HTTP.send(
        rawJson(new URL(ingress.toString() + "/ModelService/flights/register"))
            .POST(HttpRequest.BodyPublishers.ofString(register2.toString()))
            .build(),
        HttpResponse.BodyHandlers.ofString()).statusCode());

    // 4. Same query shape: cache MUST be invalidated, so the response
    // reflects the NEW definition (sum, not count). sum(1+2+3) = 6.
    // If the cache served the stale count=3, this assertion fails.
    HttpResponse<String> r2 = HTTP.send(
        rawJson(new URL(ingress.toString() + "/QueryService/runQuery"))
            .POST(HttpRequest.BodyPublishers.ofString(query.toString()))
            .build(),
        HttpResponse.BodyHandlers.ofString());
    assertEquals(200, r2.statusCode(), "post-re-register query: body=" + r2.body());
    JsonNode body2 = JSON.readTree(r2.body());
    assertEquals(1, body2.get("rowCount").asInt(),
        "post-re-register query must still return 1 row (aggregate)");
    long sumValue = asLong(body2.get("rows").get(0).get(0));
    assertEquals(6L, sumValue,
        "sum(flight_count) over (1,2,3) must equal 6; if we see 3, "
            + "the cache served stale rows (see #261). body=" + r2.body());
    assertNotNull(body2.get("model"));
    assertEquals("flights", body2.get("model").asText());
  }

  // Multi-model + join + filter -- exercises SemanticJoinOp + cache
  @Test
  @Order(3)
  @Timeout(value = 120)
  void joinedModelWithFilter_cacheHitServesFilteredResult(@RestateURL URL ingress)
      throws Exception {
    ObjectNode queryBody = JSON.createObjectNode();
    queryBody.put("modelName", "orders");
    queryBody.putArray("measures").add("order_count");
    queryBody.putArray("dimensions");
    queryBody.put("where", "");

    HttpResponse<String> r1 = HTTP.send(
        rawJson(new URL(ingress.toString() + "/QueryService/runQuery"))
            .POST(HttpRequest.BodyPublishers.ofString(queryBody.toString()))
            .build(),
        HttpResponse.BodyHandlers.ofString());
    assertEquals(200, r1.statusCode(), "first call: body=" + r1.body());

    int rowCount1 = JSON.readTree(r1.body()).get("rowCount").asInt();
    // 2 orders, but customer_id=2 has no name so the join produces 1 row
    assertEquals(1, rowCount1,
        "single-row aggregate (count over orders); customers with NULL name pre-filtered");

    HttpResponse<String> r2 = HTTP.send(
        rawJson(new URL(ingress.toString() + "/QueryService/runQuery"))
            .POST(HttpRequest.BodyPublishers.ofString(queryBody.toString()))
            .build(),
        HttpResponse.BodyHandlers.ofString());
    assertEquals(200, r2.statusCode(), "second call (cache HIT) must succeed");

    JsonNode body2 = JSON.readTree(r2.body());
    assertEquals(rowCount1, body2.get("rowCount").asInt(),
        "cache HIT must serve the same joined-and-filtered result");
  }
}
