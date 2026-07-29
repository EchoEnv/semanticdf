package io.semanticdf.platform;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
 * Always-replay / crash-replay integration test for the journal
 * path. Issues closed:
 *
 * <ul>
 *   <li><b>#266</b>: Zero v0.2.2 platform tests use
 *       {@code RESTATE_WORKER__INVOKER__INACTIVITY_TIMEOUT=0m}
 *       (the always-replay gate). The Restate skill mandates:
 *       <i>"Any change to handler business logic must be covered
 *       by a Testcontainers test with always-replay enabled, and
 *       that test must pass before declaring the work done."</i>
 *       PRs #248, #250, #252–#255, #257 all rewrote journal
 *       payloads or decode arms. None of that was replay-tested.
 *       A non-determinism bug in any of these passes the v0.2.2
 *       suite and only fails on a production retry.
 * </ul>
 *
 * <p>This class wires the Restate TestKit container with
 * {@code RESTATE_WORKER__INVOKER__INACTIVITY_TIMEOUT=0m}, which
 * forces every handler invocation to be journal-replayed
 * end-to-end (no "skip on same-JVM restart" shortcut). All
 * platform-side journal paths must survive this.
 *
 * <p>The keystone scenarios:
 * <ol>
 *   <li>Register + query → identical results on repeated calls
 *       (cache HIT path, journaled replay returns the cached
 *       RestateCachedRow).</li>
 *   <li>Re-register → query → returns NEW data (cache invalidation
 *       survives replay; the HotReloadingModelRegistry overlay
 *       mutation works under replay).</li>
 *   <li>Many sequential identical queries (idempotency check:
 *       no side-effect leakage into the journal).</li>
 * </ol>
 *
 * <p>Trade-off: always-replay costs 2-5x more journal entries
 * per test run (every invocation replays). At P1's QPS this is
 * acceptable; this is a correctness gate, not a load test.
 */
@RestateTest(
    environment = {
        "RESTATE_WORKER__INVOKER__INACTIVITY_TIMEOUT=0m",
        "RESTATE_WORKER__INVOKER__INACTIVITY_TIMER_INTERVAL=0m"
    })
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class PlatformAlwaysReplayTest {

  static final SparkSession spark = createSpark();
  static final Path modelsDir;

  static {
    try {
      modelsDir = Files.createTempDirectory("always-replay-models");

      Files.writeString(
          modelsDir.resolve("flights.yml"),
          "flights:\n"
              + "  table: flights_tbl\n"
              + "  dimensions:\n"
              + "    carrier: carrier\n"
              + "  measures:\n"
              + "    m: \"count(flight_count)\"\n");

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
    } catch (Exception e) {
      throw new RuntimeException("could not set up the fixture", e);
    }
  }

  static final ObjectMapper JSON = new ObjectMapper();
  static final HttpClient HTTP = HttpClient.newBuilder()
      .connectTimeout(Duration.ofSeconds(5))
      .build();

  static final ResultCache CACHE = ResultCache.inMemory(64);
  static final YamlModelRegistry BOOT_REGISTRY =
      YamlModelRegistry.load(modelsDir.toString(), spark);
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
        .appName("platform-always-replay-test")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate();
  }

  @AfterAll
  static void tearDownAll() {
    if (spark != null) spark.stop();
  }

  private HttpRequest.Builder rawJson(URL url) {
    return HttpRequest.newBuilder()
        .uri(URI.create(url.toString()))
        .timeout(Duration.ofSeconds(30))
        .header("Content-Type", "application/json")
        .header("Accept", "application/json");
  }

  // 1) Keystone: register + query + re-query. Each invocation must
  //    succeed under always-replay (the journal is the source of
  //    truth, not the in-memory cache).
  @Test
  @Order(1)
  @Timeout(value = 120)
  void registerThenQuery_isReplayedIdentically(@RestateURL URL ingress) throws Exception {
    // Register flights with measure m = count(flight_count)
    String yaml =
        "flights:\n"
            + "  table: flights_tbl\n"
            + "  dimensions:\n"
            + "    carrier: carrier\n"
            + "  measures:\n"
            + "    m: \"count(flight_count)\"\n";
    ObjectNode register = JSON.createObjectNode();
    register.put("modelName", "flights");
    register.put("yaml", yaml);
    assertEquals(200, HTTP.send(
        rawJson(new URL(ingress.toString() + "/ModelService/flights/register"))
            .POST(HttpRequest.BodyPublishers.ofString(register.toString()))
            .build(),
        HttpResponse.BodyHandlers.ofString()).statusCode());

    // Query once
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
    assertEquals(200, r1.statusCode(), "first query: " + r1.body());

    // Query again — under always-replay, this triggers the journal
    // replay path. Must return identical results.
    HttpResponse<String> r2 = HTTP.send(
        rawJson(new URL(ingress.toString() + "/QueryService/runQuery"))
            .POST(HttpRequest.BodyPublishers.ofString(query.toString()))
            .build(),
        HttpResponse.BodyHandlers.ofString());
    assertEquals(200, r2.statusCode(), "replayed query: " + r2.body());

    JsonNode b1 = JSON.readTree(r1.body());
    JsonNode b2 = JSON.readTree(r2.body());
    assertEquals(b1.get("rowCount").asInt(), b2.get("rowCount").asInt(),
        "rowCount must match across replays");
    // row[0][0] = the count value (3 for count(flight_count))
    long v1 = b1.get("rows").get(0).get(0).asLong();
    long v2 = b2.get("rows").get(0).get(0).asLong();
    assertEquals(v1, v2, "value must match across replays");
    assertEquals(3L, v1, "count(flight_count) over (1,2,3) = 3");
  }

  // 2) Re-register with a different measure expression. Under
  //    always-replay, the journaled register handler is replayed on
  //    each invocation; the cache invalidation must still fire.
  @Test
  @Order(2)
  @Timeout(value = 120)
  void reRegister_invalidatesAndQuery_runsFresh(@RestateURL URL ingress) throws Exception {
    // Re-register flights with m = sum(flight_count). The cache
    // invalidation must propagate (PR #261).
    String yaml =
        "flights:\n"
            + "  table: flights_tbl\n"
            + "  dimensions:\n"
            + "    carrier: carrier\n"
            + "  measures:\n"
            + "    m: \"sum(flight_count)\"\n";
    ObjectNode register = JSON.createObjectNode();
    register.put("modelName", "flights");
    register.put("yaml", yaml);
    assertEquals(200, HTTP.send(
        rawJson(new URL(ingress.toString() + "/ModelService/flights/register"))
            .POST(HttpRequest.BodyPublishers.ofString(register.toString()))
            .build(),
        HttpResponse.BodyHandlers.ofString()).statusCode());

    // Query: post-invalidation, the result must reflect the new
    // definition (sum instead of count).
    ObjectNode query = JSON.createObjectNode();
    query.put("modelName", "flights");
    query.putArray("measures").add("m");
    query.putArray("dimensions");
    query.put("where", "");
    HttpResponse<String> r = HTTP.send(
        rawJson(new URL(ingress.toString() + "/QueryService/runQuery"))
            .POST(HttpRequest.BodyPublishers.ofString(query.toString()))
            .build(),
        HttpResponse.BodyHandlers.ofString());
    assertEquals(200, r.statusCode(), "post-re-register query: " + r.body());
    JsonNode body = JSON.readTree(r.body());
    assertEquals(1, body.get("rowCount").asInt(), "single-row aggregate");
    long v = body.get("rows").get(0).get(0).asLong();
    assertEquals(6L, v,
        "sum(flight_count) over (1,2,3) = 6; if 3, the cache "
            + "invalidation didn't fire under always-replay");
  }

  // 3) Idempotency: many sequential identical queries. Each
  //    invocation is replayed; the journal state must not
  //    accumulate side-effects that change the answer.
  @Test
  @Order(3)
  @Timeout(value = 180)
  void repeatedQuery_isIdempotent_acrossManyInvocations(@RestateURL URL ingress) throws Exception {
    ObjectNode query = JSON.createObjectNode();
    query.put("modelName", "flights");
    query.putArray("measures").add("m");
    query.putArray("dimensions");
    query.put("where", "");

    long firstValue = -1;
    for (int i = 0; i < 10; i++) {
      HttpResponse<String> r = HTTP.send(
          rawJson(new URL(ingress.toString() + "/QueryService/runQuery"))
              .POST(HttpRequest.BodyPublishers.ofString(query.toString()))
              .build(),
          HttpResponse.BodyHandlers.ofString());
      assertEquals(200, r.statusCode(), "invocation " + i + ": " + r.body());
      JsonNode body = JSON.readTree(r.body());
      assertEquals(1, body.get("rowCount").asInt(),
          "invocation " + i + " rowCount drift");
      long v = body.get("rows").get(0).get(0).asLong();
      if (i == 0) {
        firstValue = v;
      } else {
        assertTrue(firstValue == v,
            "invocation " + i + " drifted: first=" + firstValue + " this=" + v);
      }
    }
  }
}