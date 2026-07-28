package io.semanticdf.platform;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import dev.restate.sdk.testing.BindService;
import dev.restate.sdk.testing.RestateTest;
import dev.restate.sdk.testing.RestateURL;

import io.semanticdf.cache.ResultCache;
import io.semanticdf.platform.audit.AuditService;
import io.semanticdf.platform.audit.NoOpAuditEventStore;
import io.semanticdf.platform.catalog.CatalogService;
import io.semanticdf.platform.model.ModelService;
import io.semanticdf.platform.model.NoOpModelStore;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;

import org.apache.spark.sql.SparkSession;

/**
 * End-to-end smoke covering services whose handler signatures don't
 * journal a {@code Row[]} (i.e. doesn't ship Spark rows through
 * Restate's journal). Covers {@link AuditService} (small records)
 * and {@link CatalogService} (small records) at the Restate ingress.
 *
 * <p>The corresponding smoke for {@code QueryService.runQuery} lives in
 * {@code QueryServiceEndToEndTest} (in the {@code query} sub-package).
 * That class invokes {@code QueryService.runQuery} directly with a
 * real Spark + real {@code YamlModelRegistry}, sidestepping the
 * Restate journal path that would otherwise need to serialize
 * thousands of {@code Row} objects. Both halves together cover the
 * full v0.2.2 surface.
 */
@RestateTest
class PlatformEndToEndTest {

  // Spark field placed BEFORE any @BindService field that references it
  // (Java initializes static fields top-down). RestateTest's class-load
  // timing forces us to use a static field rather than @BeforeAll.
  static final SparkSession spark = createSpark();

  static final ObjectMapper JSON = new ObjectMapper();
  static final HttpClient HTTP = HttpClient.newBuilder()
      .connectTimeout(Duration.ofSeconds(5))
      .build();

  @BindService
  final ModelService modelService =
      new ModelService(new NoOpModelStore(), spark, ResultCache.NoOp());

  @BindService
  final CatalogService catalogService =
      new CatalogService(new NoOpModelStore());

  @BindService
  final AuditService auditService =
      new AuditService(new NoOpAuditEventStore());

  private static SparkSession createSpark() {
    return SparkSession.builder()
        .master("local[2]")
        .appName("platform-end-to-end-test")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate();
  }

  // ----------------------------------------------------------------------
  // Restate ingress: AuditService
  // ----------------------------------------------------------------------

  @org.junit.jupiter.api.Test
  @org.junit.jupiter.api.Timeout(value = 60)
  void auditService_append_isReplaySafeAcrossTwoAppends(
      @RestateURL java.net.URL ingress) throws Exception {
    String hash = "e2e-hash-" + System.nanoTime();
    String firstEmitBody = JSON.writeValueAsString(
        Map.of("tenant", "tenant-A",
               "eventType", "platform.e2e",
               "timestamp", System.currentTimeMillis(),
               "dedupHash", hash,
               "payload", "{\"smoke\":\"first\"}"));
    String secondEmitBody = JSON.writeValueAsString(
        Map.of("tenant", "tenant-A",
               "eventType", "platform.e2e",
               "timestamp", System.currentTimeMillis(),
               "dedupHash", hash,
               "payload", "{\"smoke\":\"second\"}"));

    HttpRequest first =
        HttpRequest.newBuilder()
            .uri(ingress.toURI().resolve("/AuditService/tenant-A/append"))
            .timeout(Duration.ofSeconds(20))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(firstEmitBody))
            .build();
    HttpRequest second =
        HttpRequest.newBuilder()
            .uri(ingress.toURI().resolve("/AuditService/tenant-A/append"))
            .timeout(Duration.ofSeconds(20))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(secondEmitBody))
            .build();

    assertEquals(200, HTTP.send(first, HttpResponse.BodyHandlers.ofString()).statusCode());
    assertEquals(200, HTTP.send(second, HttpResponse.BodyHandlers.ofString()).statusCode());

    // GET requests in Restate must be body-less with no content-type.
    HttpRequest getOffset =
        HttpRequest.newBuilder()
            .uri(ingress.toURI().resolve("/AuditService/tenant-A/getLastWriteOffset"))
            .timeout(Duration.ofSeconds(20))
            .GET()
            .build();
    HttpResponse<String> offsetResp =
        HTTP.send(getOffset, HttpResponse.BodyHandlers.ofString());
    assertEquals(200, offsetResp.statusCode(),
        "getLastWriteOffset: body=" + offsetResp.body());
    long offset = JSON.readTree(offsetResp.body()).asLong();
    assertEquals(1L, offset,
        "duplicate-hash appends are short-circuited; offset advances only on first emit");
  }

  // ----------------------------------------------------------------------
  // Restate ingress: CatalogService
  // ----------------------------------------------------------------------

  @org.junit.jupiter.api.Test
  @org.junit.jupiter.api.Timeout(value = 60)
  void catalogService_describeModel_unknownReturnsNull(
      @RestateURL java.net.URL ingress) throws Exception {
    String body = JSON.writeValueAsString(Map.of("modelName", "nonexistent"));
    HttpRequest req =
        HttpRequest.newBuilder()
            .uri(ingress.toURI().resolve("/CatalogService/describeModel"))
            .timeout(Duration.ofSeconds(20))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build();
    HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());

    assertEquals(200, resp.statusCode(),
        "describeModel: body=" + resp.body());
    JsonNode tree = JSON.readTree(resp.body());
    assertTrue(tree.isNull(),
        "unknown model \u2192 describeModel returns JSON null; got: " + resp.body());
  }
}
