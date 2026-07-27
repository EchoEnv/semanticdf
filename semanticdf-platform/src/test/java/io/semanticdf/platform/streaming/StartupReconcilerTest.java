package io.semanticdf.platform.streaming;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpExchange;
import java.time.Duration;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test for {@link StartupReconciler} using a tiny in-memory HTTP
 * server (Java's built-in {@code com.sun.net.httpserver.HttpServer})
 * standing in for the platform's local Restate ingress.
 *
 * <p>We don't need Testcontainers + a real Restate server for this
 * test — we just need to confirm that the reconciler correctly
 * issues HTTP POSTs with the right shape (URL, body) and handles
 * the responses correctly (skip on terminal status, fail on
 * errors).
 */
class StartupReconcilerTest {

  /** Generous timeout for tests — the mocked server responds in <1ms. */
  private static final java.time.Duration HTTP_TIMEOUT = Duration.ofSeconds(30);

  private HttpServer httpServer;
  private int port;
  private InMemoryCatalog catalog;
  /** Captures every {@code /send} invocation by stream-id. */
  private final Map<String, List<String>> sentBodiesByStreamId =
      new ConcurrentHashMap<>();
  /** Captures every {@code /getStatus} call. */
  private final Map<String, List<String>> statusCallsByStreamId =
      new ConcurrentHashMap<>();
  /** Configurable status response per stream-id ("running" by default). */
  private final Map<String, String> statusByStreamId = new ConcurrentHashMap<>();
  /** Captures raw request URIs (URL-encoded form, not decoded). */
  private final java.util.List<String> rawRequestUris =
      java.util.Collections.synchronizedList(new java.util.ArrayList<>());

  @BeforeEach
  void setUp() throws IOException {
    catalog = new InMemoryCatalog();

    httpServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    httpServer.createContext(
        "/StreamingService",
        exchange -> {
          // Capture the raw path BEFORE routing — used by tests
          // that want to verify URL encoding was applied (the
          // path-based dispatcher below can't decode %2F for
          // matching against the test catalog keys).
          rawRequestUris.add(exchange.getRequestURI().getRawPath());
          String[] parts = exchange.getRequestURI().getPath().split("/");
          // /StreamingService/{streamId}/{handler}/{method}
          // /StreamingService/s1/run/send
          // /StreamingService/s1/getStatus/send
          if (parts.length < 5) {
            exchange.sendResponseHeaders(404, -1);
            exchange.close();
            return;
          }
          String streamId = parts[2];
          String handler = parts[3];
          String method = parts[4];
          byte[] body = exchange.getRequestBody().readAllBytes();
          String bodyStr = new String(body);

          switch (handler) {
            case "run":
              if (!"send".equals(method)) {
                exchange.sendResponseHeaders(404, -1);
                exchange.close();
                return;
              }
              sentBodiesByStreamId
                  .computeIfAbsent(streamId, k -> new ArrayList<>())
                  .add(bodyStr);
              exchange.sendResponseHeaders(200, 0);
              exchange.close();
              break;
            case "getStatus":
              // /StreamingService/{key}/getStatus/send — same pattern
              statusCallsByStreamId
                  .computeIfAbsent(streamId, k -> new ArrayList<>())
                  .add(bodyStr);
              byte[] respBody =
                  statusByStreamId
                      .getOrDefault(streamId, "running")
                      .getBytes();
              exchange.sendResponseHeaders(200, respBody.length);
              try (OutputStream os = exchange.getResponseBody()) {
                os.write(respBody);
              }
              exchange.close();
              break;
            default:
              exchange.sendResponseHeaders(404, -1);
              exchange.close();
          }
        });
    httpServer.start();
    port = httpServer.getAddress().getPort();
  }

  @AfterEach
  void tearDown() {
    if (httpServer != null) {
      httpServer.stop(0);
    }
  }

  @Test
  void run_invokeRunOnRunningStream() {
    catalog.register("s1", "m", "q", "/ckpt");
    statusByStreamId.put("s1", "running");

    StartupReconciler.Summary sum =
        new StartupReconciler(
                catalog, URI.create("http://127.0.0.1:" + port), HttpClient.newHttpClient(), HTTP_TIMEOUT)
            .run();

    assertEquals(1, sum.total());
    assertEquals(1, sum.actedOn());
    assertEquals(0, sum.failed());
    assertEquals(1, sentBodiesByStreamId.getOrDefault("s1", List.of()).size());
  }

  @Test
  void run_skipsStoppedStreams() {
    catalog.register("s1", "m", "q", "/ckpt");
    statusByStreamId.put("s1", "stopped");

    StartupReconciler.Summary sum =
        new StartupReconciler(
                catalog, URI.create("http://127.0.0.1:" + port), HttpClient.newHttpClient(), HTTP_TIMEOUT)
            .run();

    assertEquals(1, sum.actedOn(), "skipped counts as acted");
    assertEquals(0, sum.failed());
    assertTrue(sentBodiesByStreamId.getOrDefault("s1", List.of()).isEmpty());
  }

  @Test
  void run_skipsFailedStreams() {
    catalog.register("s1", "m", "q", "/ckpt");
    statusByStreamId.put("s1", "failed");

    StartupReconciler.Summary sum =
        new StartupReconciler(
                catalog, URI.create("http://127.0.0.1:" + port), HttpClient.newHttpClient(), HTTP_TIMEOUT)
            .run();

    assertEquals(1, sum.actedOn());
    assertTrue(sentBodiesByStreamId.getOrDefault("s1", List.of()).isEmpty());
  }

  @Test
  void run_skipsBlockedStreams() {
    catalog.register("s1", "m", "q", "/ckpt");
    statusByStreamId.put("s1", "failed-restart");

    StartupReconciler.Summary sum =
        new StartupReconciler(
                catalog, URI.create("http://127.0.0.1:" + port), HttpClient.newHttpClient(), HTTP_TIMEOUT)
            .run();

    assertEquals(1, sum.actedOn());
    assertTrue(sentBodiesByStreamId.getOrDefault("s1", List.of()).isEmpty());
  }

  @Test
  void run_emptyCatalog() {
    StartupReconciler.Summary sum =
        new StartupReconciler(
                catalog, URI.create("http://127.0.0.1:" + port), HttpClient.newHttpClient(), HTTP_TIMEOUT)
            .run();

    assertEquals(0, sum.total());
    assertEquals(0, sum.actedOn());
    assertEquals(0, sum.failed());
  }

  @Test
  void run_sendBodyContainsMetadata() {
    catalog.register("s1", "orders-model", "sum(amount)", "/ckpt/orders");
    statusByStreamId.put("s1", "running");

    new StartupReconciler(
            catalog, URI.create("http://127.0.0.1:" + port), HttpClient.newHttpClient(), HTTP_TIMEOUT)
        .run();

    // The mock's path-based routing treats 'tenant-prod%2Forders-stream'

    String body = sentBodiesByStreamId.get("s1").get(0);
    assertTrue(body.contains("orders-model"));
    assertTrue(body.contains("sum(amount)"));
    assertTrue(body.contains("/ckpt/orders"));
  }

  @Test
  void run_urlEncodesStreamIdContainingSpecialChars() throws Exception {
    // PR #236 (reclassified URL safety): stream-id may contain URL-
    // unsafe characters like '/'. Without encoding, the ingress
    // path would route to a different workflow key (silent cross-
    // stream corruption) or 404. This test pins the URL-encoding
    // contract — the URL hit the server with the encoded form, so
    // the upstream routing can rely on stream-id decoding rules.
    String trickyStreamId = "tenant-prod/orders-stream";
    catalog.register(trickyStreamId, "m", "q", "/ckpt");
    statusByStreamId.put(trickyStreamId, "running");

    rawRequestUris.clear();

    new StartupReconciler(
            catalog, URI.create("http://127.0.0.1:" + port), HttpClient.newHttpClient(), HTTP_TIMEOUT)
        .run();

    // The mock's path-based routing treats the encoded stream-id as
    // one segment (it can't decode %2F for status lookup, so it
    // reports 'unknown' and the sweep skips). What matters is the
    // RAW request URI — verify the URL was constructed with %2F
    // encoding. Raw '/' in stream-id would route to wrong key in
    // production (silent cross-stream contamination: catalog says
    // reconcile stream A, but ingress invokes run() on stream B).
    boolean sawEncoded = false;
    boolean sawUnencodedRawId = false;
    for (String rawPath : rawRequestUris) {
      if (rawPath.contains("%2F")) sawEncoded = true;
      // Look for the raw stream-id literal "tenant-prod/orders-stream"
      // appearing in the URL — that's the bad case (un-encoded slash).
      if (rawPath.contains("tenant-prod/orders-stream")) {
        sawUnencodedRawId = true;
      }
    }
    assertTrue(sawEncoded,
        "URL must use %2F for stream-id containing '/' (prevents cross-stream contamination)");
    assertFalse(sawUnencodedRawId,
        "raw '/' in stream-id path would route to wrong key in production");
  }

  /**
   * In-memory catalog impl for tests. Records all 4 fields per
   * stream-id so the sweep can reconstruct request bodies.
   */
  static final class InMemoryCatalog implements StreamCatalog {
    final Map<String, StreamMetadata> data = new ConcurrentHashMap<>();

    void register(String streamId, String modelName, String q, String ckpt) {
      data.put(streamId, new StreamMetadata(streamId, modelName, q, ckpt));
    }

    @Override
    public void registerIfAbsent(
        String streamId, String modelName, String queryShape, String checkpointLocation) {
      data.putIfAbsent(streamId, new StreamMetadata(streamId, modelName, queryShape, checkpointLocation));
    }

    @Override
    public List<StreamMetadata> findAll() {
      return new ArrayList<>(data.values());
    }

    @Override
    public void close() {}
  }
}
