package io.semanticdf.platform.streaming;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Bulk-startup reconciliation (DE-H2 from PR #232 senior review).
 *
 * <p>After a platform JVM death, the in-JVM
 * {@link StreamingQueryHandleRegistry} is empty. Restate's journal
 * (in Postgres) survives, so workflow state is preserved — but
 * each {@code run} workflow whose Spark query was lost has no live
 * backing process. Reconciling requires a new invocation to
 * {@code run()} so its auto-detect branch ({@code
 * StreamingService.run} step 5a) can fire.
 *
 * <p>The reconciler reads the catalog for previously-registered
 * streams and POSTs an invocation to the platform's local
 * Restate ingress for each. Each invocation is fire-and-forget at
 * the HTTP layer (we don't read the response — we don't care
 * about its result; we just want the workflow to be entered so
 * reconciliation can happen asynchronously).
 *
 * <p><b>Design notes (PR #234 review):</b>
 * <ul>
 *   <li><b>Raw HTTP, no new dep</b> (Architect-H3). The Restate
 *       ingress accepts {@code POST /StreamingService/{key}/send}
 *       with a JSON body containing the {@code run} handler's
 *       argument. We use Java's built-in {@link HttpClient}.
 *   <li><b>P1 single-replica</b>. Multi-replica will need a
 *       distributed lease in Postgres (DE-C1, deferred to P3).
 *   <li><b>Bounded parallelism</b>. The submissions run on a small
 *       executor — same pattern as the drain timeout (PR #228).
 *   <li><b>Best-effort.</b> A failure to submit one stream doesn't
 *       stop the sweep; we log and continue. Failed streams
 *       remain in the catalog; operators see them via the summary
 *       log line and can manually invoke {@code /restart}.
 *   <li><b>Skip terminal-status streams via {@code getStatus()}.</b>
 *       Before invoking {@code run()}, query the workflow's status.
 *       Skip if status is {@code stopped} or {@code failed} or
 *       {@code failed-restart} — these are operator-terminated or
 *       operator-blocked states we must not resurrect. This avoids
 *       the catalog-says-running-but-journal-says-stopped drift
 *       hazard.
 * </ul>
 *
 * <p>Visible-for-testing — package-private ctor so unit tests can
 * inject a recording HTTP backend.
 */
public final class StartupReconciler {

  private final StreamCatalog catalog;
  private final URI localIngress;
  private final HttpClient httpClient;
  private final Duration totalTimeout;

  /**
   * Production constructor — reads {@code SEMANTICDF_RECONCILE_TIMEOUT_MS}
   * (default {@value #DEFAULT_TOTAL_TIMEOUT_MS}ms). For tests, prefer the
   * 4-arg constructor that takes an explicit duration.
   */
  public StartupReconciler(StreamCatalog catalog, URI localIngress) {
    this(
        catalog,
        localIngress,
        HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build(),
        Duration.ofMillis(resolveTotalTimeoutMs()));
  }

  /** Test constructor — visible-for-testing. */
  StartupReconciler(
      StreamCatalog catalog,
      URI localIngress,
      HttpClient httpClient,
      Duration totalTimeout) {
    this.catalog = catalog;
    this.localIngress = localIngress;
    this.httpClient = httpClient;
    this.totalTimeout = totalTimeout;
  }

  /**
   * Default upper bound on the entire sweep. With 8 parallel workers and
   * per-request timeout 5s, ~125 streams fit in 30s; for more streams,
   * raise via env var {@code SEMANTICDF_RECONCILE_TIMEOUT_MS}.
   */
  static final long DEFAULT_TOTAL_TIMEOUT_MS = 30_000L;

  private static long resolveTotalTimeoutMs() {
    String raw = System.getenv("SEMANTICDF_RECONCILE_TIMEOUT_MS");
    if (raw == null || raw.isBlank()) {
      return DEFAULT_TOTAL_TIMEOUT_MS;
    }
    try {
      long v = Long.parseLong(raw.trim());
      return v > 0 ? v : DEFAULT_TOTAL_TIMEOUT_MS;
    } catch (NumberFormatException e) {
      System.err.println(
          "semanticdf-platform: invalid SEMANTICDF_RECONCILE_TIMEOUT_MS='"
              + raw
              + "', using default "
              + DEFAULT_TOTAL_TIMEOUT_MS
              + "ms");
      return DEFAULT_TOTAL_TIMEOUT_MS;
    }
  }

  /**
   * Sweep the catalog and submit a {@code run} invocation for
   * each registered stream-id. Best-effort: errors are logged but
   * don't fail startup.
   *
   * @return a summary of how many streams were invoked, skipped,
   *         and failed
   */
  public Summary run() {
    List<StreamCatalog.StreamMetadata> streams = catalog.findAll();
    int total = streams.size();
    if (total == 0) {
      System.out.println(
          "semanticdf-platform: startup reconciliation — catalog empty, no work to do");
      return new Summary(0, 0, 0);
    }

    AtomicInteger invoked = new AtomicInteger(0);
    AtomicInteger skipped = new AtomicInteger(0);
    AtomicInteger failed = new AtomicInteger(0);

    // Bounded parallelism — same pattern as drain (PR #228). For
    // P1 the assumption is <1000 streams; sequential would also
    // be fine. The executor's sole purpose here is to bound
    // concurrent in-flight HTTP requests to a small number.
    ExecutorService executor =
        Executors.newFixedThreadPool(
            Math.min(8, total),
            r -> {
              Thread t = new Thread(r, "semanticdf-platform-reconcile");
              t.setDaemon(true);
              return t;
            });

    try {
      for (StreamCatalog.StreamMetadata meta : streams) {
        executor.submit(
            () -> {
              try {
                invokeRun(meta);
                invoked.incrementAndGet();
              } catch (SkippedException skip) {
                skipped.incrementAndGet();
                System.out.println(
                    "semanticdf-platform: startup reconciliation skipped "
                        + meta.streamId()
                        + " (status="
                        + skip.getMessage()
                        + ")");
              } catch (Exception e) {
                failed.incrementAndGet();
                System.err.println(
                    "semanticdf-platform: startup reconciliation failed for "
                        + meta.streamId()
                        + ": "
                        + e.getMessage());
              }
            });
      }
    } finally {
      executor.shutdown();
      try {
        if (!executor.awaitTermination(totalTimeout.toMillis(), TimeUnit.MILLISECONDS)) {
          executor.shutdownNow();
        }
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        executor.shutdownNow();
      }
    }

    int i = invoked.get();
    int s = skipped.get();
    int f = failed.get();
    System.out.println(
        "semanticdf-platform: startup reconciliation complete — total="
            + total
            + " invoked="
            + i
            + " skipped="
            + s
            + " failed="
            + f);
    return new Summary(total, i + s, f);
  }

  /**
   * Submit a {@code run} invocation for one stream. Pre-checks
   * the workflow's current status via {@code getStatus()} and
   * skips if the journal says the stream is in a terminal/blocked
   * state. The body is a JSON-serialized {@code StreamRunRequest}.
   */
  private void invokeRun(StreamCatalog.StreamMetadata meta)
      throws IOException, InterruptedException, SkippedException {
    String streamId = meta.streamId();
    String status = readStatus(streamId);
    switch (status) {
      case "running":
      case "starting":
        // Proceed to invoke run().
        break;
      case "stopped":
      case "failed":
      case "failed-restart":
        // Operator-terminated or operator-blocked. Don't resurrect.
        throw new SkippedException(status);
      case "unknown":
      default:
        // No prior run() ever executed for this stream-id. The
        // catalog row is stale (operator manually cleaned the
        // journal, or table populated via migration). Skip with
        // a warning — but since this indicates drift between
        // catalog and journal, log at WARN level.
        System.err.println(
            "semanticdf-platform: WARN — catalog has stream-id="
                + streamId
                + " but journal says '"
                + status
                + "'. Skipping (cleanup needed).");
        throw new SkippedException(status);
    }

    String runUrl = localIngress.toString() + "/StreamingService/" + streamId + "/run/send";
    String body =
        "{"
            + "\"modelName\":\""
            + jsonEscape(meta.modelName())
            + "\","
            + "\"queryShape\":\""
            + jsonEscape(meta.queryShape())
            + "\","
            + "\"checkpointLocation\":\""
            + jsonEscape(meta.checkpointLocation())
            + "\""
            + "}";

    HttpRequest req =
        HttpRequest.newBuilder()
            .uri(URI.create(runUrl))
            .timeout(Duration.ofSeconds(5))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build();
    HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
    if (resp.statusCode() >= 400) {
      throw new IOException("HTTP " + resp.statusCode() + ": " + resp.body());
    }
  }

  /**
   * Call {@code StreamingService.getStatus()} via the local
   * Restate ingress. {@code getStatus()} is {@code @Shared} and
   * idempotent. Returned as a String ("running", "stopped", "failed",
   * "failed-restart", "starting", or "unknown").
   *
   * <p>Wrapped body: Restate's ingress protocol for a
   * {@code Void}-returning handler accepts an empty JSON body.
   */
  private String readStatus(String streamId) throws IOException, InterruptedException {
    String url = localIngress.toString() + "/StreamingService/" + streamId + "/getStatus/send";
    HttpRequest req =
        HttpRequest.newBuilder()
            .uri(URI.create(url))
            .timeout(Duration.ofSeconds(3))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(""))
            .build();
    HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
    if (resp.statusCode() == 404) {
      // Workflow doesn't exist in journal — treat as unknown.
      return "unknown";
    }
    if (resp.statusCode() >= 400) {
      throw new IOException("HTTP " + resp.statusCode() + ": " + resp.body());
    }
    // Restate ingress responses are wrapped — we just need the
    // raw body, which IS the handler's return value when the
    // handler returns a primitive/String.
    return resp.body();
  }

  /** Minimal JSON string escaper (for run() body's three fields). */
  private static String jsonEscape(String s) {
    if (s == null) {
      return "";
    }
    StringBuilder sb = new StringBuilder(s.length());
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      switch (c) {
        case '"' -> sb.append("\\\"");
        case '\\' -> sb.append("\\\\");
        case '\n' -> sb.append("\\n");
        case '\r' -> sb.append("\\r");
        case '\t' -> sb.append("\\t");
        default -> sb.append(c);
      }
    }
    return sb.toString();
  }

  /** A stream was deliberately skipped (terminal status). */
  private static final class SkippedException extends Exception {
    SkippedException(String status) {
      super(status);
    }
  }

  /**
   * Summary of the sweep for observability.
   *
   * @param total    catalog size (rows)
   * @param actedOn  sum of invoked + skipped (excluding failed)
   * @param failed   streams we couldn't reach (Postgres blip, ingress down)
   */
  public record Summary(int total, int actedOn, int failed) {}
}
