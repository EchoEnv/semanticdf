package io.semanticdf.platform;

import dev.restate.sdk.endpoint.Endpoint;
import dev.restate.sdk.http.vertx.RestateHttpServer;

import io.semanticdf.platform.audit.AuditService;
import io.semanticdf.platform.catalog.CatalogService;
import io.semanticdf.platform.model.ModelService;
import io.semanticdf.platform.query.QueryService;
import io.semanticdf.platform.streaming.ModelRegistry;
import io.semanticdf.platform.streaming.SparkStreamingQueryLauncher;
import io.semanticdf.platform.streaming.StreamingQueryHandleRegistry;
import io.semanticdf.platform.streaming.StreamingQueryLauncher;
import io.semanticdf.platform.streaming.StreamingService;
import io.semanticdf.platform.streaming.YamlModelRegistry;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

/**
 * PlatformApplication — main entry point for the semanticdf-platform daemon.
 *
 * Boots the Restate runtime with all 5 services bound, and starts the
 * HTTP server. The Restate runtime is the in-process Vert.x server —
 * the same process hosts both the platform's services and the HTTP
 * ingress. In v1, one process per node; in P3, 3 replicas across AZs.
 *
 * In v2, an admin client could also bind here for operational tooling
 * (status / restart / drain); the SDK ships {@code dev.restate.admin-client}.
 */
public final class PlatformApplication {

  private PlatformApplication() {}

  public static void main(String[] args) throws IOException {
    // --- Configuration from environment ---
    //
    // MODELS_DIR — directory of *.yml model files. Default: ./models (relative
    // to the working directory of the platform process).
    //
    // SPARK_APP_NAME — Spark application name shown in the Spark UI. Default:
    // "semanticdf-platform".
    //
    // SPARK_MASTER — Spark master URL. Default: "local[*]" (local mode, all
    // cores). In production, set this to "spark://host:port" or "yarn".
    String modelsDir = System.getenv().getOrDefault("MODELS_DIR", "./models");
    String sparkAppName = System.getenv().getOrDefault("SPARK_APP_NAME", "semanticdf-platform");
    String sparkMaster = System.getenv().getOrDefault("SPARK_MASTER", "local[*]");

    // --- Spark session ---
    //
    // The platform creates one SparkSession at startup and shares it across
    // all StreamingService handlers. Per platform-architecture.md §1.3, the
    // platform's "engine adapter" owns SparkSession creation. For P1 the
    // adapter is local-mode Spark; future versions may use Spark Connect.
    SparkSession spark =
        SparkSession.builder()
            .appName(sparkAppName)
            .master(sparkMaster)
            .getOrCreate();

    // --- Streaming lifecycle wiring ---
    StreamingQueryHandleRegistry handles = new StreamingQueryHandleRegistry();
    ModelRegistry models = YamlModelRegistry.load(modelsDir, spark);
    StreamingQueryLauncher launcher = new SparkStreamingQueryLauncher(spark);

    System.out.println(
        "semanticdf-platform: loaded "
            + ((YamlModelRegistry) models).size()
            + " models from "
            + modelsDir
            + ": "
            + ((YamlModelRegistry) models).registeredModels());

    // Bind all 5 services into one Endpoint.
    Endpoint endpoint = Endpoint.builder()
        .bind(new ModelService())
        .bind(new QueryService())
        .bind(new StreamingService(models, launcher, handles))
        .bind(new AuditService())
        .bind(new CatalogService())
        .build();

    // Start the HTTP server on port 8080 (or $PORT). The same process
    // hosts the platform's REST surface and the Restate runtime.
    int port = Integer.parseInt(System.getenv().getOrDefault("PORT", "8080"));
    // The Restate SDK's listen() is fire-and-forget — it returns the
    // bound port (not a server handle). We capture the port for logging.
    // On SIGTERM the JVM exits; the Vert.x thread is killed; the kernel
    // releases the socket. The shutdown hook below logs so operators
    // see the shutdown sequence in their logs.
    int boundPort = RestateHttpServer.listen(endpoint, port);

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      System.out.println("semanticdf-platform: shutdown hook firing; bound port "
          + boundPort + " will be released");

      // Graceful drain — stop active streaming queries BEFORE spark.stop().
      //
      // Without this, queries are killed mid-batch when spark.stop() tears
      // down the SparkContext. The query's writer may have unflushed data
      // (Kafka producer, in-progress file write, etc.); losing that data
      // silently is worse than a slower shutdown.
      //
      // query.stop() blocks until the query stops (Spark's internal stop
      // timeout applies). We iterate the registry's snapshot — if a query
      // is added concurrently, it may be missed, but the next SIGTERM
      // catches it. The drain is best-effort; exceptions from individual
      // queries don't block the JVM exit.
      int drained = drainQueries(handles);
      if (drained > 0) {
        System.out.println("semanticdf-platform: drained " + drained
            + " active streaming query/queries");
      }

      // Stop the SparkSession to release driver memory, daemon threads,
      // and the SparkContext's RPC clients. Without this the JVM leaks
      // ~1-2GB heap and a half-dozen threads per platform instance
      // (matches the leak the MCP server's Main.scala explicitly guards
      // against — see semanticdf-mcp/src/main/scala/io/semanticdf/mcp/Main.scala).
      //
      // Best-effort: if spark.stop() throws, the JVM exits anyway. We log
      // so operators can see the failure.
      try {
        spark.stop();
        System.out.println("semanticdf-platform: SparkSession stopped");
      } catch (Throwable t) {
        System.err.println("semanticdf-platform: spark.stop() failed: " + t.getMessage());
      }
    }, "semanticdf-platform-shutdown"));

    System.out.println("semanticdf-platform listening on http://localhost:" + port);
  }

  /**
   * Stop every live {@link StreamingQuery} in the registry. Returns the
   * number of queries that were drained (attempts, not necessarily
   * successful stops — operators want the full picture).
   *
   * <p>Each {@code query.stop()} is wrapped in a {@link Future#get(long, TimeUnit)}
   * with a per-query timeout ({@link #DRAIN_TIMEOUT_MS}). If a query
   * hangs past the timeout, the Future is cancelled (which interrupts
   * the worker thread), and we log the timeout — the JVM's own shutdown
   * hook timeout will eventually force-exit if many queries hang.
   *
   * <p>Visible-for-testing — package-private so unit tests can drive the
   * drain with a fake registry.
   *
   * @param handles the runtime-local handle registry
   * @return the number of queries we attempted to stop
   */
  static int drainQueries(StreamingQueryHandleRegistry handles) {
    return drainQueries(handles, DRAIN_TIMEOUT_MS);
  }

  /**
   * Per-query timeout for {@link #drainQueries(StreamingQueryHandleRegistry, long)}.
   * Spark's own {@code spark.sql.streaming.stopTimeout} controls how long
   * {@code query.stop()} blocks before throwing; this is a hard cap that
   * bounds our shutdown sequence regardless of Spark's setting.
   *
   * <p>10s per query is the default. With N concurrent streams, the worst-
   * case drain time is N×10s. Operators tune via the
   * {@code SEMANTICDF_DRAIN_TIMEOUT_MS} env var.
   */
  static final long DEFAULT_DRAIN_TIMEOUT_MS = 10_000L;

  /** Resolved at class-load from the env var; falls back to the default. */
  static final long DRAIN_TIMEOUT_MS = resolveDrainTimeoutMs();

  private static long resolveDrainTimeoutMs() {
    String raw = System.getenv("SEMANTICDF_DRAIN_TIMEOUT_MS");
    if (raw == null || raw.isBlank()) {
      return DEFAULT_DRAIN_TIMEOUT_MS;
    }
    try {
      long v = Long.parseLong(raw.trim());
      return v > 0 ? v : DEFAULT_DRAIN_TIMEOUT_MS;
    } catch (NumberFormatException e) {
      System.err.println(
          "semanticdf-platform: invalid SEMANTICDF_DRAIN_TIMEOUT_MS='"
              + raw
              + "', using default "
              + DEFAULT_DRAIN_TIMEOUT_MS
              + "ms");
      return DEFAULT_DRAIN_TIMEOUT_MS;
    }
  }

  /**
   * Drain with an explicit per-query timeout. Visible-for-testing.
   */
  static int drainQueries(StreamingQueryHandleRegistry handles, long perQueryTimeoutMs) {
    AtomicInteger count = new AtomicInteger(0);
    ExecutorService executor =
        Executors.newSingleThreadExecutor(
            r -> {
              Thread t = new Thread(r, "semanticdf-platform-drain");
              t.setDaemon(true);
              return t;
            });
    // Volatile flag: set true when the shutdown hook thread itself is
    // interrupted (e.g., JVM's shutdown hook timeout fires). The forEach
    // lambda checks this flag at the start of each iteration to bail out
    // early — without it, the drain continues iterating after the JVM
    // has already given up on us, which wastes time on queries that will
    // be killed by spark.stop() anyway.
    java.util.concurrent.atomic.AtomicBoolean interrupted =
        new java.util.concurrent.atomic.AtomicBoolean(false);
    try {
      handles.forEach(
          (streamId, query) -> {
            // If the shutdown hook thread was interrupted (e.g., JVM's
            // own shutdown hook timeout fired), skip remaining queries —
            // spark.stop() will tear them down anyway.
            if (interrupted.get()) {
              return;
            }
            // Count BEFORE attempting — operators want to see the total
            // drain attempts, not just the successful stops.
            count.incrementAndGet();
            Future<Void> future =
                executor.submit(
                    (Callable<Void>)
                        () -> {
                          query.stop();
                          return null;
                        });
            try {
              future.get(perQueryTimeoutMs, TimeUnit.MILLISECONDS);
            } catch (TimeoutException te) {
              // The query is hung. Cancel the future (interrupts the
              // worker thread); Spark may or may not honor the interrupt
              // but cancelling the future prevents the executor's queue
              // from blocking on this task forever.
              future.cancel(true);
              System.err.println(
                  "semanticdf-platform: drain timed out for stream-id="
                      + streamId
                      + " after "
                      + perQueryTimeoutMs
                      + "ms; JVM shutdown hook timeout will force-exit");
            } catch (InterruptedException ie) {
              // The shutdown hook itself was interrupted — re-set the
              // interrupt flag (best practice), set our early-exit flag
              // so the forEach lambda bails out on subsequent iterations,
              // and skip this query.
              Thread.currentThread().interrupt();
              interrupted.set(true);
              System.err.println(
                  "semanticdf-platform: drain interrupted at stream-id="
                      + streamId
                      + "; remaining queries will be killed by spark.stop()");
              future.cancel(true);
              return;
            } catch (Throwable t) {
              // query.stop() can throw TimeoutException (Spark's internal
              // stop timeout), StreamingQueryException, or any runtime
              // exception. We log and continue — we're shutting down
              // anyway; missing one query is better than hanging the JVM.
              System.err.println(
                  "semanticdf-platform: drain failed for stream-id="
                      + streamId
                      + ": "
                      + t.getMessage());
            }
          });
    } finally {
      executor.shutdownNow();
    }
    return count.get();
  }
}
