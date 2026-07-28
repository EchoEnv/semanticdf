package io.semanticdf.platform;

import dev.restate.admin.client.ApiClient;
import dev.restate.admin.model.RegisterDeploymentRequest;
import dev.restate.admin.model.RegisterDeploymentRequestAnyOf;
import dev.restate.sdk.endpoint.Endpoint;
import dev.restate.sdk.http.vertx.RestateHttpServer;

import io.semanticdf.platform.audit.AuditEventStore;
import io.semanticdf.platform.audit.AuditService;
import io.semanticdf.platform.audit.NoOpAuditEventStore;
import io.semanticdf.platform.audit.PostgresAuditEventStore;
import io.semanticdf.platform.model.ModelService;
import io.semanticdf.platform.model.ModelStore;
import io.semanticdf.platform.model.NoOpModelStore;
import io.semanticdf.platform.model.PostgresModelStore;
import io.semanticdf.platform.catalog.CatalogService;
import io.semanticdf.platform.query.QueryService;
import io.semanticdf.cache.ResultCache;
import io.semanticdf.platform.catalog.CatalogService;
import io.semanticdf.platform.model.ModelService;
import io.semanticdf.platform.query.QueryService;
import io.semanticdf.platform.streaming.HotReloadingModelRegistry;
import io.semanticdf.platform.streaming.ModelRegistry;
import io.semanticdf.platform.streaming.PostgresStreamCatalog;
import io.semanticdf.platform.streaming.SparkStreamingQueryLauncher;
import io.semanticdf.platform.streaming.StartupReconciler;
import io.semanticdf.platform.streaming.StreamCatalog;
import io.semanticdf.platform.streaming.StreamingQueryHandleRegistry;
import io.semanticdf.platform.streaming.StreamingQueryLauncher;
import io.semanticdf.platform.streaming.StreamingService;
import io.semanticdf.platform.streaming.YamlModelRegistry;

import io.semanticdf.tools.SdfSession;
import java.io.IOException;
import java.net.URI;
import scala.Option;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
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

  /**
   * Redact credentials from a Spark Connect URL for logging.
   * Spark Connect URLs of the form {@code sc://host:port;token=...} or
   * {@code sc://host:port?...&token=...} carry a session token after
   * the first {@code ;} or {@code ?} delimiter. We strip everything
   * after that point so the URL is safe to log.
   *
   * <p>Visible-for-testing — package-private.
   */
  static String redactConnectUrl(String url) {
    if (url == null) return "";
    int cut = url.length();
    int semi = url.indexOf(';');
    if (semi >= 0 && semi < cut) cut = semi;
    int qm = url.indexOf('?');
    if (qm >= 0 && qm < cut) cut = qm;
    return cut < url.length() ? url.substring(0, cut) + "<redacted>" : url;
  }

  /**
   * Build the {@link AuditEventStore} from env vars. Visible-for-testing
   * pattern — the composition root in {@link #main} calls this once at
   * startup, hands the result to {@link AuditService}, and registers a
   * close-on-shutdown hook in {@code Runtime.addShutdownHook}.
   *
   * <p>Env vars:
   * <ul>
   *   <li>{@code SEMANTICDF_AUDIT_PERSIST=true|false} —
   *       default false (preserves pre-PR-A behavior: events reach
   *       the journal's {@code LAST_DEDUP_HASH} but no Postgres row
   *       is written).
   *   <li>{@code SEMANTICDF_CATALOG_JDBC_URL},
   *       {@code SEMANTICDF_CATALOG_USER},
   *       {@code SEMANTICDF_CATALOG_PASSWORD} — Postgres
   *       connection (shared with the stream catalog).
   * </ul>
   */
  static AuditEventStore buildAuditEventStoreFromEnv() {
    String persist = System.getenv().getOrDefault("SEMANTICDF_AUDIT_PERSIST", "false");
    if (!"true".equalsIgnoreCase(persist)) {
      System.out.println(
          "semanticdf-platform: SEMANTICDF_AUDIT_PERSIST not set to true — "
              + "audit events journal-only (no Postgres writes). Set "
              + "SEMANTICDF_AUDIT_PERSIST=true with SEMANTICDF_CATALOG_JDBC_URL "
              + "to enable durable audit persistence.");
      return new NoOpAuditEventStore();
    }
    String jdbcUrl = System.getenv("SEMANTICDF_CATALOG_JDBC_URL");
    if (jdbcUrl == null || jdbcUrl.isBlank()) {
      throw new IllegalStateException(
          "SEMANTICDF_AUDIT_PERSIST=true but SEMANTICDF_CATALOG_JDBC_URL is unset \u2014 "
              + "either set both, or unset SEMANTICDF_AUDIT_PERSIST to disable audit persistence");
    }
    String user = System.getenv().getOrDefault("SEMANTICDF_CATALOG_USER", "semanticdf");
    String password =
        System.getenv().getOrDefault("SEMANTICDF_CATALOG_PASSWORD", "semanticdf");
    System.out.println(
        "semanticdf-platform: SEMANTICDF_AUDIT_PERSIST=true \u2014 audit events "
            + "durable in " + AuditEventStore.class.getSimpleName()
            + " against " + redactConnectUrl(jdbcUrl));
    return new PostgresAuditEventStore(jdbcUrl, user, password);
  }

  /**
   * Build the {@link ModelStore} from env vars. Visible-for-testing
   * pattern — mirrors {@link #buildAuditEventStoreFromEnv}.
   *
   * <p>Env vars:
   * <ul>
   *   <li>{@code SEMANTICDF_MODELS_PERSIST=true|false} —
   *       default false (preserves pre-PR-B behavior: register()
   *       updates the journal but no Postgres row is written).
   *   <li>{@code SEMANTICDF_CATALOG_JDBC_URL} +
   *       {@code SEMANTICDF_CATALOG_USER} +
   *       {@code SEMANTICDF_CATALOG_PASSWORD} — Postgres
   *       connection (shared with the stream catalog + audit
   *       event store).
   * </ul>
   */
  static ModelStore buildModelStoreFromEnv() {
    String persist = System.getenv().getOrDefault("SEMANTICDF_MODELS_PERSIST", "false");
    if (!"true".equalsIgnoreCase(persist)) {
      System.out.println(
          "semanticdf-platform: SEMANTICDF_MODELS_PERSIST not set to true — "
              + "model registry journal-only (no Postgres writes). Set "
              + "SEMANTICDF_MODELS_PERSIST=true with SEMANTICDF_CATALOG_JDBC_URL "
              + "to enable durable model storage.");
      return new NoOpModelStore();
    }
    String jdbcUrl = System.getenv("SEMANTICDF_CATALOG_JDBC_URL");
    if (jdbcUrl == null || jdbcUrl.isBlank()) {
      throw new IllegalStateException(
          "SEMANTICDF_MODELS_PERSIST=true but SEMANTICDF_CATALOG_JDBC_URL is unset \u2014 "
              + "either set both, or unset SEMANTICDF_MODELS_PERSIST to disable model persistence");
    }
    String user = System.getenv().getOrDefault("SEMANTICDF_CATALOG_USER", "semanticdf");
    String password =
        System.getenv().getOrDefault("SEMANTICDF_CATALOG_PASSWORD", "semanticdf");
    System.out.println(
        "semanticdf-platform: SEMANTICDF_MODELS_PERSIST=true \u2014 model registry "
            + "durable in " + ModelStore.class.getSimpleName()
            + " against " + redactConnectUrl(jdbcUrl));
    return new PostgresModelStore(jdbcUrl, user, password);
  }

  /**
   * Register this platform's deployment with an external Restate server's
   * admin API. Visible-for-testing — extracted from {@link #main(String[])}
   * so the registration path can be exercised without booting Spark.
   *
   * <p>Env-var shape:
   * <pre>
   *   RESTATE_INGRESS_URL=http://restate-host:8080/
   * </pre>
   * The admin URL is derived by swapping {@code :8080} for {@code :9070}
   * (the Restate admin port). The service-handler URL is reported as
   * {@code http://host.docker.internal:<boundPort>} so the external
   * Restate container can reach this JVM (which is running on the host).
   *
   * <p>Returns the deployment ID assigned by the external Restate, or
   * throws if the registration call fails. Callers should treat
   * exceptions as best-effort: a failed registration falls back to the
   * in-process restate runtime that's already listening.
   */
  static String registerWithExternalRestate(
      String externalIngress, int boundPort, String serviceHandlerUrlEnv)
      throws Exception {
    String adminUrl = externalIngress.replaceAll(":8080/?$", ":9070");
    // The SDK appends `/<operation_path>` to the basePath without
    // normalizing the separator. If the basePath ends in `/`, the
    // resulting URL is `//deployments`, which HttpServers reject as
    // "no context found". Strip the trailing `/` here.
    String normalizedBasePath = adminUrl.replaceAll("/$", "");
    // The 3-arg constructor accepts a basePath; passing it as the third
    // arg. setBasePath() does NOT update the host/port/scheme fields
    // the request builder uses -- only the constructor does.
    ApiClient adminClient = new ApiClient(
        java.net.http.HttpClient.newBuilder(),
        new com.fasterxml.jackson.databind.ObjectMapper(),
        normalizedBasePath);
    var deploymentApi = new dev.restate.admin.api.DeploymentApi(adminClient);
    // Service-handler URL the external Restate uses to call back to us.
    //   Default: http://host.docker.internal:<port> (works when Restate
    //     is in Docker with `extra_hosts: host.docker.internal:host-gateway`
    //     and the platform runs on the Docker bridge gateway host).
    //   Override: when the env var is non-null/non-blank, use it.
    //     Set SEMANTICDF_SERVICE_HANDLER_URL when the platform is reachable
    //     from Restate only via an external IP (e.g., UFW blocks docker-bridge
    //     to host traffic, or Restate runs on a different machine).
    //     Example: SEMANTICDF_SERVICE_HANDLER_URL=http://203.0.113.1:9093
    String defaultHandler = "http://host.docker.internal:" + boundPort;
    String serviceHandlerUrl =
        (serviceHandlerUrlEnv == null || serviceHandlerUrlEnv.isBlank())
            ? defaultHandler
            : serviceHandlerUrlEnv;
    RegisterDeploymentRequest req = new RegisterDeploymentRequest(
        new RegisterDeploymentRequestAnyOf().uri(serviceHandlerUrl));
    var resp = deploymentApi.createDeployment(req);
    return resp.getId();
  }

  /**
   * Convenience overload: reads {@code SEMANTICDF_SERVICE_HANDLER_URL} from
   * the environment. Tests use the 3-arg form with explicit values.
   */
  static String registerWithExternalRestate(String externalIngress, int boundPort)
      throws Exception {
    return registerWithExternalRestate(
        externalIngress,
        boundPort,
        System.getenv("SEMANTICDF_SERVICE_HANDLER_URL"));
  }

  public static void main(String[] args) throws IOException {
    // --- Configuration from environment ---
    //
    // MODELS_DIR — directory of *.yml model files. Default: ./models (relative
    // to the working directory of the platform process).
    //
    // SPARK_APP_NAME — Spark application name shown in the Spark UI / Connect logs.
    // Default: "semanticdf-platform".
    //
    // SPARK_MASTER — currently UNUSED (PR #240 reserved the env var for
    // future single-node tuning). The library's SdfSession hardcodes
    // 'local[*]' for the local-mode fallback. Set SEMANTICDF_SPARK_CONNECT_URL
    // instead for production. Default: "local[*]" (preserved as a hint in
    // startup logs).
    //
    // SEMANTICDF_SPARK_CONNECT_URL — when set (e.g. "sc://spark-connect:15002"),
    // the platform becomes a control plane and obtains its SparkSession from
    // a long-running Spark Connect cluster over gRPC. The cluster's JVM
    // lifetime is decoupled from the platform's. This is the production
    // topology. Unset = legacy local-mode (in-process Spark driver), kept for
    // tests and quickstart. SPARK 4.0+ REQUIRED for Connect mode
    // (SdfSession.scala:81-88 throws UnsupportedOperationException on
    // Spark 3.x). See platform-architecture.md for the rationale.
    //
    // SEMANTICDF_AUDIT_PERSIST — when set to "true" (default: false), audit
    // events emitted by RestateAuditSink (currently via StreamingService.run
    // / run → started/restarted events) are persisted to Postgres via
    // PostgresAuditEventStore instead of being journal-only. Idempotent on
    // (tenant, ts, dedup_hash); uses the platform's existing Postgres
    // (SEMANTICDF_CATALOG_JDBC_URL/_USER/_PASSWORD). See PR-A plan in
    // platform-services-completion-plan.md.
    String modelsDir = System.getenv().getOrDefault("MODELS_DIR", "./models");
    String sparkAppName = System.getenv().getOrDefault("SPARK_APP_NAME", "semanticdf-platform");
    String sparkMaster = System.getenv().getOrDefault("SPARK_MASTER", "local[*]");

    // --- Spark session ---
    //
    // P1 (pre-#240): in-process Spark driver, master from SPARK_MASTER (default
    //   "local[*]"). One Spark JVM per platform JVM.
    //
    // PR #240: flag-gated via SEMANTICDF_SPARK_CONNECT_URL — when set, the
    //   session is a Spark Connect CLIENT to a long-running remote cluster.
    //   SparkConnect mode requires Spark 4.0+ (SdfSession throws a clear
    //   error on 3.5 with a hint to build with -Pspark4). The platform's
    //   shutdown hook calls spark.stop() on both modes — for Connect this
    //   just closes the client's gRPC connection; the remote server's
    //   lifecycle is unaffected.
    SparkSession spark =
        SdfSession.createFromEnv(sparkAppName, Option.empty());

    // PR #241: Register a minimal spark-cleanup shutdown hook IMMEDIATELY
    // after SparkSession creation. The main shutdown hook (registered
    // later, after all services are bound and the daemon sweep is up)
    // handles the full graceful sequence: drain → spark.stop() →
    // catalog.close(). But if main() throws BEFORE that later hook is
    // registered — e.g., YamlModelRegistry.load fails, or Restate bind
    // fails, or any subsequent step errors — this early hook ensures
    // SparkSession.close() (and the gRPC-channel release in Connect
    // mode) happens anyway. SparkSession.stop() is idempotent — both
    // this hook and the later hook can safely call it in the success
    // path (Spark treats the second call as a no-op).
    Runtime.getRuntime().addShutdownHook(
        new Thread(
            () -> {
              try {
                spark.stop();
                System.out.println(
                    "semanticdf-platform: early-shutdown SparkSession released");
              } catch (Throwable t) {
                System.err.println(
                    "semanticdf-platform: early-shutdown spark.stop() failed: "
                        + t.getMessage());
              }
            },
            "semanticdf-platform-shutdown-spark-early"));

    if (System.getenv(SdfSession.RemoteUrlEnvVar()) != null) {
      // PR #240: control-plane mode. Redact credentials (anything after a
      // ';' or '?' delimiter in sc:// URLs is a token) before logging.
      System.out.println(
          "semanticdf-platform: Spark Connect mode \u2014 control plane against "
              + redactConnectUrl(System.getenv(SdfSession.RemoteUrlEnvVar())));
    } else {
      System.out.println(
          "semanticdf-platform: local Spark mode \u2014 master=" + sparkMaster
              + " (set " + SdfSession.RemoteUrlEnvVar() + " to switch to Spark Connect)");
    }

    // --- Streaming lifecycle wiring ---
    StreamingQueryHandleRegistry handles = new StreamingQueryHandleRegistry();
    // H3 fix: wrap the boot-time YamlModelRegistry in a HotReloadingModelRegistry
    // so successful ModelService.register() calls propagate to QueryService
    // and StreamingService without a JVM restart. The delegate (YamlModelRegistry)
    // remains the read-only baseline; the decorator adds a ConcurrentHashMap
    // overlay mutated by ModelService.register's STEP F.
    YamlModelRegistry yamlRegistry = YamlModelRegistry.load(modelsDir, spark);
    ModelRegistry models = new HotReloadingModelRegistry(yamlRegistry);
    StreamingQueryLauncher launcher = new SparkStreamingQueryLauncher(spark);

    System.out.println(
        "semanticdf-platform: loaded "
            + yamlRegistry.size()
            + " models from "
            + modelsDir
            + ": "
            + yamlRegistry.registeredModels()
            + " (hot-reload wrapper enabled: H3)");

    // --- Stream catalog (DE-H2 — bulk startup reconciliation) ---
    //
    // Postgres-backed durable list of stream-ids. The
    // StartupReconciler reads this at boot to re-invoke run() on
    // each previously-active stream, triggering the auto-detect
    // branch in StreamingService.run to recreate Spark queries
    // lost with the previous JVM.
    //
    // Configuration via env vars (default = platform's docker-compose):
    //   SEMANTICDF_CATALOG_JDBC_URL  — JDBC URL
    //   SEMANTICDF_CATALOG_USER      — DB user
    //   SEMANTICDF_CATALOG_PASSWORD  — DB password
    //
    // Pass catalog=null to disable startup reconciliation (P1
    // single-replica without Postgres); StreamingService.register
    // becomes a no-op.
    final StreamCatalog catalog;
    {
      StreamCatalog built = null;
      String catalogJdbcUrl = System.getenv("SEMANTICDF_CATALOG_JDBC_URL");
      if (catalogJdbcUrl != null && !catalogJdbcUrl.isBlank()) {
        String catalogUser = System.getenv().getOrDefault("SEMANTICDF_CATALOG_USER", "semanticdf");
        String catalogPassword =
            System.getenv().getOrDefault("SEMANTICDF_CATALOG_PASSWORD", "semanticdf");
        built = new PostgresStreamCatalog(catalogJdbcUrl, catalogUser, catalogPassword);
      } else {
        System.out.println(
            "semanticdf-platform: SEMANTICDF_CATALOG_JDBC_URL not set — "
                + "disabling bulk startup reconciliation. Operators must call "
                + "/restart manually after JVM restarts.");
      }
      catalog = built;
    }

    // --- Audit-event store (PR-A: durable audit log to Postgres) ---
    //
    // Configuration:
    //   SEMANTICDF_AUDIT_PERSIST=true|false (default: false)
    //     false = NoOpAuditEventStore; the journal's LAST_DEDUP_HASH
    //             short-circuits within a tenant but no Postgres
    //             row is written. Existing behavior preserved.
    //     true  = PostgresAuditEventStore against the platform's
    //             existing Postgres (shares the JDBC URL with the
    //             stream catalog).
    //   Requires SEMANTICDF_CATALOG_JDBC_URL/_USER/_PASSWORD.
    final AuditEventStore auditStore = buildAuditEventStoreFromEnv();

    // --- Model registry store (PR-B: durable model YAML + lineage) ---
    //
    // Configuration:
    //   SEMANTICDF_MODELS_PERSIST=true|false (default: false)
    //     false = NoOpModelStore; the journal's CURRENT_VERSION /
    //             MANIFEST_HASH are updated but no Postgres row
    //             is written. Existing behavior preserved.
    //     true  = PostgresModelStore against the platform's
    //             existing Postgres (shares the JDBC URL with the
    //             stream catalog).
    //   Requires SEMANTICDF_CATALOG_JDBC_URL/_USER/_PASSWORD.
    final ModelStore modelStore = buildModelStoreFromEnv();

    // Cache seam for ModelService — when a successful register()
    // bumps CURRENT_VERSION, ModelService calls
    // cache.invalidateByModelAndVersion(name, version). For P1,
    // the default is ResultCache.NoOp (no cache active until
    // PR-C wires it for QueryService); v0.2.3+ can pass the
    // library's InMemoryResultCache.
    final ResultCache resultCache = ResultCache.NoOp();

    // Bind all 5 services into one Endpoint.
    Endpoint endpoint = Endpoint.builder()
        .bind(new ModelService(modelStore, spark, resultCache, models))
        .bind(new QueryService(models, spark, resultCache))
        .bind(new StreamingService(models, launcher, handles, catalog))
        .bind(new AuditService(auditStore))
        .bind(new CatalogService(modelStore))
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

    // VERIFICATION-MODE external-Restate registration.
    //
    // When RESTATE_INGRESS_URL is set, the platform ALSO registers
    // its deployment with the external Restate server so the user
    // can probe the registered services + invocations via the
    // external Restate's admin API (port 9070).
    //
    // The in-process RestateHttpServer.listen() above remains in
    // effect — the SDK needs an in-process runtime to handle the
    // calls. The external Restate then routes incoming calls to
    // this in-process runtime via the service-handler URL.
    //
    // This is the verification-mode path. Mirrors PR #240's Spark
    // Connect toggle: opt-in, not a default-on cutover.
    String externalIngress = System.getenv("RESTATE_INGRESS_URL");
    if (externalIngress != null && !externalIngress.isBlank()) {
      try {
        String deploymentId =
            registerWithExternalRestate(
                externalIngress,
                boundPort,
                System.getenv("SEMANTICDF_SERVICE_HANDLER_URL"));
        String handlerUrl =
            System.getenv().getOrDefault(
                "SEMANTICDF_SERVICE_HANDLER_URL",
                "http://host.docker.internal:" + boundPort);
        System.out.println(
            "semanticdf-platform: registered deployment with external Restate at "
                + externalIngress.replaceAll(":8080/?$", ":9070")
                + " -- service handler URL = " + handlerUrl
                + " deploymentId=" + deploymentId);
      } catch (Exception e) {
        System.err.println(
            "semanticdf-platform: external Restate registration failed (continuing "
                + "with in-process runtime only): " + e.getMessage());
      }
    }

    // DE-H2: bulk startup reconciliation. After the HTTP server is
    // listening (so the sweep's POSTs reach the ingress), walk the
    // catalog and re-invoke run() on each previously-active stream.
    // The run() handler's auto-detect branch will see the empty
    // registry (this is a fresh JVM) and recreate the Spark query.
    //
    // Best-effort: a sweep failure logs but doesn't abort startup.
    // Operators can manually invoke /restart on individual
    // stream-ids if the sweep has issues.
    //
    // CRITICAL: the sweep runs on a DAEMON thread so it does NOT
    // block main(). Reasons (PR #235 senior review):
    //  1. With SEMANTICDF_RECONCILE_TIMEOUT_MS=30s default and a slow
    //     Postgres or N×5s swept HTTP calls, a synchronous sweep
    //     could delay readiness past kubelet's 30s liveness probe.
    //  2. RestateHttpServer.listen() is fire-and-forget — a
    //     synchronous sweep's first POSTs race with Vert.x finishing
    //     its bind/registration. Daemonizing gives Vert.x time to
    //     settle before the sweep begins (we add a small fixed
    //     delay).
    //  3. Operators don't see startup as "failed" just because the
    //     sweep was slow — readiness is independent of recovery.
    if (catalog != null) {
      final StreamCatalog catalogForSweep = catalog;
      final URI localIngress = URI.create("http://localhost:" + boundPort);
      Thread sweepThread =
          new Thread(
              () -> {
                // Brief settle delay — let Restate's Vert.x handler
                // registration finish before issuing HTTP. The
                // listen() call returned the bound port, but the
                // Vert.x runtime may still be wiring up handlers.
                try {
                  Thread.sleep(200);
                } catch (InterruptedException ie) {
                  Thread.currentThread().interrupt();
                  return;
                }
                try {
                  StartupReconciler.Summary sweepSummary =
                      new StartupReconciler(catalogForSweep, localIngress).run();
                  System.out.println(
                      "semanticdf-platform: startup reconciliation complete — "
                          + "total="
                          + sweepSummary.total()
                          + " acted="
                          + sweepSummary.actedOn()
                          + " failed="
                          + sweepSummary.failed());
                } catch (RuntimeException sweepRe) {
                  System.err.println(
                      "semanticdf-platform: startup reconciliation failed: "
                          + sweepRe.getMessage()
                          + " (operators must invoke /restart manually)");
                }
              },
              "semanticdf-platform-reconcile");
      sweepThread.setDaemon(true);
      sweepThread.start();
      System.out.println(
          "semanticdf-platform: startup reconciliation scheduled (daemon thread); "
              + "main continues without blocking.");
    }

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

      // Release Postgres connections. Done last so the platform
      // doesn't interrupt itself mid-drain. The drain uses runtime
      // queries (not Postgres), so this only affects the
      // StreamCatalog's HikariCP pool.
      final StreamCatalog catalogForShutdown = catalog;
      if (catalogForShutdown != null) {
        try {
          catalogForShutdown.close();
          System.out.println("semanticdf-platform: StreamCatalog closed");
        } catch (Throwable t) {
          System.err.println(
              "semanticdf-platform: StreamCatalog close() failed: " + t.getMessage());
        }
      }
      // PR-A: release the audit-event store's HikariCP pool (only
      // meaningful when SEMANTICDF_AUDIT_PERSIST=true). NoOp store
      // has nothing to close.
      if (auditStore != null) {
        try {
          auditStore.close();
          System.out.println("semanticdf-platform: AuditEventStore closed");
        } catch (Throwable t) {
          System.err.println(
              "semanticdf-platform: AuditEventStore close() failed: " + t.getMessage());
        }
      }
      // PR-B: release the model store's HikariCP pool (only
      // meaningful when SEMANTICDF_MODELS_PERSIST=true). NoOp store
      // has nothing to close.
      if (modelStore != null) {
        try {
          modelStore.close();
          System.out.println("semanticdf-platform: ModelStore closed");
        } catch (Throwable t) {
          System.err.println(
              "semanticdf-platform: ModelStore close() failed: " + t.getMessage());
        }
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
   *
   * <p>All queries are stopped <b>in parallel</b> via a fixed-size thread
   * pool capped at {@link #DRAIN_MAX_PARALLEL}. The global drain time is
   * bounded by {@code perQueryTimeoutMs} (not N × {@code perQueryTimeoutMs}),
   * because the queries run concurrently.
   *
   * <p>The implementation uses {@link ExecutorService#invokeAll} with the
   * per-query timeout. Each task runs {@code query.stop()}; tasks that
   * don't complete within the timeout are cancelled when {@code invokeAll}
   * returns. We then iterate the resulting {@link Future}s to report
   * per-query outcomes (success / timeout / exception).
   */
  static int drainQueries(StreamingQueryHandleRegistry handles, long perQueryTimeoutMs) {
    // Snapshot the registry into a list so we can size the pool against
    // the actual stream count. forEach is weakly consistent — a snapshot
    // is what we want for a parallel drain (no growth-during-iteration).
    List<Map.Entry<String, StreamingQuery>> snapshot = new ArrayList<>();
    handles.forEach(
        (streamId, query) -> snapshot.add(Map.entry(streamId, query)));

    int count = snapshot.size();
    if (count == 0) {
      return 0;
    }

    int poolSize = Math.min(count, DRAIN_MAX_PARALLEL);
    ExecutorService executor =
        Executors.newFixedThreadPool(
            poolSize,
            r -> {
              Thread t = new Thread(r, "semanticdf-platform-drain");
              t.setDaemon(true);
              return t;
            });

    // Build the task list — each task is a single query.stop() call.
    List<Callable<Void>> tasks = new ArrayList<>(count);
    for (Map.Entry<String, StreamingQuery> entry : snapshot) {
      tasks.add(
          () -> {
            entry.getValue().stop();
            return null;
          });
    }

    List<Future<Void>> futures;
    try {
      // invokeAll blocks until ALL tasks complete OR the timeout elapses.
      // Unfinished tasks are cancelled when it returns. This is exactly
      // the parallel-drain semantics we want: every query gets up to
      // perQueryTimeoutMs (because they're concurrent), and the global
      // drain time is bounded by that same number.
      futures = executor.invokeAll(tasks, perQueryTimeoutMs, TimeUnit.MILLISECONDS);
    } catch (InterruptedException ie) {
      // The shutdown hook thread itself was interrupted. Re-set the
      // interrupt flag (best practice), cancel any in-flight tasks, and
      // bail out — spark.stop() will tear down the rest.
      Thread.currentThread().interrupt();
      System.err.println(
          "semanticdf-platform: drain interrupted; "
              + "remaining queries will be killed by spark.stop()");
      executor.shutdownNow();
      return count;
    } finally {
      executor.shutdown();
      // Brief await — enough for already-finished tasks to release
      // their threads back to the pool. We do NOT block indefinitely
      // because we're shutting down; daemon threads will be killed by
      // the JVM exit anyway. A bounded wait just keeps the executor
      // service object clean for tests that re-invoke drain().
      try {
        executor.awaitTermination(1, TimeUnit.SECONDS);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
      }
    }

    // Report per-query outcomes. The future's state tells us what
    // happened: isDone (success or exception), isCancelled (timeout).
    int succeeded = 0;
    int timedOut = 0;
    int failed = 0;
    for (int i = 0; i < futures.size(); i++) {
      Future<Void> f = futures.get(i);
      String streamId = snapshot.get(i).getKey();
      if (f.isCancelled()) {
        timedOut++;
        System.err.println(
            "semanticdf-platform: drain timed out for stream-id="
                + streamId
                + " after "
                + perQueryTimeoutMs
                + "ms; JVM shutdown hook timeout will force-exit");
      } else {
        try {
          f.get(); // re-throws ExecutionException wrapping the underlying error
          succeeded++;
        } catch (Exception e) {
          failed++;
          // ExecutionException always wraps the underlying cause; this
          // defensive fallback handles other Exception types (the current
          // call sites only produce ExecutionException, but we don't
          // want to NPE if a future JDK call adds another type).
          Throwable cause = e.getCause() != null ? e.getCause() : e;
          System.err.println(
              "semanticdf-platform: drain failed for stream-id="
                  + streamId
                  + ": "
                  + cause.getMessage());
        }
      }
    }
    System.out.println(
        "semanticdf-platform: drain complete — "
            + succeeded
            + " stopped, "
            + timedOut
            + " timed out, "
            + failed
            + " failed");
    return count;
  }

  /**
   * Maximum number of parallel drain workers. Bounds the thread-pool size
   * so we don't spin up thousands of threads for platforms with many
   * streams (each thread carries ~512KB-1MB of stack). Operators tune
   * via {@code SEMANTICDF_DRAIN_MAX_PARALLEL}; default 16.
   */
  static final int DEFAULT_DRAIN_MAX_PARALLEL = 64;

  /**
   * Hard upper bound on {@link #DRAIN_MAX_PARALLEL} to prevent operator
   * typos (e.g. {@code SEMANTICDF_DRAIN_MAX_PARALLEL=100000}) from
   * spawning so many threads that the JVM OOMs on stack allocation.
   * 256 threads × ~1MB stack ≈ 256MB worst case, well under the
   * platform's 1GB heap budget.
   */
  static final int MAX_DRAIN_MAX_PARALLEL = 256;

  static final int DRAIN_MAX_PARALLEL = resolveDrainMaxParallel();

  private static int resolveDrainMaxParallel() {
    String raw = System.getenv("SEMANTICDF_DRAIN_MAX_PARALLEL");
    if (raw == null || raw.isBlank()) {
      return DEFAULT_DRAIN_MAX_PARALLEL;
    }
    try {
      int v = Integer.parseInt(raw.trim());
      if (v <= 0) {
        System.err.println(
            "semanticdf-platform: non-positive SEMANTICDF_DRAIN_MAX_PARALLEL='"
                + raw
                + "', using default "
                + DEFAULT_DRAIN_MAX_PARALLEL);
        return DEFAULT_DRAIN_MAX_PARALLEL;
      }
      if (v > MAX_DRAIN_MAX_PARALLEL) {
        System.err.println(
            "semanticdf-platform: SEMANTICDF_DRAIN_MAX_PARALLEL='"
                + raw
                + "' exceeds max "
                + MAX_DRAIN_MAX_PARALLEL
                + ", clamping");
        return MAX_DRAIN_MAX_PARALLEL;
      }
      return v;
    } catch (NumberFormatException e) {
      System.err.println(
          "semanticdf-platform: invalid SEMANTICDF_DRAIN_MAX_PARALLEL='"
              + raw
              + "', using default "
          + DEFAULT_DRAIN_MAX_PARALLEL);
      return DEFAULT_DRAIN_MAX_PARALLEL;
    }
  }
}
