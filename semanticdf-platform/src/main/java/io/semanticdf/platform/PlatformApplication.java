package io.semanticdf.platform;

import dev.restate.admin.client.ApiClient;
import dev.restate.admin.model.RegisterDeploymentRequest;
import dev.restate.admin.model.RegisterDeploymentRequestAnyOf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import io.semanticdf.platform.streaming.PortableStreamingQueryLauncher;
import io.semanticdf.platform.streaming.SparkPortableStreamingQueryLauncher;
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

  private static final Logger LOG = LoggerFactory.getLogger(PlatformApplication.class);

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
      LOG.info(
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
    LOG.info(
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
      LOG.info(
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
    LOG.info(
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

  /**
   * Build the {@link ResultCache} from env vars. Visible-for-testing
   * pattern -- mirrors {@link #buildAuditEventStoreFromEnv}.
   *
   * <p>Env vars:
   * <ul>
   *   <li>{@code SEMANTICDF_RESULT_CACHE=noop|memory} (default: {@code noop})
   *     <ul>
   *       <li>{@code noop} = {@link ResultCache#NoOp} (default -- no
   *         caching; every query re-executes the Spark plan).</li>
   *       <li>{@code memory} = bounded LRU cache, default 256 entries
   *         (overridable via {@code SEMANTICDF_RESULT_CACHE_ENTRIES}).</li>
   *     </ul>
   *   <li>{@code SEMANTICDF_RESULT_CACHE_ENTRIES=N} (default: 256)
   *     only honored when {@code SEMANTICDF_RESULT_CACHE=memory}.
   * </ul>
   *
   * <p>Default-off preserves the v0.2.2 behavior. The library's
   * {@code InMemoryResultCache} (bounded LRU, thread-safe) ships in
   * the cache module -- no platform-side implementation needed.
   */
  static ResultCache buildResultCacheFromEnv() {
    return buildResultCacheFromEnv(System::getenv);
  }

  /**
   * Test seam: same as {@link #buildResultCacheFromEnv()} but takes the
   * env-var lookup as a parameter so tests can supply a deterministic map.
   * Visible-for-testing only.
   */
  static ResultCache buildResultCacheFromEnv(java.util.function.Function<String, String> env) {
    String kind = env.apply("SEMANTICDF_RESULT_CACHE");
    if (kind == null) kind = "noop";
    if ("memory".equalsIgnoreCase(kind)) {
      String entriesStr = env.apply("SEMANTICDF_RESULT_CACHE_ENTRIES");
      if (entriesStr == null) entriesStr = "256";
      int entries;
      try {
        entries = Integer.parseInt(entriesStr);
      } catch (NumberFormatException nfe) {
        LOG.warn(
            "semanticdf-platform: SEMANTICDF_RESULT_CACHE_ENTRIES='"
                + entriesStr
                + "' is not an integer; falling back to 256");
        entries = 256;
      }
      if (entries <= 0) {
        LOG.warn(
            "semanticdf-platform: SEMANTICDF_RESULT_CACHE_ENTRIES="
                + entries
                + " must be > 0; falling back to 256");
        entries = 256;
      }
      LOG.info(
          "semanticdf-platform: SEMANTICDF_RESULT_CACHE=memory -- bounded LRU cache, maxEntries="
              + entries
              + " (set SEMANTICDF_RESULT_CACHE=noop to disable).");
      return ResultCache.inMemory(entries);
    }
    if (!"noop".equalsIgnoreCase(kind)) {
      LOG.warn(
          "semanticdf-platform: SEMANTICDF_RESULT_CACHE='"
              + kind
              + "' is not a known cache kind (expected 'noop' or 'memory'); falling back to NoOp.");
    }
    return ResultCache.NoOp();
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
    // SPARK_MASTER — UNUSED. The platform is a pure control plane; it does
    // NOT create a Spark JVM locally. Set SEMANTICDF_SPARK_CONNECT_URL
    // (mandatory) to a long-running Spark Connect server (e.g. "sc://spark-connect:15002").
    // The default value below is preserved only as a hint in startup logs.
    //
    // SEMANTICDF_SPARK_CONNECT_URL — MANDATORY. The platform obtains its
    // SparkSession from a long-running Spark Connect cluster over gRPC.
    // The Spark JVM lives in a separate process (the Connect server), and
    // the platform only holds a thin gRPC client to it. This is the
    // production topology — the platform's JVM lifetime is decoupled from
    // the Spark cluster's. Both batch (QueryService) and stream
    // (StreamingService) paths use the same Connect client. SPARK 4.0+
    // REQUIRED for Connect mode (SdfSession.scala:81-88 throws
    // UnsupportedOperationException on Spark 3.x). See
    // platform-architecture.md for the rationale.
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

    // --- Spark session (control-plane mode) ---
    //
    // The platform is a control plane — it does NOT create a Spark JVM
    // locally. Spark must be running externally (e.g. via
    // docker-compose's `spark-connect` service or a managed Spark
    // cluster), and the platform connects to it via Spark Connect over
    // gRPC. The SparkSession returned by SdfSession in Connect mode is a
    // thin gRPC client; the launcher's `start()` calls run on the
    // platform's JVM, but the actual work (compiling the model to a
    // DataFrame, starting the streaming query) is executed on the
    // remote Spark cluster.
    //
    // This is mandatory: the platform fails fast at startup if
    // SEMANTICDF_SPARK_CONNECT_URL is unset. The previous "legacy local
    // mode" (SparkSession.builder().master("local[*]").getOrCreate())
    // has been removed because it leaks the Spark JVM lifetime into the
    // platform's, defeating the architectural decoupling.
    //
    // For tests: the existing test suite constructs SparkSession
    // directly (SparkSession.builder().master("local[2]")...). These
    // tests do NOT go through PlatformApplication.main, so the
    // mandatory check below does not affect them. The test bootstrap
    // path is intentionally separate from the production bootstrap.
    String sparkConnectUrl = System.getenv(SdfSession.RemoteUrlEnvVar());
    // v0.3.1: Spark Connect is the PRODUCTION topology. The platform
    // is a control plane — Spark lives in a separate process (the
    // Spark Connect server) and the platform only holds a thin gRPC
    // client to it.
    //
    // v0.3.1+ demo fallback: when `SEMANTICDF_SPARK_CONNECT_URL` is
    // unset, the platform falls back to creating a Spark JVM locally
    // (legacy mode). This is INTENTIONALLY permissive for the demo
    // while the upstream Spark 4.0 classic.SparkSession$Builder.remote()
    // / handleBuilderConfig() stub makes the production path fail at
    // runtime. Production deployments set the env var to a real Spark
    // Connect URL. The fallback is logged loudly so operators see it.
    boolean connectMode = sparkConnectUrl != null && !sparkConnectUrl.isBlank();
    if (!connectMode) {
      LOG.warn(
          "====================================================================\n"
              + "semanticdf-platform: DEMO MODE — no "
              + SdfSession.RemoteUrlEnvVar()
              + " set; falling back to LOCAL Spark JVM.\n"
              + "This is INTENDED only for local demos / the PR #456 E2E tests\n"
              + "while the upstream Spark 4.0 classic.SparkSession$Builder stub is\n"
              + "fixed. Production deployments MUST set "
              + SdfSession.RemoteUrlEnvVar()
              + " to a Spark Connect server URL.\n"
              + "See semanticdf-platform/docker-compose.yml for the local Spark\n"
              + "Connect server config.\n"
              + "====================================================================");
    }
    // Pass the env var directly via flagOverride. When `connectMode`
    // is true the URL is forwarded; when false we pass None so the
    // SdfSession falls back to local Spark (matches the env var).
    SparkSession spark =
        SdfSession.createFromEnv(
            sparkAppName,
            connectMode ? scala.Option.<String>apply(sparkConnectUrl) : scala.Option.<String>empty());

    // Register a minimal spark-cleanup shutdown hook IMMEDIATELY
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
                LOG.info(
                    "semanticdf-platform: early-shutdown SparkSession released");
              } catch (Throwable t) {
                LOG.warn(
                    "semanticdf-platform: early-shutdown spark.stop() failed: "
                        + t.getMessage());
              }
            },
            "semanticdf-platform-shutdown-spark-early"));

    // Control-plane mode. Redact credentials (anything after a
    // ';' or '?' delimiter in sc:// URLs is a token) before logging.
    // Only log this when actually in connect mode (the demo fallback
    // path uses local Spark and shouldn't claim Spark Connect mode).
    if (connectMode) {
      LOG.info(
          "semanticdf-platform: Spark Connect mode \u2014 control plane against "
              + redactConnectUrl(sparkConnectUrl));
    }

    // --- Streaming lifecycle wiring ---
    StreamingQueryHandleRegistry handles = new StreamingQueryHandleRegistry();
    // H3 fix: wrap the boot-time YamlModelRegistry in a HotReloadingModelRegistry
    // so successful ModelService.register() calls propagate to QueryService
    // and StreamingService without a JVM restart. The delegate (YamlModelRegistry)
    // remains the read-only baseline; the decorator adds a ConcurrentHashMap
    // overlay mutated by ModelService.register's STEP F.
    //
    // v0.3.1+ DEMO MODE: pre-create the flights_tbl view that the
    // sample YAML model references. The YamlLoader.loadDir calls
    // `spark.table(name)` at load time to validate the table exists
    // (a legacy eager-validation design). Production deployments
    // would have the tables already registered by upstream ETL
    // pipelines. For the demo, we register a small in-memory view
    // so the model loads cleanly.
    if (!connectMode) {
      try {
        spark
            .createDataFrame(
                java.util.Arrays.asList(
                    org.apache.spark.sql.RowFactory.create("AA", 1L, 100L),
                    org.apache.spark.sql.RowFactory.create("AA", 2L, 200L),
                    org.apache.spark.sql.RowFactory.create("UA", 3L, 300L)),
                new org.apache.spark.sql.types.StructType()
                    .add("carrier", org.apache.spark.sql.types.DataTypes.StringType)
                    .add("flight_count", org.apache.spark.sql.types.DataTypes.LongType)
                    .add("total_distance", org.apache.spark.sql.types.DataTypes.LongType))
            .createOrReplaceTempView("flights_tbl");
        LOG.info("semanticdf-platform: demo seed data loaded into flights_tbl");
      } catch (Throwable t) {
        LOG.warn("semanticdf-platform: demo seed data load failed: " + t.getMessage());
      }
    }
    YamlModelRegistry yamlRegistry = YamlModelRegistry.load(modelsDir, spark);
    ModelRegistry models = new HotReloadingModelRegistry(yamlRegistry);
    // v0.3.1: capture the loaded YAML models as a Map for the
    // engine-portable engine providers' model registries. The Spark
    // provider ignores this map (its query path uses core.Model
    // directly via the ModelRegistry above).
    final java.util.Map<String, io.semanticdf.core.model.Model> loadedCoreModels =
        yamlRegistry.getAllModels();
    StreamingQueryLauncher launcher = new SparkStreamingQueryLauncher(spark);
    // v0.3.1 Phase 5: engine-portable streaming launcher. The
    // StreamingService dispatches here when the model is registered
    // as a core.Model (via models.getModel). For Spark-only deployments,
    // this is a no-op add (the portable launcher is Spark-coupled
    // but takes the engine-portable Model). Future work: register
    // non-Spark engines (Trino / DuckDB / PG / Hera / UC / HMS) as
    // alternative portable launchers.
    PortableStreamingQueryLauncher portableLauncher = new SparkPortableStreamingQueryLauncher(spark);

    LOG.info(
        "semanticdf-platform: loaded "
            + yamlRegistry.size()
            + " models from "
            + modelsDir
            + ": "
            + yamlRegistry.registeredModels()
            + " (hot-reload wrapper enabled: H3)");

    // --- Stream catalog (bulk startup reconciliation) ---
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
        LOG.info(
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

    // Cache seam for ModelService and QueryService:
    //   - When a successful register() bumps CURRENT_VERSION, ModelService
    //     calls cache.invalidateModel(name).
    //   - QueryService.runQuery consults the cache before compiling.
    // Default: NoOp (no caching -- every query re-executes the Spark plan).
    // Opt-in via SEMANTICDF_RESULT_CACHE=memory (bounded LRU).
    final ResultCache resultCache = buildResultCacheFromEnv();

    // v0.3.1 Phase 4: construct the engine registry with Spark as the
    // default engine. The QueryService uses this registry for the
    // engine-portable query path (when the model is registered as a
    // core.Model). Per the design doc, the platform's QueryService
    // routes through the engine registry instead of going direct to
    // Spark — this is what makes the platform engine-portable.
    //
    // Scala-side construction: ScalaEngineProvider takes a Scala
    // Map<String, SemanticTable> (the sparkTableRegistry) which is
    // only used for the LEGACY `explain` path. For Phase 4's query
    // path (portable, uses core.Model), the map is unused. We pass
    // v0.3.1: optionally construct DuckDB and PostgreSQL engines
    // via reflection (so the platform pom doesn't need direct deps
    // on those adapters at compile time). When the adapter jars
    // are on the classpath, the engines register themselves.
    // In connectMode (production), the engines are real Spark Connect
    // servers and these local fallbacks are skipped (the multi-engine
    // demo only applies to the local-Spark path).
    java.util.Optional<?> duckdbEngine = connectMode
        ? java.util.Optional.empty()
        : buildDuckDBEngineOptional(spark);
    java.util.Optional<?> pgEngine = connectMode
        ? java.util.Optional.empty()
        : buildPostgreSQLEngineOptional(spark);

    // v0.3.1: pass the loaded YAML models to the engine registry so
    // the DuckDB/PostgreSQL providers' modelRegistry is non-empty.
    // Per scala-data-driven-refactor §1: data (models) in core,
    // behavior (engine lookup) in the adapter.
    io.semanticdf.core.engine.MCPEngineRegistry engineRegistry = buildEngineRegistry(
        spark, duckdbEngine, pgEngine,
        loadedCoreModels,
        loadedCoreModels);
    LOG.info(
        "semanticdf-platform: engine registry default='{}' available={}",
        engineRegistry.defaultEngine(),
        engineRegistry.availableProviders());

    // Bind all 5 services into one Endpoint.
    Endpoint endpoint = Endpoint.builder()
        .bind(new ModelService(modelStore, spark, resultCache, models))
        .bind(new QueryService(models, spark, resultCache, engineRegistry))
        .bind(new StreamingService(models, launcher, handles, catalog, portableLauncher))
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
    // This is the verification-mode path. Mirrors the Spark Connect
    // toggle: opt-in, not a default-on cutover.
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
        LOG.info(
            "semanticdf-platform: registered deployment with external Restate at "
                + externalIngress.replaceAll(":8080/?$", ":9070")
                + " -- service handler URL = " + handlerUrl
                + " deploymentId=" + deploymentId);
      } catch (Exception e) {
        LOG.warn(
            "semanticdf-platform: external Restate registration failed (continuing "
                + "with in-process runtime only): " + e.getMessage());
      }
    }

    // Bulk startup reconciliation. After the HTTP server is
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
    // block main(). Reasons:
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
                  LOG.info(
                      "semanticdf-platform: startup reconciliation complete — "
                          + "total="
                          + sweepSummary.total()
                          + " acted="
                          + sweepSummary.actedOn()
                          + " failed="
                          + sweepSummary.failed());
                } catch (RuntimeException sweepRe) {
                  LOG.warn(
                      "semanticdf-platform: startup reconciliation failed: "
                          + sweepRe.getMessage()
                          + " (operators must invoke /restart manually)");
                }
              },
              "semanticdf-platform-reconcile");
      sweepThread.setDaemon(true);
      sweepThread.start();
      LOG.info(
          "semanticdf-platform: startup reconciliation scheduled (daemon thread); "
              + "main continues without blocking.");
    }

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      LOG.info("semanticdf-platform: shutdown hook firing; bound port "
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
        LOG.info("semanticdf-platform: drained " + drained
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
        LOG.info("semanticdf-platform: SparkSession stopped");
      } catch (Throwable t) {
        LOG.warn("semanticdf-platform: spark.stop() failed: " + t.getMessage());
      }

      // Release Postgres connections. Done last so the platform
      // doesn't interrupt itself mid-drain. The drain uses runtime
      // queries (not Postgres), so this only affects the
      // StreamCatalog's HikariCP pool.
      final StreamCatalog catalogForShutdown = catalog;
      if (catalogForShutdown != null) {
        try {
          catalogForShutdown.close();
          LOG.info("semanticdf-platform: StreamCatalog closed");
        } catch (Throwable t) {
          LOG.warn(
              "semanticdf-platform: StreamCatalog close() failed: " + t.getMessage());
        }
      }
      // PR-A: release the audit-event store's HikariCP pool (only
      // meaningful when SEMANTICDF_AUDIT_PERSIST=true). NoOp store
      // has nothing to close.
      if (auditStore != null) {
        try {
          auditStore.close();
          LOG.info("semanticdf-platform: AuditEventStore closed");
        } catch (Throwable t) {
          LOG.warn(
              "semanticdf-platform: AuditEventStore close() failed: " + t.getMessage());
        }
      }
      // PR-B: release the model store's HikariCP pool (only
      // meaningful when SEMANTICDF_MODELS_PERSIST=true). NoOp store
      // has nothing to close.
      if (modelStore != null) {
        try {
          modelStore.close();
          LOG.info("semanticdf-platform: ModelStore closed");
        } catch (Throwable t) {
          LOG.warn(
              "semanticdf-platform: ModelStore close() failed: " + t.getMessage());
        }
      }
    }, "semanticdf-platform-shutdown"));

    LOG.info("semanticdf-platform listening on http://localhost:" + port);
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
      LOG.warn(
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
      LOG.warn(
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
        LOG.warn(
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
          LOG.warn(
              "semanticdf-platform: drain failed for stream-id="
                  + streamId
                  + ": "
                  + cause.getMessage());
        }
      }
    }
    LOG.info(
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
        LOG.warn(
            "semanticdf-platform: non-positive SEMANTICDF_DRAIN_MAX_PARALLEL='"
                + raw
                + "', using default "
                + DEFAULT_DRAIN_MAX_PARALLEL);
        return DEFAULT_DRAIN_MAX_PARALLEL;
      }
      if (v > MAX_DRAIN_MAX_PARALLEL) {
        LOG.warn(
            "semanticdf-platform: SEMANTICDF_DRAIN_MAX_PARALLEL='"
                + raw
                + "' exceeds max "
                + MAX_DRAIN_MAX_PARALLEL
                + ", clamping");
        return MAX_DRAIN_MAX_PARALLEL;
      }
      return v;
    } catch (NumberFormatException e) {
      LOG.warn(
          "semanticdf-platform: invalid SEMANTICDF_DRAIN_MAX_PARALLEL='"
              + raw
              + "', using default "
          + DEFAULT_DRAIN_MAX_PARALLEL);
      return DEFAULT_DRAIN_MAX_PARALLEL;
    }
  }

  /**
   * v0.3.1 Phase 4: build the {@link io.semanticdf.core.engine.MCPEngineRegistry}
   * with Spark as the default engine.
   *
   * <p>The registry is what makes the platform engine-portable. The
   * QueryService routes through it when the model is registered as a
   * {@code core.Model}. For v0.3.1, only Spark is wired (no other
   * engine providers are registered); future work adds Trino / DuckDB /
   * PG / Hera / UC / HMS providers (per the platform-architecture.md
   * "engine-agnostic" goal).
   *
   * <p>Per the design doc, the Spark provider's
   * {@code sparkTableRegistry} is only used for the LEGACY {@code explain}
   * path; the portable query path uses the Model directly. We pass an
   * empty Scala map.
   *
   * <p>Per {@code error-handling-style.md}: registry construction throws
   * {@code IllegalArgumentException} if the default engine is
   * unregistered or unavailable at startup (per the MCPEngineRegistry
   * doc: "misconfigured boots must fail loud"). The platform fails to
   * start cleanly with a typed error — operators see the misconfig.
   */
  static io.semanticdf.core.engine.MCPEngineRegistry buildEngineRegistry(
      org.apache.spark.sql.SparkSession spark) {
    // Per scala-impact-analysis: this 1-arg overload is the original
    // Spark-only path (PRs #453, #458). For the multi-engine demo,
    // use the 5-arg overload below.
    return io.semanticdf.spark.PlatformEngineRegistryBuilder.buildSparkDefaultStatic(spark);
  }

  /** v0.3.1: construct a DuckDB engine (in-memory, with sample
    * data) via reflection. Returns Optional.empty() if the duckdb
    * adapter is not on the classpath. The shared in-memory cache
    * is used so subsequent connections see the same data.
    * Per scala-jvm-safety: the DuckDB connection is opened on
    * demand by the engine; no native resource is held here. */
  @SuppressWarnings("unchecked")
  static java.util.Optional<?> buildDuckDBEngineOptional(
      org.apache.spark.sql.SparkSession spark) {
    try {
      // Per scala-jvm-safety: use a shared in-memory DuckDB
      // (cache=shared&name=...) so engine queries see the seeded
      // data. The default in-memory is per-connection which would
      // lose the seed between engine construction and query.
      String cacheName = "semanticdf-demo";
      String jdbcUrl = "jdbc:duckdb:?cache=shared&name=" + cacheName;
      try (java.sql.Connection conn = java.sql.DriverManager
          .getConnection(jdbcUrl);
          java.sql.Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE IF NOT EXISTS flights_tbl (" +
            "carrier VARCHAR, flight_count BIGINT, total_distance BIGINT)");
        try (java.sql.ResultSet rs = stmt.executeQuery(
            "SELECT COUNT(*) FROM flights_tbl")) {
          rs.next();
          if (rs.getInt(1) == 0) {
            stmt.execute("INSERT INTO flights_tbl VALUES " +
                "('AA', 1, 100), ('AA', 2, 200), ('UA', 3, 300)");
          }
        }
      }
      Class<?> engineCls = Class.forName("io.semanticdf.duckdb.DuckDBEngine");
      Class<?> jdcCls = Class.forName("io.semanticdf.duckdb.JdbcDuckDBConnection");
      Object engine = engineCls.getMethod("instance").invoke(null);
      Class<?> func0Cls = Class.forName("scala.Function0");
      Object func0 = java.lang.reflect.Proxy.newProxyInstance(
          func0Cls.getClassLoader(),
          new Class<?>[] { func0Cls },
          (proxy, method, args) -> {
            if ("apply".equals(method.getName())) {
              return jdcCls.getMethod("fromUrl", String.class)
                  .invoke(null, jdbcUrl);
            }
            return null;
          });
      engineCls.getMethod("withConnectionFactory", func0Cls)
          .invoke(engine, func0);
      return java.util.Optional.of(engine);
    } catch (Throwable t) {
      LOG.warn("semanticdf-platform: DuckDB engine unavailable: " + t.getMessage());
      return java.util.Optional.empty();
    }
  }

  /** v0.3.1: construct a PostgreSQL engine (JDBC to localhost:5432)
    * via reflection. Returns Optional.empty() if the postgresql
    * adapter is not on the classpath or the database is unreachable. */
  @SuppressWarnings("unchecked")
  static java.util.Optional<?> buildPostgreSQLEngineOptional(
      org.apache.spark.sql.SparkSession spark) {
    String jdbcUrl = System.getenv().getOrDefault(
        "SEMANTICDF_CATALOG_JDBC_URL", "jdbc:postgresql://localhost:5432/semanticdf");
    String user = System.getenv().getOrDefault("SEMANTICDF_CATALOG_USER", "semanticdf");
    String pass = System.getenv().getOrDefault("SEMANTICDF_CATALOG_PASSWORD", "semanticdf");
    try {
      Class<?> clientCls = Class.forName("io.semanticdf.postgresql.JdbcPostgreSqlClient");
      Class<?> engineCls = Class.forName("io.semanticdf.postgresql.PostgreSqlEngine");
      // Per scala-error-handling: extract the database from the
      // JDBC URL path (jdbc:postgresql://host:port/dbname). Avoids
      // hardcoding "seman" or "public" - works for any DB name.
      String pgDb = "postgresql";
      if (jdbcUrl != null && jdbcUrl.contains("/")) {
        String tail = jdbcUrl.substring(jdbcUrl.lastIndexOf("/") + 1);
        if (!tail.isBlank() && !tail.contains("?")) {
          pgDb = tail;
        }
      }
      Object client = clientCls.getConstructors()[0].newInstance(jdbcUrl, user, pass);
      // Per scala-impact-analysis: find the right constructor by parameter types
      // (PostgreSqlClient, String). The default getConstructors().head can
      // pick a synthetic Scala bridge in some builds.
      java.lang.reflect.Constructor<?> pgCtor = null;
      for (java.lang.reflect.Constructor<?> c : engineCls.getConstructors()) {
        if (c.getParameterCount() == 2
            && c.getParameterTypes()[0].getName().endsWith("PostgreSqlClient")
            && c.getParameterTypes()[1] == String.class) {
          pgCtor = c;
          break;
        }
      }
      if (pgCtor == null) {
        pgCtor = engineCls.getConstructors()[0];
      }
      Object engine = pgCtor.newInstance(client, "semanticdf");
      System.err.println("[DEBUG-PG] engine identity: " +
          engineCls.getMethod("identity").invoke(engine)
          + " dbField=" + engineCls.getMethod("database").invoke(engine));
      // Seed the Postgres table with the same sample data so queries
      // find rows (re-runs are idempotent via IF NOT EXISTS + count check).
      try (java.sql.Connection conn = java.sql.DriverManager
          .getConnection(jdbcUrl, user, pass);
          java.sql.Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE IF NOT EXISTS flights_tbl (" +
            "carrier VARCHAR, flight_count BIGINT, total_distance BIGINT)");
        try (java.sql.ResultSet rs = stmt.executeQuery(
            "SELECT COUNT(*) FROM flights_tbl")) {
          rs.next();
          if (rs.getInt(1) == 0) {
            stmt.execute("INSERT INTO flights_tbl VALUES " +
                "('AA', 1, 100), ('AA', 2, 200), ('UA', 3, 300)");
          }
        }
      }
      return java.util.Optional.of(engine);
    } catch (Throwable t) {
      LOG.warn("semanticdf-platform: PostgreSQL engine unavailable: " + t.getMessage());
      return java.util.Optional.empty();
    }
  }

  /** v0.3.1: 5-arg overload that constructs the multi-engine
    * registry. Per scala-data-driven-refactor §1: data in core
    * (the engine-portable model + SourceRef), behavior in
    * adapters (the engine providers). The DuckDB/PostgreSQL
    * providers are loaded via reflection (the platform pom
    * doesn't have hard compile-time deps on those adapters).
    */
  static io.semanticdf.core.engine.MCPEngineRegistry buildEngineRegistry(
      org.apache.spark.sql.SparkSession spark,
      java.util.Optional<?> duckdb,
      java.util.Optional<?> postgres,
      java.util.Map<String, io.semanticdf.core.model.Model> duckModelRegistry,
      java.util.Map<String, io.semanticdf.core.model.Model> pgModelRegistry) {
    return io.semanticdf.spark.PlatformEngineRegistryJavaBuilder.build(
        spark,
        (java.util.Optional<Object>) (java.util.Optional<?>) duckdb,
        (java.util.Optional<Object>) (java.util.Optional<?>) postgres,
        duckModelRegistry,
        pgModelRegistry);
  }
}
