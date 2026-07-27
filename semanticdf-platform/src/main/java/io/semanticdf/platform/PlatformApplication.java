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
import org.apache.spark.sql.SparkSession;

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
    }, "semanticdf-platform-shutdown"));

    System.out.println("semanticdf-platform listening on http://localhost:" + port);
  }
}
