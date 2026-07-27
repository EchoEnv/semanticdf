package io.semanticdf.platform;

import dev.restate.sdk.endpoint.Endpoint;
import dev.restate.sdk.http.vertx.RestateHttpServer;

import io.semanticdf.platform.audit.AuditService;
import io.semanticdf.platform.catalog.CatalogService;
import io.semanticdf.platform.model.ModelService;
import io.semanticdf.platform.query.QueryService;
import io.semanticdf.platform.streaming.StreamingQueryHandleRegistry;
import io.semanticdf.platform.streaming.StreamingQueryLauncher;
import io.semanticdf.platform.streaming.StreamingService;
import io.semanticdf.platform.streaming.ModelRegistry;

import java.io.IOException;

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
    // --- Streaming lifecycle wiring ---
    //
    // The handle registry is a plain runtime-local map — no deps.
    // The model registry and query launcher are wired from the
    // environment in a follow-up (when the CatalogService owns model
    // loading and the engine adapter owns SparkSession creation).
    // For P1 they throw clearly so operators see an actionable error
    // rather than a silent no-op.
    StreamingQueryHandleRegistry handles = new StreamingQueryHandleRegistry();
    ModelRegistry models = PlatformApplication::modelNotConfigured;
    StreamingQueryLauncher launcher = PlatformApplication::launcherNotConfigured;

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

  // --- P1 placeholder implementations (replaced by catalog + engine wiring) ---

  /** Throws until the CatalogService integration configures a real model source. */
  private static io.semanticdf.SemanticTable modelNotConfigured(String modelName) {
    throw new IllegalStateException(
        "ModelRegistry not configured. Set up the catalog integration to load models "
            + "(see docs/design/platform-architecture.md). Requested model: " + modelName);
  }

  /** Throws until the engine adapter provides a SparkSession-backed launcher. */
  private static org.apache.spark.sql.streaming.StreamingQuery launcherNotConfigured(
      io.semanticdf.SemanticTable model, StreamingService.StreamRunRequest request) {
    throw new IllegalStateException(
        "StreamingQueryLauncher not configured. The engine adapter must provide a "
            + "SparkSession-backed launcher (see docs/design/platform-architecture.md).");
  }
}
