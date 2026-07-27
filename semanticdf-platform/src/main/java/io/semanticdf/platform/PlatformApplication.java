package io.semanticdf.platform;

import dev.restate.sdk.endpoint.Endpoint;
import dev.restate.sdk.http.vertx.RestateHttpServer;

import io.semanticdf.platform.audit.AuditService;
import io.semanticdf.platform.catalog.CatalogService;
import io.semanticdf.platform.model.ModelService;
import io.semanticdf.platform.query.QueryService;
import io.semanticdf.platform.streaming.StreamingService;

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
    // Bind all 5 services into one Endpoint.
    Endpoint endpoint = Endpoint.builder()
        .bind(new ModelService())
        .bind(new QueryService())
        .bind(new StreamingService())
        .bind(new AuditService())
        .bind(new CatalogService())
        .build();

    // Start the HTTP server on port 8080 (or $PORT). The same process
    // hosts the platform's REST surface and the Restate runtime.
    int port = Integer.parseInt(System.getenv().getOrDefault("PORT", "8080"));
    RestateHttpServer server = RestateHttpServer.listen(endpoint, port);

    // Register a JVM shutdown hook so SIGTERM (and Ctrl-C) drain the
    // server cleanly — close the listening socket, finish in-flight
    // requests, release the Vert.x event loop. Without this hook the
    // JVM exits with the server still in the middle of an accept loop,
    // and the kernel's TIME_WAIT connections linger on the next start.
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      System.out.println("semanticdf-platform: shutdown hook firing");
      try {
        server.close();
      } catch (Exception e) {
        System.err.println("semanticdf-platform: error closing server: " + e.getMessage());
      }
    }, "semanticdf-platform-shutdown"));

    System.out.println("semanticdf-platform listening on http://localhost:" + port);
  }
}
