package io.semanticdf.platform;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link PlatformApplication#registerWithExternalRestate(String, int)}.
 *
 * <p>The external-Restate registration path sends a {@code POST /deployments}
 * to the external Restate's admin API. We can't reach the real Restate
 * container in a fast unit test, so we spin up a tiny in-process
 * {@link HttpServer} that records the request body and returns a valid
 * {@code RegisterDeploymentResponse}. This proves:
 *
 * <ol>
 *   <li>The admin URL is derived from the ingress URL by swapping
 *       {@code :8080} for {@code :9070}.
 *   <li>The service-handler URL uses {@code host.docker.internal:<boundPort>}.
 *   <li>A failure on the admin side surfaces as an exception (caller
 *       decides what to do).
 * </ol>
 *
 * <p>Key implementation detail discovered while writing this test: the
 * Restate SDK's Java OpenAPI client concatenates {@code basePath + "/" + operationPath}
 * WITHOUT normalizing the separator. If {@code basePath} ends in {@code /},
 * the request becomes {@code POST //deployments} which most HTTP servers
 * (including Restate 1.7) treat as a different path. The production code
 * strips the trailing {@code /} before passing to {@code ApiClient}'s
 * constructor; the {@code registerWithExternalRestate_callIsGatedByEnvVar}
 * test pins this invariant at the source level.
 */
class PlatformApplicationExternalRestateTest {

  private HttpServer fakeAdminServer;
  private final AtomicReference<String> capturedPath = new AtomicReference<>();
  private final AtomicReference<String> capturedMethod = new AtomicReference<>();
  private final AtomicReference<String> capturedBody = new AtomicReference<>();
  private int fakeAdminPort;

  @BeforeEach
  void setUp() throws IOException {
    fakeAdminServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    fakeAdminServer.createContext(
        "/deployments",
        exchange -> {
          String path = exchange.getRequestURI().getPath();
          capturedPath.set(path);
          capturedMethod.set(exchange.getRequestMethod());
          String body =
              new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
          capturedBody.set(body);
          byte[] response =
              "{\"id\":\"dp_test_12345\",\"services\":[]}".getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(200, response.length);
          exchange.getResponseBody().write(response);
          exchange.close();
        });
    fakeAdminServer.start();
    fakeAdminPort = fakeAdminServer.getAddress().getPort();
  }

  @AfterEach
  void tearDown() {
    if (fakeAdminServer != null) {
      fakeAdminServer.stop(0);
    }
  }

  /**
   * Happy path: registration swaps {@code :8080} → {@code :9070} and
   * sends a {@code POST /deployments} whose body names the service-handler URL.
   */
  @Test
  void registerWithExternalRestate_sendsPostToAdminPortWithServiceHandlerUri() throws Exception {
    String deploymentId =
        PlatformApplication.registerWithExternalRestate(
            "http://127.0.0.1:" + fakeAdminPort + "/", 9091);

    assertEquals("dp_test_12345", deploymentId);
    assertEquals("POST", capturedMethod.get());
    assertEquals("/deployments", capturedPath.get(),
        "SDK path must be /deployments, not //deployments (test-pinned regression)");
    assertNotNull(capturedBody.get());
    assertTrue(
        capturedBody.get().contains("\"uri\":\"http://host.docker.internal:9091\""),
        "expected service-handler uri in request body, got: " + capturedBody.get());
  }

  /**
   * The boundPort parameter is propagated into the service-handler URI.
   * If the platform is on port 9876, the URI MUST say 9876.
   */
  @Test
  void registerWithExternalRestate_usesBoundPortInServiceHandlerUri() throws Exception {
    PlatformApplication.registerWithExternalRestate(
        "http://127.0.0.1:" + fakeAdminPort + "/", 9876);
    assertTrue(
        capturedBody.get().contains(":9876"),
        "service-handler URI should carry boundPort=9876, got: " + capturedBody.get());
  }

  /**
   * Ingress URL with no trailing slash — must still hit {@code /deployments}
   * (not {@code //deployments}). The basePath-stripping logic in
   * {@link PlatformApplication#registerWithExternalRestate} handles both.
   */
  @Test
  void registerWithExternalRestate_handlesIngressUrlWithoutTrailingSlash() throws Exception {
    PlatformApplication.registerWithExternalRestate(
        "http://127.0.0.1:" + fakeAdminPort, 9091);
    assertEquals("/deployments", capturedPath.get());
  }

  /**
   * Failure path: when the admin server returns a non-2xx, the SDK
   * throws. The caller (PlatformApplication.main) catches and continues.
   */
  @Test
  void registerWithExternalRestate_propagatesExceptionOnFailure() throws Exception {
    fakeAdminServer.removeContext("/deployments");
    fakeAdminServer.createContext(
        "/deployments",
        exchange -> {
          byte[] body = "{\"message\":\"bad request\"}".getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(400, body.length);
          exchange.getResponseBody().write(body);
          exchange.close();
        });

    assertThrows(
        Exception.class,
        () ->
            PlatformApplication.registerWithExternalRestate(
                "http://127.0.0.1:" + fakeAdminPort + "/", 9091));
  }

  /**
   * Structural check: the call site must be inside the
   * {@code if (RESTATE_INGRESS_URL != null && ...)} guard. Without this,
   * the platform would attempt registration unconditionally.
   */
  @Test
  void registerWithExternalRestate_callIsGatedByEnvVar() throws Exception {
    String src =
        new String(
            java.nio.file.Files.readAllBytes(
                java.nio.file.Path.of(
                    "src/main/java/io/semanticdf/platform/PlatformApplication.java")),
            StandardCharsets.UTF_8);
    // The 2-arg overload delegates to the 3-arg one with System.getenv().
    // The call site in main() uses the 3-arg form. Find either.
    int call2 = src.indexOf("registerWithExternalRestate(externalIngress, boundPort);");
    int call3 = src.indexOf("registerWithExternalRestate(\n                externalIngress,");
    int callIdx = (call2 > 0) ? call2 : call3;
    int guardIdx = src.lastIndexOf("if (externalIngress != null && !externalIngress.isBlank())");
    assertTrue(callIdx > 0, "call site must exist");
    assertTrue(guardIdx > 0 && guardIdx < callIdx, "call must be inside the env-var guard");
  }

  /**
   * Structural check: the 3-arg overload accepts a non-null/non-blank
   * env-var value and uses it as the service-handler URL. Pins the
   * SEMANTICDF_SERVICE_HANDLER_URL contract at the source level.
   */
  @Test
  void registerWithExternalRestate_envVarContractPinnedAtSourceLevel() throws Exception {
    String src =
        new String(
            java.nio.file.Files.readAllBytes(
                java.nio.file.Path.of(
                    "src/main/java/io/semanticdf/platform/PlatformApplication.java")),
            StandardCharsets.UTF_8);
    // The 3-arg overload must read the env-var parameter and use it as the
    // service-handler URL when non-blank.
    assertTrue(
        src.contains("SEMANTICDF_SERVICE_HANDLER_URL"),
        "source must reference SEMANTICDF_SERVICE_HANDLER_URL");
    // The 2-arg overload must read the env var via System.getenv.
    assertTrue(
        src.contains("System.getenv(\"SEMANTICDF_SERVICE_HANDLER_URL\")"),
        "2-arg overload must read env var via System.getenv");
  }

  /**
   * The basePath must be normalized so the SDK doesn't send {@code //deployments}
   * (double-slash). This is the keystone regression test for the trailing-slash
   * stripping logic in {@link PlatformApplication#registerWithExternalRestate}.
   */
  @Test
  void registerWithExternalRestate_stripsTrailingSlashFromBasePath() throws Exception {
    String src =
        new String(
            java.nio.file.Files.readAllBytes(
                java.nio.file.Path.of(
                    "src/main/java/io/semanticdf/platform/PlatformApplication.java")),
            StandardCharsets.UTF_8);
    // The basePath construction must include a trailing-slash strip.
    assertTrue(
        src.contains(".replaceAll(\"/$\", \"\")"),
        "basePath must strip trailing slash to avoid SDK emitting //deployments");
  }
  /**
   * When {@code SEMANTICDF_SERVICE_HANDLER_URL} is set (non-null, non-blank),
   * the registration sends that exact URL to the external Restate. This
   * lets the platform register with a Restate that's reachable only via
   * an external IP (e.g., behind a host firewall that blocks the Docker
   * bridge).
   */
  @Test
  void registerWithExternalRestate_honorsServiceHandlerUrlOverride() throws Exception {
    String override = "http://203.0.113.1:9093/";
    PlatformApplication.registerWithExternalRestate(
        "http://127.0.0.1:" + fakeAdminPort + "/", 9091, override);
    assertTrue(
        capturedBody.get().contains("\"uri\":\"http://203.0.113.1:9093/\""),
        "expected override service-handler URI in request body, got: " + capturedBody.get());
    assertTrue(
        !capturedBody.get().contains("host.docker.internal"),
        "override must replace the default host.docker.internal URI, got: "
            + capturedBody.get());
  }

  /**
   * When the override is null or blank, falls back to the default
   * {@code host.docker.internal:<boundPort>}. Preserves the v0.2.2 default
   * for the docker-compose dev setup.
   */
  @Test
  void registerWithExternalRestate_fallsBackToDefaultWhenOverrideBlank() throws Exception {
    PlatformApplication.registerWithExternalRestate(
        "http://127.0.0.1:" + fakeAdminPort + "/", 9091, "");
    assertTrue(
        capturedBody.get().contains("\"uri\":\"http://host.docker.internal:9091\""),
        "expected default service-handler URI when override is blank, got: "
            + capturedBody.get());
  }

  /**
   * The 2-arg overload (no env-var parameter) reads
   * {@code SEMANTICDF_SERVICE_HANDLER_URL} from the environment via
   * {@link System#getenv}. In a unit test JVM with no such env var set,
   * it falls back to the default. This pins the env-var contract at the
   * test-suite level.
   */
  @Test
  void registerWithExternalRestate_twoArgOverloadFallsBackToDefaultInTestJvm() throws Exception {
    String preExisting = System.getenv("SEMANTICDF_SERVICE_HANDLER_URL");
    assertTrue(preExisting == null || preExisting.isBlank(),
        "test JVM must not have SEMANTICDF_SERVICE_HANDLER_URL set; was: "
            + preExisting);

    PlatformApplication.registerWithExternalRestate(
        "http://127.0.0.1:" + fakeAdminPort + "/", 9091);
    assertTrue(
        capturedBody.get().contains("\"uri\":\"http://host.docker.internal:9091\""),
        "2-arg overload without env var must use the default, got: "
            + capturedBody.get());
  }
}
