package io.semanticdf.platform;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.semanticdf.tools.SdfSession;
import org.junit.jupiter.api.Test;

/**
 * Tests that pin the PR #240 contract: the platform's Spark Connect
 * mode is selected purely by the {@code SEMANTICDF_SPARK_CONNECT_URL}
 * env var, and the library's {@link SdfSession} is the single source
 * of truth for env-var name + factory.
 *
 * <p>We don't boot a full {@code PlatformApplication.main} (which
 * creates a {@code SparkSession} and binds Restate); the actual
 * env-var-reading code lives in {@code SdfSession.createFromEnv}
 * which is library-side and covered by {@code SdfSessionSpec}. This
 * test class verifies the platform CONTRACTS:
 *
 * <ol>
 *   <li>The library exposes a typed env-var constant (no string-typo
 *       drift between platform and library).
 *   <li>The default behavior is local mode (env unset).
 *   <li>Connect URLs are redacted before being logged.
 * </ol>
 *
 * <p>End-to-end Spark Connect verification requires a running
 * Spark Connect container and is out of scope for this test class
 * (planned as a separate Testcontainers {@code *IT} profile).
 */
class PlatformApplicationSparkConnectTest {

  @Test
  void envVarName_matchesLibraryConstant() {
    // Pin: SdfSession.RemoteUrlEnvVar is the single source of truth
    // for the env-var name. The platform's PlatformApplication calls
    // SdfSession.createFromEnv, which reads this same constant.
    // Any rename of the env var flows through both sides atomically.
    assertEquals("SEMANTICDF_SPARK_CONNECT_URL", SdfSession.RemoteUrlEnvVar());
  }

  @Test
  void envVarUnset_localModeIsTheDefault() {
    // Without the env var, the platform logs "local Spark mode" and
    // constructs an in-process SparkSession via the standard
    // SparkSession.builder().master(...) path inside SdfSession.create.
    // This test pins the default-state assumption that production
    // deployments must opt out of by setting the env var.
    String url = System.getenv(SdfSession.RemoteUrlEnvVar());
    assertNull(url, "by default SEMANTICDF_SPARK_CONNECT_URL is unset — local mode");
  }

  // Note: 'env var set' and 'createFromEnv end-to-end' tests were
  // REMOVED — the original reviewer (PR #240 dispatch) flagged them
  // as false-positive coverage that relied on System.setProperty instead
  // of env-var mutation, didn't actually exercise the createFromEnv
  // path, and could pass while the production code path fails. The
  // library's SdfSessionSpec covers createFromEnv end-to-end; the
  // platform's contract is the env-var constant name + default.
  // Real Connect deployment coverage needs a Testcontainers *IT
  // profile (follow-up PR).

  @Test
  void redactConnectUrl_stripsSemicolonDelimitedToken() {
    // Spark Connect URLs of the form sc://host:port;token=... carry
    // the session token after the first ';'. We strip it.
    String redacted = PlatformApplication.redactConnectUrl("sc://spark:15002;token=abc123");
    assertEquals("sc://spark:15002<redacted>", redacted);
  }

  @Test
  void redactConnectUrl_stripsQueryDelimitedToken() {
    // sc://host:port?token=... — strip after '?'
    String redacted = PlatformApplication.redactConnectUrl("sc://spark:15002?token=abc123");
    assertEquals("sc://spark:15002<redacted>", redacted);
  }

  @Test
  void redactConnectUrl_passesThroughCleanUrls() {
    // No delimiter → no redaction. sc:// URLs without credentials
    // are safe to log as-is.
    assertEquals(
        "sc://spark:15002", PlatformApplication.redactConnectUrl("sc://spark:15002"));
  }

  @Test
  void redactConnectUrl_handlesNullAndEmpty() {
    assertEquals("", PlatformApplication.redactConnectUrl(null));
    assertEquals("", PlatformApplication.redactConnectUrl(""));
  }
}
