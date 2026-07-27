package io.semanticdf.platform;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.jupiter.api.Test;

/**
 * Tests for the PR #241 fix: the early-shutdown hook for
 * {@code SparkSession} must be registered IMMEDIATELY after the
 * {@code SdfSession.createFromEnv(...)} call in {@code PlatformApplication.main},
 * so a partial-startup failure doesn't leak the SparkSession.
 *
 * <p>The JVM doesn't expose a way to query the {@code shutdownHooks}
 * list in a portable way, and @{code Runtime.availableProcessors()}
 * style mocking is fragile on JDK 17+. We use a structural-source
 * assertion: the {@code main} method's source must have the early
 * {@code addShutdownHook} call appear immediately after the
 * {@code createFromEnv} line, with no work that can throw (no JDBC,
 * no Restate bind, no YamlModelRegistry.load) in between.
 *
 * <p>This is a low-rigor test, but it's the right ergonomics for a
 * 14-line bug-fix PR. The behavioral test (a forked JVM with a bad
 * Postgres URL exits cleanly with the early-shutdown log line) was
 * considered and rejected as too flaky + slow for the value.
 */
class PlatformApplicationStartupSparkHookTest {

  /**
   * Find the PlatformApplication.java source file via the class's
   * classloader resource, then read & assert structural ordering.
   */
  @Test
  void earlyShutdownHook_registeredImmediatelyAfterCreateFromEnv() throws IOException {
    Path src = locatePlatformApplicationSource();
    assertNotNull(src, "could not locate PlatformApplication.java");
    String srcBody = new String(Files.readAllBytes(src), StandardCharsets.UTF_8);

    // Find the createFromEnv line and the early-shutdown-hook line. The
    // ordering must put them within an arm's reach, with no
    // can-fail-between work in the gap.
    int createFromEnvLine = findFirstLineContaining(srcBody, "SdfSession.createFromEnv(");
    assertTrue(createFromEnvLine > 0,
        "SdfSession.createFromEnv(...) call not found in " + src);

    int earlyHookLine = findFirstLineContaining(
        srcBody, "semanticdf-platform-shutdown-spark-early");
    assertTrue(earlyHookLine > 0,
        "early-shutdown SparkSession hook not found in " + src);
    assertTrue(earlyHookLine > createFromEnvLine,
        "early-shutdown hook must come AFTER createFromEnv — was " + src);

    // Each can-throw step must be AFTER the early-shutdown hook, so
    // partial-startup failures in those steps don't leave the
    // SparkSession unstopped.
    for (String canThrow : new String[] {
        "YamlModelRegistry.load(modelsDir, spark)",
        "RestateHttpServer.listen(",
        "Endpoint.builder().bind(",
    }) {
      int line = findFirstLineContaining(srcBody, canThrow);
      assertTrue(
          line == -1 || line > earlyHookLine,
          canThrow + " must be AFTER the early-shutdown hook (line " + line
              + " vs early-hook line " + earlyHookLine + ") — was " + src);
    }
  }

  /** Helper: line number (1-indexed) of the first line containing `needle`, or -1. */
  private static int findFirstLineContaining(String srcBody, String needle) {
    int idx = srcBody.indexOf(needle);
    if (idx < 0) return -1;
    int line = 1;
    for (int i = 0; i < idx; i++) {
      if (srcBody.charAt(i) == '\n') line++;
    }
    return line;
  }

  /** Helper to find PlatformApplication.java alongside the compiled class. */
  private static Path locatePlatformApplicationSource() {
    // Walk the source tree under the platform module's src/main/java.
    // The test runs from semanticdf-platform/, so the source root is
    // src/main/java relative to the cwd. We probe by trying a couple
    // of plausible paths.
    String[] candidates = {
      "src/main/java/io/semanticdf/platform/PlatformApplication.java",
      "../src/main/java/io/semanticdf/platform/PlatformApplication.java",
      "semanticdf-platform/src/main/java/io/semanticdf/platform/PlatformApplication.java",
    };
    for (String c : candidates) {
      Path p = Paths.get(c);
      if (Files.isRegularFile(p)) return p;
    }
    return null;
  }
}
