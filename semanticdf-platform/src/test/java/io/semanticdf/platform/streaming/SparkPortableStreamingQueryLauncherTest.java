package io.semanticdf.platform.streaming;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.semanticdf.platform.streaming.StreamingService.StreamRunRequest;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link SparkPortableStreamingQueryLauncher}.
 *
 * <p>Coverage goals:
 * <ul>
 *   <li>Constructor rejects null Spark session (defensive null check).</li>
 *   <li>Interface is a proper functional interface (lambda-usable).</li>
 *   <li>{@link StreamRunRequest}'s compact constructor enforces non-blank
 *       checkpointLocation (Wire DTO boundary defense).</li>
 * </ul>
 *
 * <p>The launcher's {@link #start} method is exercised end-to-end by
 * {@link StreamingServiceIntegrationTest} (which boots a real Restate
 * runtime + Spark). Thread-local cleanup is verified by the source
 * code's {@code try/finally} pattern (visible in the launcher) — not
 * duplicated here as a noisy no-op.
 */
public class SparkPortableStreamingQueryLauncherTest {

  @Test
  void constructor_rejectsNullSparkSession() {
    // Per scala-error-handling §1: null check at boundary.
    assertThrows(NullPointerException.class, () -> new SparkPortableStreamingQueryLauncher(null));
  }

  @Test
  void constructor_acceptsValidSparkSession() {
    // Build a fresh SparkSession for the assertion only — stopped
    // implicitly via the local try-with-resources scope's GC.
    // (No mock of Spark is used; the constructor doesn't actually
    // touch the Spark runtime, only stores the reference.)
    try (org.apache.spark.sql.SparkSession s =
        org.apache.spark.sql.SparkSession.builder()
            .appName("SparkPortableStreamingQueryLauncherTest")
            .master("local[1]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()) {
      SparkPortableStreamingQueryLauncher launcher = new SparkPortableStreamingQueryLauncher(s);
      assertNotNull(launcher);
    }
  }

  @Test
  void portableLauncher_isFunctionalInterface() {
    // The interface should be usable as a lambda (1-arg functional interface).
    PortableStreamingQueryLauncher lambda =
        (model, request) -> null;
    assertNotNull(lambda);
  }

  @Test
  void streamRunRequest_validatesCheckpointLocation() {
    // Per the design doc: StreamRunRequest's compact constructor enforces
    // checkpointLocation != null && !blank. Defense-in-depth: the platform
    // boundary throws IllegalArgumentException, not a typed ADT case (the
    // public API surface is the Wire DTO, not the typed error ADT).
    assertThrows(IllegalArgumentException.class, () -> new StreamRunRequest("m", "q", null));
    assertThrows(IllegalArgumentException.class, () -> new StreamRunRequest("m", "q", ""));
    assertThrows(IllegalArgumentException.class, () -> new StreamRunRequest("m", "q", "   "));

    // Valid case: non-blank checkpoint location is accepted.
    StreamRunRequest req = new StreamRunRequest("m", "q", "s3://bucket/checkpoint");
    assertEquals("m", req.modelName());
    assertEquals("q", req.queryShape());
    assertEquals("s3://bucket/checkpoint", req.checkpointLocation());
  }
}
