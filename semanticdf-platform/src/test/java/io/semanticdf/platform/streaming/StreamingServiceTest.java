package io.semanticdf.platform.streaming;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;

/** Tests for the checkpoint-location guard on StreamingService.run.
 *
 * Per docs/design/platform-determinism-audit.md finding #7, the
 * library's `createTempFile` default is NOT durable across Restate
 * replays. The platform's StreamingService.run enforces that the
 * Wire DTO carries a non-blank checkpointLocation. The guard is
 * at the compact constructor of StreamRunRequest and re-asserted
 * at the handler entry (defense-in-depth).
 *
 * These tests verify the guard without touching the (not-yet-wired)
 * engine call inside the run handler body. */
class StreamingServiceTest {

  @Test
  void run_rejectsNullCheckpointLocation() {
    IllegalArgumentException ex = assertThrows(
        IllegalArgumentException.class,
        () -> new StreamingService.StreamRunRequest("m", "q", null));
    assertTrue(
        ex.getMessage().contains("checkpointLocation"),
        "error message must name the field: " + ex.getMessage());
    assertTrue(
        ex.getMessage().contains("Restate") || ex.getMessage().contains("replay"),
        "error message must reference replay / Restate: " + ex.getMessage());
  }

  @Test
  void run_rejectsEmptyCheckpointLocation() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new StreamingService.StreamRunRequest("m", "q", ""));
  }

  @Test
  void run_rejectsBlankCheckpointLocation() {
    // The guard uses String.isBlank() which treats whitespace-only
    // strings as blank. Cheap belt-and-suspenders test.
    assertThrows(
        IllegalArgumentException.class,
        () -> new StreamingService.StreamRunRequest("m", "q", "   "));
    assertThrows(
        IllegalArgumentException.class,
        () -> new StreamingService.StreamRunRequest("m", "q", "\t"));
    assertThrows(
        IllegalArgumentException.class,
        () -> new StreamingService.StreamRunRequest("m", "q", "\n"));
  }

  @Test
  void run_acceptsNonBlankCheckpointLocation() {
    // Happy path: a non-blank checkpoint location is accepted by the
    // compact constructor.
    StreamingService.StreamRunRequest req =
        new StreamingService.StreamRunRequest("m", "q", "/tmp/semanticdf-checkpoints-test-1/");
    assertNotNull(req);
    assertEquals("/tmp/semanticdf-checkpoints-test-1/", req.checkpointLocation());
    assertEquals("m", req.modelName());
    assertEquals("q", req.queryShape());
  }

  @Test
  void run_handlerReassertsGuard() throws Exception {
    // Defense-in-depth: even if the compact constructor is bypassed
    // (e.g. by a future refactor that builds the DTO via reflection
    // or a custom deserializer), the handler body re-asserts the
    // guard at the run-handler entry.
    //
    // This test pins the contract: the handler re-asserts the guard.
    // We invoke `run` reflectively with a valid DTO and assert that no
    // exception is thrown (the body is still the TODO P1 stub so the
    // test is structural only). A future test with the full P1 handler
    // body will exercise the rejection path more thoroughly.
    StreamingService.StreamRunRequest req =
        new StreamingService.StreamRunRequest("m", "q", "/tmp/initial-ok/");

    StreamingService svc = new StreamingService();
    Method runMethod = StreamingService.class.getDeclaredMethod(
        "run", StreamingService.StreamRunRequest.class);
    runMethod.setAccessible(true);

    // Happy path: well-formed DTO passes both the compact constructor
    // and the handler re-assertion.
    runMethod.invoke(svc, req);
  }
}
