package io.semanticdf.platform.streaming;

import dev.restate.sdk.Restate;
import dev.restate.sdk.common.StateKey;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.lang.reflect.Method;

/**
 * Tests for {@link StreamingService}.
 *
 * <p>Two layers:
 * <ul>
 *   <li><b>Checkpoint guard</b> — the compact-constructor validation on
 *       {@link StreamingService.StreamRunRequest} (Critical audit finding #7).
 *       These don't need a Restate runtime.
 *   <li><b>Constructor wiring</b> — the service requires its three deps
 *       (ModelRegistry, StreamingQueryLauncher, StreamingQueryHandleRegistry)
 *       and rejects nulls.
 *   <li><b>auditPayload JSON</b> — the private helper that builds the
 *       streaming.started audit-event payload, tested via reflection to
 *       guard against JSON injection and field-order regressions.
 * </ul>
 *
 * <p>The full lifecycle (run → state transitions → audit emit → stop) requires
 * a Restate in-process runtime; that integration test is a follow-up.
 */
class StreamingServiceTest {

  // --- Checkpoint guard (compact constructor) ---

  @Test
  void run_rejectsNullCheckpointLocation() {
    IllegalArgumentException ex =
        assertThrows(
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
    StreamingService.StreamRunRequest req =
        new StreamingService.StreamRunRequest("m", "q", "/tmp/semanticdf-checkpoints-test-1/");
    assertNotNull(req);
    assertEquals("/tmp/semanticdf-checkpoints-test-1/", req.checkpointLocation());
    assertEquals("m", req.modelName());
    assertEquals("q", req.queryShape());
  }

  // --- Constructor wiring ---

  @Test
  void constructor_rejectsNullModels() {
    assertThrows(
        NullPointerException.class,
        () ->
            new StreamingService(
                null,
                (model, req) -> null,
                new StreamingQueryHandleRegistry()));
  }

  @Test
  void constructor_rejectsNullLauncher() {
    assertThrows(
        NullPointerException.class,
        () ->
            new StreamingService(
                name -> null, null, new StreamingQueryHandleRegistry()));
  }

  @Test
  void constructor_rejectsNullHandles() {
    assertThrows(
        NullPointerException.class,
        () -> new StreamingService(name -> null, (model, req) -> null, null));
  }

  @Test
  void constructor_acceptsAllDeps() {
    StreamingService svc =
        new StreamingService(
            name -> null, (model, req) -> null, new StreamingQueryHandleRegistry());
    assertNotNull(svc);
  }

  // --- auditPayload JSON correctness ---

  @Test
  void auditPayload_containsAllRequestFields() throws Exception {
    String payload = invokeAuditPayload("s1", "orders", "sum(amount)", "/ckpt/orders");
    assertTrue(payload.contains("\"streamId\":\"s1\""), payload);
    assertTrue(payload.contains("\"modelName\":\"orders\""), payload);
    assertTrue(payload.contains("\"queryShape\":\"sum(amount)\""), payload);
    assertTrue(payload.contains("\"checkpointLocation\":\"/ckpt/orders\""), payload);
  }

  @Test
  void auditPayload_escapesQuotesAndBackslashes() throws Exception {
    // A model name with a quote and backslash must not break the JSON.
    String payload = invokeAuditPayload("s1", "mo\"del\\name", "q", "c");
    assertTrue(payload.contains("\\\""), "double-quote must be escaped: " + payload);
    assertTrue(payload.contains("\\\\"), "backslash must be escaped: " + payload);
  }

  @Test
  void auditPayload_handlesNullStreamId() throws Exception {
    // streamId is a separate param (not part of StreamRunRequest); it can be null.
    String payload = invokeAuditPayload(null, "m", "q", "/c");
    assertTrue(payload.contains("\"streamId\":\"\""), payload);
  }

  @Test
  void auditPayload_isValidJsonShape() throws Exception {
    // The payload is a flat object with 4 string fields; verify the braces
    // and quote/colon structure without pulling a JSON parser dependency.
    String payload = invokeAuditPayload("s", "m", "q", "c");
    assertTrue(payload.startsWith("{"), payload);
    assertTrue(payload.endsWith("}"), payload);
    assertEquals(4, countOccurrences(payload, "\":"), "expected 4 key-value pairs");
  }

  @Test
  void auditPayload_handlesNullModelAndQuery() throws Exception {
    // checkpointLocation can never be null (compact constructor rejects it),
    // but modelName and queryShape can. They should serialize as empty strings.
    String payload = invokeAuditPayload("s1", null, null, "/ckpt");
    assertEquals(0, countOccurrences(payload, ":null"), "nulls must be empty strings: " + payload);
    assertTrue(payload.contains("\"modelName\":\"\""), payload);
    assertTrue(payload.contains("\"queryShape\":\"\""), payload);
    assertTrue(payload.contains("\"checkpointLocation\":\"/ckpt\""), payload);
  }

  // --- safeStop exception handling (Issue #7 fix) ---

  @Test
  void safeStop_swallowsTimeoutException() {
    // A query whose stop() throws TimeoutException (Spark's internal deadline)
    // must not propagate — the handle has already been removed from the
    // registry; an unhandled exception would orphan the Spark query.
    org.apache.spark.sql.streaming.StreamingQuery query = throwingQuery(
        new java.util.concurrent.TimeoutException("stop deadline exceeded"));
    RecordingState state = new RecordingState(0L);

    assertDoesNotThrow(() -> StreamingService.safeStop(query, state));
    assertEquals(1L, (long) state.lastSetValue, "ERROR_COUNT must increment on failure");
  }

  @Test
  void safeStop_swallowsRuntimeException() {
    // Any RuntimeException from stop() is also swallowed.
    org.apache.spark.sql.streaming.StreamingQuery query =
        throwingQuery(new RuntimeException("unexpected"));
    RecordingState state = new RecordingState(5L); // existing error count

    assertDoesNotThrow(() -> StreamingService.safeStop(query, state));
    assertEquals(6L, (long) state.lastSetValue, "ERROR_COUNT must increment by exactly 1");
  }

  @Test
  void safeStop_normalStopDoesNotIncrementErrorCount() {
    // A query whose stop() succeeds must not increment ERROR_COUNT.
    org.apache.spark.sql.streaming.StreamingQuery query = normalQuery();
    RecordingState state = new RecordingState(0L);

    assertDoesNotThrow(() -> StreamingService.safeStop(query, state));
    org.junit.jupiter.api.Assertions.assertNull(
        state.lastSetValue, "ERROR_COUNT must not be touched on success");
  }

  // --- Helpers ---

  /** A {@link StreamingQuery} whose {@code stop()} throws the given exception. */
  private static org.apache.spark.sql.streaming.StreamingQuery throwingQuery(Exception ex) {
    return (org.apache.spark.sql.streaming.StreamingQuery)
        java.lang.reflect.Proxy.newProxyInstance(
            org.apache.spark.sql.streaming.StreamingQuery.class.getClassLoader(),
            new Class<?>[] {org.apache.spark.sql.streaming.StreamingQuery.class},
            (proxy, method, args) -> {
              if ("stop".equals(method.getName())) throw ex;
              Class<?> rt = method.getReturnType();
              if (rt == boolean.class) return false;
              if (rt == int.class) return 0;
              if (rt == long.class) return 0L;
              return null;
            });
  }

  /** A {@link StreamingQuery} whose {@code stop()} is a no-op. */
  private static org.apache.spark.sql.streaming.StreamingQuery normalQuery() {
    return (org.apache.spark.sql.streaming.StreamingQuery)
        java.lang.reflect.Proxy.newProxyInstance(
            org.apache.spark.sql.streaming.StreamingQuery.class.getClassLoader(),
            new Class<?>[] {org.apache.spark.sql.streaming.StreamingQuery.class},
            (proxy, method, args) -> {
              Class<?> rt = method.getReturnType();
              if (rt == boolean.class) return false;
              if (rt == int.class) return 0;
              if (rt == long.class) return 0L;
              return null;
            });
  }

  /** A minimal {@link Restate.State} that records the last set value. */
  private static final class RecordingState implements Restate.State {
    private final java.util.HashMap<String, Object> store = new java.util.HashMap<>();
    Object lastSetValue;

    RecordingState(long initialErrorCount) {
      store.put("errorCount", initialErrorCount);
    }

    @Override
    public <T> java.util.Optional<T> get(StateKey<T> key) {
      @SuppressWarnings("unchecked")
      T v = (T) store.get(key.name());
      return java.util.Optional.ofNullable(v);
    }

    @Override
    public <T> void set(StateKey<T> key, T value) {
      store.put(key.name(), value);
      lastSetValue = value;
    }

    @Override
    public void clear(StateKey<?> key) {
      store.remove(key.name());
    }

    @Override
    public java.util.Collection<String> getAllKeys() {
      return store.keySet();
    }

    @Override
    public void clearAll() {
      store.clear();
    }
  }

  private static void assertNullValue(Object o) {
    org.junit.jupiter.api.Assertions.assertNull(o);
  }

  /** Invoke the private static auditPayload method via reflection. */
  private static String invokeAuditPayload(
      String streamId, String modelName, String queryShape, String checkpointLocation)
      throws Exception {
    StreamingService.StreamRunRequest req =
        new StreamingService.StreamRunRequest(modelName, queryShape, checkpointLocation);
    Method m =
        StreamingService.class.getDeclaredMethod("auditPayload", String.class, req.getClass());
    m.setAccessible(true);
    return (String) m.invoke(null, streamId, req);
  }

  private static int countOccurrences(String haystack, String needle) {
    int count = 0;
    int idx = 0;
    while ((idx = haystack.indexOf(needle, idx)) != -1) {
      count++;
      idx += needle.length();
    }
    return count;
  }
}
