package io.semanticdf.platform.streaming;

import dev.restate.sdk.Restate;
import dev.restate.sdk.annotation.Shared;
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

  // --- Active-monitor loop decision matrix ---

  @Test
  void decide_nextAction_stopSignaledWins() {
    // Stop signal takes priority over termination and handle state.
    assertEquals(
        StreamingService.LoopAction.STOP_ON_SIGNAL,
        StreamingService.decideNextAction(true, true, false));
    assertEquals(
        StreamingService.LoopAction.STOP_ON_SIGNAL,
        StreamingService.decideNextAction(true, false, true));
    assertEquals(
        StreamingService.LoopAction.STOP_ON_SIGNAL,
        StreamingService.decideNextAction(true, true, true));
  }

  @Test
  void decide_nextAction_terminatedExits() {
    // No stop signal, terminated (true) → exit.
    assertEquals(
        StreamingService.LoopAction.EXIT_TERMINATED,
        StreamingService.decideNextAction(false, true, true));
  }

  @Test
  void decide_nextAction_handleAbsentExits() {
    // No stop signal, handle gone, query still alive at tick → exit.
    // (handlePresent=false is the "JVM death" condition — we cannot recover.)
    assertEquals(
        StreamingService.LoopAction.EXIT_TERMINATED,
        StreamingService.decideNextAction(false, false, false));
  }

  @Test
  void decide_nextAction_aliveContinues() {
    // No stop signal, handle present, query still running → continue.
    assertEquals(
        StreamingService.LoopAction.CONTINUE,
        StreamingService.decideNextAction(false, true, false));
  }

  @Test
  void stop_isSharedAnnotated() throws Exception {
    // The @Shared annotation is critical: without it, stop() cannot fire
    // while run() is blocked in awaitTermination(5000), so the loop would
    // never see the stop signal during the 5-second blocks. This test
    // pins the contract.
    java.lang.reflect.Method stopMethod =
        StreamingService.class.getDeclaredMethod("stop", Void.class);
    Shared shared = stopMethod.getAnnotation(Shared.class);
    assertNotNull(shared, "StreamingService.stop must be @Shared");
  }

  @Test
  void restart_isNotSharedAnnotated() throws Exception {
    // Architectural correction: restart must NOT be @Shared because it
    // writes workflow state (RESTART_COUNT, LAST_RESTART_AT, audit emit).
    // @Shared handlers are read-only by Restate contract. Without the
    // correction, invoking restart would throw at runtime.
    //
    // The trade-off: restart serializes against in-flight run for the
    // same stream-id, which is the correct ordering (don't restart
    // while the original handler is still mid-flight).
    java.lang.reflect.Method restartMethod =
        StreamingService.class.getDeclaredMethod("restart", Void.class);
    Shared shared = restartMethod.getAnnotation(Shared.class);
    org.junit.jupiter.api.Assertions.assertNull(
        shared, "StreamingService.restart must NOT be @Shared (it writes state)");
  }

  // --- Post-crash reconciliation (PR feature) ---

  /** A launcher that records the request it was started with and
   * returns a fresh proxy each call. Used to verify that
   * reconcileAfterJvmCrash calls launcher.start exactly once with
   * the right args. */
  private static final class RecordingLauncher implements StreamingQueryLauncher {
    final java.util.List<StreamingService.StreamRunRequest> started = new java.util.ArrayList<>();
    final java.util.concurrent.atomic.AtomicInteger callCount =
        new java.util.concurrent.atomic.AtomicInteger(0);
    final org.apache.spark.sql.streaming.StreamingQuery toReturn;

    RecordingLauncher(org.apache.spark.sql.streaming.StreamingQuery toReturn) {
      this.toReturn = toReturn;
    }

    @Override
    public org.apache.spark.sql.streaming.StreamingQuery start(
        io.semanticdf.SemanticTable model, StreamingService.StreamRunRequest request) {
      started.add(request);
      callCount.incrementAndGet();
      return toReturn;
    }
  }

  /** A model registry that returns a fixed model. */
  private static final class StubModelRegistry implements ModelRegistry {
    private final io.semanticdf.SemanticTable model;

    StubModelRegistry(io.semanticdf.SemanticTable model) {
      this.model = model;
    }

    @Override
    public io.semanticdf.SemanticTable get(String name) {
      return model;
    }
  }

  @Test
  void recreateQueryForResume_startsQueryAndRegistersIt() {
    // The simple, testable core of reconciliation: launcher.start is
    // called exactly once with the original request, and the new
    // handle is registered so the monitor loop can find it.
    org.apache.spark.sql.streaming.StreamingQuery freshQuery = normalQuery();
    StreamingQueryHandleRegistry reg = new StreamingQueryHandleRegistry();
    RecordingLauncher launcher = new RecordingLauncher(freshQuery);
    StreamingService service =
        new StreamingService(
            new StubModelRegistry(null), launcher, reg);

    StreamingService.StreamRunRequest req =
        new StreamingService.StreamRunRequest(
            "orders", "sum(amount)", "/ckpt/orders");

    service.recreateQueryForResume("stream-1", null, req);

    assertEquals(1, launcher.callCount.get(), "launcher.start called exactly once");
    assertSame(req, launcher.started.get(0), "launcher called with the original request");
    assertSame(
        freshQuery,
        reg.get("stream-1"),
        "new handle is registered in the local registry");
  }

  @Test
  void recreateQueryForResume_propagatesLauncherException() {
    // If launcher.start throws, the exception must propagate unchanged
    // and the registry must NOT be touched. The caller's catch block
    // (in reconcileAfterJvmCrash) marks the workflow as failed-restart
    // and the failed-start side effect is all-or-nothing.
    StreamingQueryHandleRegistry reg = new StreamingQueryHandleRegistry();
    StreamingQueryLauncher failingLauncher =
        (model, request) -> {
          throw new RuntimeException("Spark checkpoint not found");
        };
    StreamingService service =
        new StreamingService(new StubModelRegistry(null), failingLauncher, reg);

    StreamingService.StreamRunRequest req =
        new StreamingService.StreamRunRequest(
            "orders", "sum(amount)", "/ckpt/orders");

    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () -> service.recreateQueryForResume("stream-1", null, req));
    assertEquals("Spark checkpoint not found", thrown.getMessage());
    // Registry must NOT have been touched — the recreation is all-or-nothing.
    assertNullValue(reg.get("stream-1"));
  }

  @Test
  void reconcileAfterJvmCrash_updatesJournalState() {
    // The journal state updates (RESTART_COUNT, LAST_RESTART_AT) must
    // happen even outside a Restate context (the try/catch around
    // Restate.instantNow() falls back to System.currentTimeMillis()).
    // This pins the contract: any time reconcileAfterJvmCrash runs,
    // the operator-visible counters increment.
    RecordingState state = new RecordingState(0L);
    org.apache.spark.sql.streaming.StreamingQuery freshQuery = normalQuery();
    StreamingQueryHandleRegistry reg = new StreamingQueryHandleRegistry();
    RecordingLauncher launcher = new RecordingLauncher(freshQuery);
    StreamingService service =
        new StreamingService(new StubModelRegistry(null), launcher, reg);

    StreamingService.StreamRunRequest req =
        new StreamingService.StreamRunRequest(
            "orders", "sum(amount)", "/ckpt/orders");

    long beforeMs = System.currentTimeMillis();
    service.reconcileAfterJvmCrash("stream-1", null, req, state);
    long afterMs = System.currentTimeMillis();

    // Restart count: 0 → 1
    assertEquals(1L, state.store.get("restartCount"));
    // Last restart at: wall clock between before and after.
    long restartAt = (long) state.store.get("lastRestartAt");
    assertTrue(
        restartAt >= beforeMs && restartAt <= afterMs,
        "lastRestartAt must be in [before, after], got " + restartAt);
    // Handle is in the registry.
    assertSame(freshQuery, reg.get("stream-1"));
  }

  @Test
  void reconcileAfterJvmCrash_bumpsExistingCount() {
    // Multiple sequential reconciliations must accumulate, not reset.
    RecordingState state = new RecordingState(0L);
    org.apache.spark.sql.streaming.StreamingQuery freshQuery = normalQuery();
    StreamingQueryHandleRegistry reg = new StreamingQueryHandleRegistry();
    RecordingLauncher launcher = new RecordingLauncher(freshQuery);
    StreamingService service =
        new StreamingService(new StubModelRegistry(null), launcher, reg);
    StreamingService.StreamRunRequest req =
        new StreamingService.StreamRunRequest(
            "orders", "sum(amount)", "/ckpt/orders");

    // Pre-populate restart count to simulate a stream that has been
    // through N reconciliations already.
    state.store.put("restartCount", 5L);

    service.reconcileAfterJvmCrash("stream-1", null, req, state);
    assertEquals(6L, state.store.get("restartCount"));
  }

  @Test
  void reconcileAfterJvmCrash_marksFailedRestartOnException() {
    // If the launcher throws, the workflow must end up in
    // STATUS=failed-restart and ERROR_COUNT incremented so operators
    // can detect the condition via getStatus() + journal inspection.
    RecordingState state = new RecordingState(2L); // existing error count
    StreamingQueryHandleRegistry reg = new StreamingQueryHandleRegistry();
    StreamingQueryLauncher failingLauncher =
        (model, request) -> {
          throw new RuntimeException("checkpoint mismatch");
        };
    StreamingService service =
        new StreamingService(new StubModelRegistry(null), failingLauncher, reg);
    StreamingService.StreamRunRequest req =
        new StreamingService.StreamRunRequest(
            "orders", "sum(amount)", "/ckpt/orders");

    assertThrows(
        RuntimeException.class,
        () -> service.reconcileAfterJvmCrash("stream-1", null, req, state));

    assertEquals(
        "failed-restart",
        state.store.get("status"),
        "STATUS must be 'failed-restart' for operator visibility");
    assertEquals(
        3L,
        state.store.get("errorCount"),
        "ERROR_COUNT must increment by exactly 1");
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
