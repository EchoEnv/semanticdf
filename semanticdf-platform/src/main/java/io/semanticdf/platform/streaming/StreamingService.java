package io.semanticdf.platform.streaming;

import dev.restate.sdk.Restate;
import dev.restate.sdk.annotation.Handler;
import dev.restate.sdk.annotation.Workflow;
import dev.restate.sdk.common.DurablePromiseKey;
import dev.restate.sdk.common.StateKey;

import io.semanticdf.SemanticTable;
import io.semanticdf.platform.audit.AuditService;
import org.apache.spark.sql.streaming.StreamingQuery;

/**
 * StreamingService — long-running streaming-query lifecycle controller.
 *
 * <p>Key: <b>stream-id</b> (client-supplied). One workflow execution per
 * streaming query.
 *
 * <h2>Lifecycle model: Option A (controller, not data path)</h2>
 *
 * The workflow <em>owns the lifecycle</em> of a Spark streaming query — it
 * starts, observes, and stops it — but it is <b>not</b> the data path. Spark's
 * micro-batch loop runs in the driver; the workflow journals the coordination
 * facts (status, checkpoint location, started-at, error count) and drives the
 * query handle via the {@link StreamingQueryHandleRegistry}.
 *
 * <p>{@code run} is a <b>start-and-record</b> handler: it validates the
 * request, resolves the model, starts the query (inside {@code Restate.run}\n * so the decision is journaled), emits a dedupHash audit event, sets
 * {@code STATUS = "running"}, and returns. The query keeps running in the
 * driver after {@code run} returns.
 *
 * <p>{@code stop} looks up the live handle (runtime-local), calls
 * {@code query.stop()}, sets {@code STATUS = "stopped"}, and resolves the
 * {@code STOP_SIGNAL} promise for future active-loop coordination.
 *
 * <h2>Journaled vs runtime-local state</h2>
 *
 * <table>
 *   <tr><th>State</th><th>Lives in</th><th>Why</th></tr>
 *   <tr><td>STATUS, MODEL_NAME, QUERY_SHAPE, CHECKPOINT_LOCATION,
 *          STARTED_AT, ERROR_COUNT</td>
 *       <td>Restate journal (workflow state)</td>
 *       <td>Coordination facts — recoverable from replay, queryable</td></tr>
 *   <tr><td>StreamingQuery handle</td>
 *       <td>Runtime-local {@link StreamingQueryHandleRegistry}</td>
 *       <td>Spark runtime object — not serializable, lost on JVM death</td></tr>
 *   <tr><td>STOP_SIGNAL promise</td>
 *       <td>Restate journal (DurablePromise)</td>
 *       <td>Cross-handler signal — resolves when stop() is called</td></tr>
 * </table>
 *
 * <h2>Replay semantics</h2>
 *
 * On JVM failure, Restate replays the workflow from the journal. The query
 * start (wrapped in {@code Restate.run}) is <b>not re-executed</b> — Restate
 * replays the journaled completion. The runtime-local handle is gone. The
 * reconciliation logic (re-start the query from the same checkpoint) is a
 * follow-up concern; for P1, {@code stop()} after a replay is a no-op on the
 * handle (the query died with the JVM) but still transitions {@code STATUS}
 * in the journal.
 *
 * <h2>Checkpoint-location guard</h2>
 *
 * The {@link StreamRunRequest#checkpointLocation} field is REQUIRED (non-null,
 * non-blank). The library's default is a per-JVM {@code createTempFile} path
 * that is NOT durable across replays — Restate's journal replay would create a
 * different path, breaking Spark's checkpoint continuity (silent data loss or
 * stream re-processing). The compact constructor on {@link StreamRunRequest}
 * enforces this at the Wire DTO boundary; {@link #run(StreamRunRequest)}
 * re-asserts it as defense-in-depth.
 *
 * <p>See {@code docs/design/platform-determinism-audit.md} finding #7.
 */
@Workflow
public class StreamingService {

  // --- Workflow state keys (journaled, recoverable from replay) ---

  private static final StateKey<String> STATUS =
      StateKey.of("status", String.class);
  private static final StateKey<String> MODEL_NAME =
      StateKey.of("modelName", String.class);
  private static final StateKey<String> QUERY_SHAPE =
      StateKey.of("queryShape", String.class);
  private static final StateKey<String> CHECKPOINT_LOCATION =
      StateKey.of("checkpointLocation", String.class);
  private static final StateKey<Long> STARTED_AT =
      StateKey.of("startedAt", Long.class);
  private static final StateKey<Long> ERROR_COUNT =
      StateKey.of("errorCount", Long.class);

  /** Cross-handler signal: resolved by {@link #stop(Void)} to coordinate
   * cancellation with a future active-loop {@code run} implementation. */
  private static final DurablePromiseKey<Void> STOP_SIGNAL =
      DurablePromiseKey.of("stop", Void.class);

  /** The default tenant for audit events (P1: single-tenant; P2: per-request). */
  static final String DEFAULT_TENANT = "default";

  // --- Dependencies (constructor-injected) ---

  private final ModelRegistry models;
  private final StreamingQueryLauncher launcher;
  private final StreamingQueryHandleRegistry handles;

  public StreamingService(
      ModelRegistry models,
      StreamingQueryLauncher launcher,
      StreamingQueryHandleRegistry handles) {
    this.models = java.util.Objects.requireNonNull(models, "models");
    this.launcher = java.util.Objects.requireNonNull(launcher, "launcher");
    this.handles = java.util.Objects.requireNonNull(handles, "handles");
  }

  /**
   * Start a streaming query and journal its lifecycle state.
   *
   * <p>Steps:
   * <ol>
   *   <li>Validate the checkpoint location (defense-in-depth).
   *   <li>Initialize workflow state (status, model, query, checkpoint,
   *       started-at, error-count).
   *   <li>Resolve the model via {@link ModelRegistry}.
   *   <li>Start the query inside {@code Restate.run} (journals the decision;
   *       stores the handle runtime-local).
   *   <li>Emit a {@code streaming.started} audit event via {@link AuditService},
   *       carrying the dedupHash so Restate retries and cross-invocation
   *       duplicates are collapsed at the audit boundary.
   *   <li>Set {@code STATUS = "running"}.
   * </ol>
   *
   * @param request the validated run request
   */
  @Handler
  public void run(StreamRunRequest request) {
    // Defense-in-depth: the compact constructor on StreamRunRequest
    // already rejected null/blank checkpointLocation. Re-assert at the
    // handler entry because the handler is the boundary that journals
    // state — failing here is cheaper than letting the bad value flow
    // into a Restate.run block.
    requireCheckpoint(request.checkpointLocation());

    String streamId = Restate.key();
    var state = Restate.state();

    // 1. Initialize journaled lifecycle state.
    state.set(STATUS, "starting");
    state.set(MODEL_NAME, request.modelName());
    state.set(QUERY_SHAPE, request.queryShape());
    state.set(CHECKPOINT_LOCATION, request.checkpointLocation());
    state.set(STARTED_AT, Restate.instantNow().toEpochMilli());
    state.set(ERROR_COUNT, 0L);

    // 2. Resolve the model (deterministic lookup — safe outside Restate.run).
    SemanticTable model;
    try {
      model = models.get(request.modelName());
    } catch (RuntimeException e) {
      state.set(STATUS, "failed");
      state.set(ERROR_COUNT, 1L);
      throw e;
    }

    // 3. Start the query inside Restate.run. The side effect (Spark starting
    //    the query) is journaled as a completion; on replay the lambda is
    //    skipped, so the query is NOT re-started. The handle is stored
    //    runtime-local — it cannot be journaled (not serializable).
    final SemanticTable resolvedModel = model;
    Restate.run(
        "start-streaming-query",
        () -> {
          StreamingQuery query = launcher.start(resolvedModel, request);
          handles.put(streamId, query);
        });

    // 4. Emit the dedupHash audit event. This is a synchronous cross-service
    //    call — Restate journals the invocation and the response. On replay,
    //    the response is replayed without re-invoking AuditService, so the
    //    event is emitted exactly once. The dedupHash is belt-and-suspenders
    //    for the (rare) case where two different workflows share the same
    //    query shape but somehow land in the same audit key.
    String dedupHash =
        StreamingDedupHash.streamingStarted(
            streamId,
            request.modelName(),
            request.queryShape(),
            request.checkpointLocation());
    String payload = auditPayload(streamId, request);
    long ts = Restate.instantNow().toEpochMilli();

    Restate.virtualObject(AuditService.class, DEFAULT_TENANT)
        .append(
            new AuditService.AuditEventRequest(
                DEFAULT_TENANT,
                StreamingDedupHash.STREAMING_STARTED,
                ts,
                dedupHash,
                payload));

    // 5. Mark running.
    state.set(STATUS, "running");
  }

  /**
   * Stop the streaming query for this stream-id.
   *
   * <p>Looks up the live handle in the runtime-local registry and calls
   * {@code query.stop()}. Sets {@code STATUS = "stopped"} and resolves the
   * {@code STOP_SIGNAL} promise. If the handle is absent (e.g. after a JVM
   * failure + replay), the query is already dead — the handler logs the
   * condition and still transitions the journaled status.
   *
   * @param ignored unused (the stream-id comes from the workflow key)
   */
  @Handler
  public void stop(Void ignored) {
    String streamId = Restate.key();
    var state = Restate.state();

    // Stop the live query handle (runtime-local — not journaled).
    // Wrap in Restate.run so the stop decision is journaled and
    // idempotent across replays. The lambda wraps query.stop() in
    // a try/catch because Spark's stop() can throw TimeoutException
    // when the query doesn't stop within its internal deadline —
    // without the catch, the exception would propagate as a journaled
    // failure, but the handle is already removed from the registry,
    // so the Spark query would be orphaned (running in the driver
    // with no platform reference). We swallow the exception and
    // record the failure in ERROR_COUNT instead; the next restart
    // cycle reconciles via checkpoint.
    StreamingQuery query = handles.remove(streamId);
    if (query != null) {
      Restate.run(
          "stop-streaming-query",
          () -> {
            safeStop(query, state);
          });
    }
    // else: handle absent (post-replay, or stop called before run). The
    // query is not running in this JVM; transition the journaled status only.

    state.set(STATUS, "stopped");

    // Resolve the STOP_SIGNAL promise for future active-loop coordination.
    Restate.promiseHandle(STOP_SIGNAL).resolve(null);
  }

  /**
   * Read the current status of this stream.
   *
   * <p>Uses {@code @Shared} so external observers (operators, dashboards) can
   * poll without serializing against the {@code run} handler.
   *
   * @return the journaled status ("starting", "running", "stopped", "failed"),
   *         or "unknown" if the workflow has never executed {@code run}.
   */
  @dev.restate.sdk.annotation.Shared
  @Handler
  public String getStatus() {
    return Restate.state().get(STATUS).orElse("unknown");
  }

  // --- Helpers ---

  /**
   * Stop a {@link StreamingQuery}, swallowing any exception thrown by
   * {@code query.stop()}. The handle has already been removed from the
   * registry at the call site — if {@code stop()} throws, the query is
   * orphaned (running in the Spark driver with no platform reference).
   * We increment {@code ERROR_COUNT} so operators can detect the
   * condition via {@code getStatus()} + journal inspection, and continue
   * the workflow's shutdown (the next restart cycle reconciles via the
   * durable checkpoint).
   *
   * <p>Visible-for-testing — invoked via reflection from
   * {@code StreamingServiceTest} to verify the exception-swallowing
   * behavior without a Restate runtime.
   *
   * @param query the live query to stop (non-null; the caller has already
   *              removed it from the handle registry)
   * @param state the workflow's state store for recording the error count
   */
  static void safeStop(StreamingQuery query, dev.restate.sdk.Restate.State state) {
    try {
      query.stop();
    } catch (Exception e) {
      // Spark's StreamingQuery.stop() can throw TimeoutException if the
      // query doesn't terminate within its internal deadline, or
      // StreamingQueryException for other failure modes. The handle is
      // already removed; we can't do anything about the orphaned query
      // here, so we record the error and continue.
      state.set(
          ERROR_COUNT,
          state.get(ERROR_COUNT).orElse(0L) + 1);
    }
  }

  private static void requireCheckpoint(String checkpointLocation) {
    if (checkpointLocation == null || checkpointLocation.isBlank()) {
      throw new IllegalArgumentException(
          "StreamingService.run: checkpointLocation is required and must be non-blank");
    }
  }

  /** Build the JSON payload for the streaming.started audit event. */
  private static String auditPayload(String streamId, StreamRunRequest request) {
    return "{\"streamId\":\""
        + escape(streamId)
        + "\",\"modelName\":\""
        + escape(request.modelName())
        + "\",\"queryShape\":\""
        + escape(request.queryShape())
        + "\",\"checkpointLocation\":\""
        + escape(request.checkpointLocation())
        + "\"}";
  }

  /** Minimal JSON string escaper (avoids pulling a JSON builder for a 4-field payload). */
  private static String escape(String s) {
    if (s == null) {
      return "";
    }
    StringBuilder sb = new StringBuilder(s.length());
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      switch (c) {
        case '"' -> sb.append("\\\"");
        case '\\' -> sb.append("\\\\");
        case '\n' -> sb.append("\\n");
        case '\r' -> sb.append("\\r");
        case '\t' -> sb.append("\\t");
        default -> sb.append(c);
      }
    }
    return sb.toString();
  }

  /**
   * Request DTO for {@link #run(StreamRunRequest)}.
   *
   * <p>The compact constructor enforces that {@code checkpointLocation} is
   * non-null and non-blank. This is the Wire DTO boundary check for Critical
   * finding #7 of the deterministic-purity audit (see
   * {@code docs/design/platform-determinism-audit.md}).
   */
  public record StreamRunRequest(
      String modelName, String queryShape, String checkpointLocation) {
    public StreamRunRequest {
      if (checkpointLocation == null || checkpointLocation.isBlank()) {
        throw new IllegalArgumentException(
            "StreamingService.run: checkpointLocation is required and must be non-blank. "
                + "The library's default is a per-JVM temp path that is lost across Restate"
                + " replays, breaking Spark checkpoint continuity. Provide a stable, durable"
                + " path on the operator's storage (persistent volume or DB-backed checkpoint"
                + " store).");
      }
    }
  }
}
