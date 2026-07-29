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
 * <h2>Lifecycle model: Option A controller + Option B active-monitor loop</h2>
 *
 * The workflow <em>owns the lifecycle</em> of a Spark streaming query — it
 * starts, observes, and stops it — but it is <b>not</b> the data path. Spark's
 * micro-batch loop runs in the driver; the workflow journals the coordination
 * facts (status, checkpoint location, started-at, error count) and drives the
 * query handle via the {@link StreamingQueryHandleRegistry}.
 *
 * <p>{@code run} is a <b>start-and-monitor</b> handler:
 * <ol>
 *   <li>validates the request (checkpoint non-blank)
 *   <li>resolves the model via {@link ModelRegistry}
 *   <li>starts the query inside {@code Restate.run} (decision is journaled)
 *   <li>emits a dedupHash audit event via {@link AuditService}
 *   <li>transitions {@code STATUS} to "running"
 *   <li>enters the <b>active-monitor loop</b> — checks {@code STOP_SIGNAL}
 *       and the query's termination status in a bounded 5-second tick
 * </ol>
 *
 * <p>{@code stop} is a <b>signal-only</b> handler: it resolves the
 * {@code STOP_SIGNAL} DurablePromise. The active-monitor loop inside
 * {@code run} is the <b>sole owner of physical query shutdown</b>
 * ({@code query.stop()}) — this prevents races between two handlers
 * both calling {@code query.stop()}. {@code stop} is annotated
 * {@code @Shared} so it can fire concurrently with {@code run}'s loop.
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
 *       <td>Cross-handler signal — resolves when {@code stop()} is called</td></tr>
 * </table>
 *
 * <h2>Replay semantics</h2>
 *
 * On JVM failure, Restate replays the workflow from the journal. The query
 * start (wrapped in {@code Restate.run}) is <b>not re-executed</b> — Restate
 * replays the journaled completion. The runtime-local handle is gone, so
 * the monitor loop's first tick returns {@code true} (handle absent) and
 * the loop exits with {@code STATUS = "stopped"}. Auto-restart from
 * checkpoint is a follow-up concern; for P1, JVM death = stream death
 * (operator restarts manually).
 *
 * <h2>Journal cost</h2>
 *
 * Per 5-second tick, the loop journals two entries:
 * <ol>
 *   <li>{@code Restate.promise(STOP_SIGNAL).peek()} — a journaled READ
 *       (one {@code PeekPromiseCommandMessage})
 *   <li>{@code Restate.run("tick", () -> q.awaitTermination(5000))} — a
 *       journaled RUN (one {@code RunCommandMessage}, plus the journaled
 *       boolean return value)
 * </ol>
 *
 * That is approximately 34,560 entries/day/stream (~1M/month). Acceptable
 * for P1; the Option C batched-ticks optimization is documented but not
 * implemented (would reduce by ~50% at the cost of weaker per-tick
 * observability).
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
  private static final StateKey<Long> RESTART_COUNT =
      StateKey.of("restartCount", Long.class);
  private static final StateKey<Long> LAST_RESTART_AT =
      StateKey.of("lastRestartAt", Long.class);

  /**
   * Set to {@code true} when a recreation path (initial start or
   * reconciliation) failed in a way that should NOT auto-retry.
   * Cleared by {@link #restart(Void)} or
   * {@link #clearReconcileBlock(Void)} when the operator decides the
   * underlying cause is fixed. Critical to prevent infinite retry
   * loops on persistent failures (model file deleted, permissions
   * wrong, etc.) — see PR #233 / DE-M1 / Architect-C1.
   */
  private static final StateKey<Boolean> RECONCILE_BLOCKED =
      StateKey.of("reconcileBlocked", Boolean.class);

  /**
   * Cumulative count of audit-emit failures that did NOT block the
   * workflow (because the query was already running and a block
   * would have orphaned it). Operators read this via
   * {@link #getAuditLossCount()} to detect silent audit-log gaps.
   */
  private static final StateKey<Long> AUDIT_LOSS_COUNT =
      StateKey.of("auditLossCount", Long.class);

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
  private final AuditSink auditSink;
  private final StreamCatalog catalog;

  /**
   * Production constructor — wires the default Restate-backed audit sink
   * so audit emits are journaled as cross-service calls. No catalog
   * (used by tests that don't care about bulk reconciliation).
   */
  public StreamingService(
      ModelRegistry models,
      StreamingQueryLauncher launcher,
      StreamingQueryHandleRegistry handles) {
    this(models, launcher, handles, new RestateAuditSink(), null);
  }

  /**
   * Production constructor — wires the default Restate-backed audit sink
   * AND the supplied catalog for bulk startup reconciliation.
   * Used by {@link PlatformApplication}.
   */
  public StreamingService(
      ModelRegistry models,
      StreamingQueryLauncher launcher,
      StreamingQueryHandleRegistry handles,
      StreamCatalog catalog) {
    this(models, launcher, handles, new RestateAuditSink(), catalog);
  }

  /**
   * Backward-compat production constructor — supplies an explicit
   * {@link AuditSink} but no catalog. Used by tests that pass a
   * recording/no-op audit sink without caring about reconciliation.
   */
  public StreamingService(
      ModelRegistry models,
      StreamingQueryLauncher launcher,
      StreamingQueryHandleRegistry handles,
      AuditSink auditSink) {
    this(models, launcher, handles, auditSink, null);
  }

  /**
   * Test constructor — allows injecting a recording or no-op audit
   * sink AND a catalog (or null) so handlers can be exercised
   * without a Restate handler context. Visible-for-testing.
   */
  StreamingService(
      ModelRegistry models,
      StreamingQueryLauncher launcher,
      StreamingQueryHandleRegistry handles,
      AuditSink auditSink,
      StreamCatalog catalog) {
    this.models = java.util.Objects.requireNonNull(models, "models");
    this.launcher = java.util.Objects.requireNonNull(launcher, "launcher");
    this.handles = java.util.Objects.requireNonNull(handles, "handles");
    this.auditSink = java.util.Objects.requireNonNull(auditSink, "auditSink");
    // catalog may be null — handlers that maintain it must null-check.
    this.catalog = catalog;
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

    // Step 0: previous-failure blocker. PR #233 / DE-M1 / Architect-C1
    // — a previous run/restart failure set RECONCILE_BLOCKED. Without
    // this guard, Restate would retry the workflow indefinitely
    // (RuntimeException from a handler is reported as a transient
    // failure and retried). Throw a TerminalException with code 400
    // (BAD_REQUEST) to halt retries and surface a clear operator
    // message. Operators clear the block via restart() or
    // clearReconcileBlock() after fixing the underlying cause.
    if (state.get(RECONCILE_BLOCKED).orElse(false)) {
      String blockedStatus = state.get(STATUS).orElse("unknown");
      throw new dev.restate.sdk.common.TerminalException(
          dev.restate.sdk.common.TerminalException.BAD_REQUEST_CODE,
          "StreamingService.run: workflow is blocked from auto-retry "
              + "after a previous failure (stream-id="
              + streamId
              + ", current status="
              + blockedStatus
              + "). Operators must invoke /restart or "
              + "/clearReconcileBlock after fixing the underlying cause.");
    }

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
      state.set(RECONCILE_BLOCKED, true);
      state.set(ERROR_COUNT, 1L);
      throw e;
    }

    // 3. Start the query inside Restate.run. The side effect (Spark
    //    starting the query) is journaled as a completion; on replay
    //    the lambda is skipped, so the query is NOT re-started. The
    //    handle is stored runtime-local — it cannot be journaled (not
    //    serializable).
    //
    // PR #237 (architect-flagged ship-blocker): WITHOUT the try/catch
    // below, a persistent launcher failure (Spark misconfigured,
    // checkpoint path unwritable, etc.) throws out of Restate.run
    // and propagates. Restate retries the entire handler, but step 0
    // (the BLOCKED guard) only halts retries if BLOCKED is set —
    // and step 3 doesn't set it. NET EFFECT: the journal grows
    // unboundedly until operators manually /restart, defeating
    // PR #233's RECONCILE_BLOCKED contract.
    //
    // Wrap in try/catch mirroring step 2's pattern. Throw a
    // TerminalException directly so Restate doesn't even attempt
    // one retry (the BLOCKED guard would catch the retry, but the
    // retry itself is wasted journal entries).
    // PR #237 (architect-flagged ship-blocker): startQueryWithFailureTracking
    // wraps Restate.run in a try/catch that sets STATUS=failed and
    // RECONCILE_BLOCKED=true on launcher errors, then throws a
    // TerminalException so Restate doesn't retry. See the helper's
    // javadoc for the full rationale.
    final SemanticTable resolvedModel = model;
    startQueryWithFailureTracking(streamId, resolvedModel, request, state);

    // 4. Emit the dedupHash audit event. This is a synchronous cross-service
    //    call — Restate journals the invocation and the response. On replay,
    //    the response is replayed without re-invoking AuditService, so the
    //    event is emitted exactly once. The dedupHash is belt-and-suspenders
    //    for the (rare) case where two different workflows share the same
    //    query shape but somehow land in the same audit key.

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
    long ts = journaledNow();

    // Audit failure on initial start does NOT block — the query is
    // already registered in the registry, so blocking would orphan
    // it (Architect-C3 from PR #233 review). We increment a
    // dedicated AUDIT_LOSS_COUNT counter that operators can read via
    // getAuditLossCount() to detect silent audit-log gaps. The
    // workflow continues to monitorLoop with the live query.
    try {
      auditSink.emit(
          DEFAULT_TENANT,
          StreamingDedupHash.STREAMING_STARTED,
          ts,
          dedupHash,
          payload);
    } catch (RuntimeException auditRe) {
      state.set(
          AUDIT_LOSS_COUNT,
          state.get(AUDIT_LOSS_COUNT).orElse(0L) + 1L);
    }

    // 5. CATALOG: register this stream-id in the durable catalog so
    // the StartupReconciler can find it on next JVM startup.
    //
    // PR #236 (DE-H3 fix): moved BEFORE state.set(STATUS, "running")
    // to eliminate the catalog/journal drift window. With this
    // ordering, the journal's STATUS=running and the catalog row
    // are written together at the same point in the handler —
    // a JVM death either happens before both (rollback) or after
    // both (committed), with no in-between state where the journal
    // says running but the catalog has no row.
    //
    // Failure semantics: ON CONFLICT DO NOTHING (catalog, id:114)
    // makes re-attempts idempotent. A JVM death after the catalog
    // write but before STATUS=running means the catalog row
    // exists for a workflow whose journal says STATUS=starting.
    // On restart, the workflow replays from scratch: the catalog
    // write retries idempotently (no-op via ON CONFLICT), and
    // STATUS=running is set again on successful step 5.
    //
    // The catalog.write is wrapped in try/catch to keep the
    // workflow healthy even if Postgres is unreachable: we'd
    // lose auto-recovery but the workflow itself isn't broken.
    if (catalog != null) {
      try {
        catalog.registerIfAbsent(
            streamId,
            request.modelName(),
            request.queryShape(),
            request.checkpointLocation());
      } catch (RuntimeException catalogRe) {
        // Non-fatal — log and continue. The workflow is healthy
        // (we're past step 4); we just lost the auto-recovery on
        // next JVM startup. Operators can manually invoke
        // /restart after fixing the underlying Postgres issue.
        System.err.println(
            "semanticdf-platform: catalog write failed for stream-id="
                + streamId
                + " ("
                + catalogRe.getMessage()
                + "). Auto-recovery on next JVM startup will be unavailable.");
      }
    }

    // 5b. Mark running. (Catalog write was step 5 above; reordered
    // in PR #236 so catalog and journal are written together.)
    state.set(STATUS, "running");

    // 5a. RECONCILIATION: detect post-crash replay and recreate the query.
    //
    // On a normal fresh start: step 3's Restate.run closure put the
    // StreamingQuery into the local handles registry. handles.get(streamId)
    // is non-null — we skip this branch.
    //
    // On a normal same-JVM replay (e.g., Restate worker restart mid-handler):
    // step 3's cached result is returned WITHOUT re-running the closure,
    // but the registry still has the query from the first execution.
    // handles.get(streamId) is non-null — we skip this branch.
    //
    // On a cross-JVM replay (this JVM died and a new one picked up the
    // workflow): step 3 returns the cached "started" result, but the
    // registry is in a different process — handles.get(streamId) is null.
    // We MUST recreate the query, or the monitor loop's first tick would
    // see the empty handle, return terminated=true, and exit with
    // STATUS=stopped — silently losing the running stream. Spark's
    // checkpoint recovery makes the recreation idempotent: the new
    // query resumes from the last committed offset, so no data is
    // lost or double-processed.
    //
    // Note: this is the ONE place in the workflow where we deliberately
    // break Restate's determinism contract — the launcher.start call is
    // a non-deterministic side effect outside any Restate.run block.
    // Same-JVM replay never enters this branch (registry non-empty),
    // so determinism is preserved in the common case.
    //
    // H2 from PR #232 review: if the previous JVM died AFTER the
    // STOP_SIGNAL was resolved (operator hit /stop) but BEFORE the
    // STATUS=stopped journal entry was written, naive reconciliation
    // would create a fresh query just to immediately stop it again
    // — operator noise (a spurious streaming.restarted audit event
    // followed by STATUS=stopped). Skip reconciliation in this case
    // and just mark STATUS=stopped.
    if (handles.get(streamId) == null) {
      if (Restate.promise(STOP_SIGNAL).peek().isReady()) {
        // Previous JVM resolved /stop but didn't finish journaling
        // STATUS=stopped. Honor the stop intent — don't recreate.
        state.set(STATUS, "stopped");
        return;
      }
      reconcileAfterJvmCrash(streamId, resolvedModel, request, state);
    }

    // 6. Active-monitor loop. The loop stays alive until one of:
    //    a) STOP_SIGNAL is resolved by a concurrent {@link #stop(Void)} call
    //    b) The query terminates (naturally or abnormally)
    //    c) The runtime-local handle disappears (post-replay or pre-run)
    //
    // Loop body uses Option B (awaitTermination inside Restate.run) per the
    // active-monitor design validated by parallel senior subagent review.
    // Journal cost: 2 entries per 5s = ~34,560/day/stream. Acceptable for v1;
    // optimize to Option C (batched ticks) only if journal cost becomes a
    // measured problem.
    //
    // The loop is the SOLE owner of physical query shutdown — {@link #stop}
    // only resolves the STOP_SIGNAL promise; the loop calls safeStop(). This
    // prevents races between two handlers both calling query.stop().
    monitorLoop(streamId, state);
  }

  /**
   * The active-monitor loop body. Extracted as a package-private method so
   * unit tests can verify the per-iteration decision logic without booting
   * a Restate runtime.
   *
   * <p>Returns when one of:
   * <ul>
   *   <li>STOP_SIGNAL is resolved → stop the query, STATUS=stopped
   *   <li>{@code awaitTermination} returns true (naturally or via the
   *       5000ms timeout) → STATUS=stopped
   *   <li>The runtime-local handle is absent → STATUS=stopped
   * </ul>
   *
   * <p>The loop is deterministic on replay: {@code peek()} replays the
   * journaled promise state, {@code Restate.run("tick", ...)} replays the
   * journaled return value. On replay with no handle (post-JVM-death), the
   * first tick returns {@code true} (terminated) and the loop exits cleanly.
   */
  private void monitorLoop(String streamId, dev.restate.sdk.Restate.State state) {
    while (true) {
      // Check stop signal — peek() returns Output<Void>; isReady() is the
      // completion check. peek() is a journaled READ (1 entry/call), so no
      // need to wrap it in Restate.run.
      boolean stopSignaled = Restate.promise(STOP_SIGNAL).peek().isReady();

      // Wait up to 5s for query termination. Restate.run journals the call;
      // on replay the journaled boolean is returned without re-running the
      // closure. awaitTermination throws StreamingQueryException if the
      // query terminated abnormally.
      //
      // PR #265: an abnormal Spark death (StreamingQueryException)
      // MUST NOT be classified as a clean stop. The v0.2.2 bug
      // mapped it to `terminated=true` → STATUS=stopped, which
      // `StartupReconciler.invokeRun` then SKIPS on JVM restart
      // (it's the same path operator-requested stops take), so the
      // dead stream was silently never resurrected. ERROR_COUNT
      // and RECONCILE_BLOCKED were also not incremented, so
      // operators had no signal. Fix: mark STATUS=failed, set
      // RECONCILE_BLOCKED=true, bump ERROR_COUNT — mirroring the
      // startQueryWithFailureTracking pattern. The state mutations
      // ARE journaled (Restate.run journals state writes performed
      // inside its closures as part of the completion record;
      // verified by PR #237).
      // The closure also returns true if the handle is absent
      // (post-replay or stop called before run).
      boolean terminated =
          Restate.run(
              "tick",
              Boolean.class,
              () -> {
                StreamingQuery q = handles.get(streamId);
                if (q == null) {
                  return Boolean.TRUE;
                }
                try {
                  return Boolean.valueOf(q.awaitTermination(5000));
                } catch (org.apache.spark.sql.streaming.StreamingQueryException sqe) {
                  // Abnormal termination — mark failed, not stopped.
                  // We can't `throw` here (would cause Restate.run to
                  // retry forever). The state mutations below are the
                  // durable failure signal.
                  state.set(STATUS, "failed");
                  state.set(RECONCILE_BLOCKED, true);
                  state.set(
                      ERROR_COUNT, state.get(ERROR_COUNT).orElse(0L) + 1L);
                  return Boolean.TRUE;
                }
              });

      boolean handlePresent = handles.get(streamId) != null;
      LoopAction action = decideNextAction(stopSignaled, handlePresent, terminated);

      switch (action) {
        case STOP_ON_SIGNAL:
          // Stop requested — the active loop owns the physical shutdown.
          Restate.run(
              "stop-on-signal",
              () -> {
                StreamingQuery q = handles.remove(streamId);
                if (q != null) {
                  safeStop(q, state);
                }
              });
          state.set(STATUS, "stopped");
          return;
        case EXIT_TERMINATED:
          // Query terminated. The tick closure already set
          // STATUS=failed + RECONCILE_BLOCKED=true + bumped ERROR_COUNT
          // for abnormal Spark deaths (StreamingQueryException). For
          // natural termination or handle-gone (post-replay / stop
          // before run), STATUS is still "started" — set it to
          // "stopped" now. Don't overwrite a "failed" state.
          handles.remove(streamId);
          if (!"failed".equals(state.get(STATUS).orElse(null))) {
            state.set(STATUS, "stopped");
          }
          return;
        case CONTINUE:
          // 5s elapsed, query still running, no stop signal. Loop again.
          break;
      }
    }
  }

  /**
   * Signal stop — resolves the {@code STOP_SIGNAL} DurablePromise. The
   * active-monitor loop (running inside {@link #run}) is the sole owner
   * of the physical {@code query.stop()} call; this handler is a
   * fire-and-forget signal.
   *
   * <p><b>Concurrency:</b> annotated {@code @Shared} so it can fire
   * concurrently with the {@code run} handler's active-monitor loop.
   * Without {@code @Shared}, the loop would block the {@code run} handler
   * for up to 5s per tick (inside {@code awaitTermination}), and the
   * stop signal would not be observed during those windows.
   *
   * <p>Idempotent — resolving an already-resolved promise is a no-op in
   * Restate. If {@code stop} is called before {@code run} (or after a
   * JVM-death replay), the promise is resolved; the loop's next tick
   * sees it and exits with {@code STATUS=stopped}.
   *
   * @param ignored unused (the stream-id comes from the workflow key)
   */
  @dev.restate.sdk.annotation.Shared
  @Handler
  public void stop(Void ignored) {
    // Signal-only — the active loop owns the physical shutdown.
    Restate.promiseHandle(STOP_SIGNAL).resolve(null);
  }

  /**
   * Operator-triggered reconciliation. Recreates a StreamingQuery that
   * was lost to a JVM crash, replaying the same logic that
   * {@link #run(StreamRunRequest)} uses auto-detectively.
   *
   * <p>Use this when the platform restarts and an operator wants to
   * force-recreate a query without waiting for Restate's natural
   * workflow replay. Idempotent: if a handle is already present, the
   * call is a no-op.
   *
   * <p><b>Concurrency:</b> NOT annotated {@code @Shared} because this
   * handler writes workflow state (RESTART_COUNT, LAST_RESTART_AT,
   * audit emit) — {@code @Shared} handlers are read-only by Restate
   * contract. The lack of {@code @Shared} means a {@code restart} call
   * serializes against an in-flight {@code run} for the same stream-id,
   * which is the correct ordering (stop the old handler before
   * recreating).
   *
   * <p><b>Multi-replica limitations:</b> for P3 deployments with
   * multiple replicas, this handler alone is insufficient — two
   * replicas could each see an "empty" registry and both call
   * {@code launcher.start}, racing on the same checkpoint. P3 requires
   * a Postgres-backed stream-lease (deferred). P1 single-replica is
   * safe.
   *
   * <p><b>Limitations:</b> the new query is constructed with a fresh
   * {@code StreamRunRequest} derived from the journaled state — if the
   * model's {@code queryShape} has changed since the original
   * checkpoint, Spark will fail to resume from the checkpoint location
   * and the caller must {@link #stop(Void)} + re-{@link #run} with the
   * new shape.
   *
   * @param ignored unused (the stream-id comes from the workflow key)
   */
  @Handler
  public void restart(Void ignored) {
    String streamId = Restate.key();
    var state = Restate.state();

    // Clear the previous-failure blocker at the entry. This is the
    // operator-driven retry path: they've inspected the failure,
    // fixed the underlying cause, and are now telling the workflow
    // to try again. If the retry fails again, the catch in
    // reconcileAfterJvmCrash will re-set the flag.
    //
    // Order matters: clear BEFORE validation checks so a failed
    // validation (e.g., model file still missing) leaves the block
    // set (DE-M2 from PR #233 review).
    state.set(RECONCILE_BLOCKED, false);

    // If a handle is already present, the registry is in sync with the
    // journal — nothing to reconcile. Idempotent no-op.
    if (handles.get(streamId) != null) {
      return;
    }

    // Refuse to restart a workflow that was cleanly stopped (only).
    // Operators who want to resume a stopped stream must invoke run()
    // with the full request, which starts a fresh lifecycle.
    //
    // STATUS=failed and STATUS=failed-restart ARE allowed — these
    // are exactly what restart() exists to recover from. Operators
    // call restart() after fixing the underlying cause (PR #233
    // / Architect-H1).
    String currentStatus = state.get(STATUS).orElse("unknown");
    if ("stopped".equals(currentStatus)) {
      throw new IllegalStateException(
          "StreamingService.restart: cannot restart a stopped workflow "
              + "(stream-id="
              + streamId
              + "). Operators must invoke run() with a fresh request.");
    }

    // If the journal has no model + checkpoint recorded, the workflow
    // never executed run() — fail loud instead of silently doing nothing.
    // Operators get a clear "no prior run() to reconcile from" signal
    // they can act on, rather than a 200 OK with no effect.
    String modelName = state.get(MODEL_NAME).orElse(null);
    String checkpointLocation = state.get(CHECKPOINT_LOCATION).orElse(null);
    String queryShape = state.get(QUERY_SHAPE).orElse(null);
    if (modelName == null || checkpointLocation == null || queryShape == null) {
      throw new IllegalStateException(
          "StreamingService.restart: no prior run() recorded (stream-id="
              + streamId
              + "). Operators must invoke run() to start a fresh stream.");
    }

    // Resolve the model from the registry. If the model file is still
    // missing, mark STATUS=failed-restart AND set the block so the
    // workflow doesn't loop forever (DE-M1 / Architect-C2 from PR
    // #233 review). Operators must fix the file and call restart()
    // again, or call clearReconcileBlock() to bypass.
    SemanticTable model;
    try {
      model = models.get(modelName);
    } catch (RuntimeException e) {
      state.set(STATUS, "failed-restart");
      state.set(RECONCILE_BLOCKED, true);
      state.set(ERROR_COUNT, state.get(ERROR_COUNT).orElse(0L) + 1L);
      throw e;
    }

    // Reconstruct the request from journaled state. Note: this loses
    // any custom streaming options the caller passed at run-time;
    // Spark will use checkpoint defaults, which is the right
    // behavior for resume-from-checkpoint.
    StreamRunRequest request =
        new StreamRunRequest(modelName, queryShape, checkpointLocation);

    reconcileAfterJvmCrash(streamId, model, request, state);
  }

  /**
   * Read the current status of this stream.
   *
   * <p>Uses {@code @Shared} so external observers (operators, dashboards) can
   * poll without serializing against the {@code run} handler.
   *
   * @return the journaled status ("starting", "running", "stopped",
   *         "failed", "failed-restart"), or "unknown" if the workflow
   *         has never executed {@code run}.
   */
  @dev.restate.sdk.annotation.Shared
  @Handler
  public String getStatus() {
    return Restate.state().get(STATUS).orElse("unknown");
  }

  /**
   * Read the number of post-crash reconciliations this workflow has
   * performed since its initial {@code run()}. Zero means a clean
   * lifecycle (no JVM crashes); nonzero flags operational instability.
   *
   * <p>Uses {@code @Shared} so dashboards can poll without serializing
   * against the {@code run} handler.
   *
   * <p>If the journal is corrupt (e.g., the stored value isn't a Long),
   * the exception propagates to the caller — surfaces the corruption to
   * operators rather than masking it with a sentinel.
   *
   * @return the journaled restart count (0 if the workflow has never
   *         reconciled)
   */
  @dev.restate.sdk.annotation.Shared
  @Handler
  public long getRestartCount() {
    return Restate.state().get(RESTART_COUNT).orElse(0L);
  }

  /**
   * Read the cumulative count of audit-emit failures that did NOT
   * block the workflow. Operators monitor this to detect silent
   * audit-log gaps (network blips, AuditService back-pressure,
   * transient Restate issues). A non-zero value with STATUS=running
   * means audit is degraded but the query itself is healthy.
   *
   * <p>Uses {@code @Shared} so dashboards can poll.
   *
   * @return 0 if the audit path has been healthy; otherwise the
   *         cumulative count of failed emits.
   */
  @dev.restate.sdk.annotation.Shared
  @Handler
  public long getAuditLossCount() {
    return Restate.state().get(AUDIT_LOSS_COUNT).orElse(0L);
  }

  /**
   * Operator escape hatch: clear the previous-failure block flag so
   * the next {@link #run(StreamRunRequest)} attempt proceeds. Use
   * this when:
   *
   * <ul>
   *   <li>An operator has manually fixed the underlying cause (e.g.,
   *       restored a deleted model file, fixed permissions, restarted
   *       an upstream service) and wants to let the platform retry
   *       without going through {@link #restart(Void)}.
   *   <li>An automated CI smoke test hit a transient failure and
   *       needs to reset before the next test run.
   * </ul>
   *
   * <p>NOT annotated {@code @Shared} — this handler writes workflow
   * state. Per-key serialization in Restate ensures the clear and the
   * next retry are ordered.
   *
   * @param ignored unused (the stream-id comes from the workflow key)
   */
  @Handler
  public void clearReconcileBlock(Void ignored) {
    Restate.state().set(RECONCILE_BLOCKED, false);
  }

  // --- Helpers ---

  /**
   * The per-iteration decision of the active-monitor loop. Pure function
   * — given the three observed signals, returns the action to take.
   *
   * <p>Extracted as a package-private static method so unit tests can
   * verify the decision matrix without booting a Restate runtime or a
   * Spark session. The loop body delegates to this method after
   * collecting the three signals.
   *
   * @param stopSignaled  true if {@code STOP_SIGNAL.peek().isReady()}
   * @param handlePresent true if {@code handles.get(streamId) != null}
   * @param terminated    true if {@code awaitTermination} returned true
   *                      (or the handle was absent at tick time)
   * @return the action the loop should take this iteration
   */
  enum LoopAction { STOP_ON_SIGNAL, EXIT_TERMINATED, CONTINUE }

  static LoopAction decideNextAction(
      boolean stopSignaled, boolean handlePresent, boolean terminated) {
    if (stopSignaled) {
      return LoopAction.STOP_ON_SIGNAL;
    }
    // Treat handle-absent OR query-terminated as "exit". The tick closure
    // already returns true when handle is absent, so this collapses to one
    // exit condition at the loop level.
    if (terminated || !handlePresent) {
      return LoopAction.EXIT_TERMINATED;
    }
    return LoopAction.CONTINUE;
  }

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

  /**
   * Post-crash reconciliation: recreate a StreamingQuery that was lost
   * when this JVM died. Called from {@link #run(StreamRunRequest)} when
   * the journal says STATUS=running but the local registry has no handle
   * for this stream.
   *
   * <p><b>This is the only place in the workflow where we break Restate's
   * determinism contract:</b> the {@code launcher.start(...)} call runs
   * outside any {@code Restate.run} block, so its effect is NOT journaled.
   * We accept this because:
   *
   * <ol>
   *   <li>The branch is taken at most once per JVM lifecycle (the registry
   *       stays non-empty after the recreation; same-JVM replays skip).
   *   <li>Spark's checkpoint recovery makes recreation idempotent — the
   *       new query resumes from the last committed offset, no data
   *       is lost or double-processed.
   *   <li>On failure, we set STATUS=failed-restart and re-throw, which
   *       causes Restate to retry the workflow (next replay takes
   *       this branch again).
   * </ol>
   *
   * <p>Emits a {@link StreamingDedupHash#STREAMING_RESTARTED} audit event
   * so operators can distinguish "recreated after JVM crash" from
   * "fresh start".
   *
   * <p>Visible-for-testing — package-private so unit tests can verify
   * the side effects without a Restate runtime.
   */
  void reconcileAfterJvmCrash(
      String streamId,
      SemanticTable model,
      StreamRunRequest request,
      dev.restate.sdk.Restate.State state) {
    // Delegate to the testable overload with the journaled "now".
    // Production callers don't pass nowMs explicitly — they rely on
    // journaled time. Tests use the overload below.
    reconcileAfterJvmCrash(streamId, model, request, state, journaledNow());
  }

  /**
   * Testable overload of {@link #reconcileAfterJvmCrash(String, SemanticTable, StreamRunRequest, dev.restate.sdk.Restate.State)}
   * that takes an explicit {@code nowMs} instead of calling
   * {@code Restate.instantNow()}. Package-private so unit tests can
   * drive the full reconciliation logic without a Restate runtime.
   *
   * <p>The {@code AuditSink} used is whatever was wired at construction
   * time — production uses {@link RestateAuditSink}, tests use a
   * no-op or recording sink.
   */
  void reconcileAfterJvmCrash(
      String streamId,
      SemanticTable model,
      StreamRunRequest request,
      dev.restate.sdk.Restate.State state,
      long nowMs) {
    try {
      // 1. Recreate the query and register it. If this throws, the
      //    registry is still empty (put happens after successful launch)
      //    so no cleanup is needed — just mark blocked.
      recreateQueryForResume(streamId, model, request);

      // 2. Bump the restart counter and last-restart-at in the journal.
      long prevCount = state.get(RESTART_COUNT).orElse(0L);
      long newCount = prevCount + 1L;
      state.set(RESTART_COUNT, newCount);
      state.set(LAST_RESTART_AT, nowMs);

      // 3. Emit the restart audit event. Best-effort — the query is
      //    already registered, so blocking on audit failure would
      //    orphan it (Architect-C3 from PR #233 review). We
      //    increment AUDIT_LOSS_COUNT instead; operators read it via
      //    getAuditLossCount() to detect silent audit-log gaps. The
      //    workflow continues to monitorLoop normally.
      String dedupHash =
          StreamingDedupHash.streamingRestarted(
              streamId,
              request.modelName(),
              newCount,
              request.checkpointLocation());
      String payload =
          "{\"streamId\":\""
              + escape(streamId)
              + "\",\"modelName\":\""
              + escape(request.modelName())
              + "\",\"queryShape\":\""
              + escape(request.queryShape())
              + "\",\"checkpointLocation\":\""
              + escape(request.checkpointLocation())
              + "\",\"restartCount\":"
              + newCount
              + "}";
      try {
        auditSink.emit(
            DEFAULT_TENANT,
            StreamingDedupHash.STREAMING_RESTARTED,
            nowMs,
            dedupHash,
            payload);
      } catch (RuntimeException auditRe) {
        state.set(
            AUDIT_LOSS_COUNT,
            state.get(AUDIT_LOSS_COUNT).orElse(0L) + 1L);
      }
    } catch (RuntimeException e) {
      // Recreation failed. Registry is empty (recreate throws before
      // put). Mark failed-restart AND set the blocked flag so the
      // next run() invocation throws a TerminalException (Architect-C1
      // from PR #233 review) instead of looping forever.
      state.set(STATUS, "failed-restart");
      state.set(RECONCILE_BLOCKED, true);
      state.set(ERROR_COUNT, state.get(ERROR_COUNT).orElse(0L) + 1L);
      throw e;
    }
  }

  /**
   * Returns the journaled "now" as epoch-millis. Requires a Restate
   * handler context — callers are inside handlers' bodies.
   *
   * <p>No fallback to {@code System.currentTimeMillis()}: that's a
   * non-deterministic side effect that violates Restate's journal
   * determinism contract. Use {@code Restate.instantNow()} inside the
   * handler; if your code is in a test without a handler context,
   * restructure it to use a deterministic test double (the AuditSink
   * interface replaces what was previously the need for a wall-clock
   * fallback).
   */
  private static long journaledNow() {
    return Restate.instantNow().toEpochMilli();
  }

  /**
   * Recreate a Spark streaming query and register it in the local
   * handle registry. Extracted from {@link #reconcileAfterJvmCrash}
   * so it can be unit-tested without a Restate runtime.
   *
   * <p>Spark resumes from {@code request.checkpointLocation()} so the
   * new query covers the same offset range as the original (minus any
   * in-flight micro-batch the old query hadn't committed).
   *
   * @throws RuntimeException if the launcher fails; the caller is
   *         responsible for marking the workflow as failed-restart
   */
  void recreateQueryForResume(
      String streamId, SemanticTable model, StreamRunRequest request) {
    StreamingQuery fresh = launcher.start(model, request);
    handles.put(streamId, fresh);
  }

  /**
   * Step 3 of {@link #run} extracted for testability. Wraps
   * {@code Restate.run("start-streaming-query", ...)} in a try/catch
   * that protects against the PR #237 ship-blocker.
   *
   * <p><b>Without the catch (the PR #237 bug):</b> a persistent
   * launcher failure (Spark misconfigured, checkpoint path
   * unwritable, etc.) throws out of {@code Restate.run} and the
   * handler exits with a raw exception. Restate retries the entire
   * handler from step 1. Step 0's BLOCKED guard won't halt retries
   * — because step 3 didn't set BLOCKED. NET EFFECT: the journal
   * grows unboundedly and operators see no signal, defeating the
   * PR #233 {@code RECONCILE_BLOCKED} contract.
   *
   * <p><b>The catch behavior:</b>
   * <ul>
   *   <li>{@code STATUS = "failed"} + {@code RECONCILE_BLOCKED = true}
   *       + {@code ERROR_COUNT++} — operators see {@code STATUS=failed}
   *       via {@code getStatus()}.
   *   <li>Throw a {@link dev.restate.sdk.common.TerminalException}
   *       with HTTP 400 (BAD_REQUEST_CODE) so Restate marks the
   *       invocation as terminally failed rather than retrying.
   *   <li>The query registry is consistent: the closure threw
   *       before {@code handles.put}, so no live handle exists
   *       for an orphan query.
   * </ul>
   *
   * <p>To retry after the operator fixes the underlying cause, they
   * invoke {@code /clearReconcileBlock} (or simply {@code /restart}
   * which clears the block at entry).
   *
   * <p>Visible-for-testing — package-private so unit tests can drive
   * the failure path with a stub launcher.
   */
  void startQueryWithFailureTracking(
      String streamId,
      SemanticTable model,
      StreamRunRequest request,
      dev.restate.sdk.Restate.State state) {
    try {
      Restate.run(
          "start-streaming-query",
          () -> {
            StreamingQuery query = launcher.start(model, request);
            handles.put(streamId, query);
          });
    } catch (RuntimeException launcherFailure) {
      state.set(STATUS, "failed");
      state.set(RECONCILE_BLOCKED, true);
      state.set(ERROR_COUNT, state.get(ERROR_COUNT).orElse(0L) + 1L);
      throw new dev.restate.sdk.common.TerminalException(
          dev.restate.sdk.common.TerminalException.BAD_REQUEST_CODE,
          "StreamingService.run: launcher.start failed (stream-id="
              + streamId
              + ", cause="
              + launcherFailure.getMessage()
              + "). Operators must invoke /restart after fixing the underlying cause.");
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

  /** Minimal JSON string escaper — see {@link JsonEscaper#escape(String)}. */
  private static String escape(String s) {
    return JsonEscaper.escape(s);
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
