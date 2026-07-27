package io.semanticdf.platform.audit;

import dev.restate.sdk.Restate;
import dev.restate.sdk.annotation.Handler;
import dev.restate.sdk.annotation.VirtualObject;
import dev.restate.sdk.common.StateKey;

import java.time.Instant;
import java.util.List;

/**
 * AuditService — per-tenant audit log.
 *
 * Key: tenant. Per-key serialization in Restate replaces the prior
 * "in-process ring buffer with global contention" pattern from
 * {@code InMemoryAuditSink}. The journal holds only the last-write
 * offset + last dedup hash for idempotency; the actual audit events
 * live in Postgres (durable, queryable) via {@link AuditEventStore}.
 *
 * State placement rule (per docs/design/platform-architecture.md):
 *   - Journal: last write offset, last dedup hash (coordination only —
 *     the fast-path dedup gate before hitting Postgres)
 *   - Postgres (via {@link AuditEventStore}): the actual audit events
 *     — durable, queryable, retention-managed
 *
 * Dedup-hash contract: the dedup hash must be supplied by the caller
 * (RestateAuditSink for streaming events; QueryService for query
 * events). For query events, the hash is computed by the library's
 * {@code io.semanticdf.audit.AuditEvent.dedupHashOf}; for streaming
 * events, by {@code StreamingDedupHash}. The service just persists
 * whatever it's handed.
 *
 * Determinism discipline: the {@code ts} on the journal update uses
 * {@code Restate.instantNow()} (replay-stable), not
 * {@code System.currentTimeMillis()} (wall-clock-of-write, drifts on
 * replay).
 */
@VirtualObject
public class AuditService {

  private static final StateKey<Long> LAST_WRITE_OFFSET = StateKey.of("lastWriteOffset", Long.class);
  private static final StateKey<String> LAST_DEDUP_HASH = StateKey.of("lastDedupHash", String.class);

  private final AuditEventStore store;

  /**
   * Constructor. Used by {@link io.semanticdf.platform.PlatformApplication}
   * (composition root) which wires a {@link PostgresAuditEventStore} when
   * {@code SEMANTICDF_AUDIT_PERSIST=true}, or a {@link NoOpAuditEventStore}
   * otherwise. Tests substitute their own store via the same constructor
   * — visible-for-testing pattern mirrors {@code StreamingService}'s
   * 5-constructor overload chain.
   */
  public AuditService(AuditEventStore store) {
    this.store = java.util.Objects.requireNonNull(store, "store");
  }

  /**
   * Convenience for tests + future ProgrammaticMain helpers. Equivalent
   * to {@code new AuditService(NoOpAuditEventStore())} — the
   * journal-only dedup-path that lets {@link #append} short-circuit
   * via {@code LAST_DEDUP_HASH} without a Postgres round-trip.
   */
  public static AuditService noOp() {
    return new AuditService(new NoOpAuditEventStore());
  }

  /**
   * Append an audit event. The dedup hash prevents duplicate writes
   * within the same journal key:
   *
   * <ol>
   *   <li><b>Fast path (journal check):</b> if the journal's
   *       {@code LAST_DEDUP_HASH} already equals
   *       {@code request.dedupHash()}, this call is a no-op.
   *   <li><b>Replay-safe write (in {@code Restate.run}):</b> call
   *       the {@link AuditEventStore#append} so the Postgres
   *       INSERT — including any retried-by-HikariCP insert — is
   *       journaled. On replay, the cached result returns without
   *       re-executing the lambda.
   *   <li><b>Journal bookkeeping:</b> update
   *       {@code LAST_DEDUP_HASH} + {@code LAST_WRITE_OFFSET} so the
   *       next append on the same tenant can short-circuit.
   * </ol>
   *
   * <p>Failures: if {@link AuditEventStore#append} throws, the
   * {@code Restate.run} lambda propagates it; the handler throws;
   * Restate retries. The journal's {@code LAST_DEDUP_HASH} is only
   * updated on the success path, so retries re-attempt the
   * Postgres write — which is itself idempotent via
   * {@code ON CONFLICT DO NOTHING}. Net effect: exactly-once
   * persistence across restarts.
   */
  @Handler
  public void append(AuditEventRequest request) {
    var state = Restate.state();
    String currentHash = state.get(LAST_DEDUP_HASH).orElse("");
    if (currentHash.equals(request.dedupHash())) {
      // Already written — short-circuit the Postgres hop.
      return;
    }
    final Instant ts = Instant.ofEpochMilli(request.timestamp());
    final Long newOffset =
        Restate.run(
            "audit.append",
            Long.class,
            () ->
                store.append(
                    request.tenant(), request.eventType(), ts,
                    request.dedupHash(), request.payload()));
    state.set(LAST_DEDUP_HASH, request.dedupHash());
    state.set(LAST_WRITE_OFFSET, newOffset);
  }

  /**
   * Read the current write offset.
   *
   * <p>{@code @Shared} so concurrent reads don't serialize against
   * writes. Reads from the journal directly (cheap); the actual
   * events live in Postgres via {@link AuditEventStore#queryRecent}.
   */
  @dev.restate.sdk.annotation.Shared
  @Handler
  public long getLastWriteOffset() {
    return Restate.state().get(LAST_WRITE_OFFSET).orElse(0L);
  }

  /**
   * Windowed readback of audit events.
   *
   * <p>{@code @Shared}: concurrent reads are not serialized.
   * The Postgres query runs inside {@code Restate.run} so its
   * exact result IS journaled (returning a different row set
   * across replays would be a correctness issue — concurrent
   * appends could shift the window). This trades replay
   * determinism for the ability to do a windowed scan; the
   * restate-runtime's own invocations of {@code Restate.run} are
   * idempotent so a retry that produces a slightly different
   * row set within the same restate invocation would be
   * observable.
   *
   * <p>Note: the {@code AuditableAuditService.append} fast-path
   * dedup makes append retries safe; this queryRecent pattern
   * assumes a more complex concurrency model — operators using
   * the read API in production should pair with the audit-event
   * offset, not assume monotonic-window semantics.
   */
  @dev.restate.sdk.annotation.Shared
  @Handler
  public List<AuditEventStore.AuditEventRow> queryRecent(QueryRecentRequest req) {
    return Restate.run(
        "query-recent",
        (Class<List<AuditEventStore.AuditEventRow>>) (Class<?>) List.class,
        () -> store.queryRecent(req.tenant(), req.since(), req.until(), req.limit()));
  }

  /** Request DTO for {@link #append(AuditEventRequest)}. */
  public record AuditEventRequest(
      String tenant,
      String eventType,
      long timestamp,
      String dedupHash,
      String payload) {}

  /** Request DTO for {@link #queryRecent(QueryRecentRequest)}. */
  public record QueryRecentRequest(String tenant, Instant since, Instant until, int limit) {}
}
