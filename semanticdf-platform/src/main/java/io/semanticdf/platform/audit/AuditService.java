package io.semanticdf.platform.audit;

import dev.restate.sdk.Restate;
import dev.restate.sdk.annotation.Handler;
import dev.restate.sdk.annotation.VirtualObject;
import dev.restate.sdk.common.StateKey;

/**
 * AuditService — per-tenant audit log.
 *
 * Key: tenant. Per-key serialization in Restate replaces the prior
 * "in-process ring buffer with global contention" pattern from
 * {@code InMemoryAuditSink}. The journal holds only the last-write
 * offset + last dedup hash for idempotency; the actual audit events
 * live in Postgres (append-only, time-partitioned).
 *
 * State placement rule (per docs/design/platform-architecture.md):
 *   - Journal: last write offset, last dedup hash (coordination only)
 *   - Postgres: the actual audit events (queryable, long-retention)
 */
@VirtualObject
public class AuditService {

  private static final StateKey<Long> LAST_WRITE_OFFSET = StateKey.of("lastWriteOffset", Long.class);
  private static final StateKey<String> LAST_DEDUP_HASH = StateKey.of("lastDedupHash", String.class);

  /** Append an audit event. The dedup hash prevents duplicate writes
   * (Restate gives us exactly-once within an invocation; the dedup
   * hash is belt-and-suspenders for cross-invocations). */
  @Handler
  public void append(AuditEventRequest request) {
    var state = Restate.state();
    String currentHash = state.get(LAST_DEDUP_HASH).orElse("");
    if (currentHash.equals(request.dedupHash())) {
      // Already written; no-op
      return;
    }
    // TODO P1: insert into Postgres audit_events table, update offset
    state.set(LAST_DEDUP_HASH, request.dedupHash());
    state.set(LAST_WRITE_OFFSET, state.get(LAST_WRITE_OFFSET).orElse(0L) + 1);
  }

  /** Read the current write offset. */
  @dev.restate.sdk.annotation.Shared
  @Handler
  public long getLastWriteOffset() {
    return Restate.state().get(LAST_WRITE_OFFSET).orElse(0L);
  }

  /** Request DTO for {@link #append(AuditEventRequest)}. */
  public record AuditEventRequest(
      String tenant,
      String eventType,
      long timestamp,
      String dedupHash,
      String payload
  ) {}
}
