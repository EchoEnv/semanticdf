package io.semanticdf.platform.audit;

import java.io.Closeable;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;

/**
 * Durable storage for the platform's audit events.
 *
 * <p>State-placement rule (per {@code docs/design/platform-architecture.md}
 * §2.3): journal = coordination (recent, recoverable from replay);
 * Postgres = record (durable, queryable). The audit log is a record —
 * it lives here, not in the Restate journal.
 *
 * <p>Idempotency contract: {@link #append} is keyed on
 * {@code (tenant, ts, dedupHash)}. The implementation MUST treat
 * duplicate inserts as no-ops (returning the existing row's offset,
 * not an error). Postgres implements this with
 * {@code INSERT ... ON CONFLICT DO NOTHING RETURNING offset_value};
 * {@link NoOpAuditEventStore} returns a synthetic offset for the
 * no-Postgres path.
 *
 * <p>The {@code payload} is opaque JSON; the store treats it as a
 * black-box string. Event-type-specific decoding is the caller's job.
 * For query events, the dedup-hash is computed by the library's
 * {@code io.semanticdf.audit.AuditEvent.dedupHashOf}; for streaming
 * events, by the platform's {@code StreamingDedupHash}. The store
 * just persists whichever the caller hands in.
 *
 * <p>{@link #close} releases connection-pool resources.
 *
 * @see NoOpAuditEventStore
 * @see PostgresAuditEventStore
 */
public interface AuditEventStore extends Closeable {

  /**
   * Persist an audit event. Idempotent on
   * {@code (tenant, ts, dedupHash)} — repeated calls with the
   * same key return the same offset.
   *
   * @param tenant    the audit tenant (P1: always {@code "default"})
   * @param eventType event-type discriminator
   *                  (e.g. {@code "streaming.started"},
   *                  {@code "query.executed"})
   * @param ts        when the event occurred (replay-stable in
   *                  Restate context via {@code Restate.instantNow()})
   * @param dedupHash SHA-256 hex digest; for query events, use
   *                  {@code AuditEvent.dedupHashOf}; for streaming
   *                  events, use {@code StreamingDedupHash}
   * @param payload   JSON-encoded event body
   * @return the offset_value (BIGSERIAL) for the persisted row;
   *         equal across repeated calls with the same key
   * @throws SQLException on persistence failure
   */
  long append(String tenant, String eventType, Instant ts, String dedupHash, String payload)
      throws SQLException;

  /**
   * Windowed readback. The implementation MUST use the
   * {@code (tenant, ts)} index for the time-range scan; PG
   * partition-pruning is expected to apply when {@code since} /
   * {@code until} span fewer partitions than the table holds.
   *
   * @param tenant the audit tenant
   * @param since  inclusive lower bound
   * @param until  exclusive upper bound
   * @param limit  maximum number of rows to return (caller-side cap;
   *               implementations MAY also apply a hard ceiling)
   * @return events in ascending {@code ts} order, never exceeding
   *         {@code limit}
   */
  List<AuditEventRow> queryRecent(String tenant, Instant since, Instant until, int limit);

  /**
   * DDL bootstrap. Implementations MUST be idempotent — calling
   * repeatedly must not fail if the schema already exists.
   *
   * @throws SQLException on DDL failure
   */
  void ensureSchema() throws SQLException;

  /**
   * Readback row shape. Mirrors the persisted columns — payload
   * is opaque JSON.
   */
  record AuditEventRow(
      String tenant,
      String eventType,
      Instant ts,
      String dedupHash,
      String payload) {}
}
