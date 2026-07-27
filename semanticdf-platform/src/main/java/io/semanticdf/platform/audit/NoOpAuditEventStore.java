package io.semanticdf.platform.audit;

import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The {@code SEMANTICDF_AUDIT_PERSIST=false} (default) backing for
 * {@link AuditEventStore}.
 *
 * <p>Audit events reach {@code AuditService}'s journal
 * {@code LAST_DEDUP_HASH} / {@code LAST_WRITE_OFFSET} (dedup) but
 * are NOT persisted to Postgres. The synthetic offset that
 * {@link #append} returns is a monotonic counter shared across
 * tenants — operators see growing journal offsets on the per-tenant
 * Restate VirtualObject and can distinguish this no-op mode from
 * real Postgres-backed persistence only by checking
 * {@code SEMANTICDF_AUDIT_PERSIST}.
 *
 * <p>{@link #queryRecent} returns an empty list — there is no
 * write-side history to read. Operators wanting historical audit
 * events must flip {@code SEMANTICDF_AUDIT_PERSIST=true} (or run a
 * dedicated retention pipeline on the in-memory event stream).
 *
 * <p>The synthetic offset MUST be globally monotonic across all
 * tenants on the same JVM instance, otherwise {@code AuditService}'s
 * journal offsets would collide across tenants on replay — instead
 * we let it diverge across JVM restarts (it starts at 1 again),
 * which is fine because Restate's journal is per-key.
 */
public final class NoOpAuditEventStore implements AuditEventStore {

  private final AtomicLong syntheticOffset = new AtomicLong(1L);

  @Override
  public long append(String tenant, String eventType, Instant ts, String dedupHash,
                     String payload) throws SQLException {
    if (tenant == null || tenant.isBlank()) {
      throw new IllegalArgumentException("tenant must be non-blank");
    }
    if (dedupHash == null || dedupHash.isBlank()) {
      throw new IllegalArgumentException("dedupHash must be non-blank");
    }
    // Monotonic in-JVM. Note: this counter is NOT Postgres-compatible —
    // operators reading the journal offset see a different value than
    // what would be in Postgres if persistence were enabled.
    return syntheticOffset.getAndIncrement();
  }

  @Override
  public List<AuditEventRow> queryRecent(String tenant, Instant since, Instant until, int limit) {
    // No write-side history; return empty. Operators get audit reads from
    // the audit_events table only when SEMANTICDF_AUDIT_PERSIST=true.
    return List.of();
  }

  @Override
  public void ensureSchema() throws SQLException {
    // No schema to ensure in no-op mode.
  }

  @Override
  public void close() {
    // No resources to release.
  }
}
