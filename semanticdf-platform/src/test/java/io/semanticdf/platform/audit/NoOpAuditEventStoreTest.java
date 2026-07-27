package io.semanticdf.platform.audit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Pure-unit tests for {@link NoOpAuditEventStore} — the
 * {@code SEMANTICDF_AUDIT_PERSIST=false} (default) backing.
 *
 * <p>The store has no behavior beyond monotonic-offset allocation
 * and field-level argument validation. These tests are quick
 * regression guards; the real Postgres-backed behavior is covered
 * by {@link PostgresAuditEventStoreTest}.
 */
class NoOpAuditEventStoreTest {

  @Test
  void append_returnsMonotonicOffsets() throws Exception {
    AuditEventStore store = new NoOpAuditEventStore();
    long a = store.append("t1", "x", Instant.now(), "h1", "p");
    long b = store.append("t1", "x", Instant.now(), "h2", "p");
    long c = store.append("t2", "y", Instant.now(), "h3", "p");
    assertTrue(c > b, "monotonic across tenants");
    assertTrue(b > a, "monotonic within a tenant");
  }

  @Test
  void append_rejectsBlankTenantOrDedupHash() {
    AuditEventStore store = new NoOpAuditEventStore();
    Instant now = Instant.now();
    assertThrows(IllegalArgumentException.class,
        () -> store.append(null, "x", now, "h", "p"));
    assertThrows(IllegalArgumentException.class,
        () -> store.append("", "x", now, "h", "p"));
    assertThrows(IllegalArgumentException.class,
        () -> store.append("t", "x", now, null, "p"));
    assertThrows(IllegalArgumentException.class,
        () -> store.append("t", "x", now, "", "p"));
  }

  @Test
  void queryRecent_alwaysReturnsEmpty() {
    AuditEventStore store = new NoOpAuditEventStore();
    List<AuditEventStore.AuditEventRow> result =
        store.queryRecent("t", Instant.now().minusSeconds(60), Instant.now(), 100);
    assertEquals(0, result.size(), "no-op store has no readback history");
  }

  @Test
  void ensureSchema_close_areNoOps() throws Exception {
    AuditEventStore store = new NoOpAuditEventStore();
    store.ensureSchema(); // does not throw
    store.close();         // does not throw
    // Calling close() twice is also a no-op.
    store.close();
  }
}
