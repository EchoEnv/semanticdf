package io.semanticdf.platform.audit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Instant;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

/**
 * Test for {@link PostgresAuditEventStore} against a real Postgres
 * (Testcontainers).
 *
 * <p>Mirrors {@code PostgresStreamCatalogTest} — same GenericContainer
 * (no extra testcontainers-postgresql dep needed), same truncate-
 * per-test isolation, same setUp/tearDown lifecycle.
 */
class PostgresAuditEventStoreTest {

  static GenericContainer<?> postgres;
  static PostgresAuditEventStore store;

  @BeforeAll
  static void setUp() {
    postgres =
        new GenericContainer<>("postgres:16-alpine")
            .withEnv("POSTGRES_USER", "test")
            .withEnv("POSTGRES_PASSWORD", "test")
            .withEnv("POSTGRES_DB", "semanticdf_test")
            .withExposedPorts(5432);
    postgres.start();

    String jdbcUrl =
        "jdbc:postgresql://"
            + postgres.getHost()
            + ":"
            + postgres.getMappedPort(5432)
            + "/semanticdf_test";
    store = new PostgresAuditEventStore(jdbcUrl, "test", "test");

    try (Connection conn = store.dataSource.getConnection();
        PreparedStatement stmt = conn.prepareStatement("SELECT 1")) {
      stmt.executeQuery();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @BeforeEach
  void truncateForTest() {
    try (Connection conn = store.dataSource.getConnection();
        PreparedStatement stmt =
            conn.prepareStatement(
                "DELETE FROM " + PostgresAuditEventStore.TABLE_NAME)) {
      stmt.executeUpdate();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @AfterAll
  static void tearDown() {
    if (store != null) {
      store.close();
    }
    if (postgres != null) {
      postgres.stop();
    }
  }

  // --- happy path ---

  @Test
  void append_writesToPostgresAndReturnsOffset() throws Exception {
    Instant now = Instant.now();
    long off = store.append("t1", "streaming.started", now, "hash-1", "{\"k\":\"v\"}");
    // The exact BIGSERIAL value depends on what prior tests have
    // inserted (DELETE doesn't reset sequences). Verify it's a
    // positive long — the contract is monotonicity, not the
    // starting value.
    assertEquals(true, off > 0L, "offset must be positive: " + off);

    try (Connection conn = store.dataSource.getConnection();
        PreparedStatement sel =
            conn.prepareStatement(
                "SELECT event_type, dedup_hash, payload FROM "
                    + PostgresAuditEventStore.TABLE_NAME
                    + " WHERE tenant='t1' AND dedup_hash='hash-1'")) {
      ResultSet rs = sel.executeQuery();
      assertEquals(true, rs.next());
      assertEquals("streaming.started", rs.getString(1));
      assertEquals("hash-1", rs.getString(2));
      assertEquals("{\"k\":\"v\"}", rs.getString(3));
      assertEquals(false, rs.next(), "should be exactly one row");
    }
  }

  // --- idempotency ---

  @Test
  void append_idempotentOnSameDedupHash() throws Exception {
    Instant now = Instant.now();
    long first = store.append("t2", "streaming.started", now, "same-hash", "{}");
    long second = store.append("t2", "streaming.started", now, "same-hash", "{}");
    long third = store.append("t2", "streaming.started", now, "same-hash", "{}");

    assertEquals(first, second, "second call must return same offset");
    assertEquals(first, third, "third call must return same offset");

    // Verify a single row exists.
    try (Connection conn = store.dataSource.getConnection();
        PreparedStatement count =
            conn.prepareStatement(
                "SELECT COUNT(*) FROM "
                    + PostgresAuditEventStore.TABLE_NAME
                    + " WHERE tenant='t2'")) {
      ResultSet rs = count.executeQuery();
      rs.next();
      assertEquals(1L, rs.getLong(1), "idempotent insert must produce one row");
    }
  }

  // --- queryRecent ---

  @Test
  void queryRecent_honorsTimeRange() throws Exception {
    // Postgres TIMESTAMPTZ stores microsecond precision; Instant.now()
    // has nanosecond precision. Truncate to micros before INSERT to
    // avoid a 1-1000-ns drift between the original value and the
    // round-tripped value via getTimestamp().toInstant().
    Instant base = Instant.now().truncatedTo(ChronoUnit.MICROS);
    Instant t1 = base.minusSeconds(60);
    Instant t2 = base.minusSeconds(30);
    Instant t3 = base;
    Instant t4 = base.plusSeconds(30);

    store.append("t3", "type-a", t1, "h1", "p1");
    store.append("t3", "type-a", t2, "h2", "p2");
    store.append("t3", "type-b", t3, "h3", "p3");
    store.append("t3", "type-a", t4, "h4", "p4");

    List<AuditEventStore.AuditEventRow> windowed =
        store.queryRecent("t3", t2, t4, /*limit*/ 100);
    assertEquals(2, windowed.size(), "expected exactly t2 and t3 (since inclusive, until exclusive)");
    assertEquals(t2, windowed.get(0).ts());
    assertEquals(t3, windowed.get(1).ts());
  }

  @Test
  void queryRecent_honorsLimit() throws Exception {
    Instant base = Instant.now().truncatedTo(ChronoUnit.MICROS);
    for (int i = 0; i < 5; i++) {
      store.append("t4", "t", base.plusSeconds(i), "h" + i, "p" + i);
    }
    List<AuditEventStore.AuditEventRow> limited =
        store.queryRecent("t4", base.minusSeconds(1), base.plusSeconds(100), /*limit*/ 3);
    assertEquals(3, limited.size(), "limit=3 must cap at 3 rows");
    assertEquals(base, limited.get(0).ts());
    assertEquals(base.plusSeconds(1), limited.get(1).ts());
    assertEquals(base.plusSeconds(2), limited.get(2).ts());
  }

  @Test
  void queryRecent_returnsEmptyForUnknownTenant() throws Exception {
    List<AuditEventStore.AuditEventRow> none =
        store.queryRecent("unknown", Instant.now().minusSeconds(60), Instant.now(), 100);
    assertEquals(0, none.size());
  }

  // --- isolation: not-yet-existing month ---

  @Test
  void ensureSchema_createsPartitionForNextMonth() throws Exception {
    // Manually create the partition for the month AFTER the
    // boot-default range (current + next 2 → next + 3 is needed).
    YearMonth target = YearMonth.now(ZoneOffset.UTC).plusMonths(3);
    try (Connection conn = store.dataSource.getConnection()) {
      store.createMonthlyPartition(conn, target);
    }

    // Insert one event in that month — should succeed.
    Instant tsInTargetMonth =
        target.atDay(15).atStartOfDay(ZoneOffset.UTC).toInstant();
    store.append("t5", "future", tsInTargetMonth, "h", "p");

    // Verify the partition is reachable via queryRecent across
    // the future-month window.
    List<AuditEventStore.AuditEventRow> found =
        store.queryRecent(
            "t5",
            target.atDay(1).atStartOfDay(ZoneOffset.UTC).toInstant(),
            target.atEndOfMonth().plusDays(1).atStartOfDay(ZoneOffset.UTC).toInstant(),
            100);
    assertEquals(1, found.size());
  }

  // --- argument validation ---

  @Test
  void append_rejectsNullOrBlankFields() {
    Instant now = Instant.now();
    assertThrows(IllegalArgumentException.class,
        () -> store.append(null, "t", now, "h", "p"));
    assertThrows(IllegalArgumentException.class,
        () -> store.append("", "t", now, "h", "p"));
    assertThrows(IllegalArgumentException.class,
        () -> store.append("t", null, now, "h", "p"));
    assertThrows(IllegalArgumentException.class,
        () -> store.append("t", "t", now, null, "p"));
    assertThrows(IllegalArgumentException.class,
        () -> store.append("t", "t", null, "h", "p"));
    assertThrows(IllegalArgumentException.class,
        () -> store.append("t", "t", now, "h", null), "payload must be non-null (may be empty)");
  }

  @Test
  void queryRecent_rejectsBlankTenantAndNonPositiveLimit() {
    Instant now = Instant.now();
    assertThrows(IllegalArgumentException.class,
        () -> store.queryRecent("", now, now.plusSeconds(1), 100));
    assertThrows(IllegalArgumentException.class,
        () -> store.queryRecent(null, now, now.plusSeconds(1), 100));
    assertEquals(0, store.queryRecent("t", now, now.plusSeconds(1), 0).size(),
        "limit=0 must return empty without hitting the DB");
    assertEquals(0, store.queryRecent("t", now, now.plusSeconds(1), -1).size(),
        "negative limit must return empty without hitting the DB");
  }

  // --- offset monotonicity across tenants ---

  @Test
  void append_offsetsAreMonotonicAcrossTenants() throws Exception {
    Instant now = Instant.now();
    long a1 = store.append("ta", "t", now, "h1", "p");
    long b1 = store.append("tb", "t", now, "h2", "p");
    long a2 = store.append("ta", "t", now.plusSeconds(1), "h3", "p");
    long b2 = store.append("tb", "t", now.plusSeconds(2), "h4", "p");

    assertNotEquals(a1, b1, "different (ts, dedup_hash) tuples must produce different offsets");
    assertNotEquals(a1, a2);
    assertNotEquals(b1, b2);
    // BIGSERIAL is globally monotonic on the table, regardless of
    // tenant — operators reading the offset across tenants see a
    // consistent log position.
    assertEquals(true, a2 > a1);
    assertEquals(true, b2 > b1);
  }
}
