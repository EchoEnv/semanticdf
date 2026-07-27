package io.semanticdf.platform.audit;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

/**
 * Postgres-backed {@link AuditEventStore}.
 *
 * <p>Mirrors {@code streaming/PostgresStreamCatalog} for consistency:
 * same {@code semanticdf_catalog} schema, same pool sizing (HikariCP,
 * 4 connections, 2s connection-timeout), same {@code CREATE SCHEMA/
 * TABLE IF NOT EXISTS} discipline, same {@code INSERT ... ON CONFLICT
 * DO NOTHING} idempotency.
 *
 * <p><b>Schema:</b>
 * <pre>{@code
 * CREATE TABLE semanticdf_catalog.audit_events (
 *   tenant        TEXT        NOT NULL,
 *   ts            TIMESTAMPTZ NOT NULL,
 *   event_type    TEXT        NOT NULL,
 *   dedup_hash    TEXT        NOT NULL,
 *   payload       TEXT        NOT NULL,
 *   offset_value  BIGSERIAL,
 *   PRIMARY KEY (tenant, ts, dedup_hash)
 * ) PARTITION BY RANGE (ts);
 * }</pre>
 *
 * <p><b>Partitioning:</b> monthly partitions on {@code ts}, with
 * current-month + next-2-months pre-created at boot. Insertion
 * outside that window fails — operators running this against an
 * older-than-current-month dataset must run {@link #ensureSchema}
 * again before the rollover, OR
 * the path is explicitly out-of-scope for P1 (the platform's audit
 * stream is recent-only).
 *
 * <p><b>Indices:</b>
 * <ul>
 *   <li>{@code (tenant, ts DESC)} — primary read path
 *   <li>{@code (tenant, event_type, ts DESC)} — read-by-event-type
 * </ul>
 * Both attached to the parent table so PG propagates them to partitions.
 */
public final class PostgresAuditEventStore implements AuditEventStore {

  static final String SCHEMA = "semanticdf_catalog";
  static final String TABLE_NAME = SCHEMA + ".audit_events";

  /** Visible-for-testing — tests in the same package open this to verify schema state. */
  final HikariDataSource dataSource;

  /**
   * Construct with explicit JDBC URL + credentials. Used by
   * {@code PlatformApplication} (production) and unit tests
   * (Testcontainers Postgres).
   */
  public PostgresAuditEventStore(String jdbcUrl, String user, String password) {
    this.dataSource =
        new HikariDataSource(
            new HikariConfig() {
              {
                setJdbcUrl(jdbcUrl);
                setUsername(user);
                setPassword(password);
                setMaximumPoolSize(4);
                setMinimumIdle(1);
                setConnectionTimeout(2_000L);
                setPoolName("semanticdf-platform-audit");
              }
            });
    initializeSchema();
  }

  /** Run at construction. {@code CREATE SCHEMA}/{@code CREATE TABLE}/{@code CREATE PARTITION}/{@code CREATE INDEX} IF NOT EXISTS. */
  private void initializeSchema() {
    try (Connection conn = dataSource.getConnection()) {
      try (PreparedStatement schema =
          conn.prepareStatement("CREATE SCHEMA IF NOT EXISTS " + SCHEMA)) {
        schema.execute();
      }
      try (PreparedStatement table =
          conn.prepareStatement(
              "CREATE TABLE IF NOT EXISTS "
                  + TABLE_NAME
                  + " (tenant        TEXT        NOT NULL,"
                  + "  ts            TIMESTAMPTZ NOT NULL,"
                  + "  event_type    TEXT        NOT NULL,"
                  + "  dedup_hash    TEXT        NOT NULL,"
                  + "  payload       TEXT        NOT NULL,"
                  + "  offset_value  BIGSERIAL,"
                  + "  PRIMARY KEY (tenant, ts, dedup_hash))"
                  + " PARTITION BY RANGE (ts)")) {
        table.execute();
      }
      try (PreparedStatement idx1 =
          conn.prepareStatement(
              "CREATE INDEX IF NOT EXISTS audit_events_tenant_ts_idx ON "
                  + TABLE_NAME + " (tenant, ts DESC)")) {
        idx1.execute();
      }
      try (PreparedStatement idx2 =
          conn.prepareStatement(
              "CREATE INDEX IF NOT EXISTS audit_events_tenant_event_type_ts_idx ON "
                  + TABLE_NAME + " (tenant, event_type, ts DESC)")) {
        idx2.execute();
      }
      // Pre-create current month + next 2. Operators running this
      // against a dataset older than 2 months from now will fail at
      // insert; ensureSchema() may be re-invoked to extend.
      YearMonth currentMonth = YearMonth.now(ZoneOffset.UTC);
      for (int delta = 0; delta <= 2; delta++) {
        YearMonth ym = currentMonth.plusMonths(delta);
        createMonthlyPartition(conn, ym);
      }
    } catch (SQLException e) {
      throw new IllegalStateException(
          "Failed to initialize AuditEventStore schema at " + TABLE_NAME, e);
    }
  }

  /**
   * Idempotent partition creation. Safe to invoke repeatedly.
   * Visible-for-testing.
   */
  void createMonthlyPartition(Connection conn, YearMonth ym) throws SQLException {
    String partName =
        "audit_events_" + String.format("%04d%02d", ym.getYear(), ym.getMonthValue());
    String from = ym.atDay(1).toString();
    String to = ym.plusMonths(1).atDay(1).toString();
    String sql =
        "CREATE TABLE IF NOT EXISTS "
            + SCHEMA
            + "."
            + partName
            + " PARTITION OF "
            + TABLE_NAME
            + " FOR VALUES FROM ('"
            + from
            + "') TO ('"
            + to
            + "')";
    try (PreparedStatement stmt = conn.prepareStatement(sql)) {
      stmt.execute();
    }
  }

  @Override
  public void ensureSchema() {
    try (Connection conn = dataSource.getConnection()) {
      // Re-run the partition creation in case operators ran the
      // platform across a month boundary. Re-running CREATE TABLE
      // IF NOT EXISTS for existing partitions is a no-op.
      YearMonth currentMonth = YearMonth.now(ZoneOffset.UTC);
      for (int delta = 0; delta <= 2; delta++) {
        YearMonth ym = currentMonth.plusMonths(delta);
        createMonthlyPartition(conn, ym);
      }
    } catch (SQLException e) {
      throw new IllegalStateException(
          "Failed to refresh audit_events partitions near " + TABLE_NAME, e);
    }
  }

  @Override
  public long append(String tenant, String eventType, Instant ts, String dedupHash,
                     String payload) throws SQLException {
    if (tenant == null || tenant.isBlank()) {
      throw new IllegalArgumentException("tenant must be non-blank");
    }
    if (eventType == null || eventType.isBlank()) {
      throw new IllegalArgumentException("eventType must be non-blank");
    }
    if (dedupHash == null || dedupHash.isBlank()) {
      throw new IllegalArgumentException("dedupHash must be non-blank");
    }
    if (payload == null) {
      throw new IllegalArgumentException("payload must be non-null (may be empty string)");
    }
    if (ts == null) {
      throw new IllegalArgumentException("ts must be non-null");
    }
    // ON CONFLICT DO NOTHING RETURNING offset_value: returns the new
    // offset on insert, returns 0 rows on conflict (we read existing
    // offset via SELECT in the same transaction).
    try (Connection conn = dataSource.getConnection();
        PreparedStatement insert =
            conn.prepareStatement(
                "INSERT INTO "
                    + TABLE_NAME
                    + " (tenant, ts, event_type, dedup_hash, payload)"
                    + " VALUES (?, ?, ?, ?, ?)"
                    + " ON CONFLICT DO NOTHING"
                    + " RETURNING offset_value")) {
      insert.setString(1, tenant);
      insert.setTimestamp(2, java.sql.Timestamp.from(ts));
      insert.setString(3, eventType);
      insert.setString(4, dedupHash);
      insert.setString(5, payload);
      try (ResultSet rs = insert.executeQuery()) {
        if (rs.next()) {
          return rs.getLong(1);
        }
      }
      // Conflict — read the existing row's offset in the same connection.
      try (PreparedStatement sel =
          conn.prepareStatement(
              "SELECT offset_value FROM "
                  + TABLE_NAME
                  + " WHERE tenant=? AND ts=? AND dedup_hash=?")) {
        sel.setString(1, tenant);
        sel.setTimestamp(2, java.sql.Timestamp.from(ts));
        sel.setString(3, dedupHash);
        try (ResultSet rs = sel.executeQuery()) {
          if (rs.next()) {
            return rs.getLong(1);
          }
        }
      }
      throw new IllegalStateException(
          "Postgres accepted ON CONFLICT DO NOTHING but row not found — tenant="
              + tenant
              + " ts="
              + ts
              + " dedupHash="
              + dedupHash);
    }
  }

  @Override
  public List<AuditEventRow> queryRecent(String tenant, Instant since, Instant until, int limit) {
    if (tenant == null || tenant.isBlank()) {
      throw new IllegalArgumentException("tenant must be non-blank");
    }
    if (limit <= 0) {
      return List.of();
    }
    List<AuditEventRow> result = new ArrayList<>();
    // ASC by ts so operators see time-ordered history; PG index
    // (tenant, ts DESC) is used efficiently (PG can scan DESC index
    // in reverse at no extra cost).
    String sql =
        "SELECT tenant, event_type, ts, dedup_hash, payload FROM "
            + TABLE_NAME
            + " WHERE tenant=? AND ts>=? AND ts<?"
            + " ORDER BY ts ASC"
            + " LIMIT ?";
    try (Connection conn = dataSource.getConnection();
        PreparedStatement sel = conn.prepareStatement(sql)) {
      sel.setString(1, tenant);
      sel.setTimestamp(2, java.sql.Timestamp.from(since));
      sel.setTimestamp(3, java.sql.Timestamp.from(until));
      sel.setInt(4, limit);
      try (ResultSet rs = sel.executeQuery()) {
        while (rs.next()) {
          result.add(
              new AuditEventRow(
                  rs.getString("tenant"),
                  rs.getString("event_type"),
                  rs.getTimestamp("ts").toInstant(),
                  rs.getString("dedup_hash"),
                  rs.getString("payload")));
        }
      }
    } catch (SQLException e) {
      throw new IllegalStateException(
          "Failed to query " + TABLE_NAME + " for tenant=" + tenant, e);
    }
    return result;
  }

  @Override
  public void close() {
    dataSource.close();
  }

  /**
   * Visible-for-testing helper: the UTC {@link LocalDate} equivalent
   * of a timestamp, for asserting partition coverage in tests.
   */
  static LocalDate utcDate(Instant ts) {
    return ts.atZone(ZoneOffset.UTC).toLocalDate();
  }
}
