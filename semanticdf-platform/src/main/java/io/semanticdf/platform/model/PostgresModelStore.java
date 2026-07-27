package io.semanticdf.platform.model;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Postgres-backed {@link ModelStore}.
 *
 * <p>Mirrors {@code streaming/PostgresStreamCatalog} + the recent
 * {@code audit/PostgresAuditEventStore}: same
 * {@code semanticdf_catalog} schema, same pool sizing (HikariCP,
 * 4 connections, 2s connection-timeout), same {@code CREATE TABLE
 * IF NOT EXISTS} discipline, same {@code INSERT ... ON CONFLICT
 * DO NOTHING} idempotency.
 *
 * <p><b>Schema:</b>
 * <pre>{@code
 * CREATE TABLE semanticdf_catalog.models (
 *   model_name    TEXT NOT NULL,
 *   version       INT  NOT NULL,
 *   status        TEXT NOT NULL DEFAULT 'active',
 *   manifest_yaml TEXT NOT NULL,
 *   manifest_hash TEXT NOT NULL,
 *   registered_at TIMESTAMPTZ NOT NULL,
 *   lineage_json  TEXT NOT NULL DEFAULT '',
 *   PRIMARY KEY (model_name, version)
 * );
 * CREATE INDEX ON semanticdf_catalog.models (model_name);
 * }</pre>
 *
 * <p>Status is a small per-version marker ({@code active} /
 * {@code deprecated} / {@code failed}). Idempotent registration
 * preserves the original status \u2014 operators who re-register a
 * version with different status must {@code UPDATE} explicitly.
 */
public final class PostgresModelStore implements ModelStore {

  static final String SCHEMA = "semanticdf_catalog";
  static final String TABLE_NAME = SCHEMA + ".models";

  /** Visible-for-testing \u2014 tests in the same package open this to verify schema state. */
  final HikariDataSource dataSource;

  /**
   * Construct with explicit JDBC URL + credentials. Used by
   * {@code PlatformApplication} (production) and unit tests
   * (Testcontainers Postgres).
   */
  public PostgresModelStore(String jdbcUrl, String user, String password) {
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
                setPoolName("semanticdf-platform-models");
              }
            });
    initializeSchema();
  }

  /** Run at construction. {@code CREATE SCHEMA}/{@code CREATE TABLE}/{@code CREATE INDEX} IF NOT EXISTS. */
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
                  + " (model_name    TEXT        NOT NULL,"
                  + "  version       INT         NOT NULL,"
                  + "  status        TEXT        NOT NULL DEFAULT 'active',"
                  + "  manifest_yaml TEXT        NOT NULL,"
                  + "  manifest_hash TEXT        NOT NULL,"
                  + "  registered_at TIMESTAMPTZ NOT NULL,"
                  + "  lineage_json  TEXT        NOT NULL DEFAULT '',"
                  + "  PRIMARY KEY (model_name, version))")) {
        table.execute();
      }
      try (PreparedStatement idx =
          conn.prepareStatement(
              "CREATE INDEX IF NOT EXISTS models_name_idx ON "
                  + TABLE_NAME + " (model_name)")) {
        idx.execute();
      }
    } catch (SQLException e) {
      throw new IllegalStateException(
          "Failed to initialize ModelStore schema at " + TABLE_NAME, e);
    }
  }

  @Override
  public void ensureSchema() {
    try (Connection conn = dataSource.getConnection()) {
      // Idempotent re-run \u2014 IF NOT EXISTS makes this cheap. Operator
      // can invoke this on schema-version bumps without a downtime.
      try (PreparedStatement schema =
          conn.prepareStatement("CREATE SCHEMA IF NOT EXISTS " + SCHEMA)) {
        schema.execute();
      }
      try (PreparedStatement table =
          conn.prepareStatement(
              "CREATE TABLE IF NOT EXISTS "
                  + TABLE_NAME
                  + " (model_name    TEXT        NOT NULL,"
                  + "  version       INT         NOT NULL,"
                  + "  status        TEXT        NOT NULL DEFAULT 'active',"
                  + "  manifest_yaml TEXT        NOT NULL,"
                  + "  manifest_hash TEXT        NOT NULL,"
                  + "  registered_at TIMESTAMPTZ NOT NULL,"
                  + "  lineage_json  TEXT        NOT NULL DEFAULT '',"
                  + "  PRIMARY KEY (model_name, version))")) {
        table.execute();
      }
      try (PreparedStatement idx =
          conn.prepareStatement(
              "CREATE INDEX IF NOT EXISTS models_name_idx ON "
                  + TABLE_NAME + " (model_name)")) {
        idx.execute();
      }
    } catch (SQLException e) {
      throw new IllegalStateException(
          "Failed to refresh ModelStore schema at " + TABLE_NAME, e);
    }
  }

  @Override
  public ModelDefinition registerIfAbsent(
      String modelName,
      int version,
      String yaml,
      String manifestHash,
      Instant registeredAt,
      String lineageJson) throws SQLException {
    if (modelName == null || modelName.isBlank()) {
      throw new IllegalArgumentException("modelName must be non-blank");
    }
    if (version <= 0) {
      throw new IllegalArgumentException("version must be positive: " + version);
    }
    if (yaml == null) {
      throw new IllegalArgumentException("yaml must be non-null (may be empty string)");
    }
    if (manifestHash == null || manifestHash.isBlank()) {
      throw new IllegalArgumentException("manifestHash must be non-blank");
    }
    if (registeredAt == null) {
      throw new IllegalArgumentException("registeredAt must be non-null");
    }
    final String linJson = lineageJson == null ? "" : lineageJson;
    // ON CONFLICT DO NOTHING RETURNING \u2014 returns the row if inserted,
    // returns 0 rows if conflict. On conflict we read the existing
    // row in the same connection.
    try (Connection conn = dataSource.getConnection();
        PreparedStatement insert =
            conn.prepareStatement(
                "INSERT INTO "
                    + TABLE_NAME
                    + " (model_name, version, manifest_yaml, manifest_hash, registered_at, lineage_json)"
                    + " VALUES (?, ?, ?, ?, ?, ?)"
                    + " ON CONFLICT DO NOTHING"
                    + " RETURNING model_name, version, manifest_yaml, manifest_hash, registered_at, lineage_json")) {
      insert.setString(1, modelName);
      insert.setInt(2, version);
      insert.setString(3, yaml);
      insert.setString(4, manifestHash);
      insert.setTimestamp(5, java.sql.Timestamp.from(registeredAt));
      insert.setString(6, linJson);
      try (ResultSet rs = insert.executeQuery()) {
        if (rs.next()) {
          return readFromRs(rs);
        }
      }
      // Conflict \u2014 read the existing row.
      try (PreparedStatement sel =
          conn.prepareStatement(
              "SELECT model_name, version, manifest_yaml, manifest_hash,"
                  + " registered_at, lineage_json FROM "
                  + TABLE_NAME
                  + " WHERE model_name=? AND version=?")) {
        sel.setString(1, modelName);
        sel.setInt(2, version);
        try (ResultSet rs = sel.executeQuery()) {
          if (rs.next()) {
            return readFromRs(rs);
          }
        }
      }
      throw new IllegalStateException(
          "Postgres accepted ON CONFLICT DO NOTHING but row not found \u2014 "
              + "model_name=" + modelName + " version=" + version);
    }
  }

  @Override
  public List<ModelDefinition> listAll() throws SQLException {
    List<ModelDefinition> result = new ArrayList<>();
    try (Connection conn = dataSource.getConnection();
        PreparedStatement sel =
            conn.prepareStatement(
                "SELECT model_name, version, manifest_yaml, manifest_hash,"
                    + " registered_at, lineage_json FROM "
                    + TABLE_NAME
                    + " ORDER BY model_name, version");
        ResultSet rs = sel.executeQuery()) {
      while (rs.next()) {
        result.add(readFromRs(rs));
      }
    }
    return result;
  }

  @Override
  public ModelDefinition loadByName(String modelName, int version) throws SQLException {
    if (modelName == null || modelName.isBlank()) {
      throw new IllegalArgumentException("modelName must be non-blank");
    }
    if (version <= 0) {
      throw new IllegalArgumentException("version must be positive");
    }
    try (Connection conn = dataSource.getConnection();
        PreparedStatement sel =
            conn.prepareStatement(
                "SELECT model_name, version, manifest_yaml, manifest_hash,"
                    + " registered_at, lineage_json FROM "
                    + TABLE_NAME
                    + " WHERE model_name=? AND version=?")) {
      sel.setString(1, modelName);
      sel.setInt(2, version);
      try (ResultSet rs = sel.executeQuery()) {
        if (rs.next()) {
          return readFromRs(rs);
        }
        return null;
      }
    }
  }

  @Override
  public ModelDefinition loadLatest(String modelName) throws SQLException {
    if (modelName == null || modelName.isBlank()) {
      throw new IllegalArgumentException("modelName must be non-blank");
    }
    try (Connection conn = dataSource.getConnection();
        PreparedStatement sel =
            conn.prepareStatement(
                "SELECT model_name, version, manifest_yaml, manifest_hash,"
                    + " registered_at, lineage_json FROM "
                    + TABLE_NAME
                    + " WHERE model_name=?"
                    + " ORDER BY version DESC"
                    + " LIMIT 1")) {
      sel.setString(1, modelName);
      try (ResultSet rs = sel.executeQuery()) {
        if (rs.next()) {
          return readFromRs(rs);
        }
        return null;
      }
    }
  }

  @Override
  public void close() {
    dataSource.close();
  }

  /**
   * Read a {@link ModelDefinition} from a {@link ResultSet} positioned
   * on a row returned by any SELECT/RETURNING against {@code models}.
   */
  private static ModelDefinition readFromRs(ResultSet rs) throws SQLException {
    return new ModelDefinition(
        rs.getString("model_name"),
        rs.getInt("version"),
        rs.getString("manifest_yaml"),
        rs.getString("manifest_hash"),
        rs.getTimestamp("registered_at").toInstant(),
        rs.getString("lineage_json"));
  }
}
