package io.semanticdf.platform.streaming;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Postgres-backed {@link StreamCatalog}. Shares the Postgres instance
 * that Restate uses for its journal — they're under different
 * schemas (Restate is {@code public}, this catalog is {@code
 * semanticdf_catalog}), so there's no conflict.
 *
 * <p><b>Connection pool:</b> HikariCP with a small pool (4
 * connections — 1 for the sweep, 1-2 for catalog writes from
 * handlers, 1 headroom). {@code connectionTimeout=2s} so a
 * Postgres blip surfaces fast instead of blocking the sweep.
 *
 * <p><b>Schema:</b>
 * <pre>{@code
 * CREATE TABLE IF NOT EXISTS semanticdf_catalog.streaming_streams (
 *   stream_id           TEXT PRIMARY KEY,
 *   model_name          TEXT NOT NULL,
 *   query_shape         TEXT NOT NULL,
 *   checkpoint_location TEXT NOT NULL,
 *   registered_at       TIMESTAMPTZ NOT NULL DEFAULT now()
 * );
 * }</pre>
 *
 * <p>The metadata fields are the minimum needed for the
 * {@link StartupReconciler} to reconstruct a
 * {@code StreamRunRequest} for re-invoking {@code run()}.
 */
public final class PostgresStreamCatalog implements StreamCatalog {

  static final String SCHEMA = "semanticdf_catalog";
  static final String TABLE_NAME = SCHEMA + ".streaming_streams";

  /** Visible-for-testing — tests in the same package open this to verify schema state. */
  final HikariDataSource dataSource;

  /**
   * Construct with explicit JDBC URL + credentials. Used by
   * {@link PlatformApplication} (production) and unit tests
   * (Testcontainers Postgres).
   */
  public PostgresStreamCatalog(String jdbcUrl, String user, String password) {
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
                setPoolName("semanticdf-platform-catalog");
              }
            });
    initializeSchema();
  }

  /** Run at construction. {@code CREATE SCHEMA}/{@code CREATE TABLE} IF NOT EXISTS. */
  private void initializeSchema() {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement schema =
            conn.prepareStatement("CREATE SCHEMA IF NOT EXISTS " + SCHEMA);
        PreparedStatement table =
            conn.prepareStatement(
                "CREATE TABLE IF NOT EXISTS "
                    + TABLE_NAME
                    + " (stream_id TEXT PRIMARY KEY,"
                    + "  model_name TEXT NOT NULL,"
                    + "  query_shape TEXT NOT NULL,"
                    + "  checkpoint_location TEXT NOT NULL,"
                    + "  registered_at TIMESTAMPTZ NOT NULL DEFAULT now())")) {
      schema.execute();
      table.execute();
    } catch (SQLException e) {
      throw new IllegalStateException(
          "Failed to initialize StreamCatalog schema at " + TABLE_NAME, e);
    }
  }

  @Override
  public void registerIfAbsent(
      String streamId, String modelName, String queryShape, String checkpointLocation) {
    if (streamId == null || streamId.isBlank()) {
      throw new IllegalArgumentException("streamId must be non-blank");
    }
    if (modelName == null || modelName.isBlank()
        || queryShape == null || queryShape.isBlank()
        || checkpointLocation == null || checkpointLocation.isBlank()) {
      throw new IllegalArgumentException(
          "model/queryShape/checkpointLocation must all be non-blank (stream-id=" + streamId + ")");
    }
    // ON CONFLICT DO NOTHING preserves the original row's metadata —
    // operators who repeat a run() for the same stream-id should not
    // silently overwrite the registered model/checkpoint.
    try (Connection conn = dataSource.getConnection();
        PreparedStatement insert =
            conn.prepareStatement(
                "INSERT INTO "
                    + TABLE_NAME
                    + " (stream_id, model_name, query_shape, checkpoint_location)"
                    + " VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING")) {
      insert.setString(1, streamId);
      insert.setString(2, modelName);
      insert.setString(3, queryShape);
      insert.setString(4, checkpointLocation);
      insert.executeUpdate();
    } catch (SQLException e) {
      throw new IllegalStateException(
          "Failed to register stream-id=" + streamId + " in " + TABLE_NAME, e);
    }
  }

  @Override
  public List<StreamMetadata> findAll() {
    List<StreamMetadata> result = new ArrayList<>();
    try (Connection conn = dataSource.getConnection();
        PreparedStatement sel =
            conn.prepareStatement(
                "SELECT stream_id, model_name, query_shape, checkpoint_location FROM "
                    + TABLE_NAME
                    + " ORDER BY registered_at, stream_id");
        ResultSet rs = sel.executeQuery()) {
      while (rs.next()) {
        result.add(
            new StreamMetadata(
                rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4)));
      }
    } catch (SQLException e) {
      throw new IllegalStateException("Failed to query " + TABLE_NAME, e);
    }
    return result;
  }

  @Override
  public void close() {
    dataSource.close();
  }
}
