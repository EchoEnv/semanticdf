package io.semanticdf.platform.model;

import java.io.Closeable;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;

/**
 * Durable storage for the platform's model registry.
 *
 * <p>State-placement rule (per {@code docs/design/platform-architecture.md}
 * §2.3): journal = coordination (recent, recoverable from replay);
 * Postgres = record (durable, queryable). The model registry is a
 * record \u2014 it lives here, not in the Restate journal.
 *
 * <p>Idempotency contract: {@link #registerIfAbsent} is keyed on
 * {@code (model_name, version)}. The implementation MUST treat
 * duplicate inserts as no-ops, returning the existing row. PG
 * implements this with {@code INSERT ... ON CONFLICT DO NOTHING
 * RETURNING manifest_yaml}; {@link NoOpModelStore} returns the
 * supplied values without persisting.
 *
 * <p>The {@code lineageJson} field is the canonical JSON of
 * {@code io.semanticdf.lineage.WorkspaceLineage}, produced at
 * compile time by {@code Lineage.toJson(...)}. Persisting it
 * (rather than the upstreamModels) lets {@code describeModel}
 * return it directly without a re-compile.
 *
 * <p>For PR-B's P1 scope, namespace is single-tenant ("default").
 * Multi-tenant namespace scoping is a v2 concern \u2014 the schema does
 * NOT include a {@code namespace} column; listAll returns all rows.
 *
 * @see NoOpModelStore
 * @see PostgresModelStore
 */
public interface ModelStore extends Closeable {

  /**
   * Persist a model definition. Idempotent on
   * {@code (model_name, version)} \u2014 repeated calls with the
   * same key return the existing row without modification.
   *
   * @param modelName     model name (PK column 1)
   * @param version       1-based version (PK column 2)
   * @param yaml          canonical YAML body (\u2014 persisted verbatim)
   * @param manifestHash  SHA-256 hex of the YAML (computed by the
   *                      caller; distinct from the per-version dedup
   *                      key, this is the model's "fingerprint")
   * @param registeredAt  journaled {@code Restate.instantNow()}
   *                      value; replay-stable
   * @param lineageJson   the canonical {@code WorkspaceLineage} JSON
   *                      for this version (single-model map)
   * @return the persisted definition (or the existing row if a
   *         duplicate)
   */
  ModelDefinition registerIfAbsent(
      String modelName,
      int version,
      String yaml,
      String manifestHash,
      Instant registeredAt,
      String lineageJson) throws SQLException;

  /**
   * List all models, ordered by {@code (model_name, version)}.
   * The implementation may apply pagination (today: full table).
   */
  List<ModelDefinition> listAll() throws SQLException;

  /**
   * Load one version of one model. Returns {@code null} if not
   * found (callers translate that into a
   * {@code TerminalException(BAD_REQUEST_CODE)} for the Restate
   * boundary).
   */
  ModelDefinition loadByName(String modelName, int version) throws SQLException;

  /**
   * Load the latest version of one model. Convenience method over
   * {@link #loadByName} for {@code describeModel} callers.
   * Returns {@code null} if the model has no rows.
   */
  ModelDefinition loadLatest(String modelName) throws SQLException;

  /**
   * DDL bootstrap. Idempotent.
   */
  void ensureSchema() throws SQLException;

  /**
   * Readback row shape. Mirrors the persisted columns \u2014 lineage
   * JSON is opaque to callers but should round-trip via
   * {@code Lineage.fromJson}.
   */
  record ModelDefinition(
      String modelName,
      int version,
      String yaml,
      String manifestHash,
      Instant registeredAt,
      String lineageJson) {}
}
