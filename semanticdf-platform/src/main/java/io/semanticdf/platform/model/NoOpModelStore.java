package io.semanticdf.platform.model;

import java.sql.SQLException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;

/**
 * The {@code SEMANTICDF_MODELS_PERSIST=false} (default) backing for
 * {@link ModelStore}.
 *
 * <p>The model's lifecycle is journal-only: {@code CURRENT_VERSION},
 * {@code MANIFEST_HASH}, {@code REGISTRATION_STATUS} live in the
 * Restate journal. Re-invocations of {@code describeModel} return
 * empty/null because there is no Postgres readback to populate them.
 *
 * <p>Operators wanting durable model registry must flip
 * {@code SEMANTICDF_MODELS_PERSIST=true} (and
 * {@code SEMANTICDF_CATALOG_JDBC_URL}). The startup-time
 * {@code YamlModelRegistry.load(modelsDir, spark)} continues to
 * serve the in-process startup models regardless of this flag;
 * this affects only the runtime registration path.
 */
public final class NoOpModelStore implements ModelStore {

  @Override
  public ModelDefinition registerIfAbsent(
      String modelName,
      int version,
      String yaml,
      String manifestHash,
      Instant registeredAt,
      String lineageJson) {
    if (modelName == null || modelName.isBlank()) {
      throw new IllegalArgumentException("modelName must be non-blank");
    }
    if (version <= 0) {
      throw new IllegalArgumentException("version must be positive: " + version);
    }
    // Mirror the persist path's value back to the caller \u2014 the
    // caller's journal bookkeeping runs identically regardless of
    // whether the persist actually wrote to a durable store.
    return new ModelDefinition(
        modelName, version, yaml, manifestHash, registeredAt,
        lineageJson == null ? "" : lineageJson);
  }

  @Override
  public List<ModelDefinition> listAll() {
    return Collections.emptyList();
  }

  @Override
  public ModelDefinition loadByName(String modelName, int version) {
    return null;  // no Postgres = no readback
  }

  @Override
  public ModelDefinition loadLatest(String modelName) {
    return null;
  }

  @Override
  public void ensureSchema() throws SQLException {
    // No schema to ensure.
  }

  @Override
  public void close() {
    // No resources to release.
  }
}
