package io.semanticdf.platform.streaming;

import io.semanticdf.SemanticTable;
import io.semanticdf.core.model.Model;
import java.util.Optional;

/**
 * Model lookup seam — resolves a model name to either a {@link SemanticTable}
 * (for the legacy query path that needs Spark execution state) or a
 * {@link Model} (the engine-portable, journal-safe representation).
 *
 * <h2>Two lookups, two representations</h2>
 *
 * <p>Per the v0.3.2 Platform migration design doc (PR #443): the registry
 * exposes BOTH representations during the v0.3.2 transition.
 *
 * <ul>
 *   <li>{@link #get(String)} returns {@link SemanticTable} for the legacy
 *       query path (used by {@code QueryService.runQuery} via
 *       {@code CacheBridge.executeQuery}). The query path needs
 *       {@code Dataset.rdd} for {@code df.filter(where)} semantics.
 *       This is the SAME signature as before — no consumer changes.</li>
 *   <li>{@link #getModel(String)} returns {@link Optional}{@code <Model>}
 *       for the engine-portable path. {@code Model} is pure data, journal-safe,
 *       and usable by the engine registry once {@code MCPQueryRequest.where}
 *       support lands (Phase 4 of the design doc). Currently unused by
 *       platform internals but the seam is ready.</li>
 * </ul>
 *
 * <h2>Why two lookups (not one)</h2>
 *
 * <p>The query path TODAY uses raw-SQL {@code where} filters (passed
 * straight to {@code df.filter(where)}); the engine registry's
 * {@code MCPQueryRequest} doesn't yet support raw {@code where}
 * (deferred to a future PR). Migrating to {@code Model}-only at the
 * registry seam would BREAK the query path today. Adding the
 * second lookup is the additive path: existing consumers unchanged,
 * future consumers can use {@link #getModel(String)} when ready.
 *
 * <h2>Thread-safety contract</h2>
 *
 * <p>Implementations must be safe for concurrent reads (the platform's
 * handlers may invoke either method concurrently). Writes, if any,
 * must be serialized (e.g. via {@code ConcurrentHashMap}).
 *
 * <h2>Why the platform owns this (not the MCP)</h2>
 *
 * <p>Per {@code platform-architecture.md}: the platform is a <em>client</em>
 * of the MCP, not vice versa. Depending on the MCP's {@code Models} class
 * here would invert the dependency direction.
 */
public interface ModelRegistry {

  /**
   * Resolve a model by name as a {@link SemanticTable}.
   *
   * <p>Use this for the legacy query path (Spark execution). The returned
   * table carries {@code Dataset.rdd} which Jackson cannot round-trip
   * through the Restate journal — keep it out of any {@code Restate.run(...)}
   * block per the design doc.
   *
   * @param modelName the model name (must match a registered model)
   * @return the resolved {@link SemanticTable}
   * @throws ModelNotFoundException if no model is registered under {@code modelName}
   */
  SemanticTable get(String modelName);

  /**
   * Resolve a model by name as an engine-portable {@link Model}.
   *
   * <p>Use this for the future engine-portable query path (via the
   * engine registry). {@code Model} is journal-safe and engine-portable.
   *
   * <p>Returns {@link Optional#empty()} if no model is registered under
   * {@code modelName} OR if {@code modelName} is null (programmer
   * error boundary per the standard). This is the {@code Option[X]}
   * "may not exist" pattern — not an exception.
   *
   * @param modelName the model name (may be null)
   * @return the resolved {@link Model}, or {@code Optional.empty()}
   */
  Optional<Model> getModel(String modelName);

  /** Thrown when no model is registered under the requested name. */
  final class ModelNotFoundException extends RuntimeException {
    public ModelNotFoundException(String modelName, String available) {
      super(
          "Model not found: "
              + modelName
              + ". Available models: "
              + available);
    }
  }
}