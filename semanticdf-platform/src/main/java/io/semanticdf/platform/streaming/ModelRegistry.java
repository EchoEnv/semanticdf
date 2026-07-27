package io.semanticdf.platform.streaming;

import io.semanticdf.SemanticTable;

/**
 * Model lookup seam — resolves a model name to a {@link SemanticTable}.
 *
 * <p>The platform owns this contract (per {@code platform-architecture.md}),
 * not {@code semanticdf-mcp}. The MCP is a <em>client</em> of the platform;
 * depending on the MCP's {@code Models} class from here would invert the
 * dependency direction.
 *
 * <p>Implementations may load from YAML, query a Postgres registry, or wrap
 * an in-memory map. The handler treats this as a pure lookup: same name in,
 * same {@code SemanticTable} out.
 */
@FunctionalInterface
public interface ModelRegistry {

  /**
   * Resolve a model by name.
   *
   * @param modelName the model name (must match a registered model)
   * @return the resolved {@link SemanticTable}
   * @throws ModelNotFoundException if no model is registered under {@code modelName}
   */
  SemanticTable get(String modelName);

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
