package io.semanticdf.platform.streaming;

import io.semanticdf.SemanticTable;
import io.semanticdf.adapters.YamlLoader;
import java.util.Collections;
import java.util.Map;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.spark.sql.SparkSession;

/**
 * A {@link ModelRegistry} that loads models from a directory of YAML files
 * via the library's {@link YamlLoader#loadDir}.
 *
 * <p>Models are loaded eagerly at construction time (all YAML files in the
 * directory are parsed and bound to the shared {@link SparkSession}). This
 * surfaces load errors at startup rather than at first query time.
 *
 * <p>The platform owns this registry (per {@code platform-architecture.md});
 * it does <b>not</b> depend on {@code semanticdf-mcp}'s {@code Models}
 * class (that would invert the dependency direction — MCP is a client of
 * the platform, not vice versa).
 *
 * <p>Thread-safe: the underlying map is populated once at construction and
 * read-only thereafter.
 */
public final class YamlModelRegistry implements ModelRegistry {

  private final Map<String, SemanticTable> models;
  private final String available;

  /**
   * Load models from a directory of YAML files.
   *
   * @param modelsDir the directory containing {@code *.yml} model files
   * @param spark     the shared {@link SparkSession} (used to bind table references)
   * @return a new registry populated with the loaded models
   * @throws IllegalArgumentException if the directory does not exist or contains
   *                                  no YAML files
   */
  public static YamlModelRegistry load(String modelsDir, SparkSession spark) {
    scala.collection.immutable.Map<String, SemanticTable> scalaMap =
        YamlLoader.loadDir(modelsDir, spark);
    // Convert Scala immutable map → Java map via JavaConverters.
    Map<String, SemanticTable> javaMap = scala.collection.JavaConverters.mapAsJavaMap(scalaMap);
    return new YamlModelRegistry(javaMap);
  }

  private YamlModelRegistry(Map<String, SemanticTable> models) {
    if (models == null || models.isEmpty()) {
      throw new IllegalArgumentException(
          "YamlModelRegistry: no models loaded — check that the directory "
              + "contains *.yml files in the expected format");
    }
    this.models = Map.copyOf(models);
    this.available =
        this.models.keySet().stream()
            .collect(Collectors.joining(", ", "[", "]"));
  }

  @Override
  public SemanticTable get(String modelName) {
    SemanticTable m = models.get(modelName);
    if (m == null) {
      throw new ModelNotFoundException(modelName, available);
    }
    return m;
  }

  /** All registered model names (sorted for diagnostics). */
  public java.util.Set<String> registeredModels() {
    return Collections.unmodifiableSortedSet(new TreeSet<>(models.keySet()));
  }

  /** Number of registered models. */
  public int size() {
    return models.size();
  }
}
