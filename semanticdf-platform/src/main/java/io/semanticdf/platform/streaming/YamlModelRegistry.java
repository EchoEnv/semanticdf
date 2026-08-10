package io.semanticdf.platform.streaming;

import io.semanticdf.SemanticTable;
import io.semanticdf.adapters.YamlLoader;
import io.semanticdf.core.model.Model;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.spark.sql.SparkSession;

/**
 * A {@link ModelRegistry} that loads models from a directory of YAML files
 * via the library's {@link YamlLoader#loadDir}.
 *
 * <h2>Dual representation (v0.3.2 Phase 3)</h2>
 *
 * <p>Per the v0.3.2 Platform migration design doc (PR #443) Phase 3 partial:
 * the registry stores BOTH representations, populated at construction time:
 *
 * <ul>
 *   <li>{@code Map<String, SemanticTable>} — for {@link #get(String)}
 *       (legacy query path; needs {@code Dataset.rdd}).</li>
 *   <li>{@code Map<String, Model>} — for {@link #getModel(String)}
 *       (engine-portable, journal-safe; foundation for future engine-portable
 *       query path via the engine registry).</li>
 * </ul>
 *
 * <p>The {@code Model} representations are derived via
 * {@link io.semanticdf.ModelBridge#toModel(SemanticTable)} — the same
 * bridge the dual manifest reader uses (PR #441). See that bridge's
 * docstring for known limitations (filters / calculated measures / rollups
 * are not converted; measure aggregate defaults to Sum).
 *
 * <h2>Why both representations (vs. just one)</h2>
 *
 * <p>The query path TODAY uses raw-SQL {@code where} filters via
 * {@code df.filter(where)} — Spark-specific. The engine registry's
 * {@code MCPQueryRequest} doesn't yet support raw {@code where} (deferred).
 * Until that lands, the query path needs the {@code SemanticTable} form.
 * Adding the {@code Model} form is the additive path: zero consumer
 * breakage, future foundation.
 *
 * <h2>Why eager load at construction</h2>
 *
 * <p>Loads eagerly at construction time (all YAML files in the directory
 * are parsed and bound to the shared {@link SparkSession}). Surfaces load
 * errors at startup rather than at first query time.
 *
 * <h2>Thread-safety</h2>
 *
 * <p>Both maps are populated once at construction and read-only thereafter
 * ({@code Map.copyOf}). Concurrent reads from {@link #get(String)} and
 * {@link #getModel(String)} are safe without synchronization.
 */
public final class YamlModelRegistry implements ModelRegistry {

  private final Map<String, SemanticTable> tables;
  private final Map<String, Model> models;
  private final String available;

  /**
   * Load models from a directory of YAML files.
   *
   * @param modelsDir the directory containing {@code *.yml} model files
   * @param spark     the shared {@link SparkSession} (used to bind table references)
   * @return a new registry populated with the loaded models
   * @throws IllegalArgumentException if the directory does not exist or contains
   *                                  no YAML files, OR if any model's
   *                                  {@code SemanticTable → Model} conversion
   *                                  fails (typed error at the IO boundary
   *                                  per the standard)
   */
  public static YamlModelRegistry load(String modelsDir, SparkSession spark) {
    scala.collection.immutable.Map<String, SemanticTable> scalaMap =
        YamlLoader.loadDir(modelsDir, spark);
    // Convert Scala immutable map → Java map via JavaConverters.
    Map<String, SemanticTable> javaMap = scala.collection.JavaConverters.mapAsJavaMap(scalaMap);

    // v0.3.2 Phase 3 partial: also build the Models via ModelBridge.toModel
    // for the engine-portable lookup. Per the IO-boundary rule in the
    // standard, surface typed errors as IllegalArgumentException at
    // construction time (the caller is a programmer setting up the
    // platform; they need to fix their YAML).
    Map<String, Model> models = new HashMap<>();
    for (Map.Entry<String, SemanticTable> e : javaMap.entrySet()) {
      String name = e.getKey();
      SemanticTable st = e.getValue();
      io.semanticdf.core.model.ModelValidationError result =
          io.semanticdf.ModelBridge.toModel(st).fold(
              err -> err,
              m -> {
                models.put(name, m);
                return null;
              });
      if (result != null) {
        throw new IllegalArgumentException(
            "model '" + name + "' conversion failed: " + result);
      }
    }

    return new YamlModelRegistry(javaMap, models);
  }

  private YamlModelRegistry(
      Map<String, SemanticTable> tables,
      Map<String, Model> models) {
    if (tables == null || tables.isEmpty()) {
      throw new IllegalArgumentException(
          "YamlModelRegistry: no models loaded — check that the directory "
              + "contains *.yml files in the expected format");
    }
    if (models == null || !models.keySet().equals(tables.keySet())) {
      // Defense in depth: the dual maps MUST be in sync. If they're not,
      // it's a programmer error in load().
      throw new IllegalArgumentException(
          "YamlModelRegistry: tables and models maps must have identical keys");
    }
    this.tables = Map.copyOf(tables);
    this.models = Map.copyOf(models);
    this.available =
        this.tables.keySet().stream()
            .collect(Collectors.joining(", ", "[", "]"));
  }

  /**
   * Resolve a model by name as a {@link SemanticTable} (legacy query path).
   *
   * @throws ModelNotFoundException if no model is registered under {@code modelName}
   */
  @Override
  public SemanticTable get(String modelName) {
    SemanticTable t = tables.get(modelName);
    if (t == null) {
      throw new ModelNotFoundException(modelName, available);
    }
    return t;
  }

  /**
   * Resolve a model by name as an engine-portable {@link Model}.
   *
   * <p>Returns {@code Optional.empty()} for missing names or null input
   * (programmer-error boundary per the standard).
   */
  @Override
  public Optional<Model> getModel(String modelName) {
    if (modelName == null) {
      return Optional.empty();
    }
    return Optional.ofNullable(models.get(modelName));
  }

  /** All registered model names (sorted for diagnostics). */
  public java.util.Set<String> registeredModels() {
    return Collections.unmodifiableSortedSet(new TreeSet<>(tables.keySet()));
  }

  /** All registered models as a name → core.Model map (for the
    * engine-portable engine providers that need the model at
    * construction time). The Spark provider ignores this map
    * (its query path uses core.Model directly via the platform's
    * ModelRegistry). */
  public java.util.Map<String, io.semanticdf.core.model.Model> getAllModels() {
    return Collections.unmodifiableMap(models);
  }

  /** Number of registered models. */
  public int size() {
    return tables.size();
  }
}