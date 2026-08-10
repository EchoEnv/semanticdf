package io.semanticdf.platform.streaming;

import io.semanticdf.SemanticTable;
import io.semanticdf.core.model.Model;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A {@link ModelRegistry} decorator that holds a runtime overlay on top of
 * the boot-time {@link YamlModelRegistry}, so a successful
 * {@code ModelService.register} propagates to {@link QueryService} and
 * {@link StreamingService} without a JVM restart.
 *
 * <h2>Dual overlay (v0.3.1 Phase 4)</h2>
 *
 * <p>Per the v0.3.1 Platform migration design doc (PR #443), the registry
 * now maintains TWO parallel overlays:
 *
 * <ul>
 *   <li>{@code overlay} — {@code ConcurrentHashMap<String, SemanticTable>}
 *       for the legacy query path. Unchanged.</li>
 *   <li>{@code modelOverlay} — {@code ConcurrentHashMap<String, Model>}
 *       for the engine-portable query path. Added in Phase 4.</li>
 * </ul>
 *
 * <p>Writes go to BOTH overlays atomically (per-key; Restate virtual-object
 * contract serializes writes per model name). Reads consult each overlay
 * separately.
 *
 * <h2>Concurrency contract</h2>
 *
 * <p>Both overlays are {@link ConcurrentHashMap}s. Reads (from
 * {@link QueryService} / {@link StreamingService} handlers, which run
 * concurrently) and writes (from {@link ModelService.register}, which
 * is per-key serialized by Restate's virtual-object contract) are safe.
 *
 * <h2>Journal determinism</h2>
 *
 * <p>The mutation is intentionally OUTSIDE any {@code Restate.run(...)}
 * block — like the existing {@code ResultCache.invalidateModel(...)}
 * call at {@code ModelService.register} STEP E. Cache and registry state
 * are observable but not coordination state; a replay re-emits the
 * mutation, which is idempotent (the overlay already has the entry).
 */
public final class HotReloadingModelRegistry implements ModelRegistry {

  private final YamlModelRegistry delegate;
  private final ConcurrentHashMap<String, SemanticTable> overlay = new ConcurrentHashMap<>();
  // v0.3.1 Phase 4: parallel Model overlay for the engine-portable query
  // path. Populated by the new 3-arg `register(name, table, model)`.
  private final ConcurrentHashMap<String, Model> modelOverlay = new ConcurrentHashMap<>();

  public HotReloadingModelRegistry(YamlModelRegistry delegate) {
    if (delegate == null) {
      throw new IllegalArgumentException("delegate must not be null");
    }
    this.delegate = delegate;
  }

  /**
   * Resolve a model by name. Consults the overlay first; falls through
   * to the boot-time {@link YamlModelRegistry} on miss.
   *
   * @throws ModelNotFoundException if no model is registered under
   *                                {@code modelName} (in either layer)
   */
  @Override
  public SemanticTable get(String modelName) {
    SemanticTable overlayHit = overlay.get(modelName);
    if (overlayHit != null) {
      return overlayHit;
    }
    return delegate.get(modelName);
  }

  /**
   * Resolve a model by name as an engine-portable {@link Model}.
   *
   * <p>Consults the model overlay first (runtime-registered models); falls
   * through to the boot-time registry (which exposes Models derived at
   * load time via {@code ModelBridge.toModel}).
   *
   * <p>For models that fail the {@code ModelBridge.toModel} conversion
   * (per the v0.3.1 design doc's documented limitations: filters,
   * calculated measures, rollups are not converted), this returns empty.
   * The legacy query path still serves these models via {@link #get(String)}.
   */
  @Override
  public Optional<Model> getModel(String modelName) {
    if (modelName == null) {
      // Per scala-jvm-safety §1: null is a programmer-error boundary.
      // Return empty (not throw NPE) so the caller's `Optional.empty()`
      // contract holds. `ConcurrentHashMap.get(null)` throws NPE,
      // so we must check first.
      return Optional.empty();
    }
    Model overlayHit = modelOverlay.get(modelName);
    if (overlayHit != null) {
      return Optional.of(overlayHit);
    }
    return delegate.getModel(modelName);
  }

  /**
   * Add (or replace) a runtime model — LEGACY path (SemanticTable only).
   *
   * <p>Deprecated in v0.3.1 Phase 4: callers SHOULD use
   * {@link #register(String, SemanticTable, Model)} instead so the engine
   * registry path can serve the model too. The 2-arg overload remains for
   * backward compat with tests + the YamlModelRegistry-only construction
   * pattern (where Models are computed at boot time, not runtime).
   *
   * <p>For runtime registration, the register handler in
   * {@code ModelService.register} calls the 3-arg overload directly.
   */
  public void register(String modelName, SemanticTable table) {
    if (modelName == null || table == null) {
      throw new IllegalArgumentException("modelName and table must be non-null");
    }
    overlay.put(modelName, table);
  }

  /**
   * Add (or replace) a runtime model — BOTH representations.
   *
   * <p>Called by {@code ModelService.register} after a successful
   * {@code Restate.run("model.persist", ...)}. The Model is derived from
   * the SemanticTable via {@code ModelBridge.toModel}; if that conversion
   * fails (per the documented limitations), this method throws
   * {@code IllegalArgumentException} with the typed
   * {@code ModelValidationError} so the caller surfaces the failure
   * cleanly.
   *
   * <p>Idempotent: a re-invocation after a partial failure replaces
   * both entries with the same values.
   *
   * @throws IllegalArgumentException if either argument is null OR if the
   *                                  Model cannot be derived from the
   *                                  SemanticTable (typed error from the
   *                                  ModelBridge)
   */
  public void register(String modelName, SemanticTable table, Model model) {
    if (modelName == null || table == null || model == null) {
      throw new IllegalArgumentException(
          "modelName, table, and model must be non-null");
    }
    overlay.put(modelName, table);
    modelOverlay.put(modelName, model);
  }

  /**
   * All registered model names (boot-time + overlay), sorted for diagnostics.
   * Test-only diagnostic — production code uses {@link #get(String)}.
   */
  public Set<String> registeredModels() {
    Set<String> all = new TreeSet<>(delegate.registeredModels());
    all.addAll(overlay.keySet());
    return java.util.Collections.unmodifiableSet(all);
  }

  /**
   * Whether the overlay currently holds a {@link SemanticTable} entry for
   * {@code modelName}. Used by {@code ModelService.register} to decide
   * whether to mutate the runtime registry.
   */
  public boolean hasOverlay(String modelName) {
    return overlay.containsKey(modelName);
  }

  /**
   * Whether the overlay currently holds a {@link Model} entry for
   * {@code modelName}. Used by {@code ModelService.register} for
   * observability — the engine-portable path consults this.
   */
  public boolean hasModelOverlay(String modelName) {
    return modelOverlay.containsKey(modelName);
  }
}