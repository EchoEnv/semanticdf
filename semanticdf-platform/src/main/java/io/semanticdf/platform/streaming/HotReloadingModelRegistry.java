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
 * <p>Before this class existed (pre-fix-H3, see
 * {@code docs/design/platform-services-completion-plan.md}):
 * <ol>
 *   <li>{@code PlatformApplication.main} loaded a {@code YamlModelRegistry}
 *       from {@code ./models} at boot.
 *   <li>{@code ModelService.register} persisted to Postgres (when
 *       {@code SEMANTICDF_MODELS_PERSIST=true}) and bumped the journal's
 *       {@code CURRENT_VERSION} — but did NOT update the in-memory
 *       {@code YamlModelRegistry}.
 *   <li>Result: a model registered via {@code POST /ModelService/{name}/register}
 *       was visible via {@code CatalogService.listModels} (Postgres-backed)
 *       but {@code QueryService.runQuery} for that model returned
 *       "model not found" because it consulted the boot-time
 *       {@code YamlModelRegistry}.
 * </ol>
 *
 * <p>This decorator fixes the gap: {@link #register(String, SemanticTable)}
 * mutates the overlay; {@link #get(String)} consults the overlay first,
 * then falls through to the delegate. The delegate is read-only
 * (populated at construction); the overlay is the only thing that
 * changes at runtime.
 *
 * <p><b>Concurrency:</b> the overlay is a {@link ConcurrentHashMap}. Reads
 * (from {@link QueryService} / {@link StreamingService} handlers, which
 * run concurrently) and writes (from {@link ModelService.register}, which
 * is per-key serialized by Restate's virtual-object contract) are safe.
 *
 * <p><b>Journal determinism:</b> the mutation is intentionally OUTSIDE
 * any {@code Restate.run(...)} block — like the existing
 * {@code ResultCache.invalidateModel(...)} call at
 * {@code ModelService.register} STEP E. Cache and registry state are
 * observable but not coordination state; a replay re-emits the mutation,
 * which is idempotent (the overlay already has the entry).
 *
 * <p><b>Why no cache dependency:</b> cache invalidation lives in
 * {@code ModelService.register} STEP E, not here. This class is a pure
 * read/write seam over the in-memory model map.
 */
public final class HotReloadingModelRegistry implements ModelRegistry {

  private final YamlModelRegistry delegate;
  private final ConcurrentHashMap<String, SemanticTable> overlay = new ConcurrentHashMap<>();

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
   * <p><b>v0.3.2 interim limitation:</b> this delegates to the boot-time
   * {@link YamlModelRegistry} only. Runtime-registered models (added
   * via {@link #register(String, SemanticTable)}) are stored as
   * {@code SemanticTable} in the overlay; their {@code Model}
   * representation is NOT computed. This is acceptable for the
   * v0.3.2 transition — runtime registration still uses
   * {@code SemanticTable} for the query path's compatibility. A
   * future PR (Phase 4 of the design doc) will add a parallel
   * {@code Model} overlay populated when the register path migrates.
   *
   * <p>For the engine-portable path, the boot-time registry is the
   * authoritative source today.
   */
  @Override
  public Optional<Model> getModel(String modelName) {
    return delegate.getModel(modelName);
  }

  /**
   * Add (or replace) a runtime model. Called by {@code ModelService.register}
   * after a successful {@code Restate.run("model.persist", ...)}.
   *
   * <p>Idempotent: a re-invocation after a partial failure replaces the
   * entry with the same value. No cache invalidation here — see
   * {@code ModelService.register} STEP E for that concern.
   */
  public void register(String modelName, SemanticTable table) {
    if (modelName == null || table == null) {
      throw new IllegalArgumentException("modelName and table must be non-null");
    }
    overlay.put(modelName, table);
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
   * Whether the overlay currently holds an entry for {@code modelName}.
   * Used by {@code ModelService.register} to decide whether to mutate
   * the runtime registry (we skip it for journal-only registrations
   * when {@code SEMANTICDF_MODELS_PERSIST=false}).
   */
  public boolean hasOverlay(String modelName) {
    return overlay.containsKey(modelName);
  }
}