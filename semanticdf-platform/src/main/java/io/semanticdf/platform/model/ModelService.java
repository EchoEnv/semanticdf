package io.semanticdf.platform.model;

import dev.restate.sdk.Restate;
import dev.restate.sdk.annotation.Handler;
import dev.restate.sdk.annotation.VirtualObject;
import dev.restate.sdk.common.StateKey;

import io.semanticdf.SemanticTable;
import io.semanticdf.adapters.YamlLoader;
import io.semanticdf.cache.ResultCache;
import io.semanticdf.lineage.Lineage;
import io.semanticdf.platform.streaming.HotReloadingModelRegistry;
import io.semanticdf.platform.streaming.ModelRegistry;

import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.Map;

/**
 * ModelService â per-model registration and version lifecycle.
 *
 * Key: model name (e.g. {@code "flights"}). Each model name maps to one
 * {@code ModelService} instance; the per-key serialization in Restate
 * replaces the prior {@code SELECT ... FOR UPDATE} round-trips on a
 * pre-Restate design.
 *
 * State held in the journal (per the state-placement rule:
 * "journal = coordination, Postgres = record"):
 *   - CURRENT_VERSION â the latest published version
 *   - REGISTRATION_STATUS â "idle" | "in_progress" | "failed"
 *   - LAST_INVALIDATED_AT â when the lineage cache was last invalidated
 *   - MANIFEST_HASH â the SHA-256 of the current manifest, for change detection
 *
 * State held in Postgres (the platform's record store):
 *   - the actual model YAML
 *   - manifest_hash + lineage_json per version
 *   - the schema in {@link PostgresModelStore}
 *
 * Compilation contract: compile steps run inside
 * {@code Restate.run(...)} so a JVM crash mid-register replays the
 * cached compile/persist without re-executing the side effects.
 * The {@code lastWriteOffset}-equivalent for this service is the
 * {@code CURRENT_VERSION} integer.
 *
 * Cache invalidation contract: a successful register triggers
 * {@code ResultCache.invalidateByModelAndVersion(name, version)} on
 * the cache seam (no-op if absent). The cache invalidation is
 * deliberately OUTSIDE {@code Restate.run(...)} â cache state is
 * observable but not coordination state, so a re-invocation
 * after a partial failure can re-emit without double-invalidating.
 *
 * Determinism discipline: the {@code registeredAt} on the persisted
 * row uses {@code Restate.instantNow()} (replay-stable), not
 * {@code System.currentTimeMillis()}.
 */
@VirtualObject
public class ModelService {

  private static final StateKey<Integer> CURRENT_VERSION = StateKey.of("currentVersion", Integer.class);
  private static final StateKey<String> REGISTRATION_STATUS = StateKey.of("registrationStatus", String.class);
  private static final StateKey<Long> LAST_INVALIDATED_AT = StateKey.of("lastInvalidatedAt", Long.class);
  private static final StateKey<String> MANIFEST_HASH = StateKey.of("manifestHash", String.class);

  private final ModelStore store;
  private final SparkSession spark;
  private final ResultCache cache;
  private final ModelRegistry models;

  /**
   * Constructor. Used by {@link io.semanticdf.platform.PlatformApplication}
   * (composition root) which wires:
   * <ul>
   *   <li>{@link PostgresModelStore} when
   *       {@code SEMANTICDF_MODELS_PERSIST=true}, otherwise
   *       {@link NoOpModelStore};
   *   <li>{@link SparkSession} (in-process or Spark Connect, per
   *       {@code SEMANTICDF_SPARK_CONNECT_URL});
   *   <li>{@link ResultCache} (defaults to a no-op cache when
   *       null; v0.2.3+ can pass {@code InMemoryResultCache});
   *   <li>{@link ModelRegistry} (the boot-time registry, typically
   *       wrapped in {@link HotReloadingModelRegistry} so successful
   *       {@code register()} calls propagate to {@code QueryService}
   *       and {@code StreamingService} without a JVM restart).
   * </ul>
   *
   * <p>The {@code models} parameter may be {@code null} for tests
   * that don't exercise the runtime registry (the {@code instanceof}
   * check in {@link #register(RegisterRequest)} is null-safe).
   * Production always wires {@link HotReloadingModelRegistry}.
   */
  public ModelService(
      ModelStore store, SparkSession spark, ResultCache cache, ModelRegistry models) {
    this.store = java.util.Objects.requireNonNull(store, "store");
    this.spark = java.util.Objects.requireNonNull(spark, "spark");
    this.cache = cache == null ? noOpCache() : cache;
    this.models = models;
  }

  /**
   * Backward-compatible constructor for tests that don't wire a
   * {@link ModelRegistry}. Production uses the 4-arg constructor.
   */
  public ModelService(ModelStore store, SparkSession spark, ResultCache cache) {
    this(store, spark, cache, null);
  }

  /** Convenience for tests + callers that don't have a cache yet. */
  public static ModelService noOp(ModelStore store, SparkSession spark) {
    return new ModelService(store, spark, noOpCache(), null);
  }

  /**
   * Register a new model. Steps:
   * <ol>
   *   <li>Compute {@code manifestHash} over the YAML content (idempotent
   *       on hash â same hash as journal's current value means no-op).
   *   <li>Compile the YAML into a {@link SemanticTable} via the library's
   *       {@link YamlLoader#load}. <b>Happens in handler scope, NOT
   *       inside {@code Restate.run}</b> â {@code SemanticTable} carries
   *       a {@code Dataset.rdd} chain that Jackson cannot round-trip
   *       through the journal. The compile is a pure function
   *       (deterministic for fixed YAML); re-executing on journal
   *       replay is correct and cheap.
   *   <li>Compute the canonical lineage JSON via
   *       {@link Lineage#workspaceOf} + {@link Lineage#toJson}, also
   *       in handler scope (a {@code String} is Jackson-clean anyway,
   *       and a side-effect-free pure function is more cleanly
   *       journal-bypassed).
   *   <li>Persist to Postgres via
   *       {@link ModelStore#registerIfAbsent}, inside
   *       {@code Restate.run("model.persist", ...)} â the durable
   *       side effect, the only one that must survive a JVM crash
   *       mid-register.
   *   <li>Update journal state (currentVersion, manifestHash,
   *       lastInvalidatedAt).
   *   <li>Invalidate the cache (outside any Restate.run block â
   *       cache state is not coordination state).
   * </ol>
   */
  @Handler
  public void register(RegisterRequest request) {
    var state = Restate.state();
    state.set(REGISTRATION_STATUS, "in_progress");
    try {
      final String yaml = request.yaml();
      final String modelName = request.modelName();
      final String hash = computeManifestHash(yaml);

      // Fast-path dedup: same manifestHash as the journal's current
      // value is a no-op.
      String currentHash = state.get(MANIFEST_HASH).orElse("");
      if (currentHash.equals(hash)) {
        state.set(REGISTRATION_STATUS, "idle");
        return;
      }

      final int nextVersion = state.get(CURRENT_VERSION).orElse(0) + 1;
      final java.util.function.Supplier<Instant> clock = () -> Restate.instantNow();
      final Instant registeredAt = clock.get();

      // STEP A: compile in handler scope. SemanticTable carries a
      // Dataset.rdd chain that Jackson cannot round-trip through the
      // Restate journal; the compile is a pure function and re-runs
      // cheaply on journal replay. Keeping it OUT of Restate.run
      // avoids the InvalidDefinitionException that
      // ModelServiceEndToEndTest's actual @RestateTest probe surfaced.
      final SemanticTable compiled = compileFromYaml(yaml, modelName, this.spark);

      // STEP B: lineage in handler scope. Same reasoning â pure
      // function of the compiled model.
      final String lineageJson = lineageJsonFor(compiled);

      // STEP C: persist. The ONLY step inside Restate.run â the
      // durable side effect to Postgres. Replay-safe: a JVM crash
      // mid-INSERT is replayed and the ON CONFLICT DO NOTHING
      // idempotency at the ModelStore layer returns the same
      // ModelDefinition.
      final ModelStore.ModelDefinition persisted =
          Restate.run(
              "model.persist",
              ModelStore.ModelDefinition.class,
              () ->
                  store.registerIfAbsent(
                      modelName, nextVersion, yaml, hash, registeredAt, lineageJson));

      // STEP D: journal bookkeeping.
      state.set(CURRENT_VERSION, persisted.version());
      state.set(MANIFEST_HASH, persisted.manifestHash());
      state.set(LAST_INVALIDATED_AT, clock.get().toEpochMilli());
      state.set(REGISTRATION_STATUS, "idle");

      // STEP E: cache invalidation. NOT in Restate.run â cache
      // state is observable, not coordination, and a re-invocation
      // can re-emit without double-invalidating.
      // PR #261 (cache correctness fix): invalidate by model NAME,
      // not by model NAME + journal CURRENT_VERSION. The cache key
      // uses the YAML-declared version (`model.version()`), but the
      // journal's CURRENT_VERSION is a different counter. The two
      // never matched, so `invalidateByModelAndVersion(name,
      // persisted.version())` was a no-op for any model whose YAML
      // didn't declare a `version:` field (the default). Result:
      // cache served stale rows forever after a re-register.
      cache.invalidateModel(modelName);

      // STEP F (H3 fix): propagate the new SemanticTable into the
      // runtime registry so QueryService.runQuery / StreamingService.run
      // see the model on the next invocation. The instanceof check is
      // intentional -- we don't want to pollute the ModelRegistry
      // interface with mutation semantics. When the test-only
      // YamlModelRegistry is passed (no decorator), this branch is
      // a no-op and the registry remains read-only.
      if (models instanceof HotReloadingModelRegistry hot) {
        hot.register(modelName, compiled);
      }
    } catch (Exception e) {
      state.set(REGISTRATION_STATUS, "failed");
      throw e;
    }
  }

  /**
   * Read the current version.
   *
   * <p>{@code @Shared}: concurrent reads don't serialize against
   * the per-key write serialization in {@link #register}.
   */
  @dev.restate.sdk.annotation.Shared
  @Handler
  public Integer getCurrentVersion() {
    return Restate.state().get(CURRENT_VERSION).orElse(0);
  }

  /**
   * Read the most recent manifest hash, for callers wanting to
   * detect config drift.
   */
  @dev.restate.sdk.annotation.Shared
  @Handler
  public String getManifestHash() {
    return Restate.state().get(MANIFEST_HASH).orElse("");
  }

  /**
   * Compile one model's YAML content via a temp file + library's
   * {@link YamlLoader}. Visible-for-testing pattern: extracted so
   * it can be unit-tested without a Restate handler context.
   *
   * <p>Reuses {@link YamlLoader#load(String, SparkSession)} â the
   * same entry point {@code YamlModelRegistry.load(modelsDir, spark)}
   * uses at startup. The temp file is cleaned up in
   * {@code finally} â no FD leak on the register hot path.
   */
  static SemanticTable compileFromYaml(String yaml, String modelName, SparkSession spark) {
    Path tmp;
    try {
      tmp = Files.createTempFile("semanticdf-register-", ".yml");
    } catch (IOException e) {
      throw new IllegalStateException(
          "could not create temp file for register() of '" + modelName + "'", e);
    }
    try {
      try {
        Files.writeString(tmp, yaml, StandardCharsets.UTF_8);
      } catch (IOException e) {
        throw new IllegalStateException(
            "could not write YAML to " + tmp + " for model '" + modelName + "'", e);
      }
      // Library entry point â same as YamlModelRegistry.load uses.
      scala.collection.immutable.Map<String, SemanticTable> built =
          YamlLoader.load(tmp.toString(), spark);
      Map<String, SemanticTable> javaMap =
          scala.collection.JavaConverters.mapAsJavaMap(built);
      SemanticTable model = javaMap.get(modelName);
      if (model == null) {
        throw new IllegalArgumentException(
            "modelName '"
                + modelName
                + "' not found in registered YAML. Found: "
                + javaMap.keySet());
      }
      return model;
    } finally {
      try {
        Files.deleteIfExists(tmp);
      } catch (IOException ignored) {
        // Best-effort. JVM tmp-file cleanup will sweep on exit.
      }
    }
  }

  /**
   * Compute the canonical lineage JSON for one model. Visible-for-testing
   * seam. The {@code Map.of(model.name(), model)} -> scala.Map conversion
   * is uniform with {@code YamlModelRegistry.load}'s internals.
   *
   * <p>Returns the canonical JSON without pretty-print (smaller
   * persisted footprint; round-trip is lossless).
   */
  static String lineageJsonFor(SemanticTable model) {
    // Library helper `Lineage.workspaceJsonFor(model)` wraps a single
    // model in a singleton workspace and serializes via `Lineage.toJson`.
    return Lineage.workspaceJsonFor(model, false);
  }

  /**
   * Compute a manifest hash (SHA-256 hex) over the YAML content.
   * Deliberately a stable-content hash â NOT including any
   * file-path / timestamp / serialization noise (those would
   * defeat the idempotency contract).
   */
  static String computeManifestHash(String yaml) {
    try {
      MessageDigest md = MessageDigest.getInstance("SHA-256");
      md.update(yaml.getBytes(StandardCharsets.UTF_8));
      byte[] digest = md.digest();
      StringBuilder sb = new StringBuilder(digest.length * 2);
      for (byte b : digest) {
        sb.append(Character.forDigit((b >> 4) & 0xf, 16));
        sb.append(Character.forDigit(b & 0xf, 16));
      }
      return sb.toString();
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 not available", e);
    }
  }

  /** A no-op cache for callers without an injection point (P1). */
  private static ResultCache noOpCache() {
    return ResultCache.NoOp();
  }

  /** Request DTO for {@link #register(RegisterRequest)}. */
  public record RegisterRequest(String modelName, String yaml) {}
}
