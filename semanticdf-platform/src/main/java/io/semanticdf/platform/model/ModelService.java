package io.semanticdf.platform.model;

import dev.restate.sdk.Restate;
import dev.restate.sdk.annotation.Handler;
import dev.restate.sdk.annotation.VirtualObject;
import dev.restate.sdk.common.StateKey;

import io.semanticdf.SemanticTable;
import io.semanticdf.adapters.YamlLoader;
import io.semanticdf.cache.ResultCache;
import io.semanticdf.lineage.Lineage;

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
 * ModelService — per-model registration and version lifecycle.
 *
 * Key: model name (e.g. {@code "flights"}). Each model name maps to one
 * {@code ModelService} instance; the per-key serialization in Restate
 * replaces the prior {@code SELECT ... FOR UPDATE} round-trips on a
 * pre-Restate design.
 *
 * State held in the journal (per the state-placement rule:
 * "journal = coordination, Postgres = record"):
 *   - CURRENT_VERSION — the latest published version
 *   - REGISTRATION_STATUS — "idle" | "in_progress" | "failed"
 *   - LAST_INVALIDATED_AT — when the lineage cache was last invalidated
 *   - MANIFEST_HASH — the SHA-256 of the current manifest, for change detection
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
 * deliberately OUTSIDE {@code Restate.run(...)} — cache state is
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
   *       null; v0.2.3+ can pass {@code InMemoryResultCache}).
   * </ul>
   * Tests substitute their own triple via the same constructor.
   */
  public ModelService(ModelStore store, SparkSession spark, ResultCache cache) {
    this.store = java.util.Objects.requireNonNull(store, "store");
    this.spark = java.util.Objects.requireNonNull(spark, "spark");
    this.cache = cache == null ? noOpCache() : cache;
  }

  /** Convenience for tests + callers that don't have a cache yet. */
  public static ModelService noOp(ModelStore store, SparkSession spark) {
    return new ModelService(store, spark, noOpCache());
  }

  /**
   * Register a new model. Steps:
   * <ol>
   *   <li>Compute {@code manifestHash} over the YAML content (idempotent
   *       on hash — same hash as journal's current value means no-op).
   *   <li>Compile the YAML into a {@link SemanticTable} via the library's
   *       {@link YamlLoader#load}, inside {@code Restate.run("compile", ...)}
   *       (replay-safe).
   *   <li>Compute the canonical lineage JSON via
   *       {@link Lineage#workspaceOf} + {@link Lineage#toJson}, inside
   *       a {@code Restate.run("lineage", ...)}.
   *   <li>Persist to Postgres via
   *       {@link ModelStore#registerIfAbsent}, inside
   *       {@code Restate.run("persist", ...)}.
   *   <li>Update journal state (currentVersion, manifestHash,
   *       lastInvalidatedAt).
   *   <li>Invalidate the cache (outside any Restate.run block —
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

      // STEP A: compile (Restate.run so the parsed SemanticTable is
      // journaled; replay returns the cached value without re-parsing).
      final SemanticTable compiled =
          Restate.run(
              "model.compile",
              SemanticTable.class,
              () -> compileFromYaml(yaml, modelName, this.spark));

      // STEP B: lineage (Restate.run; same replay story).
      final String lineageJson =
          Restate.run(
              "model.lineage",
              String.class,
              () -> lineageJsonFor(compiled));

      // STEP C: persist (Restate.run; replay-safe).
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

      // STEP E: cache invalidation. NOT in Restate.run — cache
      // state is observable, not coordination, and a re-invocation
      // can re-emit without double-invalidating.
      cache.invalidateByModelAndVersion(modelName, persisted.version());
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
   * <p>Reuses {@link YamlLoader#load(String, SparkSession)} — the
   * same entry point {@code YamlModelRegistry.load(modelsDir, spark)}
   * uses at startup. The temp file is cleaned up in
   * {@code finally} — no FD leak on the register hot path.
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
      // Library entry point — same as YamlModelRegistry.load uses.
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
   * Deliberately a stable-content hash — NOT including any
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
