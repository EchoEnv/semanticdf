package io.semanticdf.platform.model;

import dev.restate.sdk.Restate;
import dev.restate.sdk.annotation.Handler;
import dev.restate.sdk.annotation.VirtualObject;
import dev.restate.sdk.common.StateKey;

/**
 * ModelService — per-model registration and version lifecycle.
 *
 * Key: model name (e.g. {@code "flights"}). Each model name maps to one
 * {@code ModelService} instance; the per-key serialization in Restate
 * replaces the prior "SELECT ... FOR UPDATE in Postgres + advisory lock"
 * pattern from the pre-Restate design.
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
 *   - versioned lineage
 *   - registration audit events
 *
 * Skeleton note: this is the P1 skeleton. Handler bodies are stubs;
 * the actual validation, compilation, and storage happen in subsequent PRs.
 */
@VirtualObject
public class ModelService {

  private static final StateKey<Integer> CURRENT_VERSION = StateKey.of("currentVersion", Integer.class);
  private static final StateKey<String> REGISTRATION_STATUS = StateKey.of("registrationStatus", String.class);
  private static final StateKey<Long> LAST_INVALIDATED_AT = StateKey.of("lastInvalidatedAt", Long.class);
  private static final StateKey<String> MANIFEST_HASH = StateKey.of("manifestHash", String.class);

  /**
   * Register a new model. Skeleton: just initializes the per-model state.
   * The full implementation will (1) validate the YAML, (2) call
   * {@code semanticdf.of(...)} inside a {@code Restate.run} block to
   * produce a lineage manifest, (3) persist to Postgres, (4) bump
   * {@code CURRENT_VERSION}, (5) publish to subscribed engines.
   */
  @Handler
  public void register(RegisterRequest request) {
    var state = Restate.state();
    state.set(REGISTRATION_STATUS, "in_progress");
    try {
      // TODO P1: validate, compile via semanticdf, persist to Postgres
      // Inside Restate.run("compile", () -> semanticdf.of(spark, request.yaml()))
      // For the skeleton, just initialize state.
      state.set(CURRENT_VERSION, 1);
      state.set(LAST_INVALIDATED_AT, System.currentTimeMillis());
      state.set(MANIFEST_HASH, "sha256:placeholder");
      state.set(REGISTRATION_STATUS, "idle");
    } catch (Exception e) {
      state.set(REGISTRATION_STATUS, "failed");
      throw e;
    }
  }

  /**
   * Bump a model's version. Skeleton: just increments the version counter.
   */
  @Handler
  public int bumpVersion() {
    var state = Restate.state();
    Integer current = state.get(CURRENT_VERSION).orElse(0);
    int next = current + 1;
    state.set(CURRENT_VERSION, next);
    state.set(LAST_INVALIDATED_AT, System.currentTimeMillis());
    return next;
  }

  /**
   * Read the current version. Uses {@code @Shared} so concurrent reads
   * don't serialize against writes.
   */
  @dev.restate.sdk.annotation.Shared
  @Handler
  public Integer getCurrentVersion() {
    return Restate.state().get(CURRENT_VERSION).orElse(0);
  }

  /** Request DTO for {@link #register(RegisterRequest)}. */
  public record RegisterRequest(String modelName, String yaml) {}
}
