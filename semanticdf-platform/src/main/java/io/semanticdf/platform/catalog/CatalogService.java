package io.semanticdf.platform.catalog;

import dev.restate.sdk.annotation.Handler;
import dev.restate.sdk.annotation.Service;
import io.semanticdf.platform.model.ModelStore;
import io.semanticdf.platform.model.NoOpModelStore;

import java.util.List;
import java.util.stream.Collectors;

/**
 * CatalogService \u2014 stateless catalog reads.
 *
 * <p>Plain {@code @Service} (stateless) \u2014 there's no per-key state to
 * coordinate, just read-only queries against the platform's
 * {@link ModelStore}. Putting this in Restate buys us the protocol
 * surface (gRPC + mTLS) but adds no coordination overhead.
 *
 * <p>Caffeine L1 cache in the platform's REST layer sits in front of
 * this \u2014 cache hits don't reach Restate. PR-B's
 * {@code describeModel} returns the {@code lineage_json} persisted
 * at register-time via the library's canonical {@link
 * io.semanticdf.lineage.Lineage} JSON.
 *
 * <p>The service is a {@code @Service} so reads don't queue behind
 * any in-flight writes on a per-key basis (no key here, by
 * definition \u2014 catalog reads span all models).
 */
@Service
public class CatalogService {

  private final ModelStore store;

  /**
   * Default constructor \u2014 used by {@link io.semanticdf.platform.PlatformApplication}
   * when {@code SEMANTICDF_MODELS_PERSIST=false}. Tests substitute
   * their own store via the 1-arg overload.
   */
  public CatalogService() {
    this(new NoOpModelStore());
  }

  /** Constructor injection. Visible-for-testing \u2014 mirrors the
   * test-seam pattern on the other services. */
  public CatalogService(ModelStore store) {
    this.store = java.util.Objects.requireNonNull(store, "store");
  }

  /**
   * List all models in the registry. Ordering mirrors
   * {@link ModelStore#listAll()} (by {@code (model_name, version)}).
   *
   * <p>For PR-B, returns all models regardless of namespace \u2014 the
   * schema doesn't include a namespace column. Multi-tenant
   * scoping is a v2 concern.
   */
  @Handler
  public List<ModelSummary> listModels(ListModelsRequest request) {
    try {
      return store.listAll().stream()
          .map(def -> new ModelSummary(def.modelName(), String.valueOf(def.version()), "active"))
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw new RuntimeException("CatalogService.listModels failed: " + e.getMessage(), e);
    }
  }

  /**
   * Describe one model. Returns the latest registered version's
   * lineage as JSON plus the YAML body.
   *
   * <p>Returns {@code null} if the model has no rows (operators
   * can interpret a null result as "model not registered").
   */
  @Handler
  public ModelDetail describeModel(DescribeModelRequest request) {
    try {
      ModelStore.ModelDefinition def = store.loadLatest(request.modelName());
      if (def == null) return null;
      return new ModelDetail(
          def.modelName(),
          def.version(),
          "active",
          def.yaml(),
          def.lineageJson(),
          def.manifestHash(),
          def.registeredAt().toEpochMilli());
    } catch (Exception e) {
      throw new RuntimeException("CatalogService.describeModel failed: " + e.getMessage(), e);
    }
  }

  /** Request DTO for {@link #listModels(ListModelsRequest)}. */
  public record ListModelsRequest(String namespace) {}

  /** Compact summary DTO \u2014 the wire shape is unchanged from v0.2.1. */
  public record ModelSummary(String modelName, String version, String status) {}

  /** Request DTO for {@link #describeModel(DescribeModelRequest)}. */
  public record DescribeModelRequest(String modelName) {}

  /**
   * Response DTO. PR-B adds three new fields to the v0.2.1 shape
   * (yamlBody, lineageJson, manifestHash, registeredAtMillis)
   * \u2014 all additive, no breaking change.
   */
  public record ModelDetail(
      String modelName,
      int version,
      String status,
      String yamlBody,
      String lineageJson,
      String manifestHash,
      long registeredAtMillis
  ) {}

  /** Kept on the wire for backwards compatibility with v0.2.1 readers. */
  public record FieldSummary(String name, String kind, String exprString) {}
}
