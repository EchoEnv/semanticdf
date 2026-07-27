package io.semanticdf.platform.catalog;

import dev.restate.sdk.annotation.Handler;
import dev.restate.sdk.annotation.Service;

import java.util.List;

/**
 * CatalogService — stateless catalog reads.
 *
 * Plain {@code @Service} (stateless) — there's no per-key state to
 * coordinate, just read-only queries against Postgres. Putting this
 * in Restate buys us the protocol surface (gRPC + mTLS) but adds no
 * coordination overhead.
 *
 * Skeleton: the responses are placeholders. The full implementation
 * reads from Postgres (model registry, lineage, audit) and returns
 * the appropriate slice. The Caffeine L1 cache in the platform's REST
 * layer sits in front of this — cache hits don't reach Restate.
 */
@Service
public class CatalogService {

  /** List all models in a namespace. */
  @Handler
  public List<ModelSummary> listModels(ListModelsRequest request) {
    // TODO P1: read from Postgres model registry, return summaries
    return List.of();
  }

  /** Describe one model. Returns the structured schema (dimensions,
   * measures, joins, lineage) — mirrors the v0.2.1 {@code describe_model}
   * contract from the MCP. */
  @Handler
  public ModelDetail describeModel(DescribeModelRequest request) {
    // TODO P1: load from Postgres + compute lineage via semanticdf.Lineage
    return null;
  }

  /** Request DTO for {@link #listModels(ListModelsRequest)}. */
  public record ListModelsRequest(String namespace) {}

  /** Response DTO. */
  public record ModelSummary(String modelName, String version, String status) {}

  /** Request DTO for {@link #describeModel(DescribeModelRequest)}. */
  public record DescribeModelRequest(String modelName) {}

  /** Response DTO. */
  public record ModelDetail(
      String modelName,
      int version,
      String status,
      List<FieldSummary> dimensions,
      List<FieldSummary> measures,
      List<FieldSummary> transforms
  ) {}

  public record FieldSummary(String name, String kind, String exprString) {}
}
