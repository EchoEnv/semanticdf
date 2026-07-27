package io.semanticdf.platform.query;

import dev.restate.sdk.annotation.Handler;
import dev.restate.sdk.annotation.Service;

import java.util.List;

/**
 * QueryService — stateless query routing.
 *
 * Each query is a short-lived Spark action; there's nothing to
 * suspend or journal — idempotency is the caller's cache key, not
 * Restate's durable promise. So this is a plain {@code @Service}
 * (stateless), not a VirtualObject or Workflow.
 *
 * The heavy lifting happens inside Restate's call into
 * {@code semanticdf.of(...)} — but for the P1 skeleton, we just
 * return a stub response. The full implementation will:
 *   1. Look up the model in ModelService
 *   2. Compile the query via semanticdf.of
 *   3. Submit to the engine adapter (Spark or Trino)
 *   4. Stream results back via Arrow Flight
 */
@Service
public class QueryService {

  /** Run a query. Skeleton: just echoes the query shape back. */
  @Handler
  public QueryResult runQuery(QueryRequest request) {
    // TODO P1: implement the full path through semanticdf + engine adapter
    return new QueryResult(
        request.modelName,
        request.measures(),
        List.of(),  // placeholder for rows
        false,      // placeholder for truncated
        0L          // placeholder for row_count
    );
  }

  /** Request DTO for {@link #runQuery(QueryRequest)}. */
  public record QueryRequest(
      String modelName,
      List<String> measures,
      List<String> dimensions,
      String where
  ) {}

  /** Response DTO. */
  public record QueryResult(
      String model,
      List<String> measures,
      List<List<Object>> rows,
      boolean truncated,
      long rowCount
  ) {}
}
