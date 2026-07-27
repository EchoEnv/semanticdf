package io.semanticdf.platform.streaming;

import io.semanticdf.SemanticTable;
import org.apache.spark.sql.streaming.StreamingQuery;

/**
 * The seam that turns a resolved model + a run request into a live Spark
 * {@link StreamingQuery}.
 *
 * <p>This interface isolates the most volatile decision in the streaming
 * lifecycle: <em>how</em> the engine is invoked. The default implementation
 * calls {@code SemanticTable.toStreamingQuery(spark, opts)}, but tests inject
 * a fake that records the call without booting Spark.
 *
 * <p>The returned {@code StreamingQuery} is a process-local Spark handle — it
 * <b>cannot</b> be journaled in Restate state. It lives only in the
 * {@link StreamingQueryHandleRegistry} for the lifetime of the JVM.
 */
@FunctionalInterface
public interface StreamingQueryLauncher {

  /**
   * Start a streaming query for the given model and request.
   *
   * @param model the resolved semantic table (streaming-capable)
   * @param request the validated run request (checkpoint location is non-blank)
   * @return the live Spark streaming query handle
   */
  StreamingQuery start(SemanticTable model, StreamingService.StreamRunRequest request);
}
