package io.semanticdf.platform.streaming;

import io.semanticdf.core.model.Model;
import io.semanticdf.platform.streaming.StreamingService.StreamRunRequest;
import org.apache.spark.sql.streaming.StreamingQuery;

/**
 * v0.3.1 Phase 6: engine-portable streaming launcher seam.
 *
 * <p>Mirrors {@link StreamingQueryLauncher} but takes a {@link Model}
 * (engine-portable, journal-safe) instead of a {@link
 * io.semanticdf.SemanticTable} (Spark-coupled, contains
 * {@code Dataset.rdd}).
 *
 * <h2>Why a new launcher interface</h2>
 *
 * <p>The legacy {@link StreamingQueryLauncher} takes a
 * {@code SemanticTable}, which carries a Spark {@code Dataset.rdd}
 * chain that Jackson cannot round-trip through the Restate journal.
 * Per the v0.3.1 Platform migration design doc (PR #443), the
 * registry exposes engine-portable lookups via {@code Model}; this
 * launcher is the streaming-path counterpart.
 *
 * <h2>Default implementation</h2>
 *
 * <p>{@link SparkPortableStreamingQueryLauncher} is the default —
 * uses {@code PortableQueryCompiler} to compile the {@link Model}
 * to a Spark {@code DataFrame}, then {@code DataFrame.writeStream}
 * to start the streaming query. The checkpoint location comes from
 * {@link StreamRunRequest#checkpointLocation()} (required, validated
 * at the Wire DTO boundary).
 *
 * <h2>Concurrency</h2>
 *
 * <p>Same as {@link StreamingQueryLauncher}: writes go to the JVM-local
 * {@link StreamingQueryHandleRegistry}, NOT to Restate journal state
 * (Spark {@link StreamingQuery} is not serializable).
 *
 * @see StreamingQueryLauncher
 * @see SparkPortableStreamingQueryLauncher
 */
@FunctionalInterface
public interface PortableStreamingQueryLauncher extends java.io.Serializable {

  /**
   * Start a streaming query for the given engine-portable model.
   *
   * @param model    the resolved {@code core.Model} (engine-portable)
   * @param request  the validated run request (checkpoint location is non-blank)
   * @return the live Spark {@link StreamingQuery} handle
   */
  StreamingQuery start(Model model, StreamRunRequest request);
}