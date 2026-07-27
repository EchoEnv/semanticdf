package io.semanticdf.platform.streaming;

import io.semanticdf.SemanticTable;
import io.semanticdf.StreamingSupport;
import java.util.Collections;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import scala.Function1;
import scala.Option;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

/**
 * The default {@link StreamingQueryLauncher} — calls
 * {@code SemanticTable.toStreamingQuery(spark, opts)} with the options
 * constructed from the validated {@link StreamingService.StreamRunRequest}.
 *
 * <p>For P1, only the options that the platform <em>knows about</em> are
 * populated from the request:
 * <ul>
 *   <li>{@code checkpointLocation} — from {@link StreamingService.StreamRunRequest#checkpointLocation}
 *   <li>{@code outputMode} — hardcoded to {@code "append"} (P1 default)
 *   <li>{@code foreachBatch} — no-op (consumers configure sinks via MCP/CLI)
 *   <li>{@code trigger}, {@code window}, {@code watermark}, {@code groupKeys}
 *       — left empty (operator provides these via the model's YAML
 *       {@code streaming:} block, or via a future enhancement to the Wire DTO)
 * </ul>
 *
 * <p>The platform deliberately does not expose trigger/window/watermark
 * knobs through the Wire DTO yet — they're model-shape decisions, not
 * platform concerns. The model's YAML drives these.
 */
public final class SparkStreamingQueryLauncher implements StreamingQueryLauncher {

  private final SparkSession spark;

  public SparkStreamingQueryLauncher(SparkSession spark) {
    this.spark = java.util.Objects.requireNonNull(spark, "spark");
  }

  @Override
  public StreamingQuery start(SemanticTable model, StreamingService.StreamRunRequest request) {
    StreamingSupport.StreamingQueryOptions opts = buildOptions(request);
    return model.toStreamingQuery(spark, opts);
  }

  /**
   * Construct {@link StreamingSupport.StreamingQueryOptions} from the
   * validated request. Visible-for-testing.
   */
  static StreamingSupport.StreamingQueryOptions buildOptions(
      StreamingService.StreamRunRequest request) {
    // Build an empty scala.collection.immutable.Seq via JavaConverters.
    scala.collection.immutable.Seq<String> emptyGroupKeys =
        scala.collection.JavaConverters.asScalaBufferConverter(
            Collections.<String>emptyList()).asScala().toSeq();
    return new StreamingSupport.StreamingQueryOptions(
        Option.<org.apache.spark.sql.streaming.Trigger>empty(), // trigger
        "append", // outputMode
        Option.<String>apply(request.checkpointLocation()), // checkpointLocation
        NOOP_FOREACH_BATCH, // foreachBatch — no-op; consumers wire sinks via MCP/CLI
        Option.<StreamingSupport.WindowSpec>empty(), // window
        Option.<StreamingSupport.WatermarkSpec>empty(), // watermark
        emptyGroupKeys // groupKeys
        );
  }

  /**
   * A no-op {@link Function1} for the {@code foreachBatch} callback. Scala's
   * {@code Function1} is a single-method interface; we extend
   * {@link AbstractFunction1} so Java callers don't need to implement the
   * structural type.
   */
  private static final Function1<Dataset<Row>, BoxedUnit> NOOP_FOREACH_BATCH =
      new AbstractFunction1<Dataset<Row>, BoxedUnit>() {
        @Override
        public BoxedUnit apply(Dataset<Row> v1) {
          return BoxedUnit.UNIT;
        }
      };
}
