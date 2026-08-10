package io.semanticdf.platform.streaming;

import io.semanticdf.core.engine.EngineContext;
import io.semanticdf.core.model.Model;
import io.semanticdf.platform.streaming.StreamingService.StreamRunRequest;
import io.semanticdf.spark.PortableQueryCompiler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;

/**
 * v0.3.1 Phase 6: default {@link PortableStreamingQueryLauncher}.
 *
 * <p>Compiles the engine-portable {@link Model} to a Spark
 * {@code DataFrame} via {@link PortableQueryCompiler}, then starts
 * a streaming query via {@code DataFrame.writeStream}.
 *
 * <h2>Streaming options</h2>
 *
 * <p>For v0.3.1 (per design doc), only the checkpoint location is
 * populated from the request — the rest are model-shape decisions
 * that live in the model's YAML (or future Wire DTO extensions).
 * Specifically:
 * <ul>
 *   <li>{@code checkpointLocation} — from {@link StreamRunRequest}</li>
 *   <li>{@code outputMode} — hardcoded to {@code "append"}</li>
 *   <li>{@code format} — hardcoded to {@code "memory"} (matches
 *       the legacy {@code SemanticTable.toStreamingQuery} default
 *       for testability; production operators configure sinks via
 *       MCP/CLI in future iterations)</li>
 * </ul>
 *
 * <h2>Engine-portability contract</h2>
 *
 * <p>This impl is Spark-specific. For other engines (Trino / DuckDB
 * / PG / Hera / UC / HMS), each provides its own
 * {@link PortableStreamingQueryLauncher} impl registered with the
 * engine registry (future work). The interface is engine-portable
 * (takes {@link Model}); only this default is Spark-coupled.
 *
 * <h2>JVM-safety</h2>
 *
 * <p>The Spark session is set on the {@link PortableQueryCompiler}'s
 * thread-local before {@code compile} and cleared after — no
 * instance state is captured (per scala-spark-batch-bugs §1: closures
 * are stateless Column expressions).
 */
public final class SparkPortableStreamingQueryLauncher
    implements PortableStreamingQueryLauncher, java.io.Serializable {

  // SparkSession is itself Serializable. The launcher's stream-side
  // logic is captured (transitively) by Spark's DataStreamWriter.start
  // — the writer's plan is serialized to executors, so any reference
  // the launcher holds must be Serializable. Spark is itself, the
  // StreamRunRequest is a Java record (auto-Serializable), and the
  // DataFrame's Column expressions are inherently Serializable.
  private static final long serialVersionUID = 1L;


  private final SparkSession spark;

  public SparkPortableStreamingQueryLauncher(SparkSession spark) {
    this.spark = java.util.Objects.requireNonNull(spark, "spark");
  }

  @Override
  public StreamingQuery start(Model model, StreamRunRequest request) {
    // 1. Set the thread-local Spark session (required by PortableQueryCompiler)
    PortableQueryCompiler.setSparkSession(spark);
    try {
      // 2. Compile Model -> DataFrame. Per scala-error-handling §1:
      //    Either[L, X] is the public API shape; we surface errors
      //    as IllegalArgumentException at the platform boundary.
      Dataset<Row> df = new PortableQueryCompiler().compile(model, EngineContext.defaultContext())
          .fold(
              err -> {
                throw new IllegalArgumentException(
                    "engine-portable stream compile failed for '"
                        + request.modelName() + "': " + err);
              },
              compiled -> compiled);

      // 3. Build the streaming DataStreamWriter. Per the design doc,
      //    only checkpointLocation + outputMode are populated for v0.3.1.
      DataStreamWriter<Row> writer = df.writeStream()
          .option("checkpointLocation", request.checkpointLocation())
          .outputMode("append")
          .format("memory");  // v0.3.1 default — same as legacy path

      // 4. Start the query and return the handle. Per scala-jvm-safety
      //    §2: StreamingQuery is a process-local Spark handle; we
      //    don't close it here (caller's responsibility via
      //    StreamingQueryHandleRegistry).
      // Per scala-error-handling §1: writer.start() can throw
      // java.util.concurrent.TimeoutException; surface as a typed
      // IllegalArgumentException at the platform boundary.
      try {
        return writer.start();
      } catch (java.util.concurrent.TimeoutException e) {
        throw new IllegalArgumentException(
            "engine-portable stream start timed out for '"
                + request.modelName() + "': " + e.getMessage(), e);
      }
    } finally {
      // Per scala-jvm-safety §1: clear thread-local even on failure.
      PortableQueryCompiler.clearSparkSession();
    }
  }
}