package io.semanticdf.platform.streaming;

import java.util.List;

/**
 * Durable projection of streaming workflows.
 *
 * <p>This interface exists for one purpose: <b>DE-H2</b> — at platform
 * startup, after a JVM death, recover which streams were active so
 * the {@link StartupReconciler} can re-invoke {@code run()} on each,
 * triggering {@link StreamingService}'s auto-detect branch to
 * recreate Spark queries that were lost with the previous JVM.
 *
 * <p><b>Architectural note (PR #234 review, Architect-C1):</b> the
 * catalog stores the metadata <em>needed at sweep time to invoke
 * {@code run()}</em> — stream-id + modelName + queryShape +
 * checkpointLocation. It does NOT store status (the journal has
 * that — single source of truth per
 * {@code docs/design/platform-architecture.md §2.3}). The metadata
 * is duplicated with the journal because the journal is Restate's
 * internal store and we can't read it directly from Java.
 *
 * <p><b>What this means in practice:</b>
 * <ul>
 *   <li>Insert on {@code run()} after {@code state.set(STATUS, "running")}.
 *   <li>Update is NOT exposed — the row's metadata is set once
 *       and never changes (model, query shape, checkpoint are
 *       immutable for a single workflow execution).
 *   <li>If a workflow's metadata DOES need to change, the
 *       operator calls {@code stop()} followed by {@code run()}
 *       with a fresh request — which inserts a new row (or
 *       reuses the same stream-id, depending on key strategy).
 * </ul>
 *
 * <p>Visible-for-testing — package-private methods so unit tests can
 * drive a fake catalog.
 */
public interface StreamCatalog {

  /**
   * Record that a stream has been started. Idempotent — re-inserting an
   * existing stream-id is a no-op (the original registration metadata
   * is preserved).
   *
   * @param streamId the workflow key
   * @param modelName the semantic model driving the stream
   * @param queryShape the canonical query spec (serialized)
   * @param checkpointLocation the durable checkpoint path
   */
  void registerIfAbsent(
      String streamId, String modelName, String queryShape, String checkpointLocation);

  /**
   * Enumerate all registered streams. Used by the
   * {@link StartupReconciler} to discover which workflows to
   * re-invoke.
   *
   * <p>For P1 the assumption is &lt;1000 streams per platform; a full
   * scan is fine. If the table grows past that, the
   * catalog-as-source-of-truth design has already failed and we
   * should be on the admin API for workflow enumeration (deferred to
   * a P3 follow-up).
   *
   * @return all registered streams, in insertion order
   */
  List<StreamMetadata> findAll();

  /**
   * Release the underlying connection pool. Idempotent. Called at
   * JVM shutdown so Postgres connections are released cleanly.
   */
  void close();

  /**
   * Snapshot of a row in the {@code streaming_streams} table — just
   * enough information for the {@link StartupReconciler} to
   * reconstruct an invocation body for {@link StreamingService#run}.
   */
  record StreamMetadata(
      String streamId, String modelName, String queryShape, String checkpointLocation) {}
}
