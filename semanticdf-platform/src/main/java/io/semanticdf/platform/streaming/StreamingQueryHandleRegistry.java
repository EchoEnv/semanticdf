package io.semanticdf.platform.streaming;

import java.util.concurrent.ConcurrentHashMap;
import org.apache.spark.sql.streaming.StreamingQuery;

/**
 * Process-local registry of live {@link StreamingQuery} handles, keyed by
 * the workflow's stream-id.
 *
 * <p><b>Not journaled.</b> A {@code StreamingQuery} is a Spark runtime object
 * — it holds driver state, micro-batch threads, and file-system offsets. It
 * cannot be serialized into the Restate journal. This map lives only for the
 * lifetime of the JVM.
 *
 * <p>After a JVM failure, Restate replays the workflow from the journal. The
 * journal records the decision to start the query (via {@code Restate.run}),
 * but the handle itself is gone. The reconciliation logic (re-start the query
 * from the same checkpoint) is a follow-up concern; for P1 the handle is
 * populated on first execution and consumed by {@code stop()} within the
 * same JVM.
 */
public class StreamingQueryHandleRegistry {

  private final ConcurrentHashMap<String, StreamingQuery> handles = new ConcurrentHashMap<>();

  /** Register a live query handle for a stream-id. */
  public void put(String streamId, StreamingQuery query) {
    handles.put(streamId, query);
  }

  /** Look up the live query handle for a stream-id (null if absent). */
  public StreamingQuery get(String streamId) {
    return handles.get(streamId);
  }

  /** Remove and return the handle for a stream-id (null if absent). */
  public StreamingQuery remove(String streamId) {
    return handles.remove(streamId);
  }

  /** Number of live handles (for diagnostics / tests). */
  public int size() {
    return handles.size();
  }
}
