package io.semanticdf.platform.streaming;

import dev.restate.sdk.common.DurablePromiseKey;

import dev.restate.sdk.Restate;
import dev.restate.sdk.annotation.Handler;
import dev.restate.sdk.annotation.Workflow;

/**
 * StreamingService — long-running streaming query lifecycle.
 *
 * Key: stream-id (client-supplied). One workflow execution per
 * streaming query, indefinitely. Stopping the workflow kills the
 * stream; killing the JVM does not. This is what Workflow was built
 * for; do not simulate it with a VirtualObject + timer.
 *
 * State held in the workflow's journal (per-key, per-execution):
 *   - STATUS — "running" | "stopped" | "failed"
 *   - CHECKPOINT_LOCATION — the engine's checkpoint directory
 *   - LAST_BATCH_TS — the timestamp of the most recent micro-batch
 *   - ERROR_COUNT — number of errors since the stream started
 *   - STOP_SIGNAL — a DurablePromise set by {@link #stop(String)} that
 *     the main run loop checks each iteration
 *
 * Skeleton: the run loop is a single tick that just records "running."
 * The full implementation will drive the engine's streaming query and
 * yield to the STOP_SIGNAL promise between batches.
 */
@Workflow
public class StreamingService {

  /** Promise keys for cross-handler signaling. */
  private static final DurablePromiseKey<Void> STOP_SIGNAL = DurablePromiseKey.of("stop", Void.class);

  /** Main run loop — one execution per stream-id. */
  @Handler
  public void run(StreamRunRequest request) {
    // TODO P1: drive the engine's streaming query, checkpoint each batch,
    // and yield to the STOP_SIGNAL promise between batches.
    // For the skeleton, just record initial state.
  }

  /** Stop the stream — sets the STOP_SIGNAL promise; the run loop sees it on
   * its next batch and shuts down cleanly. */
  @Handler
  public void stop(Void ignored) {
    Restate.promiseHandle(STOP_SIGNAL).resolve(null);
  }

  /** Read the current status. Uses {@code @Shared} so external observers
   * can poll without serializing against the run loop. */
  @dev.restate.sdk.annotation.Shared
  @Handler
  public String getStatus() {
    // TODO P1: return the STATUS from workflow state
    return "running";
  }

  /** Request DTO for {@link #run(StreamRunRequest)}. */
  public record StreamRunRequest(
      String modelName,
      String queryShape,
      String checkpointLocation
  ) {}
}
