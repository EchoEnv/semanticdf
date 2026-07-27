package io.semanticdf.platform.streaming;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Deterministic deduplication hash for streaming-lifecycle audit events.
 *
 * <p>The hash is computed over the <em>logical identity</em> of a streaming
 * run — the inputs that make one stream distinct from another:
 *
 * <ul>
 *   <li>event type ({@code "streaming.started"})
 *   <li>stream-id (the workflow key — unique per execution, constant across replays)
 *   <li>model name
 *   <li>canonical query shape (the serialized query spec)
 *   <li>checkpoint location
 * </ul>
 *
 * <p><b>It deliberately excludes</b> anything non-deterministic: timestamps,
 * run IDs, batch counters. Including those would break replay-safety (Restate
 * replays the journal; two replays with different {@code now()} would produce
 * different hashes and double-emit the audit event).
 *
 * <p>The stream-id IS included because two different streams that happen to
 * share the same config still deserve separate audit events — without the
 * stream-id, AuditService's dedup would silently drop the second stream's
 * event, masking a real operational signal. The stream-id is constant across
 * replays of the same workflow, so it does not break replay-safety.
 *
 * <p>This mirrors the library's {@code AuditEvent.dedupHashOf} contract (a
 * SHA-256 over the query-shape fields) but is purpose-built for the streaming
 * lifecycle. See {@code docs/design/platform-determinism-audit.md} finding #7
 * and PR #218's dedupHash contract.
 *
 * <p>The field separator is ASCII unit-separator ({@code 0x1f}) to prevent
 * value concatenation from producing ambiguous input.
 */
public final class StreamingDedupHash {

  /** The event type emitted when a streaming query transitions to running. */
  public static final String STREAMING_STARTED = "streaming.started";

  private static final byte SEP = 0x1f;

  private StreamingDedupHash() {}

  /**
   * Compute the dedup hash for a streaming-started audit event.
   *
   * @param streamId the workflow key (unique per stream execution)
   * @param modelName the semantic model driving the stream
   * @param queryShape the canonical query spec (serialized)
   * @param checkpointLocation the durable checkpoint path
   * @return a lowercase-hex SHA-256 digest
   */
  public static String streamingStarted(
      String streamId, String modelName, String queryShape, String checkpointLocation) {
    return compute(STREAMING_STARTED, streamId, modelName, queryShape, checkpointLocation);
  }

  /**
   * Compute the dedup hash for an arbitrary streaming event type. Exposed for
   * future event types ({@code streaming.stopped}, {@code streaming.failed}).
   *
   * @param eventType the event namespace (e.g. {@code "streaming.started"})
   * @param streamId the workflow key (unique per stream execution)
   * @param modelName the semantic model driving the stream
   * @param queryShape the canonical query spec (serialized)
   * @param checkpointLocation the durable checkpoint path
   * @return a lowercase-hex SHA-256 digest
   */
  public static String compute(
      String eventType,
      String streamId,
      String modelName,
      String queryShape,
      String checkpointLocation) {
    try {
      MessageDigest md = MessageDigest.getInstance("SHA-256");
      md.update(safe(eventType));
      md.update(SEP);
      md.update(safe(streamId));
      md.update(SEP);
      md.update(safe(modelName));
      md.update(SEP);
      md.update(safe(queryShape));
      md.update(SEP);
      md.update(safe(checkpointLocation));
      return toHex(md.digest());
    } catch (NoSuchAlgorithmException e) {
      // SHA-256 is mandated by the JLS for every Java platform.
      throw new IllegalStateException("SHA-256 not available", e);
    }
  }

  private static byte[] safe(String s) {
    return (s == null ? "" : s).getBytes(StandardCharsets.UTF_8);
  }

  private static String toHex(byte[] hash) {
    StringBuilder sb = new StringBuilder(hash.length * 2);
    for (byte b : hash) {
      sb.append(Character.forDigit((b >> 4) & 0xf, 16));
      sb.append(Character.forDigit(b & 0xf, 16));
    }
    return sb.toString();
  }
}
