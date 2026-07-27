package io.semanticdf.platform.streaming;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.MessageDigest;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link StreamingDedupHash}.
 *
 * <p>Verifies:
 * <ul>
 *   <li><b>Determinism</b> — same inputs always produce the same hash.
 *   <li><b>Golden vectors</b> — the hash matches an independently-computed
 *       SHA-256 over the same field sequence, guarding against silent field
 *       reordering or separator changes.
 *   <li><b>Stream-id sensitivity</b> — two streams with identical config but
 *       different stream-ids produce different hashes (so AuditService does
 *       not collapse distinct streams).
 *   <li><b>Null safety</b> — null fields are treated as empty strings.
 *   <li><b>Format</b> — the output is lowercase hex, 64 chars (SHA-256).
 * </ul>
 */
class StreamingDedupHashTest {

  private static final String EVT = StreamingDedupHash.STREAMING_STARTED;
  private static final byte SEP = 0x1f;

  @Test
  void deterministic_sameInputsSameHash() {
    String a =
        StreamingDedupHash.streamingStarted("s1", "orders", "sum(amount)", "/ckpt/orders");
    String b =
        StreamingDedupHash.streamingStarted("s1", "orders", "sum(amount)", "/ckpt/orders");
    assertEquals(a, b);
  }

  @Test
  void goldenVector_matchesIndependentSha256() throws Exception {
    // Independently compute the expected hash over the exact field
    // sequence: eventType SEP streamId SEP modelName SEP queryShape
    // SEP checkpointLocation. This guards against field reordering.
    MessageDigest md = MessageDigest.getInstance("SHA-256");
    md.update(EVT.getBytes());
    md.update(SEP);
    md.update("s1".getBytes());
    md.update(SEP);
    md.update("orders".getBytes());
    md.update(SEP);
    md.update("sum(amount)".getBytes());
    md.update(SEP);
    md.update("/ckpt/orders".getBytes());
    String expected = toHex(md.digest());

    String actual =
        StreamingDedupHash.streamingStarted("s1", "orders", "sum(amount)", "/ckpt/orders");
    assertEquals(expected, actual);
  }

  @Test
  void streamIdSensitivity_differentStreamsDifferentHash() {
    String a = StreamingDedupHash.streamingStarted("stream-A", "orders", "shape", "/ckpt");
    String b = StreamingDedupHash.streamingStarted("stream-B", "orders", "shape", "/ckpt");
    assertNotEquals(a, b, "different stream-ids must produce different hashes");
  }

  @Test
  void modelSensitivity_differentModelsDifferentHash() {
    String a = StreamingDedupHash.streamingStarted("s1", "orders", "shape", "/ckpt");
    String b = StreamingDedupHash.streamingStarted("s1", "payments", "shape", "/ckpt");
    assertNotEquals(a, b);
  }

  @Test
  void checkpointSensitivity_differentCheckpointsDifferentHash() {
    String a = StreamingDedupHash.streamingStarted("s1", "orders", "shape", "/ckpt/1");
    String b = StreamingDedupHash.streamingStarted("s1", "orders", "shape", "/ckpt/2");
    assertNotEquals(a, b);
  }

  @Test
  void nullFieldsTreatedAsEmpty() {
    String withNulls = StreamingDedupHash.streamingStarted(null, null, null, null);
    String withEmpty = StreamingDedupHash.streamingStarted("", "", "", "");
    assertEquals(withNulls, withEmpty);
  }

  @Test
  void outputIsLowercaseHexOfCorrectLength() {
    String hash = StreamingDedupHash.streamingStarted("s", "m", "q", "c");
    assertEquals(64, hash.length(), "SHA-256 hex is 64 chars");
    assertTrue(hash.matches("[0-9a-f]{64}"), "must be lowercase hex");
  }

  @Test
  void eventTypeSensitivity() {
    String started = StreamingDedupHash.compute("streaming.started", "s1", "m", "q", "c");
    String stopped = StreamingDedupHash.compute("streaming.stopped", "s1", "m", "q", "c");
    assertNotEquals(started, stopped, "different event types must produce different hashes");
  }

  // --- streaming.restarted (PR #231 reconciliation feature) ---

  @Test
  void streamingRestarted_eventTypeConstant() {
    assertEquals("streaming.restarted", StreamingDedupHash.STREAMING_RESTARTED);
    assertNotEquals(
        StreamingDedupHash.STREAMING_STARTED,
        StreamingDedupHash.STREAMING_RESTARTED,
        "restarted must be a distinct event type so audit log shows both");
  }

  @Test
  void streamingRestarted_isWellFormed() {
    String hash = StreamingDedupHash.streamingRestarted("s1", "orders", 1L, "/ckpt");
    assertEquals(64, hash.length(), "SHA-256 hex is 64 chars");
    assertTrue(hash.matches("[0-9a-f]{64}"));
  }

  @Test
  void streamingRestarted_differsFromStreamingStarted() {
    // Same identity inputs, different event types → different hashes.
    // This is the property that lets the audit log retain BOTH the
    // original streaming.started (replayed from journal) AND the
    // streaming.restarted (post-crash) without dedup collapsing them.
    String started =
        StreamingDedupHash.streamingStarted("s1", "orders", "sum(amount)", "/ckpt");
    String restarted = StreamingDedupHash.streamingRestarted("s1", "orders", 1L, "/ckpt");
    assertNotEquals(started, restarted);
  }

  @Test
  void streamingRestarted_streamIdSensitivity() {
    // Different stream-ids must produce different hashes — protects against the
    // cross-stream dedup bug the DE subagent caught in PR #221.
    String a = StreamingDedupHash.streamingRestarted("stream-A", "orders", 1L, "/ckpt");
    String b = StreamingDedupHash.streamingRestarted("stream-B", "orders", 1L, "/ckpt");
    assertNotEquals(a, b);
  }

  @Test
  void streamingRestarted_countSensitivity() {
    // Each restart ordinal must produce a distinct hash. Operators see
    // each restart as a distinct event in the audit log even though
    // streamId/modelName/checkpointLocation are identical across retries.
    // This is the property that fixes the DE finding #H3 ("restart
    // events collapsed to first attempt").
    String first = StreamingDedupHash.streamingRestarted("s1", "orders", 1L, "/ckpt");
    String second = StreamingDedupHash.streamingRestarted("s1", "orders", 2L, "/ckpt");
    String third = StreamingDedupHash.streamingRestarted("s1", "orders", 3L, "/ckpt");
    assertNotEquals(first, second);
    assertNotEquals(second, third);
    assertNotEquals(first, third);
  }

  @Test
  void streamingRestarted_sameInputsProduceSameHash() {
    String a = StreamingDedupHash.streamingRestarted("s1", "orders", 1L, "/ckpt");
    String b = StreamingDedupHash.streamingRestarted("s1", "orders", 1L, "/ckpt");
    assertEquals(a, b);
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
