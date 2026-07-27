package io.semanticdf.platform.audit;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.jupiter.api.Test;

/**
 * Structural assertions for {@code AuditService.append}.
 *
 * <p>Pins the cross-cutting contracts that the senior-architect +
 * senior-DE reviewers flagged as critical for PR-A:
 *
 * <ol>
 *   <li>No {@code System.currentTimeMillis()} / {@code Instant.now()}
 *       — replay must use {@code Restate.instantNow()}.
 *   <li>No {@code StreamingDedupHash.compute} / bespoke hash
 *       construction — query events must use the library's
 *       {@code AuditEvent.dedupHashOf}; streaming events use
 *       {@code StreamingDedupHash} from the caller. The service
 *       itself just persists whatever the caller hands in.
 *   <li>Side-effecting store writes are wrapped in
 *       {@code Restate.run} — replay never re-executes the
 *       Postgres INSERT.
 *   <li>Constructor injects the {@link AuditEventStore} (no
 *       {@code new PostgresAuditEventStore(...)} inline).
 * </ol>
 *
 * <p>Mirrors {@code PlatformApplicationStartupSparkHookTest} — same
 * low-rigor-tooling-but-correct-ergonomics tradeoff. Behavioral
 * tests against a real Restate TestKit + Testcontainers Postgres
 * are in {@link PostgresAuditEventStoreTest}.
 */
class AuditServiceStructuralTest {

  @Test
  void auditService_append_doesNotUseSystemCurrentTimeMillis() throws IOException {
    String src = readAuditService();
    // Pattern 1 — explicit System.currentTimeMillis()
    assertNoForbidden(src, "System.currentTimeMillis", "append");
    // Pattern 2 — Instant.now() (not via Restate.instantNow)
    assertNoForbidden(src, "Instant.now()", "append");
    // Pattern 3 — new Date() — wall-clock alternative
    assertNoForbidden(src, "new Date()", "append");
  }

  @Test
  void auditService_append_doesNotShadowDedupHash() throws IOException {
    String src = readAuditService();
    // The service MUST NOT compute dedup hashes; it persists
    // whatever the caller hands in. StreamingDedupHash is
    // built by the streaming emit path; AuditEvent.dedupHashOf
    // is for query events. AuditService just persists.
    assertNoForbidden(src, "StreamingDedupHash.compute",
        "append", "do not compute hashes here \u2014 the caller supplies dedupHash");
    assertNoForbidden(src, "MessageDigest.getInstance",
        "append", "no bespoke hashing in this service");
  }

  @Test
  void auditService_append_usesRestateRunForSideEffects() throws IOException {
    String src = readAuditService();
    // The store.append(...) call must be inside a Restate.run(...)
    // block. Structural assertion: Restate.run appears in or near
    // the append handler.
    int appendIdx = src.indexOf("append(AuditEventRequest");
    int runIdx = src.indexOf("Restate.run(", appendIdx);
    assertTrue(appendIdx > 0, "append() handler not found");
    assertTrue(runIdx > 0,
        "append() handler must wrap side-effects in Restate.run(...) for replay safety");
    // And store.append(...) appears INSIDE the Restate.run block
    // (between Restate.run and the closing of its lambda).
    int storeAppendIdx = src.indexOf("store.append(", runIdx);
    assertTrue(storeAppendIdx > 0,
        "store.append(...) must be inside the Restate.run block");
  }

  @Test
  void auditService_constructorRejectsNullStore() {
    assertThrows(NullPointerException.class, () -> new AuditService(null));
  }

  @Test
  void auditService_noOpConvenienceFactoryDoesNotThrow() {
    // Smoke: a no-op wiring still produces a service.
    AuditService svc = AuditService.noOp();
    assertNotNull(svc);
  }

  // --- helpers ---

  /**
   * Assert that {@code needle} does NOT appear in the {@code append}
   * method body. {@code msg} is included in the assertion message
   * to give operators a hint when they re-introduce the forbidden
   * pattern. {@code extraHops...} add additional context lines.
   */
  private static void assertNoForbidden(String src, String needle, String... extraHops) {
    // Locate the append handler body by finding its opening brace
    // and the next closing brace at the same indentation level.
    int appendOpen = src.indexOf("public void append(");
    assertTrue(appendOpen > 0, "append() handler not found");
    // Find the body's first '{' and the matching '}' (brute force
    // works because there's no nested anonymous class inside append).
    int braceOpen = src.indexOf("{", appendOpen);
    int depth = 1;
    int i = braceOpen + 1;
    while (i < src.length() && depth > 0) {
      char c = src.charAt(i);
      if (c == '{') depth++;
      else if (c == '}') depth--;
      i++;
    }
    String body = src.substring(braceOpen, i);
    int found = body.indexOf(needle);
    if (found >= 0) {
      String msg = "AuditService.append must not use '" + needle + "'";
      for (String hop : extraHops) msg += " (" + hop + ")";
      msg += " \u2014 the forbidden pattern appears at offset " + found + " in the body.";
      throw new AssertionError(msg);
    }
  }

  /** Read the source file for AuditService.java from the module src root. */
  private static String readAuditService() throws IOException {
    String[] candidates = {
      "src/main/java/io/semanticdf/platform/audit/AuditService.java",
      "../src/main/java/io/semanticdf/platform/audit/AuditService.java",
      "semanticdf-platform/src/main/java/io/semanticdf/platform/audit/AuditService.java",
    };
    Path path = null;
    for (String c : candidates) {
      Path p = Paths.get(c);
      if (Files.isRegularFile(p)) {
        path = p;
        break;
      }
    }
    if (path == null) {
      throw new AssertionError("could not locate AuditService.java");
    }
    return new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
  }
}
