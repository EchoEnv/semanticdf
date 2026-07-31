package io.semanticdf.platform.query;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.semanticdf.platform.streaming.ModelRegistry;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.jupiter.api.Test;

/**
 * Structural assertions for {@code QueryService.runQuery}.
 *
 * <p>Pins the cross-cutting contracts that PR-C inherits from the
 * plan doc:
 *
 * <ol>
 *   <li>No {@code System.currentTimeMillis()} / {@code Instant.now()}
 *       in the handler body \u2014 QueryService does NOT audit in PR-C;
 *       if/when audit is wired in a follow-up, it MUST use library's
 *       {@code Clock} seam.
 *   <li>Cache-miss execution is wrapped in {@code Restate.run(...)}.
 *   <li>Cache lookup is deterministic and OUTSIDE any
 *       {@code Restate.run(...)} block (no need to journal).
 *   <li>Constructor rejects null deps (no NPE-at-handler-time).
 * </ol>
 *
 * <p>Behavioral coverage: the cache-hit vs cache-miss invocation
 * sequence is exercised end-to-end by {@code QueryServiceIntegrationTest}
 * (the {@code @RestateTest} is gated by Docker availability and
 * skipped on machines without it; pure-unit structural tests cover
 * the contract shape regardless).
 */
class QueryServiceStructuralTest {

  /** A registry that throws on every call \u2014 we never invoke it. */
  private final ModelRegistry throwingReg =
      name -> {
        throw new UnsupportedOperationException("not used");
      };

  @Test
  void queryService_runQuery_doesNotUseSystemCurrentTimeMillisInHandler() throws IOException {
    String src = readQueryService();
    int handlerOpen = src.indexOf("public QueryResult runQuery(");
    assertTrue(handlerOpen > 0, "runQuery() handler not found");
    int bodyOpen = src.indexOf("{", handlerOpen);
    int bodyClose = findMatchingBrace(src, bodyOpen);
    String body = src.substring(bodyOpen, bodyClose);

    assertTrue(!body.contains("System.currentTimeMillis"),
        "runQuery handler must NOT call System.currentTimeMillis");
    assertTrue(!body.contains("Instant.now()"),
        "runQuery handler must NOT call Instant.now() (audit must use Clock seam in a follow-up)");
  }

  @Test
  void queryService_runQuery_cacheMissIsInsideRestateRun() throws IOException {
    String src = readQueryService();
    int handlerOpen = src.indexOf("public QueryResult runQuery(");
    int bodyOpen = src.indexOf("{", handlerOpen);
    int bodyClose = findMatchingBrace(src, bodyOpen);
    String body = src.substring(bodyOpen, bodyClose);

    int restateRunIdx = body.indexOf("Restate.run(");
    assertTrue(restateRunIdx > 0,
        "runQuery must wrap the cache-miss execution in Restate.run(...)");
    assertTrue(body.contains("\"query.execute\""),
        "Restate.run name must be the documented \"query.execute\"");
  }

  @Test
  void queryService_runQuery_cacheLookupIsOutsideRestateRun() throws IOException {
    String src = readQueryService();
    int handlerOpen = src.indexOf("public QueryResult runQuery(");
    int bodyOpen = src.indexOf("{", handlerOpen);
    int bodyClose = findMatchingBrace(src, bodyOpen);
    String body = src.substring(bodyOpen, bodyClose);

    int cacheGetIdx = body.indexOf("cache.get(");
    int cachePutIdx = body.indexOf("cache.putWithModelAndVersion(");
    int restateRunIdx = body.indexOf("Restate.run(");
    assertTrue(cacheGetIdx > 0 && cacheGetIdx < restateRunIdx,
        "cache.get(...) must come BEFORE the Restate.run(...) block");
    assertTrue(cachePutIdx > 0 && cachePutIdx > restateRunIdx,
        "cache.putWithModelAndVersion(...) must come AFTER the Restate.run(...) block");
  }

  @Test
  void queryService_constructor_rejectsNullRegistry() {
    // The registry is checked FIRST in the constructor; null spark
    // would also throw but the test focuses on the registry contract.
    assertThrows(NullPointerException.class,
        () -> QueryService.noOp(null, null));
  }

  /**
   * PR #263 (cache correctness fix): the truncated flag in
   * QueryResult must compare against the REAL cap
   * ({@code CacheBridge.DefaultMaxRows = 100,000}), not 1024. The
   * v0.2.2 bug used {@code rowCount > 1024} which told callers the
   * result was truncated when in fact it was within the driver cap.
   *
   * <p>The threshold lives in {@code toQueryResult} (where the
   * {@link CachedResult} is converted to the wire {@code QueryResult}),
   * not in {@code runQuery} itself.
   */
  @Test
  void queryService_toQueryResult_truncatedFlagUsesCacheBridgeDefaultMaxRows() throws IOException {
    String src = readQueryService();
    int toQueryResultOpen = src.indexOf("static QueryResult toQueryResult(");
    int braceOpen = src.indexOf("{", toQueryResultOpen);
    int braceClose = findMatchingBrace(src, braceOpen);
    String body = src.substring(braceOpen, braceClose);

    // The old (wrong) threshold must NOT appear anywhere in the
    // toQueryResult body. If it does, someone reintroduced the bug.
    assertTrue(!body.contains("> 1024"),
        "toQueryResult() must not compare rowCount against 1024; the real "
            + "cap is CacheBridge.DefaultMaxRows (100,000). See #263.");

    // The new threshold MUST appear, and it MUST be the env-var-aware
    // `effectiveMaxRows()` accessor (not a hand-coded literal).
    // `effectiveMaxRows` falls back to `CacheBridge.DefaultMaxRows`
    // when no `SEMANTICDF_MAX_ROWS` env-var is set, so the historical
    // 100,000 cap is preserved by default.
    assertTrue(body.contains("CacheBridge.effectiveMaxRows()"),
        "toQueryResult() must call CacheBridge.effectiveMaxRows() to compute "
            + "the truncated flag (env-var-aware; falls back to DefaultMaxRows).");
  }

  // --- helpers ---

  private static int findMatchingBrace(String src, int startIdx) {
    int depth = 1;
    int i = startIdx + 1;
    while (i < src.length() && depth > 0) {
      char c = src.charAt(i);
      if (c == '{') depth++;
      else if (c == '}') depth--;
      i++;
    }
    return i;
  }

  private static String readQueryService() throws IOException {
    String[] candidates = {
      "src/main/java/io/semanticdf/platform/query/QueryService.java",
      "../src/main/java/io/semanticdf/platform/query/QueryService.java",
      "semanticdf-platform/src/main/java/io/semanticdf/platform/query/QueryService.java",
    };
    Path path = null;
    for (String c : candidates) {
      Path p = Paths.get(c);
      if (Files.isRegularFile(p)) { path = p; break; }
    }
    if (path == null) throw new AssertionError("could not locate QueryService.java");
    return new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
  }
}
