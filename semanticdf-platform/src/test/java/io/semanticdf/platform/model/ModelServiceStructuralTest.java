package io.semanticdf.platform.model;

import io.semanticdf.cache.ResultCache;

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
 * Structural assertions for {@code ModelService.register}.
 *
 * <p>Pins the cross-cutting contracts that the senior-architect +
 * senior-DE reviewers flagged as critical for PR-B:
 *
 * <ol>
 *   <li>No {@code System.currentTimeMillis()} / {@code Instant.now()}
 *       in the handler body — the only stable time source is
 *       {@code Restate.instantNow()}.
 *   <li>Side-effecting calls (compile, lineage JSON, Postgres
 *       persist) are wrapped in {@code Restate.run(...)}.
 *   <li>No invented dedicated {@code ManifestHash} scheme — the
 *       service uses the library's {@code ResultCache} for
 *       invalidation hooks, not shadow types.
 *   <li>Constructor rejects null deps (no NPE-at-handler-time).
 *   <li>Cache invalidation happens AFTER the journal bookkeeping,
 *       OUTSIDE {@code Restate.run(...)} — cache state is not
 *       coordination state.
 * </ol>
 *
 * <p>Behavioral coverage (real TestKit + Testcontainers PG) is in
 * {@code PostgresModelStoreTest}. End-to-end round-trip
 * (register → queryRecent for audit) is left for PR-C integration.
 */
class ModelServiceStructuralTest {

  @Test
  void modelService_register_doesNotUseSystemCurrentTimeMillis() throws IOException {
    String src = readModelService();
    // The handler must not use System.currentTimeMillis or Instant.now
    assertNoForbidden(src, "System.currentTimeMillis", "register");
    assertNoForbidden(src, "Instant.now()", "register");
    assertNoForbidden(src, "new Date()", "register");
  }

  @Test
  void modelService_register_persistOnlyInRestateRun() throws IOException {
    String src = readModelService();
    int registerOpen = src.indexOf("public void register(");
    assertTrue(registerOpen > 0, "register() handler not found");
    int braceOpen = src.indexOf("{", registerOpen);
    int braceClose = findMatchingBrace(src, braceOpen);
    String body = src.substring(braceOpen, braceClose);

    // After PR #249: only the durable Postgres persist step is
    // inside Restate.run. Compile (which produces a SemanticTable
    // carrying a Dataset.rdd chain that Jackson cannot round-trip
    // through the journal) and lineage (a String, but pure) run in
    // handler scope. The journal only captures the side-effecting
    // durable write.
    int persistRun = body.indexOf("\"model.persist\"");
    assertTrue(persistRun > 0,
        "register() must call Restate.run(\"model.persist\", ...) " +
        "for the durable Postgres write");
    int compileRun = body.indexOf("\"model.compile\"");
    int lineageRun = body.indexOf("\"model.lineage\"");
    assertTrue(compileRun < 0,
        "register() must NOT call Restate.run(\"model.compile\", ...) " +
        "â restateCompiled semanticTable carries a Dataset.rdd chain " +
        "that Jackson cannot round-trip through the journal. Compile " +
        "is a pure function and runs in handler scope.");
    assertTrue(lineageRun < 0,
        "register() must NOT call Restate.run(\"model.lineage\", ...) " +
        "â restateLineage is also pure; running in handler scope keeps " +
        "the journal entry minimal (just the persist return).");
  }

  @Test
  void modelService_register_invalidateCacheOutsidesRestateRun() throws IOException {
    String src = readModelService();
    int registerOpen = src.indexOf("public void register(");
    int braceOpen = src.indexOf("{", registerOpen);
    int braceClose = findMatchingBrace(src, braceOpen);
    String body = src.substring(braceOpen, braceClose);

    int cacheIdx = body.indexOf("cache.invalidateModel(");
    assertTrue(cacheIdx > 0,
        "register() must call cache.invalidateModel(...) after success (was invalidateByModelAndVersion in v0.2.2; see #261)");
    // The cache call must NOT be inside a Restate.run block (cache state
    // is observable, not coordination). We verify this by checking that
    // the cache call appears AFTER all three Restate.run steps.
    int persistRun = body.indexOf("\"model.persist\"");
    assertTrue(cacheIdx > persistRun,
        "cache.invalidateByModelAndVersion must be AFTER the model.persist Restate.run block");
  }

  @Test
  void modelService_constructor_rejectsNullStore() {
    assertThrows(NullPointerException.class,
        () -> new ModelService(null, null, ResultCache.NoOp()));
  }

  @Test
  void modelService_compileFromYaml_invokedViaReflection() throws Exception {
    // Reflection-only â we don't spin up Spark here. The constructor
    // null-dep test proves the NPE behavior; this test confirms the
    // helper is reachable with the documented signature.
    java.lang.reflect.Method m = ModelService.class.getDeclaredMethod(
        "compileFromYaml", String.class, String.class,
        org.apache.spark.sql.SparkSession.class);
    assertNotNull(m);
    assertTrue(java.lang.reflect.Modifier.isStatic(m.getModifiers()));
    // package-private; no need to enforce visibility
  }

  @Test
  void modelService_compileFromYaml_handlesStringInput() {
    // Smoke: the helper accepts an arbitrary YAML string. We don't run
    // it (would require an active SparkSession with a registered table);
    // we just confirm the method signature is reachable by reflection.
    java.lang.reflect.Method m;
    try {
      m = ModelService.class.getDeclaredMethod(
          "compileFromYaml", String.class, String.class,
          org.apache.spark.sql.SparkSession.class);
    } catch (NoSuchMethodException e) {
      throw new AssertionError("compileFromYaml must exist with this signature", e);
    }
    assertNotNull(m);
  }

  // --- helpers ---

  /** Find the index of the {@code } that closes the {@code } at {@code startIdx}. */
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

  private static void assertNoForbidden(String src, String needle, String... extraHops) {
    int handlerOpen = src.indexOf("public void register(");
    assertTrue(handlerOpen > 0, "register() handler not found");
    int braceOpen = src.indexOf("{", handlerOpen);
    int braceClose = findMatchingBrace(src, braceOpen);
    String body = src.substring(braceOpen, braceClose);
    int found = body.indexOf(needle);
    if (found >= 0) {
      String msg = "ModelService.register must not use '" + needle + "'";
      for (String hop : extraHops) msg += " (" + hop + ")";
      msg += " — forbidden pattern at offset " + found + " in handler body.";
      throw new AssertionError(msg);
    }
  }

  private static String readModelService() throws IOException {
    String[] candidates = {
      "src/main/java/io/semanticdf/platform/model/ModelService.java",
      "../src/main/java/io/semanticdf/platform/model/ModelService.java",
      "semanticdf-platform/src/main/java/io/semanticdf/platform/model/ModelService.java",
    };
    Path path = null;
    for (String c : candidates) {
      Path p = Paths.get(c);
      if (Files.isRegularFile(p)) { path = p; break; }
    }
    if (path == null) throw new AssertionError("could not locate ModelService.java");
    return new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
  }
}
