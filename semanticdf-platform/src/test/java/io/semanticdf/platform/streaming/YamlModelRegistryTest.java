package io.semanticdf.platform.streaming;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Constructor;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link YamlModelRegistry}.
 *
 * <p>The {@code load} method requires a live Spark session (delegates to
 * the library's {@code YamlLoader.loadDir}); it's tested via the integration
 * test pattern when the platform is deployed.
 *
 * <p>This unit test focuses on the constructor's validation: null map,
 * empty map. The happy-path ({@code get}, {@code getModel},
 * {@code registeredModels}) is covered by the integration test which
 * boots a real Spark session.
 */
class YamlModelRegistryTest {

  // v0.3.2 Phase 3 partial: the constructor signature changed from
  // (Map<String, SemanticTable>) to (Map<String, SemanticTable>,
  // Map<String, Model>) — both representations are now stored.
  // The reflection target is updated accordingly.

  @Test
  void constructor_rejectsNullTables() throws Exception {
    Constructor<YamlModelRegistry> ctor =
        YamlModelRegistry.class.getDeclaredConstructor(Map.class, Map.class);
    ctor.setAccessible(true);

    // Java's reflection wraps the constructor's IllegalArgumentException
    // in InvocationTargetException; unwrap and assert on the cause.
    Throwable cause =
        assertThrows(
            java.lang.reflect.InvocationTargetException.class,
            () -> ctor.newInstance(null, Map.of()))
        .getCause();
    assertTrue(
        cause instanceof IllegalArgumentException,
        "expected IllegalArgumentException, got: " + cause);
  }

  @Test
  void constructor_rejectsEmptyTables() throws Exception {
    Constructor<YamlModelRegistry> ctor =
        YamlModelRegistry.class.getDeclaredConstructor(Map.class, Map.class);
    ctor.setAccessible(true);

    Throwable cause =
        assertThrows(
            java.lang.reflect.InvocationTargetException.class,
            () -> ctor.newInstance(Map.of(), Map.of()))
        .getCause();
    assertTrue(
        cause instanceof IllegalArgumentException,
        "expected IllegalArgumentException, got: " + cause);
    assertTrue(
        cause.getMessage().contains("no models loaded"),
        "error message must name the cause: " + cause.getMessage());
  }

  @Test
  void constructor_rejectsMismatchedKeys() throws Exception {
    Constructor<YamlModelRegistry> ctor =
        YamlModelRegistry.class.getDeclaredConstructor(Map.class, Map.class);
    ctor.setAccessible(true);

    // tables has "a", models has "b" — different keys must be rejected
    // (defense in depth: load() guarantees sync, but if someone bypasses
    // load() the constructor should fail loud).
    Map<String, Object> tables = Map.of("a", new Object());
    Map<String, Object> models = Map.of("b", new Object());

    Throwable cause =
        assertThrows(
            java.lang.reflect.InvocationTargetException.class,
            () -> ctor.newInstance(tables, models))
        .getCause();
    assertTrue(
        cause instanceof IllegalArgumentException,
        "expected IllegalArgumentException, got: " + cause);
    assertTrue(
        cause.getMessage().contains("identical keys"),
        "error message must name the cause: " + cause.getMessage());
  }
}