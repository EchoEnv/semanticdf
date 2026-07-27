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
 * empty map. The happy-path ({@code get}, {@code registeredModels}) is
 * covered by the integration test which boots a real Spark session.
 */
class YamlModelRegistryTest {

  @Test
  void constructor_rejectsNullMap() throws Exception {
    Constructor<YamlModelRegistry> ctor =
        YamlModelRegistry.class.getDeclaredConstructor(Map.class);
    ctor.setAccessible(true);

    // Java's reflection throws IllegalArgumentException directly when the
    // constructor argument is null (pre-constructor, before the body runs).
    assertThrows(IllegalArgumentException.class, () -> ctor.newInstance(null));
  }

  @Test
  void constructor_rejectsEmptyMap() throws Exception {
    Constructor<YamlModelRegistry> ctor =
        YamlModelRegistry.class.getDeclaredConstructor(Map.class);
    ctor.setAccessible(true);

    Throwable cause =
        assertThrows(
            java.lang.reflect.InvocationTargetException.class,
            () -> ctor.newInstance(Map.of()))
        .getCause();
    assertTrue(
        cause instanceof IllegalArgumentException,
        "expected IllegalArgumentException, got: " + cause);
    assertTrue(
        cause.getMessage().contains("no models loaded"),
        "error message must name the cause: " + cause.getMessage());
  }
}
