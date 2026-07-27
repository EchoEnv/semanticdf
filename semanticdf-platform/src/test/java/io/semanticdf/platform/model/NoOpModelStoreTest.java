package io.semanticdf.platform.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Pure-unit tests for {@link NoOpModelStore} \u2014 the
 * {@code SEMANTICDF_MODELS_PERSIST=false} (default) backing.
 */
class NoOpModelStoreTest {

  @Test
  void registerIfAbsent_returnsEchoOfInputs() throws Exception {
    Instant now = Instant.now();
    ModelStore store = new NoOpModelStore();
    ModelStore.ModelDefinition def =
        store.registerIfAbsent("m", 1, "yaml", "hash", now, "{\"ln\":1}");
    assertNotNull(def);
    assertEquals("m", def.modelName());
    assertEquals(1, def.version());
    assertEquals("yaml", def.yaml());
    assertEquals("hash", def.manifestHash());
    assertEquals(now, def.registeredAt());
    assertEquals("{\"ln\":1}", def.lineageJson());
  }

  @Test
  void registerIfAbsent_rejectsBlankModelNameAndPositiveVersion() {
    Instant now = Instant.now();
    ModelStore store = new NoOpModelStore();
    assertThrows(IllegalArgumentException.class,
        () -> store.registerIfAbsent(null, 1, "y", "h", now, "{}"));
    assertThrows(IllegalArgumentException.class,
        () -> store.registerIfAbsent("", 1, "y", "h", now, "{}"));
    assertThrows(IllegalArgumentException.class,
        () -> store.registerIfAbsent("m", 0, "y", "h", now, "{}"));
    assertThrows(IllegalArgumentException.class,
        () -> store.registerIfAbsent("m", -1, "y", "h", now, "{}"));
  }

  @Test
  void listAll_loadByName_loadLatest_returnEmptyOrNull() throws Exception {
    ModelStore store = new NoOpModelStore();
    assertEquals(0, store.listAll().size(), "no readback history in no-op mode");
    assertNull(store.loadByName("any", 1));
    assertNull(store.loadLatest("any"));
  }

  @Test
  void ensureSchema_close_areNoOps() throws Exception {
    ModelStore store = new NoOpModelStore();
    store.ensureSchema(); // does not throw
    store.close();         // does not throw
    store.close();         // idempotent
  }
}
