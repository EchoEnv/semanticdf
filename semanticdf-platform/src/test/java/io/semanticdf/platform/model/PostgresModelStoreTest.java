package io.semanticdf.platform.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

/**
 * Test for {@link PostgresModelStore} against a real Postgres (Testcontainers).
 *
 * <p>Pattern mirrors {@code PostgresStreamCatalogTest} and {@code
 * PostgresAuditEventStoreTest}: same GenericContainer, same
 * truncate-per-test isolation, same lifecycle.
 */
class PostgresModelStoreTest {

  static GenericContainer<?> postgres;
  static PostgresModelStore store;

  @BeforeAll
  static void setUp() {
    postgres =
        new GenericContainer<>("postgres:16-alpine")
            .withEnv("POSTGRES_USER", "test")
            .withEnv("POSTGRES_PASSWORD", "test")
            .withEnv("POSTGRES_DB", "semanticdf_test")
            .withExposedPorts(5432);
    postgres.start();

    String jdbcUrl =
        "jdbc:postgresql://"
            + postgres.getHost()
            + ":"
            + postgres.getMappedPort(5432)
            + "/semanticdf_test";
    store = new PostgresModelStore(jdbcUrl, "test", "test");

    try (Connection conn = store.dataSource.getConnection();
        PreparedStatement stmt = conn.prepareStatement("SELECT 1")) {
      stmt.executeQuery();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @BeforeEach
  void truncateForTest() {
    try (Connection conn = store.dataSource.getConnection();
        PreparedStatement stmt =
            conn.prepareStatement("DELETE FROM " + PostgresModelStore.TABLE_NAME)) {
      stmt.executeUpdate();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @AfterAll
  static void tearDown() {
    if (store != null) {
      store.close();
    }
    if (postgres != null) {
      postgres.stop();
    }
  }

  // --- happy path ---

  @Test
  void registerIfAbsent_writesRowAndReturnsDefinition() throws Exception {
    Instant now = Instant.now().truncatedTo(java.time.temporal.ChronoUnit.MICROS);
    ModelStore.ModelDefinition def =
        store.registerIfAbsent("m1", 1, "yaml-body", "hash-1", now, "{}");
    assertNotNull(def);
    assertEquals("m1", def.modelName());
    assertEquals(1, def.version());
    assertEquals("yaml-body", def.yaml());
    assertEquals("hash-1", def.manifestHash());
    assertEquals(now, def.registeredAt());

    try (Connection conn = store.dataSource.getConnection();
        PreparedStatement sel =
            conn.prepareStatement(
                "SELECT manifest_yaml, manifest_hash, lineage_json FROM "
                    + PostgresModelStore.TABLE_NAME)) {
      var rs = sel.executeQuery();
      assertEquals(true, rs.next());
      assertEquals("yaml-body", rs.getString(1));
      assertEquals("hash-1", rs.getString(2));
      assertEquals("{}", rs.getString(3));
      assertEquals(false, rs.next(), "exactly one row");
    }
  }

  // --- idempotency ---

  @Test
  void registerIfAbsent_isIdempotentOnSameVersion() throws Exception {
    Instant now = Instant.now();
    ModelStore.ModelDefinition a =
        store.registerIfAbsent("m2", 1, "original", "h1", now, "{}");
    ModelStore.ModelDefinition b =
        store.registerIfAbsent("m2", 1, "DIFFERENT", "DIFFERENT", now.plusSeconds(5), "{}");
    ModelStore.ModelDefinition c =
        store.registerIfAbsent("m2", 1, "DIFFERENT-2", "DIFFERENT-2", now.plusSeconds(10), "{}");

    assertEquals(a.manifestHash(), b.manifestHash(),
        "second call must return the original manifest hash");
    assertEquals(a.manifestHash(), c.manifestHash());
    assertEquals("original", a.yaml(),
        "the original yaml was preserved (ON CONFLICT preserves the row)");

    // Verify only ONE row exists.
    try (Connection conn = store.dataSource.getConnection();
        PreparedStatement count =
            conn.prepareStatement(
                "SELECT COUNT(*) FROM " + PostgresModelStore.TABLE_NAME)) {
      var rs = count.executeQuery();
      rs.next();
      assertEquals(1L, rs.getLong(1));
    }
  }

  // --- listAll ---

  @Test
  void listAll_returnsByInsertionOrder() throws Exception {
    Instant now = Instant.now();
    store.registerIfAbsent("m3", 1, "y", "h", now, "");
    store.registerIfAbsent("m3", 2, "y", "h", now.plusSeconds(1), "");
    store.registerIfAbsent("m4", 1, "y", "h", now.plusSeconds(2), "");

    List<ModelStore.ModelDefinition> all = store.listAll();
    assertEquals(3, all.size());
    assertEquals("m3", all.get(0).modelName());
    assertEquals(1, all.get(0).version());
    assertEquals("m3", all.get(1).modelName());
    assertEquals(2, all.get(1).version());
    assertEquals("m4", all.get(2).modelName());
    assertEquals(1, all.get(2).version());
  }

  // --- loadByName / loadLatest ---

  @Test
  void loadByName_returnsRequestedVersion() throws Exception {
    Instant now = Instant.now();
    store.registerIfAbsent("m5", 1, "v1-yaml", "h1", now, "{ln1}");
    store.registerIfAbsent("m5", 2, "v2-yaml", "h2", now.plusSeconds(1), "{ln2}");

    ModelStore.ModelDefinition v1 = store.loadByName("m5", 1);
    ModelStore.ModelDefinition v2 = store.loadByName("m5", 2);
    assertEquals("v1-yaml", v1.yaml());
    assertEquals("{ln1}", v1.lineageJson());
    assertEquals("v2-yaml", v2.yaml());
    assertEquals("{ln2}", v2.lineageJson());
    assertNotEquals(v1.registeredAt(), v2.registeredAt());
  }

  @Test
  void loadByName_returnsNullForUnknown() throws Exception {
    assertNull(store.loadByName("nonexistent", 1));
    assertNull(store.loadByName("nonexistent", 999));
  }

  @Test
  void loadLatest_returnsMaxVersion() throws Exception {
    Instant now = Instant.now();
    store.registerIfAbsent("m6", 1, "v1", "h1", now, "{}");
    store.registerIfAbsent("m6", 3, "v3", "h3", now.plusSeconds(1), "{}");
    store.registerIfAbsent("m6", 2, "v2", "h2", now.plusSeconds(2), "{}");

    ModelStore.ModelDefinition latest = store.loadLatest("m6");
    assertEquals(3, latest.version(), "loadLatest must return the highest version");
    assertEquals("v3", latest.yaml());
  }

  @Test
  void loadLatest_returnsNullForUnknown() throws Exception {
    assertNull(store.loadLatest("nonexistent"));
  }

  // --- argument validation ---

  @Test
  void registerIfAbsent_rejectsNullOrBlankFields() throws Exception {
    Instant now = Instant.now();
    assertThrows(IllegalArgumentException.class,
        () -> store.registerIfAbsent(null, 1, "y", "h", now, "{}"));
    assertThrows(IllegalArgumentException.class,
        () -> store.registerIfAbsent("", 1, "y", "h", now, "{}"));
    assertThrows(IllegalArgumentException.class,
        () -> store.registerIfAbsent("m", 0, "y", "h", now, "{}"));
    assertThrows(IllegalArgumentException.class,
        () -> store.registerIfAbsent("m", -1, "y", "h", now, "{}"));
    assertThrows(IllegalArgumentException.class,
        () -> store.registerIfAbsent("m", 1, null, "h", now, "{}"));
    assertThrows(IllegalArgumentException.class,
        () -> store.registerIfAbsent("m", 1, "y", "", now, "{}"));
    assertThrows(IllegalArgumentException.class,
        () -> store.registerIfAbsent("m", 1, "y", "h", null, "{}"));
    // null lineageJson is allowed (we store empty string)
    ModelStore.ModelDefinition linNull =
        store.registerIfAbsent("m-lineage-null", 1, "y", "h", Instant.now(), null);
    assertEquals("", linNull.lineageJson(),
        "null lineageJson must be stored as empty string");
  }
}
