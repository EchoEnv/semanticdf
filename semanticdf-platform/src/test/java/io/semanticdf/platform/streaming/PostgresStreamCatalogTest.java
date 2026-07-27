package io.semanticdf.platform.streaming;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

/**
 * Test for {@link PostgresStreamCatalog} against a real Postgres (Testcontainers).
 *
 * <p>We use Postgres from Testcontainers rather than H2/in-memory because:
 * <ol>
 *   <li>The catalog uses Postgres-specific DDL (e.g., TIMESTAMPTZ).
 *   <li>The HikariCP integration is best tested against the real driver.
 *   <li>testcontainers-core is already a test-scope transitive dep
 *       (pulled in by sdk-testing).
 * </ol>
 *
 * <p>Uses the lower-level {@link GenericContainer} with a manual
 * postgres image (rather than the {@code testcontainers-postgresql}
 * module's {@code PostgreSQLContainer}) so we don't need to add
 * another dep to the platform's pom.
 */
class PostgresStreamCatalogTest {

  static GenericContainer<?> postgres;
  static PostgresStreamCatalog catalog;

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
    catalog = new PostgresStreamCatalog(jdbcUrl, "test", "test");

    // Wait for the connection to be ready (small retry loop).
    try (var conn = catalog.dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("SELECT 1");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @BeforeEach
  void truncateForTest() {
    truncate();
  }

  @AfterAll
  static void tearDown() {
    if (catalog != null) {
      catalog.close();
    }
    if (postgres != null) {
      postgres.stop();
    }
  }

  private static void truncate() {
    try (var conn = catalog.dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("TRUNCATE TABLE " + PostgresStreamCatalog.TABLE_NAME);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  void registerIfAbsent_insertsNewRow() {
    catalog.registerIfAbsent("stream-A", "model-X", "sum(amount)", "/ckpt/A");

    List<StreamCatalog.StreamMetadata> all = catalog.findAll();
    assertEquals(1, all.size());
    assertEquals(
        new StreamCatalog.StreamMetadata("stream-A", "model-X", "sum(amount)", "/ckpt/A"),
        all.get(0));
  }

  @Test
  void registerIfAbsent_isIdempotent() {
    catalog.registerIfAbsent("stream-B", "model-Y", "count(*)", "/ckpt/B");
    catalog.registerIfAbsent("stream-B", "DIFFERENT-MODEL", "DIFFERENT", "/ckpt/B-2");

    List<StreamCatalog.StreamMetadata> all = catalog.findAll();
    assertEquals(1, all.size(), "second insert must be a no-op");
    assertEquals("model-Y", all.get(0).modelName(), "original metadata is preserved");
  }

  @Test
  void findAll_returnsByInsertionOrder() {
    catalog.registerIfAbsent("first", "m", "q", "/c");
    catalog.registerIfAbsent("second", "m", "q", "/c");
    catalog.registerIfAbsent("third", "m", "q", "/c");

    List<StreamCatalog.StreamMetadata> all = catalog.findAll();
    List<String> ids = all.stream().map(StreamCatalog.StreamMetadata::streamId).toList();
    assertEquals(List.of("first", "second", "third"), ids);
  }

  @Test
  void registerIfAbsent_rejectsNullOrBlankFields() {
    assertThrows(
        IllegalArgumentException.class,
        () -> catalog.registerIfAbsent(null, "m", "q", "/c"));
    assertThrows(
        IllegalArgumentException.class,
        () -> catalog.registerIfAbsent("stream-id", null, "q", "/c"));
    assertThrows(
        IllegalArgumentException.class,
        () -> catalog.registerIfAbsent("stream-id", "m", "  ", "/c"));
    assertThrows(
        IllegalArgumentException.class,
        () -> catalog.registerIfAbsent("stream-id", "m", "q", ""));
  }
}
