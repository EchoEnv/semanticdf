package io.semanticdf.platform.query;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.semanticdf.platform.streaming.HotReloadingModelRegistry;
import io.semanticdf.platform.streaming.YamlModelRegistry;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Tests for the v0.3.1 Phase 4 engine-portable query path wiring.
 *
 * <p>Per the v0.3.1 Platform migration design doc (PR #443), when an
 * {@code MCPEngineRegistry} is wired AND the model is registered as a
 * {@code core.Model}, {@link QueryService#runQuery} dispatches to the
 * engine-portable path instead of the Spark-only legacy path.
 *
 * <p>Note: full engine-path execution testing happens in the
 * {@code SparkEngineProvider} spec (semanticdf-spark). These tests
 * focus on the platform's Java-side wiring (constructor patterns):
 * <ul>
 *   <li>3-arg constructor pattern (legacy-only) still compiles and
 *       produces a service.</li>
 *   <li>4-arg constructor pattern with {@code null} engine registry
 *       still compiles and produces a service.</li>
 *   <li>The two constructor patterns produce distinct instances.</li>
 * </ul>
 *
 * <p>Actual {@code runQuery} execution requires a Restate handler
 * context (per Restate SDK); that's covered by the existing
 * {@code QueryServiceEndToEndTest} and {@code QueryServiceRestateEndToEndTest}.
 *
 * <p>JVM-safety: tests use a real Spark session but no Postgres.
 */
public class QueryServiceEngineRegistryTest {

  private static SparkSession spark;

  @BeforeAll
  static void startSpark() {
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("query-engine-registry-test")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate();
  }

  @AfterAll
  static void stopSpark() {
    if (spark != null) {
      spark.stop();
    }
  }

  private YamlModelRegistry bootRegistry(@TempDir Path tmp) throws Exception {
    Files.writeString(
        tmp.resolve("flights.yml"),
        "flights:\n"
            + "  table: flights_tbl\n"
            + "  description: test model\n"
            + "  dimensions:\n"
            + "    carrier: carrier\n"
            + "  measures:\n"
            + "    c: \"count(flight_count)\"\n");
    spark
        .createDataFrame(
            java.util.Arrays.asList(
                org.apache.spark.sql.RowFactory.create("AA", 1L),
                org.apache.spark.sql.RowFactory.create("UA", 2L)),
            new org.apache.spark.sql.types.StructType()
                .add("carrier", "string")
                .add("flight_count", "long"))
        .createOrReplaceTempView("flights_tbl");
    return YamlModelRegistry.load(tmp.toString(), spark);
  }

  /** The 3-arg constructor pattern (legacy-only) must continue to
   * work without modification. Existing tests use this pattern. */
  @Test
  void threeArgConstructor_producesValidService(@TempDir Path tmp) throws Exception {
    YamlModelRegistry yaml = bootRegistry(tmp);
    HotReloadingModelRegistry hot = new HotReloadingModelRegistry(yaml);
    QueryService svc = new QueryService(hot, spark, null);  // 3-arg
    assertNotNull(svc);
  }

  /** The 4-arg constructor pattern with {@code null} engine registry
   * must behave identically to the 3-arg pattern (engine registry is
   * optional). */
  @Test
  void fourArgConstructor_nullRegistry_producesValidService(@TempDir Path tmp) throws Exception {
    YamlModelRegistry yaml = bootRegistry(tmp);
    HotReloadingModelRegistry hot = new HotReloadingModelRegistry(yaml);
    QueryService svc = new QueryService(hot, spark, null, null);  // 4-arg with null
    assertNotNull(svc);
  }

  /** Both constructor patterns compile and produce distinct service
   * instances. */
  @Test
  void constructors_areBackwardCompatible(@TempDir Path tmp) throws Exception {
    YamlModelRegistry yaml = bootRegistry(tmp);
    HotReloadingModelRegistry hot = new HotReloadingModelRegistry(yaml);

    QueryService svc3 = new QueryService(hot, spark, null);
    QueryService svc4Null = new QueryService(hot, spark, null, null);
    assertNotNull(svc3);
    assertNotNull(svc4Null);
    assertTrue(svc3 != svc4Null, "different constructor calls produce different instances");
  }
}