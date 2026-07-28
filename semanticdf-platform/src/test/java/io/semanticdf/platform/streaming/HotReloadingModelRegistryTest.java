package io.semanticdf.platform.streaming;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.semanticdf.SemanticTable;
import io.semanticdf.adapters.YamlLoader;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Tests for {@link HotReloadingModelRegistry}.
 *
 * <p>These tests use a tiny SparkSession + an in-memory YAML fixture
 * (mirroring the pattern in {@code YamlModelRegistryTest}'s integration
 * cousin {@code QueryServiceEndToEndTest}). Each test stops the spark
 * session once via {@code @AfterAll}.
 */
class HotReloadingModelRegistryTest {

  private static SparkSession spark;

  @BeforeAll
  static void startSpark() {
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("hot-reload-registry-test")
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

  /**
   * Build a tiny YAML registry with a single "flights" model.
   * Reused by tests that need a delegate to wrap.
   */
  private static YamlModelRegistry bootRegistry(Path tmp) throws Exception {
    Files.writeString(
        tmp.resolve("flights.yml"),
        "flights:\n"
            + "  table: flights_tbl\n"
            + "  description: Boot-time fixture\n"
            + "  dimensions:\n"
            + "    carrier: carrier\n"
            + "  measures:\n"
            + "    c: \"count(flight_count)\"\n");
    spark
        .createDataFrame(
            java.util.Arrays.asList(
                org.apache.spark.sql.RowFactory.create("AA", 1L)),
            new org.apache.spark.sql.types.StructType()
                .add("carrier", "string")
                .add("flight_count", "long"))
        .createOrReplaceTempView("flights_tbl");
    return YamlModelRegistry.load(tmp.toString(), spark);
  }

  /**
   * Build a tiny second YAML model on the same temp dir so we can
   * produce a real {@link SemanticTable} for overlay-register tests.
   * SemanticTable has no no-arg constructor — we must go through YamlLoader.
   *
   * <p>Note: {@code @TempDir} does NOT work on static helper parameters
   * — only on instance method parameters or fields. So the helper
   * accepts a plain {@code Path} and the test's {@code @TempDir Path tmp}
   * is passed in.
   */
  private static SemanticTable tableFromYaml(Path tmp, String modelName, String tableRef) throws Exception {
    // Each model gets its own sub-dir so its YAML file doesn't conflict
    // with the boot-time registry's flights.yml (or with sibling
    // tableFromYaml calls).
    Path sub = Files.createTempDirectory(tmp, "overlay-");
    Files.writeString(
        sub.resolve(modelName + ".yml"),
        modelName + ":\n"
            + "  table: " + tableRef + "\n"
            + "  description: overlay fixture for " + modelName + "\n"
            + "  dimensions:\n"
            + "    carrier: carrier\n"
            + "  measures:\n"
            + "    c: \"count(flight_count)\"\n");
    spark
        .createDataFrame(
            java.util.Arrays.asList(
                org.apache.spark.sql.RowFactory.create("AA", 1L)),
            new org.apache.spark.sql.types.StructType()
                .add("carrier", "string")
                .add("flight_count", "long"))
        .createOrReplaceTempView(tableRef);
    scala.collection.immutable.Map<String, SemanticTable> built =
        YamlLoader.loadDir(sub.toString(), spark);
    Map<String, SemanticTable> javaMap = scala.collection.JavaConverters.mapAsJavaMap(built);
    return javaMap.get(modelName);
  }

  @Test
  void register_thenGetReturnsRegisteredTable(@TempDir Path tmp) throws Exception {
    YamlModelRegistry delegate = bootRegistry(tmp);
    HotReloadingModelRegistry reg = new HotReloadingModelRegistry(delegate);

    SemanticTable newModel = tableFromYaml(tmp, "orders", "orders_tbl");
    reg.register("orders", newModel);

    assertSame(newModel, reg.get("orders"),
        "get(name) must return the registered table instance");
  }

  @Test
  void register_doesNotMutateDelegate(@TempDir Path tmp) throws Exception {
    YamlModelRegistry delegate = bootRegistry(tmp);
    HotReloadingModelRegistry reg = new HotReloadingModelRegistry(delegate);

    SemanticTable newModel = tableFromYaml(tmp, "orders", "orders_tbl");
    reg.register("orders", newModel);

    // The delegate's own get() must still throw ModelNotFoundException
    // for "orders" — register() only touches the overlay.
    assertThrows(
        ModelRegistry.ModelNotFoundException.class,
        () -> delegate.get("orders"),
        "delegate must remain untouched — overlay-only mutation");
    // The delegate's own registeredModels() must not include "orders".
    assertTrue(!delegate.registeredModels().contains("orders"),
        "delegate.registeredModels() must not include overlay-only models");
  }

  @Test
  void get_fallsThroughToDelegateWhenNotInOverlay(@TempDir Path tmp) throws Exception {
    YamlModelRegistry delegate = bootRegistry(tmp);
    HotReloadingModelRegistry reg = new HotReloadingModelRegistry(delegate);

    // "flights" is in the boot-time delegate, NOT in the overlay.
    SemanticTable fromReg = reg.get("flights");
    assertNotNull(fromReg, "delegate model must be reachable via the decorator");
    // Identity check — same instance the delegate returns.
    assertSame(delegate.get("flights"), fromReg);
  }

  @Test
  void get_unknownModelStillThrows(@TempDir Path tmp) throws Exception {
    YamlModelRegistry delegate = bootRegistry(tmp);
    HotReloadingModelRegistry reg = new HotReloadingModelRegistry(delegate);

    assertThrows(
        ModelRegistry.ModelNotFoundException.class,
        () -> reg.get("nonexistent"),
        "model not in delegate AND not in overlay must throw");
  }

  @Test
  void register_concurrentReadsAreThreadSafe(@TempDir Path tmp) throws Exception {
    YamlModelRegistry delegate = bootRegistry(tmp);
    HotReloadingModelRegistry reg = new HotReloadingModelRegistry(delegate);

    // Pre-populate the overlay so readers have something to find.
    SemanticTable[] pre = new SemanticTable[50];
    for (int i = 0; i < 50; i++) {
      pre[i] = tableFromYaml(tmp, "model-" + i, "model_tbl_" + i);
      reg.register("model-" + i, pre[i]);
    }

    int readers = 32;
    int writes = 8;
    int iterations = 200;
    ExecutorService pool = Executors.newFixedThreadPool(readers + writes);
    CountDownLatch start = new CountDownLatch(1);
    AtomicInteger readErrors = new AtomicInteger();
    AtomicInteger writeErrors = new AtomicInteger();
    AtomicInteger readHits = new AtomicInteger();

    try {
      for (int r = 0; r < readers; r++) {
        pool.submit(
            () -> {
              try {
                start.await();
                for (int i = 0; i < iterations; i++) {
                  // Half the reads go to delegate models, half to overlay.
                  String name = (i % 2 == 0) ? "flights" : ("model-" + (i % 50));
                  SemanticTable t = reg.get(name);
                  if (t != null) readHits.incrementAndGet();
                }
              } catch (Exception e) {
                readErrors.incrementAndGet();
              }
            });
      }
      for (int w = 0; w < writes; w++) {
        final int wId = w;
        pool.submit(
            () -> {
              try {
                start.await();
                for (int i = 0; i < iterations; i++) {
                  reg.register("model-" + ((wId * 7 + i) % 50), pre[(wId * 7 + i) % 50]);
                }
              } catch (Exception e) {
                writeErrors.incrementAndGet();
              }
            });
      }

      start.countDown();
      pool.shutdown();
      assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS), "pool must finish in 30s");

      assertEquals(0, readErrors.get(), "no reader should see an exception");
      assertEquals(0, writeErrors.get(), "no writer should see an exception");
      assertTrue(readHits.get() > 0, "readers must hit successfully");
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  void register_rejectsNullArguments(@TempDir Path tmp) throws Exception {
    YamlModelRegistry delegate = bootRegistry(tmp);
    HotReloadingModelRegistry reg = new HotReloadingModelRegistry(delegate);

    // Use a real SemanticTable for the non-null side of the test.
    SemanticTable real = tableFromYaml(tmp, "orders", "orders_tbl");

    assertThrows(IllegalArgumentException.class, () -> reg.register(null, real));
    assertThrows(IllegalArgumentException.class, () -> reg.register("name", null));
  }

  @Test
  void constructor_rejectsNullDelegate() {
    assertThrows(IllegalArgumentException.class, () -> new HotReloadingModelRegistry(null));
  }

  @Test
  void registeredModels_isUnionOfOverlayAndDelegate(@TempDir Path tmp) throws Exception {
    YamlModelRegistry delegate = bootRegistry(tmp);
    HotReloadingModelRegistry reg = new HotReloadingModelRegistry(delegate);

    reg.register("orders", tableFromYaml(tmp, "orders", "orders_tbl"));
    reg.register("customers", tableFromYaml(tmp, "customers", "customers_tbl"));

    Set<String> names = reg.registeredModels();
    assertTrue(names.contains("flights"), "delegate models must appear");
    assertTrue(names.contains("orders"), "overlay models must appear");
    assertTrue(names.contains("customers"), "overlay models must appear");
    assertEquals(3, names.size(), "no duplicates between layers");
  }
}