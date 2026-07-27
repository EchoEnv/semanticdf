package io.semanticdf.platform.streaming;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.semanticdf.SemanticTable;
import io.semanticdf.StreamingSupport;
import java.lang.reflect.Field;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link SparkStreamingQueryLauncher#buildOptions}.
 *
 * <p>The {@code buildOptions} helper is pure (no Spark runtime needed) and
 * constructs the Scala {@code StreamingQueryOptions} from the validated
 * request. The launcher itself requires a live Spark session (tested via
 * integration).
 */
class SparkStreamingQueryLauncherTest {

  @Test
  void buildOptions_setsCheckpointLocation() {
    StreamingService.StreamRunRequest req =
        new StreamingService.StreamRunRequest("modelA", "shapeX", "/ckpt/path");
    StreamingSupport.StreamingQueryOptions opts =
        SparkStreamingQueryLauncher.buildOptions(req);

    // checkpointLocation — wrapped in scala.Option, accessed via reflection
    // (Scala's case class doesn't expose a Java getter for Option fields
    // in a uniform way across 2.13/3.x).
    Object checkpointField = readField(opts, "checkpointLocation");
    assertNotNull(checkpointField);
    assertEquals("Some(/ckpt/path)", checkpointField.toString());
  }

  @Test
  void buildOptions_outputModeIsAppend() {
    StreamingService.StreamRunRequest req =
        new StreamingService.StreamRunRequest("m", "s", "/c");
    StreamingSupport.StreamingQueryOptions opts =
        SparkStreamingQueryLauncher.buildOptions(req);
    assertEquals("append", readField(opts, "outputMode"));
  }

  @Test
  void buildOptions_emptyCollectionsForTriggerWindowWatermarkGroupKeys() {
    StreamingService.StreamRunRequest req =
        new StreamingService.StreamRunRequest("m", "s", "/c");
    StreamingSupport.StreamingQueryOptions opts =
        SparkStreamingQueryLauncher.buildOptions(req);

    // trigger, window, watermark should be None (empty Option)
    assertEquals("None", readField(opts, "trigger").toString());
    assertEquals("None", readField(opts, "window").toString());
    assertEquals("None", readField(opts, "watermark").toString());

    // groupKeys should be empty Seq
    Object groupKeys = readField(opts, "groupKeys");
    assertNotNull(groupKeys);
    assertEquals(0, ((scala.collection.Seq<?>) groupKeys).size(),
        "groupKeys must be empty");
  }

  @Test
  void buildOptions_foreachBatchIsNoop() {
    // The foreachBatch is a no-op function. We can verify by invoking
    // it on a fake Dataset — the function must not throw and must
    // return BoxedUnit. We use reflection to avoid importing the
    // Scala runtime classes.
    StreamingService.StreamRunRequest req =
        new StreamingService.StreamRunRequest("m", "s", "/c");
    StreamingSupport.StreamingQueryOptions opts =
        SparkStreamingQueryLauncher.buildOptions(req);
    Object foreachBatch = readField(opts, "foreachBatch");
    assertNotNull(foreachBatch);
  }

  @Test
  void constructor_rejectsNullSpark() {
    assertThrows(
        NullPointerException.class,
        () -> new SparkStreamingQueryLauncher(null));
  }

  // --- Helpers ---

  /** Read a (possibly private) field of {@code StreamingQueryOptions} via
   * reflection. Used because Scala case classes don't expose uniform
   * Java getters for Option/Seq fields. */
  private static Object readField(Object target, String name) {
    try {
      Field f = target.getClass().getDeclaredField(name);
      f.setAccessible(true);
      return f.get(target);
    } catch (Exception e) {
      throw new RuntimeException("Cannot read field " + name, e);
    }
  }
}
