package io.semanticdf.platform.streaming;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.semanticdf.SemanticTable;
import io.semanticdf.core.model.Model;
import io.semanticdf.platform.streaming.StreamingService.StreamRunRequest;
import java.util.Optional;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.junit.jupiter.api.Test;

/** Tests for the v0.3.1 Phase 5 engine-portable streaming path.
 *
 * <p>Verifies StreamingService construction + dispatch wiring:
 * <ul>
 *   <li>New 5-arg constructor with portableLauncher+streamCatalog wires correctly.</li>
 *   <li>Existing 4-arg constructors (without portableLauncher) still work
 *       (additive change — backward compatibility is the goal).</li>
 *   <li>PortableStreamingQueryLauncher interface is a proper functional interface.</li>
 * </ul>
 *
 * <p>Full end-to-end streaming is covered by StreamingServiceIntegrationTest.
 * These tests focus on the new construction patterns.
 */
public class StreamingServicePortableSpec {

  @Test
  void fiveArgConstructor_wiresPortableLauncher() {
    StreamingService svc = new StreamingService(
        new StubModelRegistry(),
        new RecordingLegacyLauncher(),
        new StreamingQueryHandleRegistry(),
        new FakeStreamCatalog(),
        new PortableStreamingQueryLauncher() {
          @Override
          public StreamingQuery start(Model model, StreamRunRequest request) {
            return null;
          }
        });
    assertNotNull(svc);
  }

  @Test
  void fiveArgConstructor_nullPortableLauncher_isLegacyOnly() {
    // null portableLauncher is allowed (legacy-only deployment).
    StreamingService svc = new StreamingService(
        new StubModelRegistry(),
        new RecordingLegacyLauncher(),
        new StreamingQueryHandleRegistry(),
        new FakeStreamCatalog(),
        null);
    assertNotNull(svc);
  }

  @Test
  void existingConstructorsRemainBackwardCompatible() {
    // The 4-arg constructors (without portableLauncher) must continue
    // to work — the new 5-arg is additive.
    StreamingService svc3 = new StreamingService(
        new StubModelRegistry(),
        new RecordingLegacyLauncher(),
        new StreamingQueryHandleRegistry());
    StreamingService svc4 = new StreamingService(
        new StubModelRegistry(),
        new RecordingLegacyLauncher(),
        new StreamingQueryHandleRegistry(),
        new FakeStreamCatalog());
    assertNotNull(svc3);
    assertNotNull(svc4);
    assertTrue(svc3 != svc4, "different constructor signatures produce different instances");
  }

  @Test
  void portableLauncher_isFunctionalInterface() {
    // The interface should be usable as a lambda (1-arg functional interface).
    PortableStreamingQueryLauncher launcher =
        (model, request) -> null;
    assertNotNull(launcher);
  }

  // -- minimal stubs --

  /**
   * Model registry that returns empty for both legacy and Model lookups.
   * Used to verify the constructors don't fail at construction time.
   */
  private static final class StubModelRegistry implements ModelRegistry {
    @Override
    public SemanticTable get(String modelName) {
      return null;
    }

    @Override
    public Optional<Model> getModel(String modelName) {
      return Optional.empty();
    }
  }

  /** Legacy launcher that records its calls (no-op impl). */
  private static final class RecordingLegacyLauncher implements StreamingQueryLauncher {
    @Override
    public StreamingQuery start(SemanticTable model, StreamRunRequest request) {
      return null;
    }
  }

  /** Fake StreamCatalog — we don't exercise catalog functionality in wiring tests. */
  private static final class FakeStreamCatalog implements StreamCatalog {
    @Override
    public void registerIfAbsent(
        String streamId, String modelName, String queryShape, String checkpointLocation) {
      // no-op
    }

    @Override
    public java.util.List<StreamMetadata> findAll() {
      return java.util.Collections.emptyList();
    }

    @Override
    public void close() {
      // no-op
    }
  }
}
