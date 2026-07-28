package io.semanticdf.platform;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.semanticdf.cache.InMemoryResultCache;
import io.semanticdf.cache.ResultCache;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link PlatformApplication#buildResultCacheFromEnv}.
 *
 * <p>Uses the test-seam overload that takes an env-lookup function so
 * tests don't need to mutate {@link System#getenv} (which is not
 * reliably writable in JDK 17+).
 */
class PlatformApplicationCacheEnvVarTest {

  @Test
  void emptyEnv_returnsNoOp() {
    ResultCache c = PlatformApplication.buildResultCacheFromEnv(name -> null);
    assertSame(ResultCache.NoOp(), c, "empty env must return NoOp");
  }

  @Test
  void noopEnv_returnsNoOp() {
    Map<String, String> env = Map.of("SEMANTICDF_RESULT_CACHE", "noop");
    ResultCache c = PlatformApplication.buildResultCacheFromEnv(env::get);
    assertSame(ResultCache.NoOp(), c, "noop env must return NoOp");
  }

  @Test
  void memoryEnv_returnsInMemoryResultCache() {
    Map<String, String> env = Map.of("SEMANTICDF_RESULT_CACHE", "memory");
    ResultCache c = PlatformApplication.buildResultCacheFromEnv(env::get);
    assertTrue(c instanceof InMemoryResultCache,
        "memory env must return InMemoryResultCache, got: " + c.getClass().getName());
  }

  @Test
  void memoryEnv_withEntries_returnsInMemoryResultCache() {
    Map<String, String> env = new HashMap<>();
    env.put("SEMANTICDF_RESULT_CACHE", "memory");
    env.put("SEMANTICDF_RESULT_CACHE_ENTRIES", "32");
    ResultCache c = PlatformApplication.buildResultCacheFromEnv(env::get);
    assertTrue(c instanceof InMemoryResultCache, "must be an in-memory cache");
  }

  @Test
  void invalidKind_fallsBackToNoOp() {
    Map<String, String> env = Map.of("SEMANTICDF_RESULT_CACHE", "redis");
    ResultCache c = PlatformApplication.buildResultCacheFromEnv(env::get);
    assertSame(ResultCache.NoOp(), c, "unknown kind must fall back to NoOp");
  }

  @Test
  void invalidEntries_fallsBackToDefault() {
    Map<String, String> env = new HashMap<>();
    env.put("SEMANTICDF_RESULT_CACHE", "memory");
    env.put("SEMANTICDF_RESULT_CACHE_ENTRIES", "not-a-number");
    ResultCache c = PlatformApplication.buildResultCacheFromEnv(env::get);
    assertTrue(c instanceof InMemoryResultCache, "must still be an in-memory cache");
  }

  @Test
  void zeroEntries_fallsBackToDefault() {
    Map<String, String> env = new HashMap<>();
    env.put("SEMANTICDF_RESULT_CACHE", "memory");
    env.put("SEMANTICDF_RESULT_CACHE_ENTRIES", "0");
    ResultCache c = PlatformApplication.buildResultCacheFromEnv(env::get);
    assertTrue(c instanceof InMemoryResultCache, "must still be an in-memory cache");
  }

  @Test
  void negativeEntries_fallsBackToDefault() {
    Map<String, String> env = new HashMap<>();
    env.put("SEMANTICDF_RESULT_CACHE", "memory");
    env.put("SEMANTICDF_RESULT_CACHE_ENTRIES", "-1");
    ResultCache c = PlatformApplication.buildResultCacheFromEnv(env::get);
    assertTrue(c instanceof InMemoryResultCache, "must still be an in-memory cache");
  }
}
