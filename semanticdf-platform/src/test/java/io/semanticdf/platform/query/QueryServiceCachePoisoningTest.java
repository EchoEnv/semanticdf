package io.semanticdf.platform.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.semanticdf.cache.CacheBridge;

import org.junit.jupiter.api.Test;

/**
 * Regression test for the post-PR-C cache-poisoning bug (DE reviewer's
 * finding C1): two {@code runQuery} calls with different {@code where}
 * filters must produce DIFFERENT cache keys, otherwise the second
 * caller's request returns the first caller's rows.
 *
 * <p>The platform's wire DTO carries a raw SQL {@code where} string,
 * not a library {@code Predicate} AST. The library's
 * {@code CacheKey.forRequest} hashes the {@code Predicate}-bearing
 * {@code audit.QueryRequest} \u2014 which the platform's
 * {@code CacheBridge.buildQueryRequest} leaves empty (since predicates
 * aren't yet built from SQL). Two different SQL filters would therefore
 * share the same library cache key, returning the wrong rows.
 *
 * <p>This test verifies the FIX: the platform uses
 * {@link CacheBridge#platformCacheKey} which hashes the raw wire
 * fields directly (including the SQL {@code where}). Two different
 * {@code where} strings produce two different keys.
 *
 * <p>Side-note: this is a pure-unit test of the cache-key helper,
 * not an end-to-end Restate/Spark test. The {@code runQuery}
 * integration test (which exercises the full cache-hit vs. miss
 * path with a real Spark session) lives in a gated
 * {@code QueryServiceIntegrationTest} class for environments where
 * Docker is available.
 */
class QueryServiceCachePoisoningTest {

  @Test
  void platformCacheKey_differentWhereProducesDifferentKeys() {
    String key_no_where = CacheBridge.platformCacheKey(
        "flights", 1,
        java.util.List.of("flight_count"),
        java.util.List.of("carrier"),
        "");
    String key_where_5 = CacheBridge.platformCacheKey(
        "flights", 1,
        java.util.List.of("flight_count"),
        java.util.List.of("carrier"),
        "distance > 500");
    String key_where_10 = CacheBridge.platformCacheKey(
        "flights", 1,
        java.util.List.of("flight_count"),
        java.util.List.of("carrier"),
        "distance > 1000");

    // The bug shipped in PR-C #244: all three would be equal because
    // the `where` was dropped on the cache key. After the fix, they
    // are distinct.
    assertTrue(!key_no_where.equals(key_where_5),
        "no-where and distance>500 must produce different cache keys");
    assertTrue(!key_no_where.equals(key_where_10),
        "no-where and distance>1000 must produce different cache keys");
    assertTrue(!key_where_5.equals(key_where_10),
        "distance>500 and distance>1000 must produce different cache keys");
  }

  @Test
  void platformCacheKey_differentVersionsProduceDifferentKeys() {
    String key_v1 = CacheBridge.platformCacheKey(
        "flights", 1, java.util.List.of(), java.util.List.of(), "");
    String key_v2 = CacheBridge.platformCacheKey(
        "flights", 2, java.util.List.of(), java.util.List.of(), "");
    assertTrue(!key_v1.equals(key_v2),
        "version 1 and version 2 must produce different cache keys (auto-invalidation)");
  }

  @Test
  void platformCacheKey_sameInputsProducesSameKey() {
    String key1 = CacheBridge.platformCacheKey(
        "flights", 1,
        java.util.List.of("flight_count", "total_distance"),
        java.util.List.of("carrier", "origin"),
        "year = 2024");
    String key2 = CacheBridge.platformCacheKey(
        "flights", 1,
        java.util.List.of("flight_count", "total_distance"),
        java.util.List.of("carrier", "origin"),
        "year = 2024");
    assertEquals(key1, key2,
        "identical inputs must produce identical cache keys (cache hit)");
  }

  @Test
  void platformCacheKey_nullWhereHandledGracefully() {
    // A null `where` should serialize like an empty string \u2014 not NPE.
    String key_null = CacheBridge.platformCacheKey(
        "flights", 1, java.util.List.of(), java.util.List.of(), null);
    String key_empty = CacheBridge.platformCacheKey(
        "flights", 1, java.util.List.of(), java.util.List.of(), "");
    assertEquals(key_null, key_empty,
        "null where must serialize identically to empty-string where");
  }

  @Test
  void platformCacheKey_looksLikeSha256Hex() {
    String key = CacheBridge.platformCacheKey(
        "flights", 1, java.util.List.of("c"), java.util.List.of(), "x>5");
    assertNotNull(key);
    // The canonical encoder returns a SHA-256 hex digest (64 chars).
    assertEquals(64, key.length(), "platform cache key must be SHA-256 hex (64 chars)");
    assertTrue(key.matches("[0-9a-f]{64}"),
        "platform cache key must be lowercase hex: " + key);
  }
}
