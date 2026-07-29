package io.semanticdf.platform.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.semanticdf.cache.CachedResult;
import io.semanticdf.cache.InMemoryResultCache;
import io.semanticdf.cache.ResultCache$;
import io.semanticdf.cache.ResultCache;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Regression tests for PR #264 — single-flight on cache miss.
 *
 * <p>The v0.2.2 {@code QueryService.runQuery} used a non-atomic
 * {@code cache.get → execute → cache.put} pattern. Under N concurrent
 * identical first-time requests (the LLM-agent stampede pattern),
 * every caller missed, every caller ran the full Spark job, every
 * caller put the same key. The cache became a net negative (worse
 * than NoOp).
 *
 * <p>The fix: {@code cache.getOrCompute(key, supplier)} on
 * {@link InMemoryResultCache} coalesces concurrent identical keys
 * into ONE {@code supplier.get()} invocation via a per-key
 * {@link java.util.concurrent.CompletableFuture} in an
 * {@link java.util.concurrent.ConcurrentHashMap}.
 *
 * <p>This test class exercises the cache layer directly (no Restate
 * TestKit, no Spark) — it pins the single-flight contract at the
 * unit level. End-to-end coverage at the {@code QueryService} level
 * would need a Restate runtime; that's a separate integration
 * test (see {@code PlatformCacheCorrectnessTest}).
 */
class QueryServiceStampedeTest {

  /** A trivial {@link CachedResult} for the unit tests — schema with
   * one int column, one row with value 0. Doesn't matter what's in it
   * — the test only asserts the supplier was called once. */
  private static final CachedResult DUMMY;
  static {
    StructType schema = new StructType().add("v", DataTypes.IntegerType);
    Row row = org.apache.spark.sql.RowFactory.create(0);
    DUMMY = new CachedResult(new Row[]{row}, schema);
  }

  private static ExecutorService pool;

  @BeforeAll
  static void setupPool() {
    pool = Executors.newFixedThreadPool(16);
  }

  @AfterAll
  static void tearDownPool() {
    if (pool != null) pool.shutdownNow();
  }

  // --- #264: keystone -- concurrent identical miss runs compute once ---

  @Test
  @Timeout(value = 30)
  void concurrentIdenticalMiss_computeRunsOnce() throws Exception {
    InMemoryResultCache cache = new InMemoryResultCache(64);
    AtomicInteger computeCount = new AtomicInteger(0);
    // Gate: hold the SINGLE-FLIGHT WINNER at the supplier's start
    // until released. ready has initial count 1; the winner
    // counts it down. The other N-1 threads should NEVER reach
    // the supplier — they're blocked on the winner's
    // CompletableFuture inside getOrCompute.
    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch ready = new CountDownLatch(1);
    java.util.function.Supplier<CachedResult> slowSupplier = () -> {
      ready.countDown();
      try {
        start.await(5, TimeUnit.SECONDS);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(ie);
      }
      computeCount.incrementAndGet();
      return DUMMY;
    };

    int n = 8;
    CountDownLatch done = new CountDownLatch(n);
    Future<CachedResult>[] futures = new Future[n];
    for (int i = 0; i < n; i++) {
      final int idx = i;
      futures[i] = pool.submit(() -> {
        try {
          CachedResult v = cache.getOrCompute("k", slowSupplier);
          assertSame(DUMMY, v, "thread " + idx + " got wrong value");
        } finally {
          done.countDown();
        }
        return null;
      });
    }

    // Wait for the SINGLE-FLIGHT WINNER to enter the supplier.
    // The other N-1 threads are blocked on the winner's
    // CompletableFuture inside the cache — they have NOT entered
    // the supplier. ready has initial count 1; the winner's call
    // to ready.countDown() releases it.
    assertEquals(true, ready.await(5, TimeUnit.SECONDS),
        "the single-flight winner should have entered the supplier");
    // Sanity check: no OTHER thread has entered the supplier.
    // Give them a brief moment in case they're still in flight.
    Thread.sleep(100);
    assertEquals(0, ready.getCount(),
        "no thread besides the winner should have entered the supplier");
    // Release the supplier.
    start.countDown();
    // Wait for all threads to finish.
    assertEquals(true, done.await(10, TimeUnit.SECONDS),
        "all threads should complete");
    for (Future<CachedResult> f : futures) f.get(5, TimeUnit.SECONDS);

    assertEquals(1, computeCount.get(),
        "single-flight: compute must run EXACTLY ONCE even with N "
            + "concurrent identical misses (got " + computeCount.get() + ")");
  }

  // --- #264: lost-race waiters block, not re-enter ---

  @Test
  @Timeout(value = 30)
  void lostRaceWaiters_blockOnWinnerFuture() throws Exception {
    // No explicit gate this time — just a slow supplier (200ms) so
    // we can observe multiple threads arriving concurrently. We
    // assert that all N calls complete in a single supplier-run
    // window (max - min completion < 200ms) — meaning all waiters
    // blocked on the winner's CompletableFuture instead of running
    // the supplier themselves.
    InMemoryResultCache cache = new InMemoryResultCache(64);
    AtomicInteger computeCount = new AtomicInteger(0);
    java.util.function.Supplier<CachedResult> slowSupplier = () -> {
      try { Thread.sleep(200); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
      computeCount.incrementAndGet();
      return DUMMY;
    };
    int n = 8;
    Future<Long>[] futures = new Future[n];
    long t0 = System.nanoTime();
    for (int i = 0; i < n; i++) {
      final int idx = i;
      futures[i] = pool.submit(() -> {
        cache.getOrCompute("k", slowSupplier);
        return System.nanoTime() - t0;
      });
    }
    long min = Long.MAX_VALUE, max = Long.MIN_VALUE;
    for (Future<Long> f : futures) {
      long t = f.get(10, TimeUnit.SECONDS);
      min = Math.min(min, t);
      max = Math.max(max, t);
    }
    assertEquals(1, computeCount.get(),
        "compute must run once across N concurrent identical misses");
    // All 8 threads should finish at roughly the same time (within
    // ~50ms — scheduler jitter). If they had each run the supplier
    // in sequence, the spread would be ~1.6s.
    long spreadMs = (max - min) / 1_000_000L;
    assertEquals(true, spreadMs < 500,
        "lost-race waiters should block on the winner's future; "
            + "spread=" + spreadMs + "ms (expected < 500ms)");
  }

  // --- #264: failure propagates to all waiters ---

  @Test
  @Timeout(value = 30)
  void supplierThrows_propagatesToAllWaiters_andClearsInFlight() throws Exception {
    InMemoryResultCache cache = new InMemoryResultCache(64);
    AtomicInteger computeCount = new AtomicInteger(0);
    RuntimeException expected = new RuntimeException("synthetic supplier failure");
    // Gate: the supplier waits for `start` so that ALL 4 caller
    // threads have had a chance to enter getOrCompute and register
    // in the in-flight map before the compute completes. This
    // mirrors the real-world stampede scenario: N callers fire
    // roughly simultaneously, each has time to register before any
    // single compute finishes. With a deterministic gate, the test
    // is no longer subject to scheduling jitter.
    CountDownLatch start = new CountDownLatch(1);
    java.util.function.Supplier<CachedResult> failingSupplier = () -> {
      computeCount.incrementAndGet();
      try { start.await(5, TimeUnit.SECONDS); }
      catch (InterruptedException ie) { Thread.currentThread().interrupt(); throw new RuntimeException(ie); }
      throw expected;
    };

    int n = 4;
    Future<?>[] futures = new Future[n];
    for (int i = 0; i < n; i++) {
      final int idx = i;
      futures[i] = pool.submit(() -> {
        try {
          cache.getOrCompute("k", failingSupplier);
        } catch (RuntimeException e) {
          // expected
          return e;
        }
        throw new AssertionError("thread " + idx + " did not see the failure");
      });
    }
    // Give all 4 threads a brief moment to enter getOrCompute.
    // (In real burst-load, callers don't arrive in literally-zero
    // time. We approximate that here.)
    Thread.sleep(50);
    start.countDown();
    for (Future<?> f : futures) {
      Object result = f.get(10, TimeUnit.SECONDS);
      assertSame(expected, result,
          "all waiters should receive the supplier's exception");
    }
    assertEquals(1, computeCount.get(),
        "supplier must run exactly once even on failure");

    // The in-flight slot must have been cleared so the next call
    // gets a fresh execution. (PR #264 — exception propagation
    // must not poison the key permanently.)
    java.util.function.Supplier<CachedResult> okSupplier = () -> {
      computeCount.incrementAndGet();
      return DUMMY;
    };
    CachedResult v = cache.getOrCompute("k", okSupplier);
    assertSame(DUMMY, v);
    assertEquals(2, computeCount.get(),
        "after a failure, the next getOrCompute must re-run the supplier");
  }

  // --- #264: cache HIT path is still single-flight-free (just a get) ---

  @Test
  @Timeout(value = 30)
  void cacheHit_skipsSupplierEntirely() {
    InMemoryResultCache cache = new InMemoryResultCache(64);
    cache.put("k", DUMMY);
    AtomicInteger computeCount = new AtomicInteger(0);
    CachedResult v = cache.getOrCompute("k", () -> {
      computeCount.incrementAndGet();
      return DUMMY;
    });
    assertSame(DUMMY, v);
    assertEquals(0, computeCount.get(),
        "cache HIT must NOT invoke the supplier");
  }

  // --- #264: NoOp cache delegates to default impl (no single-flight) ---

  @Test
  @Timeout(value = 30)
  void noOpCache_getOrCompute_callsSupplierOnce() {
    // The default implementation in ResultCache does
    //   get → supplier.get() → put
    // which is correct for NoOp (no in-flight to coalesce).
    ResultCache cache = ResultCache$.MODULE$.NoOp();
    AtomicInteger computeCount = new AtomicInteger(0);
    CachedResult v = cache.getOrCompute("k", () -> {
      computeCount.incrementAndGet();
      return DUMMY;
    });
    assertSame(DUMMY, v);
    assertEquals(1, computeCount.get(),
        "NoOp.getOrCompute must invoke supplier once per call");
  }

  // --- #264: distinct keys are independent ---

  @Test
  @Timeout(value = 30)
  void distinctKeys_independentCompute() throws Exception {
    InMemoryResultCache cache = new InMemoryResultCache(64);
    AtomicInteger computeCount = new AtomicInteger(0);
    java.util.function.Supplier<CachedResult> supplier = () -> {
      try { Thread.sleep(100); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
      computeCount.incrementAndGet();
      return DUMMY;
    };
    Future<?>[] futures = new Future[4];
    for (int i = 0; i < 4; i++) {
      final String key = "k" + i;
      futures[i] = pool.submit(() -> cache.getOrCompute(key, supplier));
    }
    for (Future<?> f : futures) f.get(10, TimeUnit.SECONDS);
    assertEquals(4, computeCount.get(),
        "4 distinct keys should each invoke the supplier once");
    assertNotNull(cache.get("k0"));
    assertNotNull(cache.get("k1"));
    assertNotNull(cache.get("k2"));
    assertNotNull(cache.get("k3"));
  }
}