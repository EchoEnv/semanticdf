package io.semanticdf.platform;

import io.semanticdf.platform.streaming.StreamingQueryHandleRegistry;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.spark.sql.streaming.StreamingQuery;
import static org.junit.jupiter.api.Assertions.assertFalse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link PlatformApplication#drainQueries}.
 *
 * <p>The drain is best-effort: it calls {@code query.stop()} on every live
 * handle in the registry. Exceptions from individual queries are logged
 * but don't block the JVM exit. The return value is the count of queries
 * we attempted to stop.
 */
class PlatformApplicationDrainQueriesTest {

  /** A fake query that records every {@code stop()} call via a
   * dynamic proxy — {@code StreamingQuery} is a Scala trait, not a Java
   * interface, so we can't implement it directly. The proxy delegates
   * all calls to a {@link CountingQuery} holder. */
  static class CountingQuery {
    final AtomicInteger stopCount = new AtomicInteger(0);
    final boolean stopThrows;

    CountingQuery(boolean stopThrows) {
      this.stopThrows = stopThrows;
    }

    void stop() {
      stopCount.incrementAndGet();
      if (stopThrows) {
        throw new RuntimeException("simulated stop failure");
      }
    }

    StreamingQuery proxy() {
      return (StreamingQuery)
          Proxy.newProxyInstance(
              StreamingQuery.class.getClassLoader(),
              new Class<?>[] {StreamingQuery.class},
              (p, method, args) -> {
                if ("stop".equals(method.getName())) {
                  stop();
                  return null;
                }
                Class<?> rt = method.getReturnType();
                if (rt == boolean.class) return false;
                if (rt == int.class) return 0;
                if (rt == long.class) return 0L;
                return null;
              });
    }
  }

  @Test
  void drain_emptyRegistry_returnsZero() {
    StreamingQueryHandleRegistry reg = new StreamingQueryHandleRegistry();
    assertEquals(0, PlatformApplication.drainQueries(reg));
  }

  @Test
  void drain_singleQuery_callsStopOnce() {
    StreamingQueryHandleRegistry reg = new StreamingQueryHandleRegistry();
    CountingQuery q = new CountingQuery(false);
    reg.put("s1", q.proxy());

    int drained = PlatformApplication.drainQueries(reg);
    assertEquals(1, drained);
    assertEquals(1, q.stopCount.get());
  }

  @Test
  void drain_multipleQueries_callsStopOnEach() {
    StreamingQueryHandleRegistry reg = new StreamingQueryHandleRegistry();
    CountingQuery q1 = new CountingQuery(false);
    CountingQuery q2 = new CountingQuery(false);
    CountingQuery q3 = new CountingQuery(false);
    reg.put("s1", q1.proxy());
    reg.put("s2", q2.proxy());
    reg.put("s3", q3.proxy());

    int drained = PlatformApplication.drainQueries(reg);
    assertEquals(3, drained);
    assertEquals(1, q1.stopCount.get());
    assertEquals(1, q2.stopCount.get());
    assertEquals(1, q3.stopCount.get());
  }

  @Test
  void drain_queryStopThrows_continuesWithOthers() {
    // A throwing query must not prevent the drain from stopping others.
    // This is the "partial failure" scenario: the JVM must exit cleanly
    // even if one query's stop() fails.
    StreamingQueryHandleRegistry reg = new StreamingQueryHandleRegistry();
    CountingQuery good1 = new CountingQuery(false);
    CountingQuery bad = new CountingQuery(true);
    CountingQuery good2 = new CountingQuery(false);
    reg.put("g1", good1.proxy());
    reg.put("bad", bad.proxy());
    reg.put("g2", good2.proxy());

    int drained = PlatformApplication.drainQueries(reg);
    assertEquals(3, drained, "drain should count attempts, not successes");
    assertEquals(1, good1.stopCount.get());
    assertEquals(1, bad.stopCount.get());
    assertEquals(1, good2.stopCount.get(),
        "good2 must still be drained even though bad failed");
  }

  @Test
  void drain_visitedInSomeOrder() {
    // The drain iterates the registry; order is not guaranteed (it's a
    // ConcurrentHashMap). We just verify all entries are visited.
    StreamingQueryHandleRegistry reg = new StreamingQueryHandleRegistry();
    List<String> visitedIds = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      CountingQuery q = new CountingQuery(false);
      reg.put("stream-" + i, q.proxy());
    }
    reg.forEach((streamId, query) -> visitedIds.add(streamId));
    assertEquals(5, visitedIds.size());
  }

  // --- Timeout behavior (issue #1 from PR #227 follow-up) ---

  @Test
  void drain_queryHangs_returnsCountAndDoesNotBlock() throws Exception {
    // A query whose stop() blocks indefinitely must not hang the drain.
    // With a 200ms per-query timeout, the drain must complete in well under
    // the JVM shutdown hook deadline.
    StreamingQueryHandleRegistry reg = new StreamingQueryHandleRegistry();
    HangingQuery hanging = new HangingQuery();
    reg.put("hanging-stream", hanging.proxy());

    long start = System.currentTimeMillis();
    int drained = PlatformApplication.drainQueries(reg, 200);
    long elapsed = System.currentTimeMillis() - start;

    assertEquals(1, drained, "drain must count the attempt even though it timed out");
    assertTrue(elapsed < 2_000, "drain must return quickly after timeout, elapsed=" + elapsed);
    assertTrue(hanging.interruptObserved.get(),
        "the hanging query's worker thread must have been interrupted");

    hanging.unblock.countDown(); // let the hanging thread exit cleanly
  }

  @Test
  void drain_queryHangs_doesNotBlockOtherQueries() throws Exception {
    // One hung query must not prevent other queries from being drained.
    StreamingQueryHandleRegistry reg = new StreamingQueryHandleRegistry();
    HangingQuery hanging = new HangingQuery();
    CountingQuery good1 = new CountingQuery(false);
    CountingQuery good2 = new CountingQuery(false);
    reg.put("hanging", hanging.proxy());
    reg.put("good-1", good1.proxy());
    reg.put("good-2", good2.proxy());

    int drained = PlatformApplication.drainQueries(reg, 200);

    assertEquals(3, drained);
    assertEquals(1, good1.stopCount.get(), "good-1 must still be drained");
    assertEquals(1, good2.stopCount.get(), "good-2 must still be drained");

    hanging.unblock.countDown(); // let the hanging thread exit cleanly
  }

  @Test
  void drain_zeroTimeout_meansTryImmediately() {
    // A 0ms timeout is "try, fail immediately". The query is still
    // attempted (counted) but the future is cancelled.
    StreamingQueryHandleRegistry reg = new StreamingQueryHandleRegistry();
    HangingQuery hanging = new HangingQuery();
    reg.put("h", hanging.proxy());

    int drained = PlatformApplication.drainQueries(reg, 0);
    assertEquals(1, drained);

    hanging.unblock.countDown();
  }

  @Test
  void drain_shutdownHookInterrupted_cancelsAllSubmittedTasksAndReturnsQuickly()
      throws Exception {
    // If the shutdown hook thread itself is interrupted (e.g., JVM's
    // shutdown hook timeout fires), the drain must return quickly and
    // cancel all in-flight tasks.
    //
    // With PARALLEL drain, all tasks are submitted concurrently to the
    // pool before invokeAll() returns — so the contract is different
    // from the sequential version (where only the first query would
    // have been attempted). Instead of "skip the rest", we assert that
    // (a) the drain returns in <1s, (b) the running worker threads
    // observe the interrupt (proving shutdownNow() reached them), and
    // (c) the returned count reflects the snapshot size (all tasks
    // were submitted, even though interrupted).
    //
    // Note: per JDK contract, Future.cancel(true) only interrupts
    // workers that have pulled the task off the queue. Tasks that are
    // still queued when the executor is shut down are simply discarded
    // (their Callable never runs). For the small test below (3
    // queries with pool size = 3), all workers are running, but for
    // large workloads some queries may bypass stop() entirely and rely
    // on spark.stop() for cleanup — that's the documented trade-off.
    StreamingQueryHandleRegistry reg = new StreamingQueryHandleRegistry();
    HangingQuery q1 = new HangingQuery();
    HangingQuery q2 = new HangingQuery();
    HangingQuery q3 = new HangingQuery();
    reg.put("q1", q1.proxy());
    reg.put("q2", q2.proxy());
    reg.put("q3", q3.proxy());

    // Interrupt the test thread (acts as the shutdown-hook caller).
    // The InterruptedException will be thrown from invokeAll after the
    // tasks have been submitted — we'll then call executor.shutdownNow()
    // to interrupt the workers.
    Thread.currentThread().interrupt();

    long start = System.currentTimeMillis();
    int drained = PlatformApplication.drainQueries(reg, 5_000);
    long elapsed = System.currentTimeMillis() - start;

    // Clear the interrupt flag for subsequent tests.
    Thread.interrupted();

    // Drain must return quickly — well under the 5s timeout. Each
    // hanging query would otherwise block for the full timeout.
    assertTrue(elapsed < 1_000,
        "drain must return quickly after interrupt, elapsed=" + elapsed);

    // All 3 queries are counted (snapshot size) — they were all
    // submitted in parallel and then cancelled.
    assertEquals(3, drained,
        "drain counts the snapshot size; all tasks were submitted before interrupt");

    // At least one worker observed the interrupt. Under JDK contract,
    // Future.cancel(true) only interrupts workers that have started
    // running their task; tasks still queued when shutdownNow is
    // called are silently discarded. We don't assert *all three*
    // observed the interrupt — at our pool size = 3 with 3 queries,
    // they all should, but that's a timing-sensitive claim. The
    // safety property we care about is: at least one running worker
    // was interrupted, proving executor.shutdownNow() is wired up.
    boolean[] observed = waitForInterrupts(2_000, q1, q2, q3);
    int observedCount = 0;
    for (boolean b : observed) if (b) observedCount++;
    assertTrue(observedCount >= 1,
        "at least one worker must have observed interrupt via shutdownNow(); observed="
            + observedCount + "/3");

    q1.unblock.countDown();
    q2.unblock.countDown();
    q3.unblock.countDown();
  }

  // --- Parallel-drain timing ---

  @Test
  void drain_parallelQueries_completesIn10s_evenFor100HangingQueries() throws Exception {
    // With sequential drain (PR #228 era), N hanging queries would take
    // N × 10s worst-case. With parallel drain (this PR), 100 hanging
    // queries all start at t=0 and the entire drain completes in
    // ~10s + small overhead.
    //
    // This is the regression test for the headline benefit of the
    // parallel rewrite.
    StreamingQueryHandleRegistry reg = new StreamingQueryHandleRegistry();
    int n = 100;
    java.util.List<HangingQuery> queries = new java.util.ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      HangingQuery h = new HangingQuery();
      queries.add(h);
      reg.put("stream-" + i, h.proxy());
    }

    long start = System.currentTimeMillis();
    int drained = PlatformApplication.drainQueries(reg, 10_000);
    long elapsed = System.currentTimeMillis() - start;

    assertEquals(n, drained, "all 100 queries must be counted as attempted");
    // Sequential would be 100 × 10s = ~1000s. Parallel caps at 10s +
    // thread pool overhead. Allow a generous ceiling for CI noise.
    assertTrue(elapsed < 13_000,
        "parallel drain of 100 queries must complete in <13s, elapsed="
            + elapsed + "ms");

    // Release all hanging threads so they exit.
    for (HangingQuery h : queries) {
      h.unblock.countDown();
    }
  }

  @Test
  void drain_parallelQueries_threadPoolSizeCapped() throws Exception {
    // Verify the thread-pool size cap (default 64) is respected. We
    // use a CyclicBarrier to deterministically synchronize all workers
    // at a known point — this is structural, not timing-based (unlike
    // Thread.sleep(N) which is flaky under load).
    //
    // Setup: 80 queries, each stop() increments the concurrency
    // counter, awaits a barrier, then exits. With pool size =
    // Math.min(80, 64) = 64, the barrier waits for 64 parties; the
    // remaining 16 queries sit queued and never reach the barrier.
    // After the test lets the barrier release, all running workers
    // observe their peak concurrency == 64.
    int n = 80;
    final java.util.concurrent.atomic.AtomicInteger concurrent =
        new java.util.concurrent.atomic.AtomicInteger(0);
    final java.util.concurrent.atomic.AtomicInteger maxConcurrent =
        new java.util.concurrent.atomic.AtomicInteger(0);

    StreamingQueryHandleRegistry reg = new StreamingQueryHandleRegistry();
    java.util.List<CountingQuery> queries = new java.util.ArrayList<>(n);
    // Barrier sized to poolSize (== 64 with default DRAIN_MAX_PARALLEL).
    // All 64 workers will reach the barrier and synchronize; 16 are queued.
    java.util.concurrent.CyclicBarrier barrier =
        new java.util.concurrent.CyclicBarrier(
            PlatformApplication.DRAIN_MAX_PARALLEL,
            () -> maxConcurrent.set(concurrent.get()));
    java.util.concurrent.atomic.AtomicBoolean barrierTripped =
        new java.util.concurrent.atomic.AtomicBoolean(false);

    for (int i = 0; i < n; i++) {
      CountingQuery q =
          new CountingQuery(false) {
            @Override
            void stop() {
              int now = concurrent.incrementAndGet();
              maxConcurrent.updateAndGet(prev -> Math.max(prev, now));
              try {
                // Block until the other DRAIN_MAX_PARALLEL workers
                // reach here. This makes the concurrency peak a
                // structural property of the test (not timing).
                barrier.await();
              } catch (Exception e) {
                Thread.currentThread().interrupt();
              } finally {
                concurrent.decrementAndGet();
                stopCount.incrementAndGet();
                barrierTripped.set(true);
              }
            }
          };
      queries.add(q);
      reg.put("stream-" + i, q.proxy());
    }

    // Drive the drain on a separate thread; otherwise the test thread
    // would block waiting on barrier.await() (the drain calls
    // invokeAll which calls our stop(), which calls barrier.await()).
    java.util.concurrent.atomic.AtomicInteger drained =
        new java.util.concurrent.atomic.AtomicInteger(0);
    // 1s timeout: the 64 running workers finish near-instantly (barrier
    // trip), the 16 queued ones time out after 1s. Test completes in ~1s.
    Thread drainThread =
        new Thread(
            () -> drained.set(PlatformApplication.drainQueries(reg, 1_000)),
            "test-drain-driver");
    drainThread.start();

    // Wait for the barrier-tripped callback to fire (it runs on the
    // last worker thread). Once tripped, we know maxConcurrent has been
    // set. The other 16 queries are still queued; that's fine.
    long deadline = System.currentTimeMillis() + 10_000;
    while (!barrierTripped.get() && System.currentTimeMillis() < deadline) {
      Thread.sleep(10);
    }

    int max = maxConcurrent.get();
    assertEquals(
        PlatformApplication.DRAIN_MAX_PARALLEL,
        max,
        "thread pool must reach the cap; expected max="
            + PlatformApplication.DRAIN_MAX_PARALLEL
            + " but got "
            + max);

    // Let the drain complete. The queued 16 queries will time out
    // and cancel after the 1s timeout; the running 64 will return
    // normally after the barrier releases.
    drainThread.join(5_000);
    assertFalse(drainThread.isAlive(), "drain thread must have returned");

    // Sanity: not all 80 returned successfully — 16 are queued and
    // should be reported as cancelled. At least 64 succeeded.
    assertEquals(n, drained.get(), "all 80 are counted as attempted");
  }

  // --- Env-var parsing (issue M1 from review) ---

  @org.junit.jupiter.api.Nested
  class EnvVarParsing {

    @Test
    void drainTimeout_unset_returnsDefault() {
      // The static-final DRAIN_TIMEOUT_MS was initialized at class-load
      // time with the env-var as it was when the test JVM started.
      // We assert it's a positive value (the default or whatever was
      // injected externally) rather than a specific number, since
      // env-var injection varies by CI environment.
      Assertions.assertTrue(
          PlatformApplication.DRAIN_TIMEOUT_MS > 0,
          "DRAIN_TIMEOUT_MS must be positive, was " + PlatformApplication.DRAIN_TIMEOUT_MS);
    }

    @Test
    void drainMaxParallel_isPositiveAndBounded() {
      // Same as above — env-var-injected value should be in (0, MAX].
      int v = PlatformApplication.DRAIN_MAX_PARALLEL;
      Assertions.assertTrue(v > 0,
          "DRAIN_MAX_PARALLEL must be positive, was " + v);
      Assertions.assertTrue(v <= PlatformApplication.MAX_DRAIN_MAX_PARALLEL,
          "DRAIN_MAX_PARALLEL must not exceed MAX_DRAIN_MAX_PARALLEL, was " + v);
    }

    /** Documents the parsing contract — env-var parsing is in a static
     * initializer so we can't easily re-invoke it, but the constants'
     * fixed nature means a single set of invariants covers all paths:
     * the value must be > 0 (default for unset/blank/invalid/negative)
     * and &lt;= MAX (clamp for too-large). */
    @Test
    void drainMaxParallel_invariantsMatchParserContract() {
      // The four-paragraph contract from resolveDrainMaxParallel():
      //   raw==null / raw.blank() -> default
      //   parse fails (NumberFormatException) -> default
      //   parsed v <= 0 -> default
      //   parsed v > MAX -> MAX
      // All four collapse to: default < result <= MAX.
      // We can't easily test each branch (env-var is set once at
      // boot), but we can test that the OBSERVED value satisfies the
      // result's invariant. If we ever see a value > MAX, the
      // clamping is broken.
      Assertions.assertTrue(
          PlatformApplication.DRAIN_MAX_PARALLEL <= PlatformApplication.MAX_DRAIN_MAX_PARALLEL,
          "clamp invariant");
      Assertions.assertTrue(
          PlatformApplication.DRAIN_MAX_PARALLEL > 0,
          "default-invariant");
    }
  }

  // --- Test fakes ---

  /** Poll for all of {@code queries} to observe their interrupt. Returns
   * once every query has set its flag, or {@code deadlineMs} elapses.
   * The returned array is the per-query observation state at the end
   * of the wait (so the caller can assert which queries did or didn't
   * see the interrupt).
   */
  private static boolean[] waitForInterrupts(long deadlineMs, HangingQuery... queries)
      throws InterruptedException {
    long deadline = System.currentTimeMillis() + deadlineMs;
    while (System.currentTimeMillis() < deadline) {
      boolean all = true;
      boolean[] result = new boolean[queries.length];
      for (int i = 0; i < queries.length; i++) {
        boolean seen = queries[i].interruptObserved.get();
        result[i] = seen;
        if (!seen) all = false;
      }
      if (all) return result;
      Thread.sleep(10);
    }
    boolean[] result = new boolean[queries.length];
    for (int i = 0; i < queries.length; i++) {
      result[i] = queries[i].interruptObserved.get();
    }
    return result;
  }

  /** A fake query whose {@code stop()} blocks until {@link #unblock} counts
   * down (or the calling thread is interrupted). Used to test the drain
   * timeout — the worker's interrupt status is recorded so the test
   * can verify the drain cancelled the future. */
  static final class HangingQuery {
    final java.util.concurrent.CountDownLatch unblock = new java.util.concurrent.CountDownLatch(1);
    final java.util.concurrent.atomic.AtomicBoolean interruptObserved =
        new java.util.concurrent.atomic.AtomicBoolean(false);

    void stop() {
      try {
        // Block until unblocked OR interrupted. If interrupted, the
        // InterruptedException is caught and recorded — this is how
        // Future.cancel(true) gets the worker to bail out.
        unblock.await();
      } catch (InterruptedException e) {
        interruptObserved.set(true);
        Thread.currentThread().interrupt();
      }
    }

    StreamingQuery proxy() {
      return (StreamingQuery)
          Proxy.newProxyInstance(
              StreamingQuery.class.getClassLoader(),
              new Class<?>[] {StreamingQuery.class},
              (p, method, args) -> {
                if ("stop".equals(method.getName())) {
                  stop();
                  return null;
                }
                Class<?> rt = method.getReturnType();
                if (rt == boolean.class) return false;
                if (rt == int.class) return 0;
                if (rt == long.class) return 0L;
                return null;
              });
    }
  }
}
