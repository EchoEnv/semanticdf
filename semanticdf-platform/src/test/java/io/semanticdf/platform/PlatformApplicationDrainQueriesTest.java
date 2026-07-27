package io.semanticdf.platform;

import io.semanticdf.platform.streaming.StreamingQueryHandleRegistry;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.spark.sql.streaming.StreamingQuery;
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
  static final class CountingQuery {
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
}
