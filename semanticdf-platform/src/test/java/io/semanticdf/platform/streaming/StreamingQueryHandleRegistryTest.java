package io.semanticdf.platform.streaming;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.lang.reflect.Proxy;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.junit.jupiter.api.Test;

/** Tests for {@link StreamingQueryHandleRegistry}. */
class StreamingQueryHandleRegistryTest {

  /** A no-op StreamingQuery via dynamic proxy — the registry never calls
   * methods on the value, only stores/retrieves it. Returns appropriate
   * defaults for primitive return types to avoid unboxing NPEs. */
  private static StreamingQuery fakeQuery() {
    return (StreamingQuery)
        Proxy.newProxyInstance(
            StreamingQuery.class.getClassLoader(),
            new Class<?>[] {StreamingQuery.class},
            (proxy, method, args) -> {
              Class<?> rt = method.getReturnType();
              if (rt == boolean.class) return false;
              if (rt == int.class) return 0;
              if (rt == long.class) return 0L;
              return null;
            });
  }

  @Test
  void putAndGetRoundTrips() {
    StreamingQueryHandleRegistry reg = new StreamingQueryHandleRegistry();
    StreamingQuery q = fakeQuery();
    reg.put("s1", q);
    assertSame(q, reg.get("s1"));
    assertEquals(1, reg.size());
  }

  @Test
  void getAbsentReturnsNull() {
    StreamingQueryHandleRegistry reg = new StreamingQueryHandleRegistry();
    assertNull(reg.get("nope"));
    assertEquals(0, reg.size());
  }

  @Test
  void removeReturnsAndClears() {
    StreamingQueryHandleRegistry reg = new StreamingQueryHandleRegistry();
    StreamingQuery q = fakeQuery();
    reg.put("s1", q);
    assertSame(q, reg.remove("s1"));
    assertNull(reg.get("s1"));
    assertEquals(0, reg.size());
  }

  @Test
  void removeAbsentReturnsNull() {
    StreamingQueryHandleRegistry reg = new StreamingQueryHandleRegistry();
    assertNull(reg.remove("nope"));
  }

  @Test
  void multipleStreamsCoexist() {
    StreamingQueryHandleRegistry reg = new StreamingQueryHandleRegistry();
    reg.put("s1", fakeQuery());
    reg.put("s2", fakeQuery());
    assertEquals(2, reg.size());
    assertNotNull(reg.get("s1"));
    assertNotNull(reg.get("s2"));
  }

  @Test
  void forEach_visitsAllEntries() {
    StreamingQueryHandleRegistry reg = new StreamingQueryHandleRegistry();
    StreamingQuery q1 = fakeQuery();
    StreamingQuery q2 = fakeQuery();
    reg.put("s1", q1);
    reg.put("s2", q2);

    java.util.Map<String, StreamingQuery> visited = new java.util.HashMap<>();
    reg.forEach(visited::put);
    assertEquals(2, visited.size());
    assertSame(q1, visited.get("s1"));
    assertSame(q2, visited.get("s2"));
  }

  @Test
  void forEach_emptyRegistryDoesNothing() {
    StreamingQueryHandleRegistry reg = new StreamingQueryHandleRegistry();
    java.util.concurrent.atomic.AtomicInteger count = new java.util.concurrent.atomic.AtomicInteger(0);
    reg.forEach((k, v) -> count.incrementAndGet());
    assertEquals(0, count.get());
  }

  private static void assertNotNull(Object o) {
    org.junit.jupiter.api.Assertions.assertNotNull(o);
  }
}
