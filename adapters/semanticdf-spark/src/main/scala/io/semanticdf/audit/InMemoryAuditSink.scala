package io.semanticdf.audit

/** In-memory audit sink — retains the last `maxEvents` in arrival order.
  *
  * Intended for tests and for the MCP `audit_log` retrieval tool
  * (which fetches a snapshot of recent events on demand). The buffer
  * is a thread-safe `java.util.ArrayDeque` (which is `Serializable`)
  * wrapped in a `synchronized` block; the cap is enforced with O(1)
  * `dequeueFirst` / `dequeueLast` on overflow.
  *
  * Not intended for production use at scale — every event is held in
  * memory until evicted. For long-running servers, swap to a
  * file-backed or queue-backed sink.
  *
  * `Serializable` is required for cluster-mode safety: when a
  * `SemanticTable` is captured in a closure (UDF, broadcast,
  * accumulator) and the closure is shipped to executors, the
  * `auditSink` field must be serializable. `java.util.ArrayDeque`
  * is `Serializable`; the `mutable.Queue` we used previously is
  * Scala-internal and not. */
private[audit] final class InMemoryAuditSink(maxEvents: Int) extends AuditSink {

  private val buf = new java.util.ArrayDeque[AuditEvent]()

  def emit(event: AuditEvent): Unit = synchronized {
    buf.addLast(event)
    while (buf.size > maxEvents) buf.removeFirst()
  }

  /** Snapshot the buffer in arrival order. Newest last. */
  override def snapshot(): Seq[AuditEvent] = synchronized {
    import scala.jdk.CollectionConverters._
    buf.asScala.toList
  }

  /** Drop every retained event. Useful for tests. */
  def clear(): Unit = synchronized { buf.clear() }
}
