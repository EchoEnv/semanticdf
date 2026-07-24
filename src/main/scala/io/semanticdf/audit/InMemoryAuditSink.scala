package io.semanticdf.audit

import scala.collection.mutable

/** In-memory audit sink — retains the last `maxEvents` in arrival order.
  *
  * Intended for tests and for the MCP `audit_log` retrieval tool
  * (which fetches a snapshot of recent events on demand). The buffer
  * is a thread-safe `mutable.Queue` wrapped in a `synchronized` block;
  * the cap is enforced with O(1) `dequeue` on overflow.
  *
  * Not intended for production use at scale — every event is held in
  * memory until evicted. For long-running servers, swap to a
  * file-backed or queue-backed sink. */
private[audit] final class InMemoryAuditSink(maxEvents: Int) extends AuditSink {

  private val buf = mutable.Queue.empty[AuditEvent]

  def emit(event: AuditEvent): Unit = synchronized {
    buf.enqueue(event)
    while (buf.size > maxEvents) buf.dequeue()
  }

  /** Snapshot the buffer in arrival order. Newest last. */
  override def snapshot(): Seq[AuditEvent] = synchronized { buf.toList }

  /** Drop every retained event. Useful for tests. */
  def clear(): Unit = synchronized { buf.clear() }
}
