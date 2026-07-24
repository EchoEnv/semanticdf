package io.semanticdf.audit

/** Pluggable destination for [[AuditEvent]]s.
  *
  * The audit pipeline is intentionally tiny: one trait, three default
  * implementations (no-op, JSONL stdout, in-memory). The library does
  * not impose a logger, a queue, or an async pipeline — the sink is
  * the caller's contract. Sinks must be thread-safe; the library
  * emits from the calling thread.
  *
  * Implementations should be:
  *   - Fast. The audit path is on the hot path of every query.
  *   - Non-throwing. An audit-sink failure must not break the query.
  *     Catch and log internally; never propagate.
  *   - Cheap to construct. Sinks are typically shared across the
  *     lifetime of a SparkSession. */
trait AuditSink {

  /** Record one event. Must not throw. */
  def emit(event: AuditEvent): Unit
}

object AuditSink {

  /** A sink that drops every event. The default. Opt-in by passing
    * a real sink to [[io.semanticdf.SemanticTable.query]] (or by
    * wiring the MCP server with one). */
  val NoOp: AuditSink = new AuditSink {
    def emit(event: AuditEvent): Unit = ()
  }

  /** JSON Lines on stdout. Each event is a single-line JSON object.
    * Designed for grep / awk / `jq` consumption; safe to tail.
    *
    * Uses `java.util.logging` rather than `println` so the output
    * goes through the user's logging config and can be redirected
    * independently of the JVM's stdout. */
  val JsonlStdout: AuditSink = new JsonlStdoutSink()

  /** A sink that retains every event in memory, in arrival order.
    * Intended for tests and for the MCP `audit_log` retrieval
    * tool. Bound by `maxEvents` (default 1024); the oldest event
    * is dropped when the cap is hit. */
  def inMemory(maxEvents: Int = 1024): AuditSink = new InMemoryAuditSink(maxEvents)
}
