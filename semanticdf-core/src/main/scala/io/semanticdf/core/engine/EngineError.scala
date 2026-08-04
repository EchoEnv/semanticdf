package io.semanticdf.core.engine

/** Engine-portable sealed ADT for compile/execute failures —
  * Phase 2 contract. Mirrors the design doc §4 "Engine contract".
  *
  * Every engine adapter (Trino, Databricks, Snowflake, custom-platform)
  * returns \`EngineError\` from \`compile\` / \`execute\` / \`explain\`.
  * The MCP server maps \`EngineError\` to MCP error envelopes via an
  * exhaustive \`toErrorDetail\` function (exhaustive on add — see
  * the design doc for the contract).
  *
  * ==Why this exists==
  *
  * Without a closed failure ADT, engine adapters would surface
  * exceptions with arbitrary messages, and the MCP server would
  * have to string-match to classify errors. That's brittle and
  * leaks engine-specific terminology. A sealed ADT forces every
  * engine adapter to return one of a closed set of cases — the
  * MCP server's error mapping is total over the ADT.
  *
  * ==Why core (engine-portable)==
  *
  * The failure modes are universal across query engines:
  * unsupported capability, connection failure, query timeout,
  * cancellation failure, source schema mismatch, etc. They are
  * not engine-specific. The \`engine\` field on each case is
  * filled in by the adapter, telling the consumer which engine
  * produced the error.
  *
  * ==Data-driven mantra compliance==
  *
  * - Pure data: sealed trait + final case classes (no behavior)
  * - Equality auto-derived (case classes)
  * - Hash codes stable (auto-derived)
  * - \`Product with Serializable\` (Java-serialization round-trip)
  * - Each case carries ONLY the data needed to identify the failure
  *
  * ==Boundary contract==
  *
  * This file compiles with zero \`org.apache.spark.*\` imports.
  * Verifiable by:
  * \`grep -r 'org.apache.spark' semanticdf-core/src/main/scala/io/semanticdf/core/engine/EngineError.scala\`
  */
sealed trait EngineError extends Product with Serializable

object EngineError {

  /** The requested capability is not supported by this engine.
    * E.g. "stream broadcast joins" on Trino without broadcast joins
    * enabled, or "deeply nested struct types" on SQLite. */
  final case class UnsupportedCapability(
      name:    String,
      reason:  String,
  ) extends EngineError

  /** The expression shape is not compatible with the target engine.
    * E.g. a measure reference that the engine doesn't know how to
    * resolve, or a cast that the engine doesn't support. */
  final case class IncompatibleExprShape(
      shape:  String,
      engine: String,
  ) extends EngineError

  /** A decimal value would overflow the target engine's DECIMAL type.
    * Each engine has its own precision/scale limits; this case
    * reports the value + the engine's limits. */
  final case class DecimalOverflow(
      value:     String,
      precision: Int,
      scale:     Int,
  ) extends EngineError

  /** A feature was deferred to a later release. \`feature\` names the
    * capability; \`release\` is the target version (e.g. "v0.4.0"). */
  final case class FeatureDeferred(
      feature: String,
      release: String,
  ) extends EngineError

  /** Cancellation was requested but failed. \`cancelStatus\` is the
    * engine's reported state (e.g. "still running", "aborted mid-pipeline"). */
  final case class CancellationFailed(
      cancelStatus: String,
  ) extends EngineError

  /** Could not connect to the engine's backend. \`reason\` is the
    * raw error from the JDBC/HTTP/Thrift client. */
  final case class ConnectionFailed(
      reason: String,
  ) extends EngineError

  /** The query timed out. \`cancelStatus\` is what the engine did
    * in response to the timeout (best-effort cancellation). */
  final case class QueryTimedOut(
      cancelStatus: String,
  ) extends EngineError

  /** The audit sink named was not available (e.g. not registered
    * at server startup, or failed to initialize on demand). */
  final case class AuditSinkUnavailable(
      name: String,
  ) extends EngineError

  /** A user-supplied provider invocation (the \`() => DataFrame\` style)
    * failed at runtime. \`name\` is the provider identifier; \`reason\`
    * is the underlying exception's message. */
  final case class ProviderInvocationFailed(
      name:   String,
      reason: String,
  ) extends EngineError

  /** The source schema changed between compile and execute (or
    * between cache and execute). \`source\` is the source name. */
  final case class SourceSchemaChanged(
      source: String,
  ) extends EngineError

  /** The requested engine name is not registered with the server.
    * \`available\` is the list of registered engine names. */
  final case class EngineUnavailable(
      name:      String,
      available: Seq[String],
      wasDefault: Boolean,
  ) extends EngineError
}