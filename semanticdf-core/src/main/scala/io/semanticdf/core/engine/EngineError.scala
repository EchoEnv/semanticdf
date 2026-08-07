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
sealed trait EngineError extends Product with Serializable {
  /** Map this error to an engine-portable structured [ErrorDetail].
    *
    * Exhaustive on add: every case returns one [ErrorDetail]. The
    * MCP server (and any other transport) can call this and ship
    * the result verbatim. The mapping is total over the ADT (the
    * Scala compiler enforces exhaustiveness).
    *
    * ==Per-case code mapping==
    *
    * - [UnsupportedCapability]   → `UNSUPPORTED_CAPABILITY`
    * - [IncompatibleExprShape]   → `INCOMPATIBLE_EXPR_SHAPE`
    * - [DecimalOverflow]         → `DECIMAL_OVERFLOW`
    * - [FeatureDeferred]         → `FEATURE_DEFERRED`
    * - [CancellationFailed]      → `CANCELLATION_FAILED`
    * - [ConnectionFailed]        → `CONNECTION_FAILED`
    * - [QueryTimedOut]           → `QUERY_TIMED_OUT`
    * - [AuditSinkUnavailable]    → `AUDIT_SINK_UNAVAILABLE`
    * - [ProviderInvocationFailed]→ `PROVIDER_INVOCATION_FAILED`
    * - [SourceSchemaChanged]     → `SOURCE_SCHEMA_CHANGED`
    * - [EngineUnavailable]       → `ENGINE_UNAVAILABLE`
    * - [ModelNotFound]           → `MODEL_NOT_FOUND`
    */
  def toErrorDetail: ErrorDetail = this match {
    case EngineError.UnsupportedCapability(name, reason) =>
      ErrorDetail(
        code    = "UNSUPPORTED_CAPABILITY",
        message = s"Engine does not support capability '$name': $reason",
        hint    = Some(s"Use an engine that supports '$name', or remove the requirement"),
        details = Map("capability" -> name, "reason" -> reason),
      )
    case EngineError.IncompatibleExprShape(shape, engine) =>
      ErrorDetail(
        code    = "INCOMPATIBLE_EXPR_SHAPE",
        message = s"Expression shape '$shape' is not compatible with engine '$engine'",
        hint    = Some(s"Rewrite the expression to a shape '$engine' supports, or pick a different engine"),
        details = Map("shape" -> shape, "engine" -> engine),
      )
    case EngineError.DecimalOverflow(value, precision, scale) =>
      ErrorDetail(
        code    = "DECIMAL_OVERFLOW",
        message = s"Decimal value '$value' exceeds the engine's DECIMAL($precision, $scale) limits",
        hint    = Some("Round or truncate the value, or widen the DECIMAL precision/scale"),
        details = Map("value" -> value, "precision" -> precision.toString, "scale" -> scale.toString),
      )
    case EngineError.FeatureDeferred(feature, release) =>
      ErrorDetail(
        code    = "FEATURE_DEFERRED",
        message = s"Feature '$feature' is deferred to $release",
        hint    = Some(s"Use a workaround, or wait for $release"),
        details = Map("feature" -> feature, "release" -> release),
      )
    case EngineError.CancellationFailed(cancelStatus) =>
      ErrorDetail(
        code    = "CANCELLATION_FAILED",
        message = s"Cancellation was requested but the engine reported status: $cancelStatus",
        hint    = Some("Inspect engine logs; the query may still be running"),
        details = Map("cancel_status" -> cancelStatus),
      )
    case EngineError.ConnectionFailed(reason) =>
      ErrorDetail(
        code    = "CONNECTION_FAILED",
        message = s"Could not connect to engine: $reason",
        hint    = Some("Check the connection string, credentials, and network"),
        details = Map("reason" -> reason),
      )
    case EngineError.QueryTimedOut(cancelStatus) =>
      ErrorDetail(
        code    = "QUERY_TIMED_OUT",
        message = s"Query timed out; engine best-effort cancellation status: $cancelStatus",
        hint    = Some("Increase the timeout, or simplify the query (e.g. add LIMIT, pre-filter)"),
        details = Map("cancel_status" -> cancelStatus),
      )
    case EngineError.AuditSinkUnavailable(name) =>
      ErrorDetail(
        code    = "AUDIT_SINK_UNAVAILABLE",
        message = s"Audit sink '$name' is not registered or failed to initialize",
        hint    = Some("Register the sink at server startup, or use the default in-memory sink"),
        details = Map("sink_name" -> name),
      )
    case EngineError.ProviderInvocationFailed(name, reason) =>
      ErrorDetail(
        code    = "PROVIDER_INVOCATION_FAILED",
        message = s"Provider '$name' invocation failed: $reason",
        hint    = Some("Check the provider's underlying data source and credentials"),
        details = Map("provider" -> name, "reason" -> reason),
      )
    case EngineError.SourceSchemaChanged(source) =>
      ErrorDetail(
        code    = "SOURCE_SCHEMA_CHANGED",
        message = s"Source '$source' schema changed between compile and execute (or cache and execute)",
        hint    = Some("Invalidate the cached plan and re-compile"),
        details = Map("source" -> source),
      )
    case EngineError.EngineUnavailable(name, available, wasDefault) =>
      ErrorDetail(
        code    = "ENGINE_UNAVAILABLE",
        message = (if (wasDefault)
          s"Default engine '$name' is unavailable"
        else
          s"Engine '$name' is not registered"
        ) + s"; available engines: [${available.mkString(", ")}]",
        hint    = Some(s"Pick one of the available engines, or register '$name' at startup"),
        details = Map(
          "engine"      -> name,
          "available"   -> available.mkString(","),
          "was_default" -> wasDefault.toString,
        ),
      )
    case EngineError.ModelNotFound(name) =>
      ErrorDetail(
        code    = "MODEL_NOT_FOUND",
        message = s"Model '$name' was not found in the engine's model registry",
        hint    = Some("Check the model name, or load the model before querying"),
        details = Map("model" -> name),
      )
  }
}

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

  /** The named model was not found in the engine's model registry.
    * Distinct from [EngineUnavailable] (which is about engine name)
    * and from [FeatureDeferred] (which means a feature is on the
    * roadmap but not implemented). `name` is the requested model
    * name. Added per the v0.3.0 pre-tag audit. */
  final case class ModelNotFound(name: String) extends EngineError
}