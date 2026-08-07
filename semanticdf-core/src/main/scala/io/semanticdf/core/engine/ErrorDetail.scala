package io.semanticdf.core.engine

/** Engine-portable structured error detail.
  *
  * Mirrors the shape of the MCP `ErrorDetail` (in
  * `io.semanticdf.mcp.Json`) but lives in `core` (engine-portable)
  * so non-MCP consumers (CLI, REST, programmatic) can reuse it.
  *
  * ==Why this exists (v0.3.0)==
  *
  * Every engine adapter (Trino, DuckDB, Databricks, custom-platform)
  * returns `EngineError` from `compile` / `execute` / `explain`.
  * The mapping from `EngineError` → user-facing structured error
  * is total over the `EngineError` ADT. Without this type in core,
  * every transport (MCP, REST, CLI) would reimplement the mapping.
  *
  * ==Design choice==
  *
  * - `code` is drawn from a CLOSED set (one per `EngineError`
  *   case). The MCP contract (`docs/agents/mcp-contract.md`)
  *   enumerates the codes.
  * - `message` is human-readable (engine-aware).
  * - `hint` is actionable (suggests a fix when possible).
  * - `details` carries the structured payload (one key per
  *   `EngineError` case field) so callers can route on it.
  *
  * ==Boundary contract==
  *
  * This file compiles with zero `org.apache.spark.*` imports.
  * Verifiable by:
  * `grep -r 'org.apache.spark' semanticdf-core/src/main/scala/io/semanticdf/core/engine/ErrorDetail.scala`
  *
  * ==Data-driven mantra compliance==
  *
  * - Pure data (final case class, no behavior).
  * - Equality + hash codes auto-derived (case class).
  * - `Product with Serializable` (auto-derived).
  * - `details: Map[String, String]` is keyed by the case field name.
  */
final case class ErrorDetail(
    code:    String,                              // e.g. "UNSUPPORTED_CAPABILITY"
    message: String,                              // human-readable, engine-aware
    hint:    Option[String] = None,               // optional actionable fix
    details: Map[String, String] = Map.empty,     // structured payload (case fields)
) extends Product with Serializable