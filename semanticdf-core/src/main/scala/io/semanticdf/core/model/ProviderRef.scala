package io.semanticdf.core.model

/** Engine-portable provider-reference ADT —
  * Phase 2 contract. Mirrors the design doc §4.1 "ProviderRef".
  *
  * `ProviderRef` is the IDENTIFIER for a registered `() => DataFrame`
  * (or equivalent) closure. The actual closure is NEVER serialized —
  * it lives in the driver-local registry, populated at server startup.
  *
  * ==Why a typed ADT (not a String)==
  *
  * A portable `SourceRef.ByProvider(provider)` carries a `ProviderRef`
  * by name. The engine resolver looks up the closure at execution
  * time by `provider.name`. A free-form `name: String` field would
  * let adapters invent new provider names that the registry
  * couldn't validate. A sealed ADT forces the closed set of
  * provider shapes.
  *
  * ==Why two variants==
  *
  * - `DataFrameSource(name, schemaHint)` — the canonical Spark-style
  *   closure that produces a `DataFrame` on demand. `schemaHint`
  *   is optional: if present, the engine resolver uses it for
  *   fast-path planning (skip the schema-describe round trip).
  *   If absent, the resolver calls the closure and inspects the
  *   resulting schema.
  * - `TableResolver(name)` — a `String => DataFrame` closure
  *   (the table name is the input). Useful for engines that
  *   want a fresh lookup per call (e.g. dynamic partitions).
  *
  * ==Why "DataFrame" in the name?==
  *
  * The design doc uses "DataFrame" as the engine-portable term for
  * a table-like result. Each engine maps this to its native shape
  * (Spark: `DataFrame`; Trino: `TableHandle`; Databricks: `DataFrame`).
  * The TYPE in the case class name is a port from the design doc;
  * the SEMANTICS is engine-portable ("a table-like result").
  *
  * ==Why core (engine-portable)==
  *
  * The provider reference is a name + shape, not a closure. The
  * name flows through the portable model and the MCP wire format.
  * The closure lives in the engine adapter layer.
  *
  * ==Data-driven mantra compliance==
  *
  * - Pure data: sealed trait + 2 case classes (no behavior)
  * - Equality auto-derived
  * - `Product with Serializable`
  *
  * ==Boundary contract==
  *
  * This file compiles with zero `org.apache.spark.*` imports.
  * Verifiable by:
  * `grep -r 'org.apache.spark' semanticdf-core/src/main/scala/io/semanticdf/core/model/ProviderRef.scala`
  */
sealed trait ProviderRef extends Product with Serializable

object ProviderRef {

  /** The canonical `() => DataFrame` provider. Invoked with no
    * arguments; produces a fresh `DataFrame` on each call. `name`
    * is the registry key; `schemaHint` is optional fast-path data. */
  final case class DataFrameSource(
      name:       String,
      schemaHint: Option[List[String]] = None,  // simplified: List[String] of column names
  ) extends ProviderRef

  /** A `String => DataFrame` provider — the input is a table name
    * (e.g. "orders", "sales.2024_q1"). The engine resolver
    * invokes with the requested table name. */
  final case class TableResolver(name: String) extends ProviderRef
}