package io.semanticdf.core.engine

import io.semanticdf.core.model.SourceRef

/** Engine-portable closed result type of source resolution —
  * Phase 2 contract. Mirrors the design doc §4.3 "SourceResolver".
  *
  * A `SourceResolver` (engine-specific) takes a `SourceRef` and
  * produces a `ResolvedSource`. The four cases are the closed set of
  * possible outcomes: the source resolved successfully to a scan,
  * the source is incompatible with the target engine, the resolver
  * hit an auth failure, or the source was not found.
  *
  * ==Why a sealed ADT and not a free-form result==
  *
  * A closed ADT forces every engine adapter to handle the closed
  * set of outcomes. Free-form result types (e.g. Option[DataFrame]
  * with a comment "None means error") would let adapters invent
  * new failure modes that the portable model and the MCP wire
  * format couldn't classify.
  *
  * ==Why a separate type from `SourceRef`==
  *
  * `SourceRef` is the IDENTITY (what the model says it wants).
  * `ResolvedSource` is the RESOLUTION (what the engine actually
  * produced). They live in different packages (`core.model` vs
  * `core.engine`) because they belong to different concerns —
  * `SourceRef` is the portable contract, `ResolvedSource` is the
  * engine-adapter boundary.
  *
  * ==Why core (engine-portable)==
  *
  * The closed result is the contract that flows through the portable
  * model after resolution. Per scala-data-driven-refactor: the data
  * (the resolution result) lives in core; the behavior (the resolver
  * that produces it) lives in the engine adapter layer.
  *
  * ==Data-driven mantra compliance==
  *
  * - Pure data: sealed trait + 4 case classes (no behavior)
  * - Equality auto-derived (case classes)
  * - `Product with Serializable` (Java-serialization round-trip)
  * - Each case carries only the data needed to identify the outcome
  *
  * ==Boundary contract==
  *
  * This file compiles with zero `org.apache.spark.*` imports.
  * Verifiable by:
  * `grep -r 'org.apache.spark' semanticdf-core/src/main/scala/io/semanticdf/core/engine/ResolvedSource.scala`
  */
sealed trait ResolvedSource extends Product with Serializable

object ResolvedSource {

  /** The source resolved successfully to a relational scan.
    * The engine resolver has produced the schema (and, where
    * supported, statistics) for the source. The portable model
    * can now use this scan in its `RelOp` tree.
    *
    * `source` is the original `SourceRef` (carried for provenance
    * — the model might want to log "resolved from X" or compare
    * the resolved schema against the model's declared schema). */
  final case class Scan(
      source:   SourceRef,
      schema:   ResolvedSchema,
  ) extends ResolvedSource

  /** The source's shape is incompatible with the target engine.
    * E.g. a `ByPath(format = "csv")` for a Trino engine that doesn't
    * have a CSV reader; a `ByName` for a non-existent catalog.
    *
    * The caller (model validator) decides whether to:
    *   - surface the incompatibility to the user
    *   - try a different resolver (e.g. fall back to a generic
    *     JDBC resolver)
    *   - reject the query with a `ResolvedSource.Incompatible`
    *     surfaced as `EngineError` to the MCP caller
    *
    * `reason` is the engine's explanation (e.g. "CSV not supported
    * by Trino; use parquet instead"). */
  final case class Incompatible(
      source: SourceRef,
      reason: String,
  ) extends ResolvedSource

  /** The resolver hit an authentication or authorization failure.
    * E.g. a Trino source that requires Kerberos auth but the
    * resolver can't authenticate; a `ByProvider` that requires
    * credentials the driver doesn't have.
    *
    * `reason` is the auth error (e.g. "Kerberos ticket expired").
    * Engine adapters SHOULD NOT log the auth error at INFO level
    * (it might contain sensitive data); use WARN at most. */
  final case class AuthFailed(
      source: SourceRef,
      reason: String,
  ) extends ResolvedSource

  /** The source was not found. E.g. a `ByName` referencing a table
    * that doesn't exist in the catalog; a `ByPath` for a path
    * that doesn't exist in object storage.
    *
    * The caller (model validator) decides whether to:
    *   - surface the not-found as an error to the user
    *   - try a different catalog/path
    *
    * `reason` is the engine's explanation (e.g. "table 'orders'
    * not found in catalog 'hive'"). */
  final case class NotFound(
      source: SourceRef,
      reason: String,
  ) extends ResolvedSource
}

/** Engine-portable resolved schema. The schema of a `Scan` after
  * the source has been resolved. Engine-portable in the sense that
  * it carries field names + data types; the engine-specific
  * implementation (e.g. Trino's `Type`, Spark's `StructType`) is
  * the data the resolver USES to produce this shape.
  *
  * The current shape is intentionally minimal — just field names
  * + data types as strings. Phase 2 follow-ups will add:
  *   - statistics (row count, size bytes)
  *   - nullability
  *   - partitioning info (for source-aware plan optimization)
  *
  * For now, the minimum viable shape is enough to express "what
  * fields does this source have" — the model's column-level
  * validation can run against this.
  *
  * The `fields` map is a `Map[String, String]` (name → type as a
  * string). Future Phase 2 work will add a typed
  * `SealedDataType` ADT (per the design doc §12 glossary).
  */
final case class ResolvedSchema(
    fields: Map[String, String] = Map.empty,
) extends Product with Serializable