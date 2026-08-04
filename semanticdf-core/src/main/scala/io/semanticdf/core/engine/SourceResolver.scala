package io.semanticdf.core.engine

import io.semanticdf.core.model.SourceRef

/** Engine-portable source-resolver contract — Phase 2 contract.
  * Mirrors the design doc §4.3.2 "SourceResolver".
  *
  * [[SourceResolver]] is the boundary trait that engine adapters
  * implement to resolve a [[SourceRef]] (the portable source
  * IDENTITY) to a [[ResolvedSource]] (the engine-specific RESOLUTION
  * result: scan with schema, or auth-failed, or not-found).
  *
  * ==Why a trait (vs. a class)==
  *
  * Per scala-data-driven-refactor §1 ("data is data, behavior lives
  * elsewhere"): the SHAPE of the contract is in core; the BODY
  * (the engine-specific resolution logic) is in each engine
  * adapter's implementation.
  *
  * Each engine adapter provides its own resolver:
  *   - Trino resolver: calls `DESCRIBE <table>` (or `SHOW COLUMNS`)
  *     against the Trino cluster
  *   - Spark resolver: reads the source DataFrame's schema
  *     (and optionally calls `ANALYZE TABLE`)
  *   - Databricks resolver: similar
  *
  * ==Why `extends Serializable`==
  *
  * The resolver is referenced from `ResolvedSource.Scan` and from
  * the `SparkProviderRegistry` / `TrinoRuntimeRegistry` (per the
  * design). It may be passed through serialization boundaries (e.g.
  * the MCP wire format). Implementations are responsible for
  * serializing their own state correctly.
  *
  * ==Why core (engine-portable)==
  *
  * The contract SHAPE (`resolve(source, identity)`) is universal.
  * The BODY is engine-specific.
  *
  * ==Data-driven mantra compliance==
  *
  * - Pure contract: trait with one abstract method (no behavior)
  * - Companion `object` with no methods — just a marker
  *
  * ==Boundary contract==
  *
  * Zero Spark imports. Verifiable by:
  * `grep -r 'org.apache.spark' semanticdf-core/src/main/scala/io/semanticdf/core/engine/SourceResolver.scala`
  *
  * Engine adapters MAY depend on Spark/Trino/whatever — they
  * implement this trait. But THIS FILE (the contract) is engine-
  * agnostic.
  */
trait SourceResolver extends Serializable {

  /** Resolve a portable source reference to an engine-specific
    * resolution result.
    *
    * The result is one of:
    *   - `ResolvedSource.Scan(source, schema)` — successful
    *     resolution, with the source's schema (and stats)
    *   - `ResolvedSource.Incompatible(source, reason)` — the
    *     source's shape isn't supported by this engine (e.g.
    *     a CSV file on Trino without a CSV reader)
    *   - `ResolvedSource.AuthFailed(source, reason)` — auth failed
    *     (e.g. Kerberos ticket expired)
    *   - `ResolvedSource.NotFound` — the source wasn't found
    *     (table doesn't exist, path doesn't resolve, etc.)
    *
    * Implementations should NOT log auth errors at INFO level (they
    * may contain sensitive data); use WARN at most.
    *
    * @param source   the portable source reference to resolve
    * @param identity the calling engine's identity (for logging /
    *                 diagnostics — implementations may use this to
    *                 format error messages)
    * @return the resolution result */
  def resolve(source: SourceRef, identity: EngineIdentity): ResolvedSource
}

object SourceResolver {

  /** A no-op resolver that returns `Incompatible` for every source.
    * Useful for engines that don't have source resolution (e.g.
    * pure SQL query engines that receive fully-resolved plans).
    *
    * Per scala-data-driven-refactor §1, this is data (a function
    * value), not behavior. It's a stateless singleton. */
  val noOp: SourceResolver = new SourceResolver {
    override def resolve(source: SourceRef, identity: EngineIdentity): ResolvedSource =
      ResolvedSource.Incompatible(source, "no resolver configured")
  }
}