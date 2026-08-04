package io.semanticdf.core.model

/** Engine-portable rollup-precompute ADT — Phase 2 contract. Mirrors
  * the design doc §4.3.3 "RollupPrecompute".
  *
  * A [[RollupPrecompute]] is the engine-side computed stats for a
  * rollup (row count, columns, source digest). It's computed at
  * registration time by the engine adapter (via the `provider`
  * thunk in [[RollupRegistration]]) and never flows through the
  * portable model — only the engine uses it for routing decisions.
  *
  * ==Why this exists (vs. fields on `RollupSpec`)==
  *
  * `RollupSpec` is the PORTABLE metadata (what the user declares).
  * `RollupPrecompute` is the ENGINE-SIDE computed state (what the
  * engine discovered). They're separate because:
  *   - `RollupSpec` survives in the v2 manifest (the wire format)
  *   - `RollupPrecompute` is computed per-engine and may differ
  *     across engines (e.g. a Trino rollup's row count might
  *     differ from the same rollup's Spark row count due to
  *     storage layout)
  *
  * Per scala-data-driven-refactor §1 ("data is data, behavior lives
  * elsewhere"): the COMPUTATION (resolving the provider to get the
  * precompute) is in the engine adapter; the DATA (the computed
  * stats) is in core.
  *
  * ==Why all fields are `Option`==
  *
  * `rowCount` is `Option[Long]` because the engine might not have
  * computed it yet (e.g. the rollup table isn't accessible). `columns`
  * is `Set[String]` directly (always present — the engine resolves
  * the source's columns via the provider). `sourceDigest` is `Option[String]`
  * for source-schema-drift detection (the engine hashes the source
  * schema to detect when the user rebuilds the rollup table without
  * re-registering).
  *
  * ==Why core (engine-portable)==
  *
  * The shape of the precompute (3 fields, all Option) is universal
  * across engines. The COMPUTATION is engine-specific (Spark
  * `df.count()`, Trino `SELECT COUNT(*) FROM ...`, etc.) — that's
  * in the engine adapter.
  *
  * ==Data-driven mantra compliance==
  *
  * - Pure data: case class (no behavior)
  * - Equality auto-derived
  * - `Product with Serializable`
  *
  * ==Boundary contract==
  *
  * Zero Spark imports. Verifiable by:
  * `grep -r 'org.apache.spark' semanticdf-core/src/main/scala/io/semanticdf/core/model/RollupPrecompute.scala`
  */
final case class RollupPrecompute(
    rowCount:     Option[Long]  = None,
    columns:      Set[String]   = Set.empty,
    sourceDigest: Option[String] = None,
) extends Product with Serializable