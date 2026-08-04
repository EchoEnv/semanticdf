package io.semanticdf.core.model

/** Engine-portable stale-rollup policy ADT — Phase 2 contract.
  * Mirrors the design doc §4.5.2 "OnStalePolicy". Used by
  * [[RollupFreshnessSpec.Track.onStale]] to declare what to do when
  * a rollup is too stale to use.
  *
  * ==Why a sealed ADT (not a String)==
  *
  * A `String` field would let callers pass `"fall_back_to_base"` /
  * `"error"` / arbitrary typos — silent failures at engine-compile
  * time. The sealed ADT forces the engine adapter to handle each
  * policy explicitly.
  *
  * Per scala-data-driven-refactor §3 ("A rule becomes data only when
  * it must change without a deploy"): the policy set is fixed at
  * compile time (FallBackToBase / Error), so a sealed ADT is correct,
  * NOT a Map.
  *
  * ==Why 2 cases (not fewer, not more)==
  *
  * The set covers the rollup-staleness policies supported by the
  * design's portable model:
  *   - **FallBackToBase** — the engine falls back to the base fact
  *     table (no error, just slower + emits a warning in the audit
  *     event)
  *   - **Error** — the engine throws at query time (for dashboards
  *     where stale data is unacceptable — financial dashboards,
  *     billing, etc.)
  *
  * More nuanced policies (e.g. "warn but use", "use with a TTL") are
  * out of scope for the v0.3.0 portable model. They can be added as
  * new ADT cases in a follow-up.
  *
  * ==Why core (engine-portable)==
  *
  * Stale-rollup policy is universal across query engines. Every
  * engine that supports rollups has a notion of "what to do when the
  * rollup is stale". The engine-specific compile (Spark's
  * branch-on-stale, Trino's fallback query, etc.) lives in the engine
  * adapter.
  *
  * ==Data-driven mantra compliance==
  *
  * - Pure data: sealed trait + 2 case objects
  * - Equality auto-derived (case objects)
  * - `Product with Serializable`
  *
  * ==Boundary contract==
  *
  * Zero Spark imports. Verifiable by:
  * `grep -r 'org.apache.spark' semanticdf-core/src/main/scala/io/semanticdf/core/model/OnStalePolicy.scala`
  */
sealed trait OnStalePolicy extends Product with Serializable

object OnStalePolicy {

  /** Fall back to the base fact table. Emit a warning in the audit
    * event. The user gets correct-but-slower results. This is the
    * default for batch use where staleness is acceptable.
    *
    * Maps to Spark's `LazyColumn` switch + a fallback branch in
    * the rollup source loader; Trino's fallback query. */
  case object FallBackToBase extends OnStalePolicy

  /** Throw `IllegalStateException` at query time. Use for
    * dashboards where stale data is unacceptable (e.g. financial
    * dashboards, billing, audit reports).
    *
    * Maps to the engine's strict-mode query. The throw is surfaced
    * as `EngineError` at the MCP boundary. */
  case object Error extends OnStalePolicy
}