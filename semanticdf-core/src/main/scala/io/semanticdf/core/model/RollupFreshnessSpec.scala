package io.semanticdf.core.model

/** Engine-portable rollup-freshness spec ADT — Phase 2 contract.
  * Mirrors the design doc §4.5.2 "RollupFreshnessSpec" (portable
  * mirror of the spark-coupled `RollupFreshness` in the existing
  * `rollup/Rollup.scala`).
  *
  * ==Why a separate type from the existing `io.semanticdf.rollup.RollupFreshness`==
  *
  * The spark-coupled `RollupFreshness` carries a closure
  * (`watermarkProvider: () => Instant`) — that closure is
  * engine-specific (Spark resolves watermarks via the `source`
  * DataFrame's last-modification time, Trino resolves via the catalog
  * metadata, etc.). The portable `RollupFreshnessSpec` carries the
  * CONTRACT (maxStaleness + onStale); the engine adapter resolves
  * the watermark.
  *
  * Per scala-data-driven-refactor §1 ("data is data, behavior lives
  * elsewhere"): the freshness CONTRACT lives in core (this ADT); the
  * freshness RESOLUTION (the watermark lookup) lives in the engine
  * adapter.
  *
  * ==Why a sealed ADT (not a single case class with nullable fields)==
  *
  * `Track` and `NoTracking` are semantically distinct — they have
  * different fields. A `RollupFreshnessSpec(maxStaleness: Option[Duration],
  * onStale: Option[OnStalePolicy])` would be a downgrade dressed as
  * an upgrade: a typo'd `null` on one field becomes silent
  * invalid state. The sealed ADT forces the engine adapter to handle
  * both cases explicitly.
  *
  * ==Why core (engine-portable)==
  *
  * Freshness tracking is universal across query engines. Every
  * engine that supports rollups has the notion of "is this rollup
  * fresh enough to use". The engine-specific compile (Spark's
  * `watermarkProvider` thunk, Trino's catalog watermark query, etc.)
  * lives in the engine adapter.
  *
  * ==Data-driven mantra compliance==
  *
  * - Pure data: sealed trait + 2 cases (Track + NoTracking)
  * - Equality auto-derived
  * - `Product with Serializable`
  *
  * ==Boundary contract==
  *
  * Zero Spark imports. Verifiable by:
  * `grep -r 'org.apache.spark' semanticdf-core/src/main/scala/io/semanticdf/core/model/RollupFreshnessSpec.scala`
  */
sealed trait RollupFreshnessSpec extends Product with Serializable

object RollupFreshnessSpec {

  /** Track freshness via the engine's watermark. The engine
    * adapter is responsible for resolving the watermark (via its
    * catalog metadata, the source DataFrame's last-modification
    * time, etc.); the portable model only declares the staleness
    * contract.
    *
    * @param maxStaleness the maximum acceptable staleness — the engine
    *                     compares the resolved watermark to `now - maxStaleness`
    *                     to decide if the rollup is fresh enough
    * @param onStale      what to do when the rollup exceeds maxStaleness
    *                     (see [[OnStalePolicy]])
    */
  final case class Track(
    maxStaleness: java.time.Duration,
    onStale:      OnStalePolicy,
  ) extends RollupFreshnessSpec

  /** Explicit opt-out: do not track freshness. The rollup is always
    * considered fresh. Use for batch / static-fact rollups where
    * staleness is acceptable.
    *
    * Freshness tracking is REQUIRED by default per the design —
    * `NoTracking` must be explicit. */
  case object NoTracking extends RollupFreshnessSpec
}