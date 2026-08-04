package io.semanticdf.core.engine

/** Engine-portable source-stats ADT — Phase 2 contract. Mirrors
  * the design doc §4.3.2 "SourceStats".
  *
  * [[SourceStats]] is the precomputed statistics for a resolved
  * source (row count estimate, byte count estimate). It's used
  * by engine adapters for query optimization (e.g. deciding
  * whether to broadcast-join a small table).
  *
  * ==Why `Option[Long]` for both fields==
  *
  * Not every engine can compute stats for every source:
  *   - Trino: `SHOW STATS FOR <table>` returns row/byte estimates
  *     (sometimes None for views)
  *   - Spark: `ANALYZE TABLE <table> COMPUTE STATISTICS` returns
  *     similar estimates (None for non-analyzed tables)
  *   - Databricks: similar (None for un-cached tables)
  *
  * `Option` makes the "unknown" case explicit.
  *
  * ==Why a case class (vs. a tuple)==
  *
  * Two `Option[Long]` fields are self-documenting as
  * `(estimatedRows, estimatedBytes)`. A `(Option[Long], Option[Long])`
  * tuple would be untyped — readers would have to remember which
  * is which.
  *
  * ==Why core (engine-portable)==
  *
  * The stats SHAPE (2 Option fields) is universal. The COMPUTATION
  * (calling `SHOW STATS`, `ANALYZE TABLE`, etc.) is engine-specific.
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
  * `grep -r 'org.apache.spark' semanticdf-core/src/main/scala/io/semanticdf/core/engine/SourceStats.scala`
  */
final case class SourceStats(
    estimatedRows:   Option[Long] = None,
    estimatedBytes:  Option[Long] = None,
) extends Product with Serializable