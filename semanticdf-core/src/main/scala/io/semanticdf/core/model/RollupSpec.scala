package io.semanticdf.core.model

/** Engine-portable rollup spec ADT — Phase 2 contract. Mirrors the
  * design doc §4.5.2 "RollupSpec" (portable mirror of the
  * spark-coupled `Rollup` value class in `rollup/Rollup.scala`).
  *
  * ==Why a separate type from the existing `io.semanticdf.rollup.Rollup`==
  *
  * The spark-coupled `Rollup` is a `final class` (not a case class)
  * with custom `equals`/`hashCode` keyed on the rollup name. It also
  * carries engine-coupled fields (`precomputedRowCount`,
  * `precomputedColumns`) that are computed at registration time via
  * a Spark DataFrame thunk.
  *
  * The portable `RollupSpec` is a pure-data case class:
  *   - equals auto-derived (structural, not name-only)
  *   - no DataFrame references (the engine resolves via `RollupRegistration`)
  *   - no precomputed stats (those are in `RollupPrecompute`, also
  *     computed at registration time)
  *
  * Per scala-data-driven-refactor §1 ("data is data, behavior lives
  * elsewhere"): the spec (this ADT) lives in core; the registration
  * (`RollupRegistration` — coming in Group 3b-ii) ties the spec to
  * an engine-specific provider.
  *
  * ==Why `dimensions: List[String]` (column names, not `Expr`)==
  *
  * The rollup's grain is declared by column NAMES (the user knows
  * which columns are in the rollup table). The rollup's MEASURES are
  * declared by name + aggregator + storage column
  * ([[RollupMeasureSpec]]). The grain is simpler than the measure
  * because the user only needs to declare "this rollup is at grain X"
  * — no expressions needed.
  *
  * ==Why core (engine-portable)==
  *
  * The rollup spec is the metadata that flows through the portable
  * model (in the v2 manifest). It carries no engine-specific state.
  * The engine adapter uses it to route queries to the right rollup.
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
  * `grep -r 'org.apache.spark' semanticdf-core/src/main/scala/io/semanticdf/core/model/RollupSpec.scala`
  */
final case class RollupSpec(
    name:       String,
    baseModel:  String,
    dimensions: List[String],
    measures:   List[RollupMeasureSpec],
    freshness:  RollupFreshnessSpec,
) extends Product with Serializable