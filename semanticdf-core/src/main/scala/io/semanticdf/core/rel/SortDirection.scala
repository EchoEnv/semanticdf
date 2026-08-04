package io.semanticdf.core.rel

/** Engine-portable sort-direction ADT — Phase 2 contract.
  *
  * Used by [[SortKey]] (which lives in the relational plan tree).
  * Mirrors the design doc §4.5.2 "SortDirection".
  *
  * ==Why a sealed ADT (not a Boolean)==
  *
  * A `Boolean` would let callers pass either `true` or `false`
  * for direction — but "Ascending" and "Descending" are closed
  * enumerated values, not just true/false. The closed ADT forces
  * the engine adapter to handle both cases explicitly (no
  * silent defaulting).
  *
  * ==Why core (engine-portable)==
  *
  * Sort direction is universal across query engines. Every
  * engine has the same notion of "ascending" vs "descending".
  * The engine-specific compile (Spark's `.asc()`/`.desc()`,
  * Trino's `ASC`/`DESC`, etc.) lives in the engine adapter.
  *
  * ==Data-driven mantra compliance==
  *
  * - Pure data: sealed trait + 2 case objects
  * - Equality auto-derived
  * - `Product with Serializable`
  *
  * ==Boundary contract==
  *
  * Zero Spark imports. Verifiable by:
  * `grep -r 'org.apache.spark' semanticdf-core/src/main/scala/io/semanticdf/core/rel/SortDirection.scala`
  */
sealed trait SortDirection extends Product with Serializable

object SortDirection {

  /** Ascending order (smallest to largest; A before Z; earliest
    * to latest). Maps to Spark's `.asc()`, Trino's `ASC` keyword. */
  case object Ascending extends SortDirection

  /** Descending order (largest to smallest; Z before A; latest
    * to earliest). Maps to Spark's `.desc()`, Trino's `DESC`
    * keyword. */
  case object Descending extends SortDirection
}