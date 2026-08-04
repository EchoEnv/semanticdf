package io.semanticdf.core.rel

/** Engine-portable null-ordering ADT — Phase 2 contract.
  *
  * Used by [[SortKey]] (which lives in the relational plan tree).
  * Mirrors the design doc §4.5.2 "NullOrdering".
  *
  * ==Why a sealed ADT (not an Int flag)==
  *
  * A primitive `Int` (with named constants `NULLS_FIRST = 0`,
  * `NULLS_LAST = 1`) would be a downgrade dressed as an upgrade:
  * a compile error becomes a silent `getOrElse` default on a
  * typo'd value. The sealed ADT forces the engine adapter to
  * handle both cases explicitly.
  *
  * Per scala-data-driven-refactor §3 ("A rule becomes data only
  * when it must change without a deploy"): the null-ordering set
  * is fixed at compile time (First / Last), so a sealed ADT is
  * correct, NOT a Map.
  *
  * ==Why core (engine-portable)==
  *
  * Null ordering is universal across SQL engines. Every engine
  * has the notion of "nulls first" vs "nulls last". The engine-
  * specific compile (Spark's `.asc_nulls_first()`,
  * Trino's `NULLS FIRST` / `NULLS LAST`, etc.) lives in the
  * engine adapter.
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
  * `grep -r 'org.apache.spark' semanticdf-core/src/main/scala/io/semanticdf/core/rel/NullOrdering.scala`
  */
sealed trait NullOrdering extends Product with Serializable

object NullOrdering {

  /** Nulls sort BEFORE non-nulls. Maps to Spark's
    * `.asc_nulls_first()` / `.desc_nulls_first()`, Trino's
    * `NULLS FIRST` keyword. */
  case object First extends NullOrdering

  /** Nulls sort AFTER non-nulls. Maps to Spark's
    * `.asc_nulls_last()` / `.desc_nulls_last()`, Trino's
    * `NULLS LAST` keyword. This is the default in most
    * SQL engines (ANSI SQL default). */
  case object Last extends NullOrdering
}