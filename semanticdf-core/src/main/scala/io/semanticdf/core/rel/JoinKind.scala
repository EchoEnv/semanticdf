package io.semanticdf.core.rel

/** Engine-portable join-kind ADT — Phase 2 contract. Mirrors the
  * design doc §4.5.2 "JoinKind". Used by `RelOp.Join` (the
  * relational-plan join node).
  *
  * ==Why a sealed ADT (not a String)==
  *
  * A `String` field would let callers pass `"inner"` / `"left"`
  * / arbitrary typos — silent failures at engine-compile time.
  * The sealed ADT forces the engine adapter to handle each kind
  * explicitly (no silent defaulting).
  *
  * Per scala-data-driven-refactor §3 ("A rule becomes data only
  * when it must change without a deploy"): the join-kind set is
  * fixed at compile time (Inner / Left / Right / Full / Cross),
  * so a sealed ADT is correct, NOT a Map.
  *
  * ==Why 5 cases (not fewer, not more)==
  *
  * The set covers the join kinds supported by the design's
  * portable model:
  *   - **Inner** — rows where both sides match
  *   - **Left** — all rows from left + matching rows from right
  *   - **Right** — all rows from right + matching rows from left
  *   - **Full** — all rows from both sides
  *   - **Cross** — Cartesian product (no condition)
  *
  * Semi-joins and anti-joins (e.g. `LEFT SEMI`, `NOT EXISTS`) are
  * absent from the v0.3.0 relational IR. They are not a
  * regression — they were never in the portable model — and they
  * can be expressed via `Filter` + `Join(Inner, condition)` if
  * needed.
  *
  * ==Why core (engine-portable)==
  *
  * Join kinds are universal across query engines. Every SQL
  * engine supports these 5 join kinds (or a subset that maps
  * cleanly to them). The engine-specific compile (Spark's
  * `joinType`, Trino's `JOIN` keyword, etc.) lives in the
  * engine adapter.
  *
  * ==Data-driven mantra compliance==
  *
  * - Pure data: sealed trait + 5 case objects
  * - Equality auto-derived (case objects)
  * - `Product with Serializable`
  *
  * ==Boundary contract==
  *
  * Zero Spark imports. Verifiable by:
  * `grep -r 'org.apache.spark' semanticdf-core/src/main/scala/io/semanticdf/core/rel/JoinKind.scala`
  */
sealed trait JoinKind extends Product with Serializable

object JoinKind {

  /** Inner join: rows where both sides match. Maps to Spark's
    * `"inner"`, Trino's `INNER JOIN`. */
  case object Inner extends JoinKind

  /** Left outer join: all rows from left + matching rows from
    * right. Maps to Spark's `"left"`, Trino's `LEFT JOIN`. */
  case object Left extends JoinKind

  /** Right outer join: all rows from right + matching rows from
    * left. Maps to Spark's `"right"`, Trino's `RIGHT JOIN`. */
  case object Right extends JoinKind

  /** Full outer join: all rows from both sides. Maps to Spark's
    * `"outer"` / `"full"`, Trino's `FULL JOIN`. */
  case object Full extends JoinKind

  /** Cross join (Cartesian product, no condition). Maps to
    * Spark's `"cross"`, Trino's `CROSS JOIN`. */
  case object Cross extends JoinKind
}