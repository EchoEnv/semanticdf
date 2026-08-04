package io.semanticdf.core.rel

import io.semanticdf.core.expr.Expr

/** Engine-portable sort-key ADT — Phase 2 contract. Mirrors the
  * design doc §4.5.2 "SortKey" (the relational-IR sort key).
  *
  * ==Why a richer SortKey than the Phase 1 `core.field.SortKey`==
  *
  * The Phase 1 `core.field.SortKey` (mirrored in PR #344) carries
  * just `name: String + direction`. That's the simpler shape used
  * by the spark-adapter's `SemanticTable` ordering API.
  *
  * The Phase 2 `core.rel.SortKey` carries `expression: Expr +
  * direction: SortDirection + nullOrdering: NullOrdering`. This is
  * the richer shape required by the relational plan tree, where
  * sort expressions can be ANY `Expr` (a field, a literal, a
  * function call, a calculation) — not just a String column name.
  * Per the design, the plan IR uses `Expr` because plans carry
  * expressions, not column-name strings.
  *
  * The two types coexist intentionally:
  *   - `core.field.SortKey` is the simpler Phase 1 mirror of the
  *     spark-adapter's ordering API. Unchanged in this PR per
  *     karpathy §3 (surgical, no opportunistic refactors).
  *   - `core.rel.SortKey` is the Phase 2 sort key for the relational
  *     plan tree. Used by `RelOp.Sort` and `Engine.compile`.
  *
  * ==Why a separate type from `Expr`==
  *
  * A sort key is "an `Expr` plus a sort direction plus a null
  * ordering". The `Expr` is the value to sort by; the direction
  * is the order; the null ordering is where nulls go. Bundling
  * these into a case class lets the relational plan tree carry a
  * `List[SortKey]` (one node per ORDER BY column).
  *
  * ==Why core (engine-portable)==
  *
  * Sort keys are universal across query engines. Every engine
  * has the notion of "ascending vs descending + nulls first vs
  * nulls last". The engine-specific compile (Spark's
  * `SortKey` wrapper, Trino's `ORDER BY`, etc.) lives in the
  * engine adapter.
  *
  * ==Data-driven mantra compliance==
  *
  * - Pure data: case class (no behavior)
  * - Equality auto-derived (case class)
  * - Hash code stable (auto-derived)
  * - `Product with Serializable`
  *
  * ==Boundary contract==
  *
  * Zero Spark imports. Verifiable by:
  * `grep -r 'org.apache.spark' semanticdf-core/src/main/scala/io/semanticdf/core/rel/SortKey.scala`
  */
final case class SortKey(
    expression:   Expr,
    direction:    SortDirection,
    nullOrdering: NullOrdering,
) extends Product with Serializable