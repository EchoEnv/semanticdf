package io.semanticdf.trino

import io.semanticdf.core.predicate.{Predicate => CorePredicate}

/** Phase 2 entry point — Trino engine adapter.
  *
  * This is a STRUCTURAL placeholder. The actual SQL lowering + source
  * resolution + result decoding land in follow-up PRs (see the
  * module README for the full roadmap). What this file establishes:
  *
  *   1. The class lives under `io.semanticdf.trino` — the package
  *      boundary for engine adapters.
  *   2. It imports from `io.semanticdf.core.predicate` (the
  *      engine-portable ADT) — NEVER from `io.semanticdf.predicate`
  *      (the Spark-bearing original) or `io.semanticdf` (the
  *      fluent API). The boundary is enforced at the import level.
  *   3. It compiles today (no methods to implement yet, just a
  *      class declaration that takes the engine-portable `Predicate`
  *      type and declares the production API).
  *
  * ==Why a placeholder==
  *
  * The full `Engine[R]` trait (compile + execute + capabilities +
  * identity) is Phase 2 design work that needs the design doc's
  * `Engine` trait to exist first. The trait is the Phase 2 contract;
  * the Trino adapter is the Phase 2 implementation.
  *
  * Per the multi-engine design (§7.1): "Trino decision gate (POC must
  * work before committing to Phase 2)" — the decision gate requires
  * a real Trino cluster. Setting up the project structure (this PR)
  * is the prerequisite for any actual POC work.
  *
  * ==Boundary contract==
  *
  * This file compiles with zero `org.apache.spark.*` imports. Verifiable by:
  * `grep -r 'org.apache.spark' adapters/semanticdf-trino/`
  *
  * ==Data-driven mantra compliance==
  *
  * No behavior methods yet — the class is purely structural. Future
  * PRs will add SQL lowering (engine-specific behavior, lives in
  * the engine adapter layer) and source resolution (engine-specific
  * behavior). Per scala-data-driven-refactor, the data is already in
  * core; this adapter will only consume it.
  */
class TrinoEngine {

  /** Engine identity — wire-side label surfaced in MCP `describe_model`
    * and OKF generation. Wire-stable string ("trino"). Renaming is
    * a breaking change to MCP clients. */
  val identity: String = "trino"

  /** The Trino engine takes a portable `Predicate` (from core) and
    * will eventually lower it to Trino SQL. This method signature is
    * a placeholder for the future SQL lowering step.
    *
    * The actual lowering algorithm:
    *   1. Walk the `CorePredicate` tree (Compare, In, IsNull, And, Or, Not).
    *   2. Emit the equivalent Trino SQL: `field = value`,
    *      `field IN (v1, v2)`, `field IS NULL`, `AND`/`OR`/`NOT`.
    *   3. Return the SQL string + parameter bindings.
    *
    * For now this just proves the API shape. A follow-up PR adds
    * the lowering implementation. */
  def lower(predicate: CorePredicate): String = {
    throw new NotImplementedError(
      "Trino SQL lowering is Phase 2 work — see " +
      "adapters/semanticdf-trino/README.md for the roadmap."
    )
  }
}