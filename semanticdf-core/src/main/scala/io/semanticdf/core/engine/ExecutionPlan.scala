package io.semanticdf.core.engine

/** Engine-portable execution-plan ADT — Phase 2 contract. Mirrors
  * the design doc §4.5.1 "ExecutionPlan" (the wrapper around an
  * engine-specific compiled result).
  *
  * An [[ExecutionPlan]] is the output of `Engine.compile` and the
  * input of `Engine.execute`. It carries:
  *   - the engine's [[EngineIdentity]] (for provenance + diagnostics)
  *   - the engine-specific compiled result (`R` — the engine's
  *     native plan, e.g. a Trino SQL string + parameter bindings,
  *     a Spark Dataset op tree, a Databricks Connect plan)
  *
  * ==Why a wrapper (vs. just `R`)==
  *
  * The wrapper adds the engine identity to every compiled plan.
  * This is essential for:
  *   - Diagnostics: when an execution fails, the error message
  *     names the engine that compiled the plan
  *   - Audit: the audit log records which engine compiled each
  *     plan
  *   - Multi-engine: a plan compiled by one engine cannot be
  *     executed by another (the wrapper enforces this — the
  *     identity wouldn't match)
  *
  * ==Why `R` is unbounded==
  *
  * `R` is the engine's native plan type — Trino uses `String`
  * (SQL + parameter bindings), Spark uses `DataFrame` (op tree),
  * Databricks uses its Connect plan. Forcing a Serializable
  * bound would prevent non-Serializable native types (Databricks
  * Connect plans, for example, may not be Serializable). In
  * practice, the engine adapters ensure their `R` is Serializable
  * (so the ExecutionPlan can survive wire-format round-trip); the
  * type system doesn't enforce it here.
  *
  * ==Why `Product with Serializable`==
  *
  * The case class itself is Serializable. The `R` field is
  * preserved through Java serialization IF R is Serializable.
  * For non-Serializable R (e.g. Databricks Connect plans), the
  * adapter must implement custom serialization.
  *
  * ==Why core (engine-portable)==
  *
  * The wrapper SHAPE (engine identity + native result) is universal
  * across engines. The CONTENT of `R` is engine-specific.
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
  * `grep -r 'org.apache.spark' semanticdf-core/src/main/scala/io/semanticdf/core/engine/ExecutionPlan.scala`
  */
final case class ExecutionPlan[R](
    engine: EngineIdentity,
    native: R,
) extends Product with Serializable