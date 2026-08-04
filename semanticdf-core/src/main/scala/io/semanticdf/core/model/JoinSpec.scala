package io.semanticdf.core.model

import io.semanticdf.core.rel.JoinKind

/** Engine-portable join-spec ADT — Phase 2 contract. Mirrors the
  * design doc §4.4.1 "JoinSpec" (the model-level declaration of how
  * this model joins to another model).
  *
  * ==Why a separate type from the existing `io.semanticdf.JoinInfo`==
  *
  * The existing spark-coupled `JoinInfo` is a DTO for MCP's
  * `describe_model` (cardinality + names + keys, computed from the
  * join metadata after the model is built). The portable
  * `JoinSpec` is the MODEL-LEVEL DECLARATION — the user writes it
  * when constructing a model, and the validator checks it. The two
  * coexist intentionally:
  *   - `JoinSpec` is the input (declarative)
  *   - `JoinInfo` is the output (descriptive)
  *
  * Per karpathy §3 (surgical, no opportunistic refactors): the
  * existing `io.semanticdf.JoinInfo` is untouched.
  *
  * ==Why `keys: List[(String, String)]`==
  *
  * The join keys are pairs of (leftKey, rightKey) — the left key
  * is a column name on this model, the right key is a column name on
  * the joined-to model. The model validator checks that both keys
  * exist in their respective schemas.
  *
  * A `List[(String, String)]` is portable (engine-portable SQL
  * engines all support equi-joins on column names). For non-equi
  * joins, the user uses a join condition expression instead (the
  * design defers non-equi joins to v0.4.0 per the deferred features
  * list).
  *
  * ==Why `rightModel: String` (not `rightModel: SourceRef`)==
  *
  * The right side of a join is another MODEL in the catalog, not a
  * raw source. The model loader resolves the model name to its
  * portable model definition, which in turn has its own source.
  *
  * ==Why core (engine-portable)==
  *
  * Join specs are universal across query engines. The engine-
  * specific compile (Spark's `join`, Trino's `JOIN`, etc.) lives in
  * the engine adapter.
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
  * `grep -r 'org.apache.spark' semanticdf-core/src/main/scala/io/semanticdf/core/model/JoinSpec.scala`
  */
final case class JoinSpec(
    name:       String,
    rightModel: String,
    kind:       JoinKind,
    keys:       List[(String, String)],
) extends Product with Serializable