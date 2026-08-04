package io.semanticdf.core.model

import io.semanticdf.core.engine.{AuditPolicy, CachePolicy, MaterializePolicy}

/** Engine-portable model-policy-defaults ADT — Phase 2 contract.
  * Mirrors the design doc §4.4.1 "ModelPolicyDefaults".
  *
  * A [[ModelPolicyDefaults]] is the aggregation of the 3 portable
  * policy ADTs ([[MaterializePolicy]] + [[CachePolicy]] +
  * [[AuditPolicy]]) that govern a model's runtime behavior.
  *
  * ==Why a separate ADT (vs. 3 fields on `Model`)==
  *
  * The 3 policies are conceptually distinct:
  *   - `materialize` controls how the model's DataFrame is materialized
  *     (None / Persist / Cache)
  *   - `cache` controls how the model's results are cached (NoCache /
  *     ReadThrough / WriteThrough)
  *   - `audit` controls how audit events are emitted (NoAudit /
  *     EmitEvents)
  *
  * Grouping them into one ADT makes the Model constructor cleaner
  * (`Model(... defaultPolicies: ModelPolicyDefaults = ...)` instead
  * of 3 separate optional fields). It also makes the validator
  * (Group 3c) simpler: "policy defaults are well-formed" is one check
  * instead of 3.
  *
  * ==Why an `object` with smart constructors==
  *
  * The design has a canonical default:
  *   - `materialize = MaterializePolicy.None`
  *   - `cache = CachePolicy.NoCache`
  *   - `audit = AuditPolicy.NoAudit`
  *
  * Per scala-data-driven-refactor §2 ("shape/validity separate"):
  * the canonical defaults live on the companion object, so callers
  * don't have to remember them.
  *
  * ==Why core (engine-portable)==
  *
  * The 3 underlying policy ADTs are in core.engine (from PR #353).
  * `ModelPolicyDefaults` aggregates them — it's a composition, not
  * a behavior. Per §1, composition lives in core.
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
  * `grep -r 'org.apache.spark' semanticdf-core/src/main/scala/io/semanticdf/core/model/ModelPolicyDefaults.scala`
  */
final case class ModelPolicyDefaults(
    materialize: MaterializePolicy,
    cache:       CachePolicy,
    audit:       AuditPolicy,
) extends Product with Serializable

object ModelPolicyDefaults {

  /** The canonical "no policy" defaults — used when the user doesn't
    * specify any policy defaults. Maps to:
    *   - `MaterializePolicy.None` (no DataFrame caching)
    *   - `CachePolicy.NoCache` (no result caching)
    *   - `AuditPolicy.NoAudit` (no audit events emitted)
    *
    * This is the conservative default; engines can override per-query
    * via the `EngineContext`. */
  val none: ModelPolicyDefaults = ModelPolicyDefaults(
    materialize = MaterializePolicy.None,
    cache       = CachePolicy.NoCache,
    audit       = AuditPolicy.NoAudit,
  )
}