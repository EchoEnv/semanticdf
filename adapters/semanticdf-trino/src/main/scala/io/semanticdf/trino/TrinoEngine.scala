package io.semanticdf.trino

import io.semanticdf.core.engine.{Capability, Engine, EngineContext, EngineError}
import io.semanticdf.core.model.Model

/** First concrete `Engine` implementation — the Trino adapter.
  *
  * Implements the `Engine[+R]` contract from `io.semanticdf.core.engine`
  * (PR #352, #353). The result type is `Any` for now because the full
  * portable `PortableExpr` / `RelOp` IR is separate Phase 2 work
  * (per the design doc §7.2 budget: "core type/expression/relational
  * nodes and validation (900-1,200 LoC)"). Once `PortableModel` and the
  * portable IR land, `TrinoEngine[R]` will use the real result type.
  *
  * ==What this class does today==
  *
  *   - `identity = "trino"` — wire-stable engine label
  *   - `capabilities` — the set of typed `Capability` features Trino
  *     supports (e.g. NestedStructTypes, BroadcastJoin, SkewJoin)
  *   - `compile` / `execute` / `explain` — currently throw
  *     `EngineError.FeatureDeferred` with a roadmap pointer; the
  *     actual SQL lowering + cluster integration land in follow-up
  *     PRs (see `adapters/semanticdf-trino/README.md`).
  *
  * ==Boundary contract==
  *
  * Zero Spark imports. Verifiable by:
  * `grep -r 'org.apache.spark' adapters/semanticdf-trino/`
  *
  * The Trino adapter consumes ONLY the engine-portable `core` types
  * (`Engine`, `EngineContext`, `Capability`, `EngineError`). NEVER the
  * Spark-bearing originals (`io.semanticdf.predicate.Predicate`, etc.).
  * If a future contributor accidentally adds a Spark import, the
  * Trino artifact would carry a transitive Spark dependency — which
  * defeats the purpose of having a Spark-free engine adapter.
  *
  * ==Data-driven mantra compliance==
  *
  * `identity` and `capabilities` are pure data (the contract is data).
  * `compile` / `execute` / `explain` are behavior — the engine-specific
  * part. Per the data-driven mantra, behavior lives in the engine
  * adapter layer, not in core. The full implementation of these
  * methods is future Phase 2 work.
  *
  * ==Consolidation plan (NOT in this PR)==
  *
  * Once `PortableModel` and the portable IR land:
  *   - Change `Engine[Any]` to `Engine[TrinoResult]` (concrete result type)
  *   - Implement `compile(model, ctx)` — walk portable op tree, emit
  *     Trino SQL with parameter binding
  *   - Implement `execute(plan, ctx)` — JDBC/HTTP execution against
  *     a real Trino cluster
  *   - Implement `explain(model, ctx)` — return Trino's `EXPLAIN` output
  *   - Add result decoding (Trino `ResultSet` → portable `ResultRow`)
  *   - Add tests against a Docker Trino cluster (the decision gate)
  */
class TrinoEngine extends Engine[Any] {

  /** Wire-stable engine label. Renaming is a breaking change to
    * MCP clients (`describe_model`, OKF generation, `audit_log`). */
  val identity: String = "trino"

  /** The set of typed capabilities this Trino engine supports.
    *
    * These are the features Trino has natively — the Engine contract
    * uses them to:
    *   - Validate at compile time: a request that needs an
    *     unsupported capability returns `EngineError.UnsupportedCapability`
    *   - Surface to consumers (MCP `list_models` lists supported
    *     features per engine)
    *   - Adapt policies (e.g. `JoinHints.preferredStrategy = Broadcast`
    *     maps to Trino's `BROADCAST` join distribution)
    *
    * This is a closed `Set` (not a stream / future) so the adapter can
    * pre-compute it once at construction.
    *
    * Note: the closed enumeration is a deliberate starting point.
    * Capabilities not in the closed set can be added as needed
    * (e.g. `Capability.Named("trino-array-distinct")` for a Trino
    * extension that doesn't have a canonical case object). */
  val capabilities: Set[Capability] = Set(
    Capability.NestedStructTypes,
    Capability.BroadcastJoin,
    Capability.SkewJoin,
    Capability.WindowRanking,
    Capability.LateBinding,
    // Note: `Materialize` is NOT in the set — Trino doesn't have a
    // native persist() equivalent; the adapter rejects this policy
    // via `EngineError.UnsupportedCapability(Materialize, ...)`.
  )

  /** Compile a portable [[Model]] to a Trino-specific plan.
    * Deferred until the full `Model` → Trino SQL pipeline is
    * built (the Model → RelOp lowering + the RelOp → Trino SQL
    * emit). The first piece (the SqlLowerer for predicates) is
    * implemented and tested separately at `SqlLowerer.lower`; this
    * method will call it once the Model-side lowering is built. */
  def compile(model: Model, ctx: EngineContext): Either[EngineError, Any] =
    Left(EngineError.FeatureDeferred(
      feature = "trino.compile.full-model",
      release = "v0.5.0",
    ))

  /** Execute a compiled plan against a Trino cluster. Deferred —
    * requires the portable IR (`PortableExpr` / `RelOp`) and the
    * Trino JDBC driver execution path. */
  def execute(plan: Any, ctx: EngineContext): Either[EngineError, Any] =
    Left(EngineError.FeatureDeferred(
      feature = "trino.execute",
      release = "v0.5.0",
    ))

  /** Return a Trino `EXPLAIN` plan description for a portable
    * [[Model]]. Deferred until the Model → Trino SQL pipeline is
    * built (the explain output requires a real SQL query). */
  def explain(model: Model, ctx: EngineContext): Either[EngineError, String] =
    Left(EngineError.FeatureDeferred(
      feature = "trino.explain",
      release = "v0.5.0",
    ))
}

object TrinoEngine {

  /** Singleton instance — the canonical Trino engine. Used by the
    * MCP `MCPEngineRegistry` (a future Phase 2 component) to register
    * the Trino adapter by name. The singleton is the simplest viable
    * pattern; future multi-instance needs (per-tenant configs, etc.)
    * would replace this with a factory. */
  val instance: TrinoEngine = new TrinoEngine
}