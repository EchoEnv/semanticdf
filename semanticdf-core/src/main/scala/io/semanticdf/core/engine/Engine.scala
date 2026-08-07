package io.semanticdf.core.engine

import io.semanticdf.core.model.Model
import io.semanticdf.core.rel.RelOp

/** Engine-portable contract for query engines —
  * Phase 2 of the multi-engine design (docs/design/multi-engine-design.md §4).
  *
  * The \`Engine[+R]\` trait is the adapter contract that every engine
  * implementation (Trino, Databricks, Snowflake, custom-platform) must
  * implement. It declares:
  *
  *   - \`identity\`: wire-stable engine label (\"trino\" / \"databricks\")
  *   - \`capabilities\`: typed \`Set[Capability]\` (which features this engine supports)
  *   - \`compile(model, ctx)\`: lower the model to an engine-specific plan
  *   - \`execute(plan, ctx)\`: run the plan and return a result
  *   - \`explain(model, ctx)\`: return a human-readable plan description
  *
  * All methods return \`Either[EngineError, ...]\` or \`EngineError\` to
  * surface compile/execute failures via the closed \`EngineError\` ADT.
  *
  * ==Why a trait (vs class / interface)==
  *
  * Per the design doc, an engine adapter IS a Scala trait that an
  * engine-specific object implements. The MCP \`MCPEngineProvider\`
  * registry holds \`Engine\` instances keyed by name. The Trino adapter
  * (\`io.semanticdf.trino.TrinoEngine\`) extends \`Engine[TrinoResult]\`;
  * the Spark adapter extends \`Engine[DataFrameResult]\` (or similar).
  *
  * ==Why core (engine-portable)==
  *
  * The contract is engine-portable — every engine implements the same
  * methods. The IMPLEMENTATIONS (Trino, Databricks, etc.) live in their
  * own engine adapter modules and depend on Spark / Trino / etc. But
  * the contract itself is data + pure function (compile / execute are
  * the engine-specific behavior; this file just declares the SHAPE of
  * the contract).
  *
  * ==Boundary contract==
  *
  * Zero Spark imports. Verifiable by:
  * \`grep -r 'org.apache.spark' semanticdf-core/src/main/scala/io/semanticdf/core/engine/Engine.scala\`
  *
  * The actual \`compile\` / \`execute\` / \`explain\` methods are
  * abstract (no body) — they return \`Nothing\` (a Scala bottom type
  * used to mark "this should never be called directly"). Engine
  * implementations override them with their own logic. This file
  * declares the SHAPE of the contract; the BODY is engine-specific.
  *
  * ==Data-driven mantra compliance==
  *
  * The trait declares abstract methods (engine-specific behavior).
  * The associated types (\`EngineError\`, \`EngineWarning\`,
  * \`Capability\`) are all pure data — they live in companion
  * sealed-ADT files. The contract is the boundary between data
  * (in core) and behavior (in engine adapters).
  *
  * ==Consolidation plan (NOT in this PR)==
  *
  * Phase 2 follow-up PRs:
  *   - Add \`EngineContext\` (typed policies: materialize, cache, audit,
  *     join hints, timeout, cancellation)
  *   - Add \`PortableQueryResult\` + \`ResultSchema\` + \`ResultRow\`
  *     (case classes for engine-neutral result shape)
  *   - Add \`PortableExpr\` + \`RelOp\` (the portable IR — separate
  *     from the fluent API's \`SemanticOp\`)
  *   - Migrate \`SemanticTableCore\` to emit portable IR + \`Engine\` calls
  */
trait Engine[R] {

  /** Wire-stable engine label surfaced in MCP \`describe_model\` and
    * OKF generation. Wire-stable string. Renaming is a breaking
    * change to MCP clients. */
  def identity: String

  /** The set of typed capabilities this engine supports. The MCP
    * server can query this to surface supported features per
    * engine in \`list_models\`. Closed \`Set\` (not a stream) so
    * adapters can pre-compute it once. */
  def capabilities: Set[Capability]

  /** Structured per-engine capability bundle \u2014 value-object view
    * of the engine's advertised features. Used by MCP
    * `describe_model` to surface supported features per engine.
    *
    * PR 9 of the 12-PR triage plan: replaces the loose
    * `Map[Capability, String]` that used to be returned. The
    * structured shape gives consumers typed fields to route on
    * (`supportedJoinKinds`, `supportsRollup`, `supportsMaterialize`)
    * instead of forcing them to parse strings.
    *
    * Default implementation derives a minimal bundle from
    * `capabilities` (no join kinds, no rollup, no materialize).
    * Engine adapters that support these features should override
    * to populate the structured fields. */
  def describeCapabilities: EngineCapabilities =
    EngineCapabilities(
      identity     = identity,
      descriptions = capabilities.map(c => c -> c.name).toMap,
    )

  /** Compile a portable [[Model]] to an engine-specific plan. Returns
    * \`Either[EngineError, R]\` — \`Left\` for any compile-time
    * failure (unsupported capability, decimal overflow, etc.).
    *
    * The actual compilation is engine-specific:
    *   - Spark adapter: walks the op tree, calls \`df.queryExecution\`
    *   - Trino adapter: emits a Trino SQL string with parameter bindings
    *   - Databricks adapter: produces a Connect plan
    *
    * Engine implementations override this. The [[Model]] is the
    * already-validated portable model (via \`Model.of\`); engine
    * adapters can rely on its invariants. */
  def compile(model: Model, ctx: EngineContext): Either[EngineError, ExecutionPlan[R]]

  /** Compile a portable [[io.semanticdf.core.rel.RelOp]] tree
    * to an engine-specific plan. Returns
    * `Either[EngineError, ExecutionPlan[R]]` — `Left` for any
    * compile-time failure.
    *
    * ==Why both `Model` AND `RelOp` overloads==
    *
    * Per the multi-engine design §4.5.1: the contract's
    * "primary" compile path takes a `RelOp` (the portable
    * relational IR), not a `Model`. The `Model` overload above
    * is provided for user ergonomics — adapters that want to skip
    * the QueryBuilder step (e.g. for performance or to test
    * hand-rolled `RelOp`s) can implement this overload directly.
    *
    * Per scala-data-driven-refactor §1 ("data is data, behavior
    * lives elsewhere"): the `RelOp` is the engine-portable data
    * shape; the engine-specific lowering is the behavior. This
    * overload makes the boundary explicit.
    *
    * ==Why `ExecutionPlan[R]` (not just `R`)==
    *
    * Per PR #398: `ExecutionPlan` is now a `sealed trait` with
    * abstract `warnings`, `requiredCapabilities`,
    * `normalizedSchema`, `isCacheable` members. The return type
    * is the inspectable shape — not just the engine-native
    * payload. This fixes the DE-5 / ARC-9 finding that
    * `compile` was returning `R` while impls were returning
    * `ExecutionPlan[R]` (a latent type mismatch hidden by
    * `R = Any`). */
  def compile(plan: RelOp, ctx: EngineContext): Either[EngineError, ExecutionPlan[R]]


  /** Run a compiled [[ExecutionPlan]] and return a portable result.
    * Returns \`Either[EngineError, R]\` — \`Left\` for any
    * execute-time failure (connection failed, query timed out,
    * source schema changed, etc.).
    *
    * The \`ExecutionPlan\` is the output of \`compile\`; its
    * \`engine\` field identifies which engine compiled it (the
    * executor verifies the identity matches its own). The \`native\`
    * field is the engine-specific compiled result. */
  def execute(plan: ExecutionPlan[R], ctx: EngineContext): Either[EngineError, R]

  /** Execute a compiled [[ExecutionPlan]] and return an
    * engine-portable `PortableQueryResult`. Per the design
    * \u00a74.5.4 "portable results": MCP, cache, and audit all
    * consume this shape (not the engine-native `R`).
    *
    * ==Why an additive overload (not a replacement)==
    *
    * The existing `execute(plan, ctx): Either[EngineError, R]`
    * stays verbatim \u2014 it's used by tests, by the `asTrinoResult`
    * / `asDuckDBResult` cast helpers, and by callers who want
    * the engine-native shape. The new overload is for callers
    * who want the portable shape (the \u00a74.5.4 contract).
    *
    * ==Default implementation==
    *
    * Engines that don't override this overload get the
    * "fall-through" behavior: the engine-native result is
    * wrapped in a one-element `PortableQueryResult` with a
    * generic schema. This is a backwards-compat default \u2014
    * adapters SHOULD override this with a real
    * `ResultEncoder` instance. */
  def executePortable(plan: ExecutionPlan[R], ctx: EngineContext): Either[EngineError, PortableQueryResult] = {
    execute(plan, ctx).map { native =>
      // Fallback: wrap the native result in a portable shape
      // with a placeholder schema. This is a last-resort
      // backward-compat path; engines with a real ResultEncoder
      // override this method.
      PortableQueryResult(
        schema   = ResultSchema(Nil),
        rows     = Vector.empty,
        metadata = Map("engine.adaptor.fallback" -> "true"),
      )
    }
  }

  /** Return a human-readable plan description (no execution).
    * Used by MCP \`explain\` tool. Returns \`Either[EngineError, String]\`.
    * The [[Model]] is the already-validated portable model. */
  def explain(model: Model, ctx: EngineContext): Either[EngineError, String]
}