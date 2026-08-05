package io.semanticdf.trino

import io.semanticdf.core.engine.{Capability, Engine, EngineContext, EngineError, EngineIdentity, ExecutionPlan}
import io.semanticdf.core.model.Model

/** First concrete `Engine` implementation — the Trino adapter.
  *
  * Implements the `Engine[R]` contract from `io.semanticdf.core.engine`
  * (PR #352, #353). The result type is `Any` for now because the full
  * portable `PortableExpr` / `RelOp` IR is separate Phase 2 work
  * (per the design doc §7.2 budget: "core type/expression/relational
  * nodes and validation (900-1,200 LoC)"). Once `PortableModel` and the
  * portable IR land, `TrinoEngine[R]` will use the real result type.
  *
  * ==What this class does==
  *
  *   - `identity = "trino"` — wire-stable engine label
  *   - `capabilities` — the set of typed `Capability` features Trino
  *     supports (e.g. NestedStructTypes, BroadcastJoin, SkewJoin)
  *   - `compile(model, ctx)` — walks the portable `Model`, returns
  *     an `ExecutionPlan[Any]` with a `ParameterizedSql` (PR #371)
  *   - `execute(plan, ctx)` — opens a Trino connection (per-request),
  *     runs the parameterized SQL, returns the `TrinoResult`
  *   - `explain(model, ctx)` — returns the parameterized SQL (a
  *     useful preview; the full Trino `EXPLAIN` output is a
  *     follow-up PR that connects to a real cluster)
  *
  * ==Why `connectionFactory` is a field (vs. `EngineContext`)==
  *
  * The Trino connection is engine-specific (not a per-query policy).
  * It belongs to the engine, not to the query context. Per the
  * data-driven mantra ("behavior lives in the engine adapter"), the
  * connection lifecycle is the engine's responsibility.
  *
  * The `connectionFactory: () => TrinoConnection` is `Option`-typed
  * so the singleton (`TrinoEngine.instance`) can exist without a
  * configured Trino cluster (useful for tests + for MCP `list_models`
  * which doesn't execute). When a query is dispatched, the engine
  * calls the factory to get a per-request connection (closed in
  * `finally`).
  *
  * ==Why no connection pool (yet)==
  *
  * Connection pooling (Apache DBCP, HikariCP, Trino's built-in pool)
  * is a follow-up optimization. For v1, the factory creates a
  * fresh connection per `execute()` call.
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
  * adapter layer, not in core.
  */
class TrinoEngine extends Engine[Any] {

  /** Wire-stable engine label. Renaming is a breaking change to
    * MCP clients (`describe_model`, OKF generation, `audit_log`). */
  val identity: String = "trino"

  /** The set of typed capabilities this Trino engine supports. */
  val capabilities: Set[Capability] = Set(
    Capability.NestedStructTypes,
    Capability.BroadcastJoin,
    Capability.SkewJoin,
    Capability.WindowRanking,
    Capability.LateBinding,
  )

  /** A description of each capability, in the Trino-specific
    * form. Useful for MCP `describe_model` output (the MCP
    * server surfaces "what does each capability mean for THIS
    * engine?" so consumers can pick the right engine for their
    * workload).
    *
    * Per scala-data-driven-refactor §1 ("data is data, behavior
    * lives elsewhere"): the descriptions are pure data (string
    * constants keyed by `Capability`). The interpretation lives
    * elsewhere (the MCP server's `describe_model` handler).
    *
    * Per the Engine trait contract: every capability in
    * `capabilities` MUST have an entry here. Future PRs that
    * add a new capability must update both sets. */
  val describeCapabilities: Map[Capability, String] = Map(
    Capability.NestedStructTypes ->
      "Trino supports nested ROW types in queries and result sets",
    Capability.BroadcastJoin ->
      "Trino supports BROADCAST join distribution for small-side optimization",
    Capability.SkewJoin ->
      "Trino supports skew-aware join optimization for hot key detection",
    Capability.WindowRanking ->
      "Trino supports window functions (ROW_NUMBER, RANK, DENSE_RANK, etc.)",
    Capability.LateBinding ->
      "Trino supports late-binding table functions via DESCRIBE at query time",
  )

  /** The connection factory. None means "no Trino cluster
    * configured" — the engine returns `EngineError.ConnectionFailed`
    * from `execute()` in that case. */
  private var _connectionFactory: Option[() => TrinoConnection] = None

  /** Set the connection factory. Called by the server bootstrap
    * once the Trino cluster is reachable. After this is set,
    * `execute()` can dispatch queries.
    *
    * Per scala-data-driven-refactor §1: this is a `var` field,
    * not a constructor parameter, because the engine is sometimes
    * constructed before the cluster is reachable (MCP server
    * startup order). The `var` allows late binding.
    *
    * Per scala-data-driven-refactor §1 ("Highest-stakes version"):
    * `var` fields are normally a Serializable smell — but this
    * field holds a `() => TrinoConnection` function (a closure),
    * not data. The engine itself doesn't cross serialization
    * boundaries (each server instance owns its own `TrinoEngine`).
    */
  def connectionFactory: Option[() => TrinoConnection] = _connectionFactory

  def withConnectionFactory(f: () => TrinoConnection): TrinoEngine = {
    _connectionFactory = Some(f)
    this
  }

  /** Compile a portable [[Model]] to a Trino-specific plan. */
  def compile(model: Model, ctx: EngineContext): Either[EngineError, ExecutionPlan[Any]] = {
    // For v1: the engine doesn't yet hold a model registry OR a
    // rollup registry, so joins and rollup selection are NOT
    // resolved at the engine level. The caller can call
    // `TrinoQueryCompiler.instance.compile(...)` directly to
    // provide the registries. Future PRs will add `modelRegistry`
    // and `rollupRegistry` fields to the engine.
    val sql = TrinoQueryCompiler.instance.compile(model, Map.empty, Map.empty, Map.empty)
    Right(ExecutionPlan(
      engine = EngineIdentity(
        name                 = identity,
        nativeVersion        = "0.286",
        engineAdapterVersion = "0.2.4",
      ),
      native = sql,
    ))
  }

  /** Execute a compiled [[ExecutionPlan]] against a Trino cluster.
    *
    * Phase 2 first end-to-end execute step (PR #372):
    *   1. Extract the `ParameterizedSql` from the plan
    *   2. Open a Trino connection (via `connectionFactory`)
    *   3. Run the prepared statement + bind parameters
    *   4. Return the `TrinoResult` (rows + columns)
    *
    * The connection is closed in `finally` so a failed query
    * doesn't leak JDBC resources.
    *
    * ==Why per-request connection (vs. shared)==
    *
    * Per the design: "Engine adapters manage their own connection
    * lifecycle." For v1, the simplest correct behavior is a fresh
    * connection per query. Future PRs add connection pooling
    * (Apache DBCP, HikariCP, or Trino's built-in pool).
    *
    * ==Why `EngineError.ConnectionFailed` (not `ExecutionFailed`)==
    *
    * The closed `EngineError` ADT (per PR #352) has the cases the
    * MCP server's error mapping handles. `ConnectionFailed(reason)`
    * covers the "no cluster configured" case. Runtime query
    * failures (e.g. a SQL syntax error from the cluster) propagate
    * as exceptions caught here and surfaced as
    * `EngineError.ConnectionFailed(reason = e.getMessage)`.
    * (Future PR: split runtime query errors into a distinct case.) */
  def execute(plan: ExecutionPlan[Any], ctx: EngineContext): Either[EngineError, Any] = {
    _connectionFactory match {
      case None =>
        Left(EngineError.ConnectionFailed(
          reason = "no Trino connection factory configured; call .withConnectionFactory(...) on the engine first",
        ))
      case Some(factory) =>
        plan.native match {
          case psql: io.semanticdf.core.engine.ParameterizedSql =>
            val connection = factory()
            try {
              val result = connection.prepareStatement(psql.sql, psql.parameters)
              Right(result)
            } catch {
              case e: Exception =>
                Left(EngineError.ConnectionFailed(
                  reason = s"execute failed: ${e.getMessage}",
                ))
            } finally {
              connection.close()
            }
          case other =>
            Left(EngineError.ConnectionFailed(
              reason = s"Trino engine expects ParameterizedSql, got: ${other.getClass.getSimpleName}",
            ))
        }
    }
  }

  /** Return a Trino `EXPLAIN` plan description for a portable
    * [[Model]]. Phase 2 first end-to-end explain: return the
    * parameterized SQL (with `?` placeholders) for now. A full
    * Trino `EXPLAIN` output (with cost estimates, partition
    * pruning, etc.) lands in a follow-up PR that connects to a
    * real cluster.
    *
    * Per the design: "explain MUST NOT execute the query." This
    * implementation is purely compile-time — no IO, no
    * connection. The Trino-specific `EXPLAIN (FORMAT JSON)` prefix
    * is a future PR. */
  def explain(model: Model, ctx: EngineContext): Either[EngineError, String] = {
    TrinoQueryCompiler.instance.compile(model, Map.empty, Map.empty, Map.empty).sql match {
      case sql: String => Right(sql)
      case _           => Left(EngineError.FeatureDeferred(
        feature = "trino.explain.full",
        release = "v0.5.0",
      ))
    }
  }
}

object TrinoEngine {

  /** Singleton instance — the canonical Trino engine (no
    * connection factory configured). Used by the MCP
    * `MCPEngineRegistry` for `list_models` / `describe_model`
    * (which don't execute). For execute, callers construct a
    * `new TrinoEngine().withConnectionFactory(...)` or set the
    * factory on the singleton before dispatch. */
  val instance: TrinoEngine = new TrinoEngine
}