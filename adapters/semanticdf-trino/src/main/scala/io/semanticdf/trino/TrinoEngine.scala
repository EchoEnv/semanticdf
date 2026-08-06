package io.semanticdf.trino

import io.semanticdf.core.engine.{Capability, Engine, EngineContext, EngineError, EngineIdentity, ExecutionPlan, ParameterizedSql, ResolvedSource, SourceResolver}
import io.semanticdf.core.model.Model
import io.semanticdf.core.schema.{SchemaField, SchemaFieldKind, SchemaSummary}

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

  /** The source resolver. None means "no catalog configured"
    * — `compile()` skips the resolution step. When set,
    * `compile()` calls `_sourceResolver.resolve(model.source)`
    * first; the resolved schema is used to validate the
    * compile-time source identity (Incompatible / NotFound →
    * `Left(EngineError.FeatureDeferred)`; Scan → compile
    * proceeds).
    *
    * ==Why a separate `SourceResolver` field==
    *
    * Per the multi-engine design §4.6 layer-separation principle:
    * the catalog layer is independent of the engine layer. A
    * `TrinoEngine` can consume ANY catalog adapter — Unity
    * Catalog, Hive Metastore, Glue, Iceberg REST, etc. — by
    * passing the corresponding `SourceResolver` impl here.
    * Default (None) means no resolver — the engine compiles
    * without schema validation.
    *
    * ==Why mutable (`var`)==
    *
    * Mirrors the `_connectionFactory` pattern below — the
    * engine is driver-local; no Spark serialization concern.
    * (The `SourceResolver` impl itself must be `Serializable`
    * — that's enforced by the `SourceResolver` trait contract
    * in core.) */
  private var _sourceResolver: Option[SourceResolver] = None

  /** Wire-stable accessor for the source resolver. Set via
    * `withSourceResolver(...)`. None means "no catalog
    * configured." */
  def sourceResolver: Option[SourceResolver] = _sourceResolver

  /** Configure the source resolver. Pass any `SourceResolver`
    * impl (Unity Catalog, Hive Metastore, Glue, etc.) — this
    * engine doesn't care which catalog adapter is wired in.
    * The resolver is consulted at every `compile(model, ctx)`
    * call: if the model references a source that the resolver
    * rejects (`Incompatible` / `NotFound`), `compile` returns
    * `Left(EngineError.FeatureDeferred)`.
    *
    * ==Why fluent (returns `this`)==
    *
    * Mirrors `withConnectionFactory` — the same fluent pattern
    * so engine configuration stays one-liner-friendly. The
    * existing singleton-hazard characterization tests (PR #387)
    * apply to this setter too.
    *
    * ==Why mutable (`_sourceResolver = Some(f)`)==
    *
    * Mirrors `withConnectionFactory`'s implementation. The
    * engine is driver-local; no Spark serialization concern.
    *
    * @param resolver the new resolver (replaces any existing one)
    * @return the same `TrinoEngine` instance (fluent) */
  def withSourceResolver(resolver: SourceResolver): TrinoEngine = {
    _sourceResolver = Some(resolver)
    this
  }

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

  /** Set the connection factory. Called by the server bootstrap
    * once the Trino cluster is reachable. After this is set,
    * `execute()` can dispatch queries.
    *
    * ==Behavior contract (pinned by `TrinoEngineFactorySpec`)==
    *
    * This method **mutates** the engine in place and returns
    * `this`. Two consequences:
    *
    *   1. Fluent usage: `new TrinoEngine().withConnectionFactory(...)`
    *      returns the same instance and works as expected.
    *
    *   2. **Singleton hazard**: do NOT call this on
    *      `TrinoEngine.instance`. The singleton is shared across
    *      all consumers; calling `withConnectionFactory` on it
    *      would mutate shared state and leak the connection
    *      factory to callers that didn't ask for it. Production
    *      code should always use `new TrinoEngine()`.
    *
    * @param f the new factory (replaces any existing one)
    * @return the same `TrinoEngine` instance (fluent) */
  def withConnectionFactory(f: () => TrinoConnection): TrinoEngine = {
    _connectionFactory = Some(f)
    this
  }

  /** Compile a portable [[Model]] to a Trino-specific plan.
    *
    * ==Source resolution flow (per multi-engine design §4.6)==
    *
    * When a `SourceResolver` is configured (via
    * `withSourceResolver(...)`), `compile()` calls it BEFORE
    * the SQL emit step:
    *   - `ResolvedSource.Scan`    → compile proceeds (resolved
    *                                schema is currently unused
    *                                by the SQL emit; the
    *                                resolution itself proves
    *                                the source identity is
    *                                valid for the configured
    *                                catalog)
    *   - `ResolvedSource.Incompatible` → `Left(FeatureDeferred)`
    *   - `ResolvedSource.NotFound`     → `Left(FeatureDeferred)`
    *   - (resolver not configured)      → compile proceeds
    *                                    (no schema validation;
    *                                    backward-compat with
    *                                    PRs <#394)
    *
    * This is the wiring that proves the §4.6 layer-separation
    * principle: any catalog adapter + this engine compose
    * cleanly. Today only Trino + Unity Catalog is wired up
    * (and tested via the integration suite); future PRs add
    * Trino + Hive Metastore, Trino + Glue, etc. without
    * changing this method.
    *
    * ==Why `FeatureDeferred` (not a new error case)==
    *
    * Per karpathy §2 ("don't add abstractions for single-use
    * code"): the `EngineError` ADT already has `FeatureDeferred`
    * for "this engine doesn't yet support that source shape".
    * Adding a new `SourceNotFound` case for a single engine
    * would be premature; the existing variant fits and the
    * MCP error mapping handles it. */
  def compile(model: Model, ctx: EngineContext): Either[EngineError, ExecutionPlan[Any]] = {
    // Step 1: source resolution (if configured). See scaladoc above.
    val resolutionResult: Either[EngineError, Unit] = _sourceResolver match {
      case None =>
        // No catalog configured — skip the resolution step
        // (backward-compat with PRs before §4.6 wiring).
        Right(())

      case Some(resolver) =>
        resolver.resolve(model.source, EngineIdentity(
          name                 = identity,
          nativeVersion        = "0.286",
          engineAdapterVersion = "0.2.4",
        )) match {
          case _: ResolvedSource.Scan =>
            // Resolved successfully — schema info is available
            // to the compile pipeline. Currently we only need
            // the resolution itself to pass; future PRs may
            // thread the schema into SQL emit.
            Right(())

          case _: ResolvedSource.NotFound =>
            Left(EngineError.FeatureDeferred(
              feature = s"trino.compile.source-not-found:${model.name}",
              release = "v0.5.0",
            ))

          case _: ResolvedSource.Incompatible =>
            Left(EngineError.FeatureDeferred(
              feature = s"trino.compile.source-incompatible:${model.name}",
              release = "v0.5.0",
            ))

          case _: ResolvedSource.AuthFailed =>
            Left(EngineError.FeatureDeferred(
              feature = s"trino.compile.source-auth-failed:${model.name}",
              release = "v0.5.0",
            ))
        }
    }

    // Step 2: SQL emit (only if resolution succeeded).
    resolutionResult.map { _ =>
      // For v1: the engine doesn't yet hold a model registry
      // OR a rollup registry, so joins and rollup selection are
      // NOT resolved at the engine level. The caller can call
      // `TrinoQueryCompiler.instance.compile(...)` directly to
      // provide the registries. Future PRs will add `modelRegistry`
      // and `rollupRegistry` fields to the engine.
      val sql = TrinoQueryCompiler.instance.compile(model, Map.empty, Map.empty, Map.empty)
      val engineId = EngineIdentity(
        name                 = identity,
        nativeVersion        = "0.286",
        engineAdapterVersion = "0.2.4",
      )
      // Per design §4.5.4 "Inspectable plans": populate
      // warnings, requiredCapabilities, normalizedSchema.
      // Trino's native plan is `ParameterizedSql` (already
      // Serializable) so `cacheable = true` is the default.
      io.semanticdf.core.engine.ExecutionPlan[ParameterizedSql](
        engine               = engineId,
        native               = sql,
        warnings             = Nil,
        requiredCapabilities = capabilities,
        normalizedSchema     = io.semanticdf.core.engine.ResultSchema(Nil),  // populated in PR 3
      )
    }
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

  /** Return the **Trino `EXPLAIN` plan** (the optimized physical
    * plan with cost estimates) as a String. Mirrors the original
    * semanticdf Spark adapter's `SemanticTable.explain(spark)`
    * behavior: calls the engine, returns the plan as text.
    *
    * ==Why this mirrors the semanticdf original==
    *
    * Per user constraint *"behavior must mirror our semanticdf
    * original"*: the original Spark adapter has two explain
    * methods (`explain()` for pure op-tree, `explain(spark)` for
    * Spark's physical plan). Both return `String`. This method
    * is the Trino equivalent of `explain(spark)` — it goes through
    * the real Trino cluster's planner to get the
    * optimizer-aware plan.
    *
    * ==Why `EXPLAIN` (default FORMAT TEXT) not `(FORMAT JSON)`==
    *
    * The original semanticdf library returns plain-text plan
    * output (via Spark's `ExplainMode.fromString("simple")`).
    * Trino's default `EXPLAIN` format is plain-text — same
    * shape as Spark's simple mode. `EXPLAIN (FORMAT JSON)` would
    * return a structured JSON for programmatic parsing — but
    * the user constraint is "mirror original", which is plain
    * text. JSON is available via separate methods in future PRs.
    *
    * ==Why a NEW method (not modifying `explain`)==
    *
    * Per karpathy §3 ("don't change what isn't asked"): the
    * existing `explain(model, ctx)` stays a pure compile-time
    * operation (no IO, no connection needed — used for the MCP
    * "list_models" / "describe_model" flow that doesn't actually
    * need a cluster). `explainPlan` is the *cluster-aware*
    * variant. Splitting them keeps each path simple.
    *
    * ==Per scala-data-driven-refactor==
    *
    * - **TYPED**: returns `Either[EngineError, String]` — errors
    *   are explicit (no `try`/`catch` leaks).
    * - **DATA-DRIVEN**: same input (model + ctx) → same output
    *   string (deterministic).
    * - **DATA-ORIENTED**: the engine's `compile` produces the
    *   input; this method consumes it. No shared mutable state.
    *
    * ==Requires a connection factory==
    *
    * Like `execute`, this method calls `connectionFactory()` to
    * borrow a JDBC connection. Configure `withConnectionFactory`
    * (or `TrinoConnectionPoolFactory.hikari(...)`) before calling. */
  def explainPlan(model: Model, ctx: EngineContext): Either[EngineError, String] = {
    compile(model, ctx).flatMap { plan =>
      val psql = plan.native.asInstanceOf[ParameterizedSql]
      // Trino's default EXPLAIN format is plain-text (analogous to
      // Spark's ExplainMode.simple). We append the prefix and let
      // Trino return rows whose first column is the plan text.
      val explainSql = ParameterizedSql(
        sql        = s"EXPLAIN ${psql.sql}",
        parameters = psql.parameters,
      )
      execute(
        ExecutionPlan(engine = plan.engine, native = explainSql),
        ctx,
      ).map { raw =>
        val result = asTrinoResult(raw)
        // EXPLAIN returns rows where column[0] is a VARCHAR
        // containing one line of the plan; concatenate into
        // a single String (matching Spark's behavior).
        result.rows.map(_.head).collect {
          case io.semanticdf.core.expr.LiteralValue.StringValue(line) => line
        }.mkString("\n")
      }
    }
  }

  /** Return a schema summary for `model` — a typed projection of
    * the model's field metadata. Mirrors the original Spark
    * library's `SemanticTableCore.schema` behavior: walk the model
    * and return one entry per field, with field name, kind, and
    * declared data type.
    *
    * ==Why `SchemaSummary` (not a DataFrame)==
    *
    * The original Spark library returns a 12-column `DataFrame`
    * describing each field. Several of those columns (`is_entity`,
    * `is_time_dimension`, `smallest_grain`) are derived from the
    * Spark op tree — they don't translate to engine-portable
    * types because the op tree is Spark-specific. The Trino
    * adapter (and every other engine adapter) instead returns the
    * engine-portable [[SchemaSummary]] — the fields derivable from
    * the [[Model]] itself. Engine-specific enrichments live in the
    * engine adapter, not in core.
    *
    * ==Why pure (no cluster round-trip)==
    *
    * Like `explain()`, this method needs no Trino cluster. It
    * walks `model.dimensions`, `model.measures`,
    * `model.calculatedMeasures`, and `model.joins` directly. The
    * result is a deterministic projection of the model — the same
    * model always produces the same `SchemaSummary`. Used by MCP
    * `describe_model` to surface field metadata without a query.
    *
    * ==Why not on the `Engine` trait==
    *
    * Per scala-data-driven-refacer §1 + karpathy §3: `schema()`
    * is engine-specific behavior. The Engine trait stays narrow
    * (`compile` / `execute` / `explain`); `schema` is adapter-
    * internal. Future adapters (Spark, Databricks, ...) each
    * implement their own. The Spark adapter would walk the op
    * tree (with is_entity / is_time_dimension enrichments) and
    * produce a richer `SchemaSummary` — the schema CONTRACT is
    * in core, the schema BEHAVIOR is in each adapter.
    *
    * ==Why `Either[EngineError, SchemaSummary]` (not `SchemaSummary`)==
    *
    * Every other `TrinoEngine` method returns
    * `Either[EngineError, T]` so consumers pattern-match on errors
    * via the closed `EngineError` ADT. Following the standing
    * pattern: same shape, same error model. The `Left` branch is
    * currently unreachable (this method is pure), but the type
    * signature stays consistent for future extensibility
    * (e.g. cluster-aware type enrichment).
    *
    * ==Mirrors Spark's `df.schema`==
    *
    * Spark's `df.schema` returns `DataFrame` of field metadata.
    * The Trino adapter's `schema` returns `SchemaSummary` of field
    * metadata. Same BEHAVIOR (walk model, return field metadata),
    * different SHAPE (typed vs DataFrame). */
  def schema(model: Model, ctx: EngineContext): Either[EngineError, SchemaSummary] = {
    val dimFields: List[SchemaField] = model.dimensions.map { d =>
      SchemaField(
        fieldName   = d.name,
        fieldKind   = SchemaFieldKind.Dimension,
        description = None,
        dataType    = d.dataType,
      )
    }
    val measureFields: List[SchemaField] = model.measures.map { m =>
      SchemaField(
        fieldName   = m.name,
        fieldKind   = SchemaFieldKind.Measure,
        description = None,
        dataType    = None,
      )
    }
    val calcFields: List[SchemaField] = model.calculatedMeasures.map { c =>
      SchemaField(
        fieldName   = c.name,
        fieldKind   = SchemaFieldKind.CalculatedMeasure,
        description = None,
        dataType    = None,
      )
    }
    val joinFields: List[SchemaField] = model.joins.flatMap { j =>
      // One JoinKey per join's source-target pair (preserves the
      // join cardinality in the metadata). Each entry carries the
      // model-side alias (None for anonymous joins).
      List(
        SchemaField(
          fieldName   = j.name,
          fieldKind   = SchemaFieldKind.JoinKey,
          description = None,
          dataType    = None,
        ),
      )
    }
    Right(SchemaSummary(
      modelName        = model.name,
      modelDescription = model.description,
      fields           = dimFields ++ measureFields ++ calcFields ++ joinFields,
    ))
  }

  /** Return up to `n` rows from executing `model`. Mirrors the
    * original Spark library's `SemanticTable.preview(n)` behavior:
    * `compile(model) -> append "LIMIT n" -> execute`.
    *
    * ==Why this exists (per user constraint: behavior must mirror
    * the original Spark library)==
    *
    * The original library exposes `preview(n)` so consumers can
    * "test a query" before committing to a full execute. This
    * method is the Trino adapter's equivalent — `compile + LIMIT n
    * + execute`. The LIMIT is appended to the parameterized SQL
    * (not parameterized as `? n`) so n is part of the SQL, not
    * a bind parameter. n is a row-count cap, not user data;
    * parameterizing it would be over-engineering.
    *
    * ==Why not on the Engine trait==
    *
    * Per scala-data-driven-refactor §1 + karpathy §3:
    * `preview()` is engine-specific behavior. The Engine trait
    * stays narrow (compile / execute / explain only); preview is
    * adapter-internal. Future adapters (Spark, Databricks, ...)
    * each implement their own. Spark's preview would call
    * `compile + dataset.limit(n)`.
    *
    * ==Why a private cast helper==
    *
    * The Engine trait's `R` parameter is `Any` for Trino (the
    * plan's `native` carries a `ParameterizedSql`, not a
    * `TrinoResult`). The trait has `execute(...): Either[..., R]`,
    * so the engine returns `Any`. For preview to return a typed
    * `TrinoResult`, we narrow at the boundary. The cast is the
    * known design wart documented in PR #372. */
  def preview(
      model: Model,
      n:     Int,
      ctx:   EngineContext,
  ): Either[EngineError, TrinoResult] = {
    if (n < 0) {
      Left(EngineError.ConnectionFailed(
        reason = s"preview n must be >= 0, got $n",
      ))
    } else {
      compile(model, ctx).flatMap { plan =>
        val psql    = plan.native.asInstanceOf[ParameterizedSql]
        val limited = ParameterizedSql(
          sql        = psql.sql + s" LIMIT $n",
          parameters = psql.parameters,
        )
        execute(
          ExecutionPlan(engine = plan.engine, native = limited),
          ctx,
        ).map(asTrinoResult)
      }
    }
  }

  /** Cast helper: narrows the trait's `R = Any` to the engine's
    * `TrinoResult` after a successful execute. Centralizes the
    * cast in one place so it's documented (vs. scattered `.asInstanceOf`
    * calls in tests).
    *
    * Per scala-data-driven-refactor §1: behavior (not data).
    * Private to TrinoEngine — only the engine knows that its
    * `R = Any` actually carries a `TrinoResult`. */
  private def asTrinoResult(x: Any): TrinoResult =
    x.asInstanceOf[TrinoResult]

  /** Return the row count of executing `model`. Mirrors the
    * original Spark library's `df.count()` pattern (e.g.
    * `SemanticTable.count()` would call `dataset.count()` on
    * the compiled DataFrame).
    *
    * ==Why this exists (per user constraint: behavior must mirror
    * the original Spark library)==
    *
    * Spark consumers write `df.count()` to get a Long. The Trino
    * equivalent is `SELECT COUNT(*) FROM (<compiled_query>)`. This
    * method provides that — the engine-level "how many rows?"
    * operation. The implementation wraps the compiled SQL in a
    * `COUNT(*)` subquery (Trino fully supports subqueries).
    *
    * ==Why not on the Engine trait==
    *
    * Per scala-data-driven-refactor §1 + karpathy §3:
    * `count()` is engine-specific behavior. The Engine trait
    * stays narrow (compile / execute / explain only); count is
    * adapter-internal. Spark's `count()` would call
    * `compile + dataset.count()`.
    *
    * ==Why a typed return (Long, not Int)==
    *
    * Spark's `DataFrame.count()` returns `Long` to handle large
    * tables (an Int overflows at ~2.1B rows). We mirror that
    * precision. Trino's `COUNT(*)` returns BIGINT which the
    * `TrinoResultDecoder` maps to `LiteralValue.LongValue`.
    *
    * ==Why SQL wrapper (not in-memory count)==
    *
    * Counting in-memory by executing the full query and calling
    * `result.rows.size` would work for small results but be
    * disastrous for large fact tables (the engine fetches
    * everything to count). The wrapper pushes the count to Trino
    * (where it can use statistics / pruning). */
  def count(
      model: Model,
      ctx:   EngineContext,
  ): Either[EngineError, Long] = {
    compile(model, ctx).flatMap { plan =>
      val psql = plan.native.asInstanceOf[ParameterizedSql]
      val countSql = ParameterizedSql(
        sql = s"""SELECT COUNT(*) AS "row_count" FROM (${psql.sql}) AS "_count_subq"""",
        parameters = psql.parameters,
      )
      execute(
        ExecutionPlan(engine = plan.engine, native = countSql),
        ctx,
      ).flatMap { raw =>
        val result = asTrinoResult(raw)
        if (result.rowCount != 1) {
          Left(EngineError.ConnectionFailed(
            reason = s"COUNT(*) must return 1 row, got ${result.rowCount}",
          ))
        } else {
          result.cell(0, 0) match {
            case Some(io.semanticdf.core.expr.LiteralValue.LongValue(n)) => Right(n)
            case Some(io.semanticdf.core.expr.LiteralValue.IntValue(n))  => Right(n.toLong)
            case other =>
              Left(EngineError.ConnectionFailed(
                reason = s"COUNT(*) returned unexpected cell: $other",
              ))
          }
        }
      }
    }
  }

  /** Compile + execute `model` and return rows as
    * `List[Map[String, LiteralValue]]` — a consumer-friendly shape
    * that mirrors Spark's `df.collect().map(_.getValuesMap(...))`
    * pattern.
    *
    * ==Why this exists (per user constraint)==
    *
    * User constraint: 'behavior must mirror original Spark library.'
    * Spark consumers typically iterate `df.collect()` rows as
    * `Map[String, Any]` for JSON serialization, MCP responses,
    * or simple logging. The Trino equivalent is the same shape
    * but with `LiteralValue` cells (portable, type-tagged).
    *
    * ==Why not on the Engine trait==
    *
    * Per scala-data-driven-refactor §1 + karpathy §3: this is
    * engine-specific consumer convenience. Spark's equivalent
    * would be `execute + collect + map(_.getValuesMap)`.
    *
    * ==Why short-circuits on compile/execute failures==
    *
    * If `compile` returns `Left`, no execution happens. If
    * `execute` returns `Left`, no transformation happens. The
    * `Either` chain preserves both error types without losing
    * detail.
    *
    * ==Why per-row map (not just TrinoResult)==
    *
    * `TrinoResult` is a column-oriented result (columns list +
    * parallel row lists). The `List[Map]` shape is
    * row-oriented — each row carries its own column names. This
    * matters for consumers that iterate rows in isolation (e.g.
    * transforming each row to JSON independently). */
  def executeAsRows(
      model: Model,
      ctx:   EngineContext,
  ): Either[EngineError, List[Map[String, io.semanticdf.core.expr.LiteralValue]]] = {
    compile(model, ctx).flatMap { plan =>
      execute(plan, ctx).map { raw =>
        val result = asTrinoResult(raw)
        result.rows.map(row => result.columns.zip(row).toMap)
      }
    }
  }

  /** Compose `preview(n)` + the row-as-map transformation of
    * `executeAsRows`. Mirrors Spark's `df.take(n).collect()`
    * pattern: get the first N rows as a row-oriented consumer
    * shape.
    *
    * ==Why this exists (per user constraint)==
    *
    * User constraint: 'behavior must mirror original Spark
    * library, with little change if necessary.' `preview(n)`
    * returns a `TrinoResult` (column-oriented); most consumers
    * want the row-oriented `List[Map]` shape. This composes
    * the two existing methods so consumers can call one method
    * instead of two.
    *
    * ==Why two-line composition is worth its own method==
    *
    * The composition IS a one-liner in the consumer:
    * `engine.preview(...).map(r => r.rows.map(row => ...))`.
    * But every consumer would write that same composition.
    * Centralizing it here:
    *   1. Names the operation (`previewAsRows` reads clearly)
    *   2. Documents the Spark mirror (df.take(n).collect())
    *   3. Provides a single contract point for tests
    *
    * ==Why not on the Engine trait==
    *
    * Same as `preview()` / `count()` / `executeAsRows()`:
    * engine-specific convenience. Adapter-internal. Each
    * adapter would compose its own. Spark's equivalent is
    * `compile + dataset.limit(n).collect() + map(_.getValuesMap)`.
    *
    * ==Why short-circuits on preview failure==
    *
    * If `preview` returns `Left`, no transformation happens.
    * The `Either` chain preserves error types without losing
    * detail. */
  def previewAsRows(
      model: Model,
      n:     Int,
      ctx:   EngineContext,
  ): Either[EngineError, List[Map[String, io.semanticdf.core.expr.LiteralValue]]] = {
    preview(model, n, ctx).map { result =>
      result.rows.map(row => result.columns.zip(row).toMap)
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