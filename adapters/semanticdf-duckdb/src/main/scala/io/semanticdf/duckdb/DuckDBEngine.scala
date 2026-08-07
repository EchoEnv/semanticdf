package io.semanticdf.duckdb

import io.semanticdf.core.engine.{Capability, Engine, EngineContext, EngineError, EngineIdentity, ExecutionPlan, ParameterizedSql, PortableQueryResult, ResolvedSource, SourceResolver}
import io.semanticdf.core.model.Model
import io.semanticdf.core.schema.{SchemaField, SchemaFieldKind, SchemaSummary}

/** Second concrete `Engine` implementation — the DuckDB adapter.
  *
  * Implements the `Engine[R]` contract from `io.semanticdf.core.engine`
  * (PR #352, #353) using DuckDB as the underlying engine.
  *
  * ==What this class does==
  *
  *   - `identity = "duckdb"` — wire-stable engine label
  *   - `capabilities` — the typed `Capability` features DuckDB
  *     supports (columnar, embedded analytics, broad SQL dialect)
  *   - `compile(model, ctx)` — walks the portable `Model`, returns
  *     an `ExecutionPlan[Any]` with a `ParameterizedSql`
  *   - `execute(plan, ctx)` — opens a DuckDB connection (per-
  *     request or pool-borrowed), runs the parameterized SQL,
  *     returns the `DuckDBResult`
  *   - `explain`, `explainPlan`, `preview`, `count`,
  *     `executeAsRows`, `previewAsRows`, `schema` — the same
  *     Spark library mirrors as the Trino adapter, ported to
  *     DuckDB's dialect
  *
  * ==Why this mirrors the Trino adapter exactly==
  *
  * Per the user's standing constraint *"use similar behavior as
  * our semanticdf orginal, user no need to change code interface"*:
  * the user-facing API surface must be IDENTICAL across engine
  * adapters. A user moving from `TrinoEngine` to `DuckDBEngine`
  * should change one import + the engine construction. The
  * terminal methods (`preview`, `count`, `executeAsRows`,
  * `previewAsRows`, `schema`, `explainPlan`) keep their exact
  * signatures.
  *
  * ==Why DuckDB-specific capabilities (vs. Trino's)==
  *
  * DuckDB and Trino support similar feature sets at the SQL
  * level, but the columnar-embedded nature of DuckDB changes
  * the relative emphasis:
  *   - `Materialize` — DuckDB's persistent storage qualifies it
  *     for the Materialize capability (Trino doesn't have this).
  *   - `LateBinding` — DuckDB supports `pragma_table_info` for
  *     late-binding schema queries.
  *
  * ==Why a separate `sourceResolver` field (per §4.6)==
  *
  * Mirrors the `TrinoEngine.withSourceResolver` pattern from
  * PR #395. The DuckDB engine can consume ANY catalog adapter
  * (Unity Catalog, Hive Metastore, Glue, ...). Same composition
  * pattern — `withSourceResolver(...)` wires it in.
  *
  * ==Boundary contract==
  *
  * Zero Spark imports. Verifiable by:
  * `grep -r 'org.apache.spark' adapters/semanticdf-duckdb/src/main/scala/io/semanticdf/duckdb/DuckDBEngine.scala`
  */
class DuckDBEngine extends Engine[Any] {

  /** Wire-stable engine label. */
  val identity: String = "duckdb"

  /** Typed capabilities. Compared to Trino's set:
  *   - DuckDB has `Materialize` (persistent storage)
  *   - DuckDB lacks `WindowRanking` (no ROW_NUMBER / RANK in v1)
  *     — added in a future PR
  *   - Other capabilities are common to both engines. */
  val capabilities: Set[Capability] = Set(
    Capability.NestedStructTypes,  // DuckDB supports STRUCT
    Capability.BroadcastJoin,      // DuckDB's broadcast join
    Capability.SkewJoin,          // DuckDB's skew handling
    Capability.LateBinding,        // pragma_table_info + DESCRIBE
    Capability.Materialize,        // DuckDB has persistent storage
  )

  /** Per-capability description (typed strings). Mirrors
    * `TrinoEngine.describeCapabilities` — same pattern, same
    * use case (MCP `describe_model`). */
  override val describeCapabilities: io.semanticdf.core.engine.EngineCapabilities =
    io.semanticdf.core.engine.EngineCapabilities(
      identity           = "duckdb",
      descriptions       = Map(
        Capability.NestedStructTypes ->
          "DuckDB supports nested STRUCT types in queries and result sets",
        Capability.BroadcastJoin ->
          "DuckDB supports broadcast joins for small-side optimization",
        Capability.SkewJoin ->
          "DuckDB supports skew-aware joins for hot key detection",
        Capability.LateBinding ->
          "DuckDB supports late-binding schema queries via DESCRIBE / pragma_table_info",
        Capability.Materialize ->
          "DuckDB has persistent storage (file-based mode) — eligible for Materialize",
      ),
      supportsMaterialize = true,
    )

  /** The connection factory. None means "no DuckDB instance
    * configured" — the engine returns `EngineError.ConnectionFailed`
    * from `execute()` in that case. */
  private var _connectionFactory: Option[() => DuckDBConnection] = None

  /** Wire-stable accessor. */
  def connectionFactory: Option[() => DuckDBConnection] = _connectionFactory

  /** Configure the connection factory. Pass any thunk that
    * borrows a `DuckDBConnection` (single-connection or pool-
    * borrowed). For the pool pattern, use
    * `DuckDBConnectionPoolFactory.hikari(url)`.
    *
    * The factory is borrowed per query and closed in `finally`
    * so a failed query doesn't leak JDBC resources. For pool-
    * borrowed connections, `close()` returns to pool. */
  def withConnectionFactory(f: () => DuckDBConnection): DuckDBEngine = {
    _connectionFactory = Some(f)
    this
  }

  /** The source resolver (per multi-engine design §4.6 layer-
    * separation). None means "no catalog configured" — `compile()`
    * skips the resolution step. When set, `compile()` calls
    * `_sourceResolver.resolve(model.source)` first. */
  private var _sourceResolver: Option[SourceResolver] = None

  /** Wire-stable accessor. */
  def sourceResolver: Option[SourceResolver] = _sourceResolver

  /** Configure the source resolver. Same pattern as Trino's
    * `withSourceResolver`. */
  def withSourceResolver(resolver: SourceResolver): DuckDBEngine = {
    _sourceResolver = Some(resolver)
    this
  }

  // -- Engine trait methods --

  /** Compile a portable [[Model]] to a DuckDB-specific plan.
    *
    * Mirrors `TrinoEngine.compile` (with the same §4.6
    * source-resolution wiring from PR #395). */
  override def compile(model: Model, ctx: EngineContext): Either[EngineError, ExecutionPlan[Any]] = {
    // PR 2: when a `SourceResolver` is configured, route through
    // the new engine-portable `RelOp` flow. When no resolver is
    // configured, keep the legacy `Model → SQL` direct path.
    val engineId = EngineIdentity(
      name                 = identity,
      nativeVersion        = "1.5.5",
      engineAdapterVersion = "0.3.0",
    )
    _sourceResolver match {
      case None =>
        val sql = DuckDBQueryCompiler.instance.compile(model, Map.empty)
        Right(io.semanticdf.core.engine.ExecutionPlan[ParameterizedSql](
          engine               = engineId,
          native               = sql,
          warnings             = Nil,
          requiredCapabilities = capabilities,
          normalizedSchema     = io.semanticdf.core.engine.ResultSchema(Nil),
        ))

      case Some(resolver) =>
        io.semanticdf.core.query.QueryBuilder.build(model, resolver, engineId).map { (plan: io.semanticdf.core.rel.RelOp) =>
          val sql = DuckDBQueryCompiler.instance.compileRelOp(plan)
          io.semanticdf.core.engine.ExecutionPlan[ParameterizedSql](
            engine               = engineId,
            native               = sql,
            warnings             = Nil,
            requiredCapabilities = capabilities,
            normalizedSchema     = io.semanticdf.core.engine.ResultSchema(Nil),
          )
        }
    }
  }

  /** Compile a portable [[io.semanticdf.core.rel.RelOp]] tree
    * directly. Mirrors `TrinoEngine.compile(plan, ctx)`. */
  def compile(plan: io.semanticdf.core.rel.RelOp, ctx: EngineContext): Either[EngineError, ExecutionPlan[Any]] = {
    val engineId = EngineIdentity(
      name                 = identity,
      nativeVersion        = "1.5.5",
      engineAdapterVersion = "0.3.0",
    )
    val sql = DuckDBQueryCompiler.instance.compileRelOp(plan)
    Right(io.semanticdf.core.engine.ExecutionPlan[ParameterizedSql](
      engine               = engineId,
      native               = sql,
      warnings             = Nil,
      requiredCapabilities = capabilities,
      normalizedSchema     = io.semanticdf.core.engine.ResultSchema(Nil),
    ))
  }

  /** Execute a compiled [[ExecutionPlan]] against a DuckDB
    * instance. Mirrors `TrinoEngine.execute` exactly. */
  override def execute(plan: ExecutionPlan[Any], ctx: EngineContext): Either[EngineError, Any] = {
    _connectionFactory match {
      case None =>
        Left(EngineError.ConnectionFailed(
          reason = "no DuckDB connection factory configured; call .withConnectionFactory(...) on the engine first",
        ))
      case Some(factory) =>
        plan.native match {
          case psql: ParameterizedSql =>
            val connection = factory()
            try {
              Right(connection.prepareStatement(psql.sql, psql.parameters))
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
              reason = s"DuckDB engine expects ParameterizedSql, got: ${other.getClass.getSimpleName}",
            ))
        }
    }
  }

  /** Override `executePortable` to return a `PortableQueryResult`
    * via the `DuckDBResultEncoder`. Per the design §4.5.4
    * "portable results": MCP / cache / audit consume this shape. */
  override def executePortable(plan: ExecutionPlan[Any], ctx: EngineContext): Either[EngineError, PortableQueryResult] = {
    val encoder = new DuckDBResultEncoder
    execute(plan, ctx).flatMap { native =>
      encoder.encode(native.asInstanceOf[DuckDBResult])
        .left.map(err => EngineError.ConnectionFailed(reason = s"duckdb result-encode failed: $err"))
    }
  }

  /** Return a DuckDB `EXPLAIN` plan description for a portable
    * [[Model]]. Pure compile-time — no IO, no connection. */
  override def explain(model: Model, ctx: EngineContext): Either[EngineError, String] = {
    compile(model, ctx).map(_.native.asInstanceOf[ParameterizedSql].sql)
  }

  /** Cluster-aware EXPLAIN — runs `EXPLAIN <sql>` against DuckDB.
    * Mirrors Trino's `explainPlan`. */
  def explainPlan(model: Model, ctx: EngineContext): Either[EngineError, String] = {
    compile(model, ctx).flatMap { plan =>
      val psql = plan.native.asInstanceOf[ParameterizedSql]
      val explainSql = ParameterizedSql(
        sql        = s"EXPLAIN ${psql.sql}",
        parameters = psql.parameters,
      )
      execute(
        ExecutionPlan(engine = plan.engine, native = explainSql),
        ctx,
      ).map { raw =>
        val result = asDuckDBResult(raw)
        result.rows.map(_.head).collect {
          case io.semanticdf.core.expr.LiteralValue.StringValue(line) => line
        }.mkString("\n")
      }
    }
  }

  /** Return up to `n` rows from executing `model`. Mirrors
    * Spark's `df.limit(n)`. */
  def preview(model: Model, n: Int, ctx: EngineContext): Either[EngineError, DuckDBResult] = {
    if (n < 0) {
      Left(EngineError.ConnectionFailed(reason = "preview n must be >= 0"))
    } else {
      compile(model, ctx).flatMap { plan =>
        val psql = plan.native.asInstanceOf[ParameterizedSql]
        val limitedSql = ParameterizedSql(
          sql        = s"${psql.sql} LIMIT $n",
          parameters = psql.parameters,
        )
        execute(
          ExecutionPlan(engine = plan.engine, native = limitedSql),
          ctx,
        ).map(asDuckDBResult)
      }
    }
  }

  /** Return the row count of `model`'s results. Mirrors
    * Spark's `df.count()`. */
  def count(model: Model, ctx: EngineContext): Either[EngineError, Long] = {
    compile(model, ctx).flatMap { plan =>
      val psql = plan.native.asInstanceOf[ParameterizedSql]
      val countSql = ParameterizedSql(
        sql        = s"SELECT COUNT(*) AS __cnt FROM (${psql.sql})",
        parameters = psql.parameters,
      )
      execute(
        ExecutionPlan(engine = plan.engine, native = countSql),
        ctx,
      ).flatMap { raw =>
        val result = asDuckDBResult(raw)
        result.rows.headOption.flatMap(_.headOption) match {
          case Some(io.semanticdf.core.expr.LiteralValue.LongValue(c)) => Right(c)
          case other => Left(EngineError.ConnectionFailed(
            reason = s"unexpected count result: $other",
          ))
        }
      }
    }
  }

  /** Execute `model` and return rows as `List[Map[String, LiteralValue]]`
    * (column-name → value). Mirrors Spark's
    * `df.collect().map(_.getValuesMap(...))`. */
  def executeAsRows(model: Model, ctx: EngineContext): Either[EngineError, List[Map[String, io.semanticdf.core.expr.LiteralValue]]] = {
    compile(model, ctx).flatMap { plan =>
      execute(plan, ctx).map { raw =>
        val result = asDuckDBResult(raw)
        result.rows.map { row =>
          result.columns.zip(row).toMap
        }
      }
    }
  }

  /** Preview + executeAsRows. Mirrors Spark's `df.take(n).collect()`. */
  def previewAsRows(model: Model, n: Int, ctx: EngineContext): Either[EngineError, List[Map[String, io.semanticdf.core.expr.LiteralValue]]] = {
    preview(model, n, ctx).map { result =>
      result.rows.map { row =>
        result.columns.zip(row).toMap
      }
    }
  }

  /** Engine-portable schema summary for `model`. Pure projection
    * of the model — walks dimensions, measures,
    * calculatedMeasures, joins. Mirrors `TrinoEngine.schema`
    * (PR #392). */
  def schema(model: Model, ctx: EngineContext): Either[EngineError, SchemaSummary] = {
    Right(SchemaSummary(
      modelName        = model.name,
      modelDescription = model.description,
      fields           =
        model.dimensions.map(d => SchemaField(d.name, SchemaFieldKind.Dimension, None, d.dataType)) ++
        model.measures.map(m => SchemaField(m.name, SchemaFieldKind.Measure, None, None)) ++
        model.calculatedMeasures.map(c => SchemaField(c.name, SchemaFieldKind.CalculatedMeasure, None, None)) ++
        model.joins.map(j => SchemaField(j.name, SchemaFieldKind.JoinKey, None, None)),
    ))
  }

  // -- private helpers --

  /** Cast `execute`'s `Any` return to `DuckDBResult`. The
    * `Engine[R]` trait uses `R = Any` for DuckDB (mirroring
    * Trino's `R = Any`). The cast is safe because `execute`
    * always returns a `DuckDBResult` when the connection
    * factory is configured. */
  private def asDuckDBResult(raw: Any): DuckDBResult =
    raw.asInstanceOf[DuckDBResult]
}

object DuckDBEngine {

  /** Singleton instance — the canonical DuckDB engine (no
    * connection or resolver configured). Used by the MCP
    * `MCPEngineProvider` registry for `list_models` /
    * `describe_model` (which don't execute). For execute,
    * callers construct `new DuckDBEngine().withConnectionFactory(...)`. */
  val instance: DuckDBEngine = new DuckDBEngine
}