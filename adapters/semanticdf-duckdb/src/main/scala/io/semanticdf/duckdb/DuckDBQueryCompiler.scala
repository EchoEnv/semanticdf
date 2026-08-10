package io.semanticdf.duckdb

import java.time.Instant

import io.semanticdf.core.engine.{ParameterizedSql, ResolvedSource}
import io.semanticdf.core.expr.{Expr, LiteralValue}
import io.semanticdf.core.model.{Model, ModelPolicyDefaults, ModelStatus, OnStalePolicy, RollupFreshnessSpec, RollupSpec, SourceRef}
import io.semanticdf.core.rel.{AggregateCall, AggregateFn}

/** Engine-specific DuckDB SQL compiler — Phase 7 second engine
  * adapter behavior.
  *
  * Walks a portable [[Model]] (the engine-portable contract from
  * `semanticdf-core`) and emits a DuckDB SQL string.
  *
  * ==What this compiler handles (v1 scope)==
  *
  *   - Dimensions (`Dimension`)               → SELECT + GROUP BY
  *   - Base measures (`Measure`)             → SELECT aggregates
  *   - Calculated measures (`CalculatedMeasure`) → SELECT post-aggregation
  *   - Filters (`FilterSpec.predicate: Expr`) → WHERE
  *
  * Mirrors [[io.semanticdf.trino.TrinoQueryCompiler]]'s v1 scope
  * (PR #368-#374). Joins and rollups are deferred — both engines
  * use the same portable model, so the deferred pieces are
  * portable concerns, not engine-specific.
  *
  * ==Why a pure function (no IO, no state)==
  *
  * Per scala-data-driven-refacer §1: the `Model` (data) lives in
  * core; the compile behavior lives in the engine adapter. This
  * compiler IS the behavior. No state, no closures, no IO — given
  * a `Model`, it produces a deterministic SQL string. Same input
  * → same output. Testable without a real DuckDB instance.
  *
  * ==Why DuckDB-specific vs. the Trino compiler==
  *
  * DuckDB's SQL dialect is mostly PostgreSQL-compatible with
  * columnar-specific tweaks. The differences that matter for us:
  *   - Identifiers are double-quoted (matches Trino)
  *   - DuckDB has `LIST` arrays, `STRUCT` types — same as Trino
  *   - DuckDB's `EXPLAIN` syntax matches Trino
  * Today the two compilers produce **near-identical SQL**. A
  * future PR can extract the shared parts into a portable
  * SQL-emit helper in `semanticdf-core`; for v1, two parallel
  * compilers is the minimum code that proves the design.
  *
  * ==Why `Map[String, SourceRef]` (not `SourceRef`) for joined models==
  *
  * Per the design's portable contract: a model's joins reference
  * source-refs by NAME. The caller resolves those names to actual
  * `SourceRef` instances via the model registry. The compiler
  * stays portable — it doesn't know which catalog holds each
  * source.
  *
  * ==Boundary contract==
  *
  * Zero Spark imports. Verifiable by:
  * `grep -r 'org.apache.spark' adapters/semanticdf-duckdb/src/main/scala/io/semanticdf/duckdb/DuckDBQueryCompiler.scala`
  */
class DuckDBQueryCompiler {

  /** Compile a portable [[Model]] to a DuckDB-specific
    * parameterized SQL statement.
    *
    * v0.3.1 (Gap 6 closure): rollup selection now mirrors Trino.
    * If `model.rollups` contains a covering fresh rollup with a
    * registered source, the FROM clause points at the rollup's
    * source and a `-- using rollup '<name>'` comment is prepended.
    * Falls back to `model.source` when no rollup covers the query
    * or all covering rollups are stale.
    *
    * Selection rules (engine-portable; mirrored from Trino):
    *   1. The rollup must be at least as coarse as the query
    *      (`queryDims ⊆ rollup.dimensions`) and answer the query
    *      (`queryMeas ⊆ rollup.measures[].name`).
    *   2. The rollup's `freshness` must permit use:
    *      - `NoTracking` → always fresh
    *      - `Track(maxStaleness, onStale)` → fresh iff
    *        `now - maxStaleness <= watermark[rollup.name]`
    *
    * @param model            the portable model
    * @param modelSources     resolved join sources by name (empty
    *                         for single-source models)
    * @param rollupSources    rollup name → rollup table's SourceRef;
    *                         empty map disables rollup selection
    * @param rollupWatermarks rollup name → last-refresh Instant;
    *                         empty map makes `Track` rollups stale
    * @param now              the current time (for the freshness
    *                         comparison); defaults to `Instant.now()`
    * @return the parameterized SQL string + bind values */
  def compile(
      model:            Model,
      modelSources:     Map[String, SourceRef]   = Map.empty,
      rollupSources:    Map[String, SourceRef]   = Map.empty,
      rollupWatermarks: Map[String, Instant]     = Map.empty,
      now:              Instant                  = Instant.now(),
  ): ParameterizedSql = {
    val params = scala.collection.mutable.ListBuffer.empty[LiteralValue]

    val selectedRollup = selectRollup(model, rollupSources, rollupWatermarks, now)
    val (effectiveSource, rollupComment) = selectedRollup match {
      case Some((rollup, rollupSource)) =>
        (rollupSource, Some(s"-- using rollup '${rollup.name}'"))
      case None =>
        (model.source, None)
    }

    val selectCols = renderSelectColumns(model, params)
    val fromClause  = renderFromClause(effectiveSource)
    val whereClause = renderWhereClause(model.filters, params)
    val groupByClause = renderGroupByClause(model, params)

    val parts = List(
      rollupComment,
      Some(s"SELECT ${selectCols.mkString(", ")}"),
      Some(s"FROM $fromClause"),
      whereClause.map(w => s"WHERE $w"),
      groupByClause.map(g => s"GROUP BY $g"),
    ).flatten

    ParameterizedSql(sql = parts.mkString(" "), parameters = params.toList)
  }

  /** Select a rollup that covers the query and is fresh.
    *
    * Returns `Some((rollup, source))` for the first covering
    * fresh rollup; `None` if no rollup covers the query or all
    * covering rollups are stale.
    *
    * Per scala-data-driven-refacer §3 (sealed ADT over Map):
    * `RollupFreshnessSpec` and `OnStalePolicy` are exhaustively
    * matched. Mirrors [[io.semanticdf.trino.TrinoQueryCompiler]]
    * exactly so behavior is consistent across engines. */
  private def selectRollup(
      model:            Model,
      rollupSources:    Map[String, SourceRef],
      rollupWatermarks: Map[String, Instant],
      now:              Instant,
  ): Option[(RollupSpec, SourceRef)] = {
    val queryDimNames  = model.dimensions.map(_.name).toSet
    val queryMeasNames = model.measures.map(_.name).toSet

    model.rollups.iterator.flatMap { rollup =>
      val coversDims  = queryDimNames.forall(rollup.dimensions.contains)
      val coversMeas  = queryMeasNames.forall(m => rollup.measures.exists(_.name == m))
      if (!coversDims || !coversMeas) None
      else if (isFresh(rollup, rollupWatermarks, now))
        rollupSources.get(rollup.name).map(source => (rollup, source))
      else None
    }.toList.headOption
  }

  /** Check if a rollup is fresh per its `RollupFreshnessSpec`.
    * Mirrors the Trino engine (PR #418 closed Gap 1's broader
    * portable compile path). */
  private def isFresh(
      rollup:           RollupSpec,
      rollupWatermarks: Map[String, Instant],
      now:              Instant,
  ): Boolean = rollup.freshness match {
    case RollupFreshnessSpec.NoTracking => true
    case RollupFreshnessSpec.Track(maxStaleness, _) =>
      rollupWatermarks.get(rollup.name) match {
        case Some(watermark) => watermark.isAfter(now.minus(maxStaleness))
        case None            => false
      }
  }

  // -- SELECT clause --

  private def renderSelectColumns(
      model:  Model,
      params: scala.collection.mutable.ListBuffer[LiteralValue],
  ): List[String] = {
    val dimCols   = model.dimensions.map(d => s""""${d.name}"""")
    val measCols  = model.measures.map(m => renderMeasure(m, params, model))
    val calcCols  = model.calculatedMeasures.map(c => s""""${c.name}" = ${renderExpr(c.expr, params, model)}""")
    (dimCols ++ measCols ++ calcCols).toList
  }

  /** Compile a portable [[io.semanticdf.core.rel.RelOp]] tree
    * to a DuckDB SQL string. The engine-portable path per
    * `Engine.compile(plan, ctx)`. Mirrors `TrinoQueryCompiler.compileRelOp`. */
  /** v0.3.1 (Gap 3 closure): thread `modelSources` through compileRelOp
    * for parity with [[TrinoQueryCompiler.compileRelOp]]. Per scala-
    * impact-analysis §1: the signature change ripples to every caller;
    * both call sites in [[DuckDBEngine]] updated.
    *
    * Note: the DuckDB synthetic-model path (compileRelOp -> relOpToModel
    * -> compile) doesn't currently emit JOIN clauses for `RelOp.Join`.
    * Joins in DuckDB go through the model-level compile path
    * (`compile(model, modelSources, ...)`), which reads `model.joins`.
    * For hand-built RelOp plans with Join nodes, use Trino's
    * compileRelOp (full support) or extend DuckDB's relOpToModel.
    * Tracked in docs/design/v0.3.1-feature-parity-backlog.md Gap 3. */
  def compileRelOp(
      plan:         io.semanticdf.core.rel.RelOp,
      modelSources: Map[String, SourceRef] = Map.empty,
  ): ParameterizedSql = {
    val params = scala.collection.mutable.ListBuffer.empty[LiteralValue]
    // v1 scope: the same 7 RelOp cases as Trino. We re-use the
    // existing Model-walking renderers via a synthetic Model
    // reconstruction. Future PR: split the renderers into
    // RelOp-specific code.
    val synthetic = relOpToModel(plan)
    compile(synthetic, modelSources).copy(parameters = params.toList)
  }

  /** Build a `Model` view of a `RelOp` for the legacy renderers.
    * v1 only — full RelOp-native rendering lands in a future PR. */
  private def relOpToModel(plan: io.semanticdf.core.rel.RelOp): Model = {
    // For v1: extract the source's SourceRef from RelOp.Scan and
    // build a minimal Model. The legacy compile() doesn't use
    // dimensions / measures / calculatedMeasures from the
    // Model — it walks the source's ResolvedSource to build SQL.
    // This is a transitional helper; future PRs render RelOp
    // natively.
    val source = plan match {
      case io.semanticdf.core.rel.RelOp.Scan(s, _, _) =>
        s match {
          case ResolvedSource.Scan(s2, _) => s2
          case _                          =>
            SourceRef.ByName(catalog = None, namespace = None, table = "")
        }
      case _ =>
        SourceRef.ByName(catalog = None, namespace = None, table = "")
    }
    Model.of(
      name               = "synthetic",
      source             = source,
      dimensions         = Nil,
      measures           = Nil,
      calculatedMeasures = Nil,
      joins              = Nil,
      defaultPolicies    = ModelPolicyDefaults.none,
      status             = ModelStatus.Draft,
    ) match {
      case Right(m) => m
      case Left(err) => throw new RuntimeException(s"relOpToModel failed: $err")
    }
  }

  private def renderMeasure(
      m:      io.semanticdf.core.model.Measure,
      params: scala.collection.mutable.ListBuffer[LiteralValue],
      model:  io.semanticdf.core.model.Model,
  ): String = {
    val alias = s""""${m.name}""""
    m.expr.fn match {
      case AggregateFn.Sum     => s"SUM(${renderExpr(m.expr.input.getOrElse(Expr.FieldRef(m.name)), params, model)}) AS $alias"
      case AggregateFn.Count   => s"COUNT(*) AS $alias"
      case AggregateFn.CountDistinct => s"COUNT(DISTINCT ${renderExpr(m.expr.input.getOrElse(Expr.FieldRef(m.name)), params, model)}) AS $alias"
      case AggregateFn.Avg     => s"AVG(${renderExpr(m.expr.input.getOrElse(Expr.FieldRef(m.name)), params, model)}) AS $alias"
      case AggregateFn.Min     => s"MIN(${renderExpr(m.expr.input.getOrElse(Expr.FieldRef(m.name)), params, model)}) AS $alias"
      case AggregateFn.Max     => s"MAX(${renderExpr(m.expr.input.getOrElse(Expr.FieldRef(m.name)), params, model)}) AS $alias"
      // -- v0.3.1 backward-compat: advanced aggregates --
      // DuckDB's syntax differs from the portable case-object names
      // (e.g. portable StddevPopulation → DuckDB STDDEV_POP; portable
      // PercentileContinuous → DuckDB QUANTILE_CONT). The previous
      // fallback `case other => other.toString.toUpperCase(input)`
      // emitted WRONG SQL for several cases (STDDEVSAMPLE instead of
      // STDDEV_SAMP, PERCENTILECONTINUOUS instead of QUANTILE_CONT,
      // etc.) — silent SQL correctness bug. These explicit mappings
      // close Gap 5 in docs/design/v0.3.1-feature-parity-backlog.md.
      //
      // Percentile* hardcodes 0.5 (median). Future: extend the
      // portable AggregateCall shape with a percentile arg so the
      // user's intended percentile is preserved. Tracked as a
      // follow-on.
      case AggregateFn.StddevSample        => s"STDDEV_SAMP(${renderExpr(m.expr.input.getOrElse(Expr.FieldRef(m.name)), params, model)}) AS $alias"
      case AggregateFn.StddevPopulation    => s"STDDEV_POP(${renderExpr(m.expr.input.getOrElse(Expr.FieldRef(m.name)), params, model)}) AS $alias"
      case AggregateFn.VarianceSample      => s"VAR_SAMP(${renderExpr(m.expr.input.getOrElse(Expr.FieldRef(m.name)), params, model)}) AS $alias"
      case AggregateFn.VariancePopulation   => s"VAR_POP(${renderExpr(m.expr.input.getOrElse(Expr.FieldRef(m.name)), params, model)}) AS $alias"
      case AggregateFn.Median               => s"MEDIAN(${renderExpr(m.expr.input.getOrElse(Expr.FieldRef(m.name)), params, model)}) AS $alias"
      case AggregateFn.PercentileContinuous => s"QUANTILE_CONT(${renderExpr(m.expr.input.getOrElse(Expr.FieldRef(m.name)), params, model)}, 0.5) AS $alias"
      case AggregateFn.PercentileDiscrete   => s"QUANTILE_DISC(${renderExpr(m.expr.input.getOrElse(Expr.FieldRef(m.name)), params, model)}, 0.5) AS $alias"
      case AggregateFn.ApproxPercentile     => s"APPROX_QUANTILE(${renderExpr(m.expr.input.getOrElse(Expr.FieldRef(m.name)), params, model)}, 0.5) AS $alias"
      case AggregateFn.First                => s"FIRST(${renderExpr(m.expr.input.getOrElse(Expr.FieldRef(m.name)), params, model)}) AS $alias"
      case AggregateFn.Last                 => s"LAST(${renderExpr(m.expr.input.getOrElse(Expr.FieldRef(m.name)), params, model)}) AS $alias"
      case other               =>
        // Truly unknown aggregate fn (future additions to the
        // sealed ADT). Fall back to the case-object name (uppercased).
        // If the rendered SQL is wrong, the user sees a DuckDB
        // syntax error at execute time — loud failure at the
        // engine boundary, not silent incorrect SQL.
        s"${other.toString.toUpperCase}(${renderExpr(m.expr.input.getOrElse(Expr.FieldRef(m.name)), params, model)}) AS $alias"
    }
  }

  // -- FROM clause --

  private def renderFromClause(source: SourceRef): String = source match {
    case SourceRef.ByName(catalog, namespace, table) =>
      // DuckDB supports 1-, 2-, or 3-part names. We emit the
      // minimum the source can express: bare table for
      // unspecified catalog+namespace, schema.table for
      // namespace only, or full 3-part for explicit catalog.
      // We don't hardcode "memory" / "main" so the demo
      // works for in-memory, file-based, and shared-cache DBs.
      (catalog, namespace) match {
        case (Some(c), Some(s)) => s""""$c"."$s"."$table""""
        case (None,    Some(s)) => s""""$s"."$table""""
        case (_,       None)    => s""""$table""""
      }
    case _: SourceRef.ByPath =>
      // Resolver would have rejected ByPath; we shouldn't reach
      // here. Emit a placeholder that surfaces the error at
      // execute time.
      "-- unsupported source: ByPath"
    case _: SourceRef.ByProvider =>
      "-- unsupported source: ByProvider"
  }

  // -- WHERE clause --

  private def renderWhereClause(
      filters: List[io.semanticdf.core.model.FilterSpec],
      params:  scala.collection.mutable.ListBuffer[LiteralValue],
  ): Option[String] = {
    if (filters.isEmpty) None
    else {
      val rendered = filters.map(f => s"(${renderExpr(f.predicate, params)})")
      Some(rendered.mkString(" AND "))
    }
  }

  // -- GROUP BY clause --

  private def renderGroupByClause(
      model:  Model,
      params: scala.collection.mutable.ListBuffer[LiteralValue],
  ): Option[String] = {
    if (model.dimensions.isEmpty) None
    else {
      val dimExprs = model.dimensions.map(d => s""""${d.name}"""")
      // Per scala-spark-batch-bugs §3 (schema drift): when a
      // calculated measure references Expr.All, include the base
      // measures' input expressions in GROUP BY so per-row data
      // survives aggregation. Same pattern as TrinoQueryCompiler.
      val usesAll = model.calculatedMeasures.exists { cm =>
        def go(e: io.semanticdf.core.expr.Expr): Boolean = e match {
          case io.semanticdf.core.expr.Expr.All(_) => true
          case io.semanticdf.core.expr.Expr.Not(e1)             => go(e1)
          case io.semanticdf.core.expr.Expr.IsNull(e1)          => go(e1)
          case io.semanticdf.core.expr.Expr.IsNotNull(e1)       => go(e1)
          case io.semanticdf.core.expr.Expr.Add(l, r)          => go(l) || go(r)
          case io.semanticdf.core.expr.Expr.Subtract(l, r)     => go(l) || go(r)
          case io.semanticdf.core.expr.Expr.Multiply(l, r)     => go(l) || go(r)
          case io.semanticdf.core.expr.Expr.Divide(l, r)       => go(l) || go(r)
          case io.semanticdf.core.expr.Expr.Modulo(l, r)       => go(l) || go(r)
          case io.semanticdf.core.expr.Expr.Equal(l, r)        => go(l) || go(r)
          case io.semanticdf.core.expr.Expr.NotEqual(l, r)     => go(l) || go(r)
          case io.semanticdf.core.expr.Expr.LessThan(l, r)    => go(l) || go(r)
          case io.semanticdf.core.expr.Expr.LessOrEqual(l, r) => go(l) || go(r)
          case io.semanticdf.core.expr.Expr.GreaterThan(l, r)    => go(l) || go(r)
          case io.semanticdf.core.expr.Expr.GreaterOrEqual(l, r) => go(l) || go(r)
          case io.semanticdf.core.expr.Expr.And(l, r)        => go(l) || go(r)
          case io.semanticdf.core.expr.Expr.Or(l, r)         => go(l) || go(r)
          case io.semanticdf.core.expr.Expr.FunctionCall(_, args) => args.exists(go)
          case _ => false
        }
        go(cm.expr)
      }
      val inputExprs: List[String] =
        if (usesAll) model.measures.flatMap(_.expr.input).distinct.map(renderExpr(_, params, model))
        else Nil
      Some((dimExprs ++ inputExprs).mkString(", "))
    }
  }

  // -- Expression rendering --

  /** Render a portable `Expr` to a DuckDB SQL fragment. */
  private def renderExpr(
      expr:   Expr,
      params: scala.collection.mutable.ListBuffer[LiteralValue] = scala.collection.mutable.ListBuffer.empty,
      model:  Model = null,
  ): String = expr match {
    case Expr.FieldRef(name)        => s""""$name""""
    case Expr.MeasureRef(name)      => s""""$name""""
    case Expr.Literal(value, _)     => renderLiteral(value, params)
    case Expr.Add(l, r)             => s"(${renderExpr(l, params, model)} + ${renderExpr(r, params, model)})"
    case Expr.Subtract(l, r)        => s"(${renderExpr(l, params, model)} - ${renderExpr(r, params, model)})"
    case Expr.Multiply(l, r)        => s"(${renderExpr(l, params, model)} * ${renderExpr(r, params, model)})"
    case Expr.Divide(l, r)          => s"(${renderExpr(l, params, model)} / ${renderExpr(r, params, model)})"
    case Expr.Modulo(l, r)          => s"(${renderExpr(l, params)} % ${renderExpr(r, params)})"
    case Expr.Equal(l, r)           => s"(${renderExpr(l, params)} = ${renderExpr(r, params)})"
    case Expr.NotEqual(l, r)        => s"(${renderExpr(l, params)} != ${renderExpr(r, params)})"
    case Expr.LessThan(l, r)        => s"(${renderExpr(l, params)} < ${renderExpr(r, params)})"
    case Expr.LessOrEqual(l, r)     => s"(${renderExpr(l, params)} <= ${renderExpr(r, params)})"
    case Expr.GreaterThan(l, r)     => s"(${renderExpr(l, params)} > ${renderExpr(r, params)})"
    case Expr.GreaterOrEqual(l, r)  => s"(${renderExpr(l, params)} >= ${renderExpr(r, params)})"
    case Expr.And(l, r)             => s"(${renderExpr(l, params)} AND ${renderExpr(r, params)})"
    case Expr.Or(l, r)              => s"(${renderExpr(l, params)} OR ${renderExpr(r, params)})"
    case Expr.Not(e)                => s"NOT (${renderExpr(e, params, model)})"
    case Expr.All(measureName) =>
      // v0.3.1 (Gap 2 closure): portable Expr.All lowerer.
      // Resolves to the alias of the named measure — the GROUP BY
      // clause (extended to include measure inputs in
      // renderGroupByClause when `All` is detected) computes the
      // per-group measure value first, and the calculated-measure
      // expression reads from that alias.
      val _ = model.measures.find(_.name == measureName).getOrElse(
        throw new IllegalArgumentException(
          s"DuckDBQueryCompiler.renderExpr: Expr.All('$measureName') references an unknown measure",
        ),
      )
      "\"" + measureName + "\""
    case other                      => s"-- unsupported expr: ${other.getClass.getSimpleName}"
  }

  /** Render a `LiteralValue` as a parameter placeholder, recording
    * the value in `params` (1-indexed `?` placeholders). */
  private def renderLiteral(
      value:  LiteralValue,
      params: scala.collection.mutable.ListBuffer[LiteralValue],
  ): String = {
    val idx = params.size + 1
    params += value
    s"?$idx"
  }
}

object DuckDBQueryCompiler {

  /** Singleton instance — matches `TrinoQueryCompiler.instance`.
    * Per the existing pattern: the compiler is stateless and
    * pure; a singleton saves allocation overhead per compile. */
  val instance: DuckDBQueryCompiler = new DuckDBQueryCompiler()
}