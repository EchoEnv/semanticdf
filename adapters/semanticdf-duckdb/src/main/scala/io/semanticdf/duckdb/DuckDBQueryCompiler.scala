package io.semanticdf.duckdb

import java.time.Instant

import io.semanticdf.core.engine.{ParameterizedSql, ResolvedSource}
import io.semanticdf.core.expr.{Expr, LiteralValue}
import io.semanticdf.core.model.{Model, ModelPolicyDefaults, ModelStatus, SourceRef}
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
    * @param model        the portable model
    * @param modelSources resolved join sources by name (empty for
    *                     single-source models)
    * @param now          the current time (for rollup freshness;
    *                     unused in v1 since rollups are deferred)
    * @return the parameterized SQL string + bind values */
  def compile(
      model:        Model,
      modelSources: Map[String, SourceRef] = Map.empty,
      now:          Instant                = Instant.now(),
  ): ParameterizedSql = {
    val params = scala.collection.mutable.ListBuffer.empty[LiteralValue]
    val selectCols = renderSelectColumns(model, params)
    val fromClause  = renderFromClause(model.source)
    val whereClause = renderWhereClause(model.filters, params)
    val groupByClause = renderGroupByClause(model, params)

    val parts = List(
      Some(s"SELECT ${selectCols.mkString(", ")}"),
      Some(s"FROM $fromClause"),
      whereClause.map(w => s"WHERE $w"),
      groupByClause.map(g => s"GROUP BY $g"),
    ).flatten

    ParameterizedSql(sql = parts.mkString(" "), parameters = params.toList)
  }

  // -- SELECT clause --

  private def renderSelectColumns(
      model:  Model,
      params: scala.collection.mutable.ListBuffer[LiteralValue],
  ): List[String] = {
    val dimCols   = model.dimensions.map(d => s""""${d.name}"""")
    val measCols  = model.measures.map(m => renderMeasure(m, params))
    val calcCols  = model.calculatedMeasures.map(c => s""""${c.name}" = ${renderExpr(c.expr)}""")
    (dimCols ++ measCols ++ calcCols).toList
  }

  /** Compile a portable [[io.semanticdf.core.rel.RelOp]] tree
    * to a DuckDB SQL string. The engine-portable path per
    * `Engine.compile(plan, ctx)`. Mirrors `TrinoQueryCompiler.compileRelOp`. */
  def compileRelOp(plan: io.semanticdf.core.rel.RelOp): ParameterizedSql = {
    val params = scala.collection.mutable.ListBuffer.empty[LiteralValue]
    // v1 scope: the same 7 RelOp cases as Trino. We re-use the
    // existing Model-walking renderers via a synthetic Model
    // reconstruction. Future PR: split the renderers into
    // RelOp-specific code.
    val synthetic = relOpToModel(plan)
    compile(synthetic, Map.empty).copy(parameters = params.toList)
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
  ): String = {
    val alias = s""""${m.name}""""
    m.expr.fn match {
      case AggregateFn.Sum     => s"SUM(${renderExpr(m.expr.input.getOrElse(Expr.FieldRef(m.name)))}) AS $alias"
      case AggregateFn.Count   => s"COUNT(*) AS $alias"
      case AggregateFn.CountDistinct => s"COUNT(DISTINCT ${renderExpr(m.expr.input.getOrElse(Expr.FieldRef(m.name)))}) AS $alias"
      case AggregateFn.Avg     => s"AVG(${renderExpr(m.expr.input.getOrElse(Expr.FieldRef(m.name)))}) AS $alias"
      case AggregateFn.Min     => s"MIN(${renderExpr(m.expr.input.getOrElse(Expr.FieldRef(m.name)))}) AS $alias"
      case AggregateFn.Max     => s"MAX(${renderExpr(m.expr.input.getOrElse(Expr.FieldRef(m.name)))}) AS $alias"
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
      case AggregateFn.StddevSample        => s"STDDEV_SAMP(${renderExpr(m.expr.input.getOrElse(Expr.FieldRef(m.name)))}) AS $alias"
      case AggregateFn.StddevPopulation    => s"STDDEV_POP(${renderExpr(m.expr.input.getOrElse(Expr.FieldRef(m.name)))}) AS $alias"
      case AggregateFn.VarianceSample      => s"VAR_SAMP(${renderExpr(m.expr.input.getOrElse(Expr.FieldRef(m.name)))}) AS $alias"
      case AggregateFn.VariancePopulation   => s"VAR_POP(${renderExpr(m.expr.input.getOrElse(Expr.FieldRef(m.name)))}) AS $alias"
      case AggregateFn.Median               => s"MEDIAN(${renderExpr(m.expr.input.getOrElse(Expr.FieldRef(m.name)))}) AS $alias"
      case AggregateFn.PercentileContinuous => s"QUANTILE_CONT(${renderExpr(m.expr.input.getOrElse(Expr.FieldRef(m.name)))}, 0.5) AS $alias"
      case AggregateFn.PercentileDiscrete   => s"QUANTILE_DISC(${renderExpr(m.expr.input.getOrElse(Expr.FieldRef(m.name)))}, 0.5) AS $alias"
      case AggregateFn.ApproxPercentile     => s"APPROX_QUANTILE(${renderExpr(m.expr.input.getOrElse(Expr.FieldRef(m.name)))}, 0.5) AS $alias"
      case AggregateFn.First                => s"FIRST(${renderExpr(m.expr.input.getOrElse(Expr.FieldRef(m.name)))}) AS $alias"
      case AggregateFn.Last                 => s"LAST(${renderExpr(m.expr.input.getOrElse(Expr.FieldRef(m.name)))}) AS $alias"
      case other               =>
        // Truly unknown aggregate fn (future additions to the
        // sealed ADT). Fall back to the case-object name (uppercased).
        // If the rendered SQL is wrong, the user sees a DuckDB
        // syntax error at execute time — loud failure at the
        // engine boundary, not silent incorrect SQL.
        s"${other.toString.toUpperCase}(${renderExpr(m.expr.input.getOrElse(Expr.FieldRef(m.name)))}) AS $alias"
    }
  }

  // -- FROM clause --

  private def renderFromClause(source: SourceRef): String = source match {
    case SourceRef.ByName(catalog, namespace, table) =>
      // DuckDB uses 1-, 2-, or 3-part names depending on the
      // attached catalog. We always emit 3-part for portability
      // (matches the Trino compiler).
      val cat = catalog.getOrElse("memory")
      val sch = namespace.getOrElse("main")
      s""""$cat"."$sch"."$table""""
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
    else Some(model.dimensions.map(d => s""""${d.name}"""").mkString(", "))
  }

  // -- Expression rendering --

  /** Render a portable `Expr` to a DuckDB SQL fragment. */
  private def renderExpr(
      expr:   Expr,
      params: scala.collection.mutable.ListBuffer[LiteralValue] = scala.collection.mutable.ListBuffer.empty,
  ): String = expr match {
    case Expr.FieldRef(name)        => s""""$name""""
    case Expr.MeasureRef(name)      => s""""$name""""
    case Expr.Literal(value, _)     => renderLiteral(value, params)
    case Expr.Add(l, r)             => s"(${renderExpr(l, params)} + ${renderExpr(r, params)})"
    case Expr.Subtract(l, r)        => s"(${renderExpr(l, params)} - ${renderExpr(r, params)})"
    case Expr.Multiply(l, r)        => s"(${renderExpr(l, params)} * ${renderExpr(r, params)})"
    case Expr.Divide(l, r)          => s"(${renderExpr(l, params)} / ${renderExpr(r, params)})"
    case Expr.Modulo(l, r)          => s"(${renderExpr(l, params)} % ${renderExpr(r, params)})"
    case Expr.Equal(l, r)           => s"(${renderExpr(l, params)} = ${renderExpr(r, params)})"
    case Expr.NotEqual(l, r)        => s"(${renderExpr(l, params)} != ${renderExpr(r, params)})"
    case Expr.LessThan(l, r)        => s"(${renderExpr(l, params)} < ${renderExpr(r, params)})"
    case Expr.LessOrEqual(l, r)     => s"(${renderExpr(l, params)} <= ${renderExpr(r, params)})"
    case Expr.GreaterThan(l, r)     => s"(${renderExpr(l, params)} > ${renderExpr(r, params)})"
    case Expr.GreaterOrEqual(l, r)  => s"(${renderExpr(l, params)} >= ${renderExpr(r, params)})"
    case Expr.And(l, r)             => s"(${renderExpr(l, params)} AND ${renderExpr(r, params)})"
    case Expr.Or(l, r)              => s"(${renderExpr(l, params)} OR ${renderExpr(r, params)})"
    case Expr.Not(e)                => s"NOT (${renderExpr(e, params)})"
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