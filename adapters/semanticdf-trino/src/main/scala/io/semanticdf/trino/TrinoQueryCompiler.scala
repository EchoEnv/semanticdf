package io.semanticdf.trino

import java.time.Instant

import io.semanticdf.core.engine.ParameterizedSql
import io.semanticdf.core.expr.{Expr, LiteralValue}
import io.semanticdf.core.model.{Model, RollupFreshnessSpec, RollupSpec, SourceRef}
import io.semanticdf.core.rel.{AggregateCall, AggregateFn}

/** Engine-specific Trino SQL compiler — Phase 2 first end-to-end
  * engine adapter behavior.
  *
  * Walks a portable [[Model]] (the engine-portable phase-2 contract
  * from PR #363) and emits a Trino SQL string. This is the FIRST
  * PR that produces real, runnable SQL from the portable model.
  *
  * ==What this compiler handles (v1 scope)==
  *
  *   - Dimensions (`Dimension`)             → SELECT clause + GROUP BY
  *   - Base measures (`Measure`)             → SELECT clause aggregates
  *   - Calculated measures (`CalculatedMeasure`) → SELECT clause expressions
  *     that reference base-measure aliases
  *   - Filters (`FilterSpec.predicate: Expr`) → WHERE clause
  *
  * ==What is NOT in this PR (deferred)==
  *
  *   - Joins (`JoinSpec`) — multi-source models are deferred
  *   - Rollups (`RollupSpec`) — the `Track` policy that picks a
  *     precomputed table is deferred (Phase 2 follow-up)
  *   - Window functions — `AggregateFn` already covers the
  *     non-window case; window semantics are deferred
  *   - `CalculatedMeasure` richer forms (the `Expr` form covers
  *     binary + arithmetic + scalar; window-bound calculated
  *     measures are deferred)
  *   - Parameter binding (`PreparedStatement` with `?` placeholders)
  *     — this PR inlines values
  *   - `ByPath` / `ByProvider` sources — the resolver rejects
  *     these per PR #367, so the compiler doesn't see them
  *
  * ==Why a pure function (no IO, no state) — per scala-data-driven-refactor §1==
  *
  * The `Model` (data) lives in core; the compile behavior lives in
  * the engine adapter. This compiler is the behavior. It has no
  * state, no closures, no IO — given a `Model`, it produces a
  * deterministic SQL string. Same input → same output. This is
  * testable without a real Trino cluster.
  *
  * ==Why it doesn't depend on `TrinoSourceResolver`==
  *
  * The resolver is needed for the EXECUTE step (which is still
  * FeatureDeferred). For COMPILE, the source's `SourceRef` is
  * already declarative — we just emit `catalog.schema.table` from
  * the `SourceRef.ByName` fields. No IO, no resolver.
  *
  * ==Boundary contract==
  *
  * Zero Spark imports. Verifiable by:
  * `grep -r 'org.apache.spark' adapters/semanticdf-trino/src/main/scala/io/semanticdf/trino/TrinoQueryCompiler.scala`
  */
class TrinoQueryCompiler {

  /** Compile a portable [[Model]] to a parameterized Trino SQL.
    *
    * The output is a [[ParameterizedSql]]: the SQL string with
    * positional `?` placeholders for literal values + the
    * ordered list of `LiteralValue` parameters. The caller
    * (the engine's `execute` step) binds the parameters via
    * Trino's `PreparedStatement`-style mechanism — this
    * prevents SQL injection.
    *
    * The output is a single SQL statement composed of:
    *   - SELECT clause (dimensions + measures + calculated measures)
    *   - FROM clause (source from `model.source` + chained JOINs,
    *     possibly substituted by a rollup source)
    *   - WHERE clause (filters composed with AND)
    *   - GROUP BY clause (all dimensions, if there are any aggregates)
    *
    * ==Why `ParameterizedSql` (vs. `String`)==
    *
    * Per the design's risk #11: "SQL injection via filter string
    * composition" — the engine MUST NOT inline user-provided
    * values into the SQL string. Parameterized binding via the
    * engine's `PreparedStatement` is the safe path. The compiler
    * emits `?` placeholders + the parameter list; the engine
    * binds at execute time.
    *
    * ==Why `modelSources` is a parameter (vs. a class field)==
    *
    * `JoinSpec.rightModel: String` references another model by name.
    * The compiler needs `SourceRef`s for the right side of each
    * join to emit the JOIN clause. The caller passes these via
    * `modelSources` at compile time. For v1, the engine passes an
    * empty map (no joins resolved at the engine level — the caller
    * is expected to resolve them). This sets up the next-step
    * Phase 2 PR: a model registry that the engine can hold.
    *
    * Mirrors the original Spark API pattern: `join_one(other, on)`,
    * `join_many(other, on)`, `join_cross(other)` — the user provides
    * the OTHER model up front. The portable `JoinSpec` is the
    * declarative record of that join; the compiler receives the
    * lookup map at compile time.
    *
    * ==Why `rollupSources` and `rollupWatermarks` are parameters==
    *
    * `RollupSpec` is a per-model declaration that "this rollup
    * covers these dimensions + measures". The compiler needs the
    * rollup's `SourceRef` (the rollup's table) and the watermark
    * (when it was last refreshed) to honor the `RollupFreshnessSpec`
    * policy. The caller passes these via the two maps at compile
    * time. The engine doesn't hold a rollup registry yet — that
    * lands in a future PR.
    *
    * @param model            the portable model to compile
    * @param modelSources     the resolution of right-model names to
    *                         source refs; empty map skips JOIN emission
    *                         for unresolvable joins
    * @param rollupSources    rollup name → rollup table's SourceRef;
    *                         empty map disables rollup selection
    * @param rollupWatermarks rollup name → last-refresh Instant;
    *                         empty map makes `Track` rollups stale
    * @param now              the current time (for the freshness
    *                         comparison); defaults to `Instant.now()`
    * @return [[ParameterizedSql]] with `?`-placeholder SQL + ordered
    *         parameters list */
  def compile(
      model:            Model,
      modelSources:     Map[String, SourceRef]   = Map.empty,
      rollupSources:    Map[String, SourceRef]   = Map.empty,
      rollupWatermarks: Map[String, Instant]     = Map.empty,
      now:              Instant                  = Instant.now(),
  ): ParameterizedSql = {
    // Accumulates the parameter values in the order they appear in
    // the SQL. The mutable state is local to this method (a `var`),
    // not on the class — the compiler instance itself is pure.
    val params = scala.collection.mutable.ListBuffer.empty[LiteralValue]

    val selectedRollup = selectRollup(model, rollupSources, rollupWatermarks, now)
    val (effectiveSource, rollupComment) = selectedRollup match {
      case Some((rollup, rollupSource)) =>
        (rollupSource, Some(s"-- using rollup '${rollup.name}'"))
      case None =>
        (model.source, None)
    }
    val selectCols = renderSelectColumns(model, params)
    val fromClause = renderFromFromSource(model, effectiveSource, modelSources)
    val whereClause = renderWhere(model.filters, params)
    val groupByClause = renderGroupBy(model, params)

    val parts = List(
      rollupComment,
      Some(s"SELECT ${selectCols.mkString(", ")}"),
      Some(s"FROM $fromClause"),
      whereClause.map(w => s"WHERE $w"),
      groupByClause.map(g => s"GROUP BY $g"),
    ).flatten

    ParameterizedSql(
      sql        = parts.mkString(" "),
      parameters = params.toList,
    )
  }

  // -- SELECT clause --

  /** Render the SELECT clause columns: dimensions + base measures
    * + calculated measures. Each column is output as
    * `<expr> AS <alias>`.
    *
    * Calculated measures are output AFTER base measures so that
    * `Expr.MeasureRef` references in the calc-measure expression
    * resolve to the base-measure alias (SQL's column-reference
    * resolution is by-name in the WITH clause, but in a flat
    * SELECT we need the alias to be visible).
    *
    * Per debug-mantra §3 (falsify): calc-measure references go to
    * BASE measurements only (not to other calc measures) — the
    * calc DAG is acyclic per `ModelValidator` (calc DAG check #4).
    *
    * The `params` buffer accumulates literal values for binding
    * — see `compile()`. */
  private def renderSelectColumns(
      model:  Model,
      params: scala.collection.mutable.ListBuffer[LiteralValue],
  ): List[String] = {
    val dimCols = model.dimensions.map { d =>
      s"${renderExpr(d.expr, params)} AS ${quoteName(d.name)}"
    }
    val measureCols = model.measures.map { m =>
      s"${renderAggregateCall(m.expr, params)} AS ${quoteName(m.name)}"
    }
    val calcCols = model.calculatedMeasures.map { cm =>
      s"${renderExpr(cm.expr, params)} AS ${quoteName(cm.name)}"
    }
    dimCols ++ measureCols ++ calcCols
  }

  // -- FROM clause --

  /** Render the FROM clause from the model's source reference,
    * chained with any `JoinSpec`s declared on the model.
    *
    * The main source is rendered as `FROM <source> AS <alias>`.
    * Each join's right side is rendered as `<kind> JOIN <right> AS
    * <alias> ON <condition>`. The aliases are derived from the
    * table name (the last component of the dotted name) for
    * readability; column references in the JOIN ON clause use
    * these aliases to disambiguate.
    *
    * Join keys are pairs of (leftKey, rightKey) — the left key
    * is a column name on the MAIN source (or on the previous join's
    * right side in a chain, but for v1 we only support single-level
    * joins), the right key is a column name on the joined-to
    * source. Multi-key joins are emitted as
    * `<leftKey1> = <rightKey1> AND <leftKey2> = <rightKey2>`.
    *
    * ==Why render the alias==
    *
    * For unqualified column names in the JOIN ON clause, the SQL
    * is ambiguous when both sides have a column with the same
    * name. The alias-qualified form (`orders"."id" = "customers"."id"`)
    * is unambiguous and survives multi-table joins.
    *
    * ==Why `modelSources` is a parameter (vs. a class field)==
    *
    * `JoinSpec.rightModel: String` references another model by name.
    * The compiler needs `SourceRef`s for the right side of each
    * join to emit the JOIN clause. The caller passes these via
    * `modelSources` at compile time. For v1, the engine passes an
    * empty map (no joins resolved at the engine level — the caller
    * is expected to resolve them).
    *
    * ==Why a placeholder for unresolvable joins==
    *
    * If a join's `rightModel` isn't in `modelSources`, the
    * compiler emits a placeholder that surfaces the issue in the
    * SQL rather than a typed error here. This is consistent with
    * the existing `ByPath` / `ByProvider` behavior: the compiler
    * surfaces incompatibilities in the SQL output.
    *
    * ==Why `ByPath` / `ByProvider` defensive placeholders==
    *
    * `SourceRef.ByPath` and `SourceRef.ByProvider` are rejected
    * by `TrinoSourceResolver` (PR #367), so a model reaching this
    * compiler should never have them in the main source. We emit
    * placeholders defensively for unmatched cases. */
  private def renderFrom(model: Model, modelSources: Map[String, SourceRef]): String = {
    val effectiveSource = model.source  // no rollup selection at this path
    renderFromFromSource(model, effectiveSource, modelSources)
  }

  /** Render the FROM clause from an explicit source (used when
    * a rollup substitutes the model's base source). */
  private def renderFromFromSource(
      model:        Model,
      source:       SourceRef,
      modelSources: Map[String, SourceRef],
  ): String = {
    val mainSource = renderSource(source)
    val mainAlias = aliasFor(source)

    val joins = model.joins.map { js =>
      modelSources.get(js.rightModel) match {
        case Some(rightSource) =>
          val rightIdent = renderSource(rightSource)
          val rightAlias = aliasFor(rightSource)
          val joinKindSql = renderJoinKind(js.kind)
          val onClause = renderJoinKeys(js, mainAlias, rightAlias)
          // Cross join has no ON clause (per SQL standard).
          // Emitting `ON` would be a syntax error.
          val onSuffix = if (js.kind == io.semanticdf.core.rel.JoinKind.Cross || onClause.isEmpty) ""
                         else s" ON $onClause"
          s"$joinKindSql $rightIdent AS ${quoteName(rightAlias)}$onSuffix"
        case None =>
          // Unresolvable rightModel — emit a placeholder so the
          // resulting SQL makes the issue visible rather than a
          // generic "table not found" downstream.
          s"<unresolved-join: rightModel='${js.rightModel}' not in modelSources>"
      }
    }

    val main = s"$mainSource AS ${quoteName(mainAlias)}"
    if (joins.isEmpty) main
    else main + " " + joins.mkString(" ")
  }

  /** Render a single source reference (independent of join context).
    * Returns the dotted table name for `ByName`, or a placeholder
    * for incompatible shapes. */
  private def renderSource(source: SourceRef): String = source match {
    case SourceRef.ByName(catalog, namespace, table) =>
      val parts = List(catalog, namespace, Some(table)).flatten
      parts.map(quoteName).mkString(".")
    case SourceRef.ByPath(format, path, _) =>
      s"<error: path-based source not supported by Trino: format=$format, path=$path>"
    case SourceRef.ByProvider(provider) =>
      val providerName = provider match {
        case io.semanticdf.core.model.ProviderRef.DataFrameSource(name, _) => name
        case io.semanticdf.core.model.ProviderRef.TableResolver(name)       => name
      }
      s"<error: ProviderRef not supported by Trino: provider=$providerName>"
  }

  /** Derive a SQL alias for a source. For `ByName`, returns the
    * table name (the last component). For `ByPath` / `ByProvider`,
    * returns a placeholder token. The alias is used to qualify
    * column references in the JOIN ON clause. */
  private def aliasFor(source: SourceRef): String = source match {
    case SourceRef.ByName(_, _, table) => table
    case SourceRef.ByPath(_, path, _)  => path.split('/').lastOption.getOrElse("path")
    case SourceRef.ByProvider(provider) => provider match {
      case io.semanticdf.core.model.ProviderRef.DataFrameSource(name, _) => name
      case io.semanticdf.core.model.ProviderRef.TableResolver(name)       => name
    }
  }

  /** Render the join kind as a SQL keyword. The 5 cases mirror the
    * original Spark API:
    *   - Inner / one-to-one → `INNER JOIN`
    *   - Left / one-to-many → `LEFT JOIN`
    *   - Right / symmetric → `RIGHT JOIN`
    *   - Full → `FULL JOIN`
    *   - Cross → `CROSS JOIN`
    *
    * Per scala-data-driven-refactor §3 (sealed ADT over Map):
    * the 5 `JoinKind` cases are exhaustively matched. Adding a
    * new case would require a compile error here. */
  private def renderJoinKind(kind: io.semanticdf.core.rel.JoinKind): String = kind match {
    case io.semanticdf.core.rel.JoinKind.Inner => "INNER JOIN"
    case io.semanticdf.core.rel.JoinKind.Left  => "LEFT JOIN"
    case io.semanticdf.core.rel.JoinKind.Right => "RIGHT JOIN"
    case io.semanticdf.core.rel.JoinKind.Full  => "FULL JOIN"
    case io.semanticdf.core.rel.JoinKind.Cross => "CROSS JOIN"
  }

  /** Render the JOIN ON clause from a `JoinSpec.keys: List[(String, String)]`.
    * Multi-key joins are emitted with AND between each pair.
    * Each key is qualified with the alias (left key with the
    * main alias, right key with the right alias). */
  private def renderJoinKeys(
      js:        io.semanticdf.core.model.JoinSpec,
      leftAlias: String,
      rightAlias: String,
  ): String = js.keys.map { case (lk, rk) =>
    s"""${quoteName(leftAlias)}.${quoteName(lk)} = ${quoteName(rightAlias)}.${quoteName(rk)}"""
  }.mkString(" AND ")

  // -- WHERE clause --

  /** Render the WHERE clause from the model's filters. All
    * filters are composed with `AND`. An empty filter list
    * returns `None` (no WHERE clause at all).
    *
    * The `params` buffer accumulates literal values for binding. */
  private def renderWhere(
      filters: List[io.semanticdf.core.model.FilterSpec],
      params:  scala.collection.mutable.ListBuffer[LiteralValue],
  ): Option[String] =
    if (filters.isEmpty) None
    else {
      val rendered = filters.map(f => s"(${renderExpr(f.predicate, params)})")
      Some(rendered.mkString(" AND "))
    }

  // -- GROUP BY clause --

  /** Render the GROUP BY clause. Only emit if there are any
    * aggregate measures (base measures with `AggregateFn.*`).
    *
    * Calculated measures don't trigger GROUP BY (they ARE
    * expressions over the aggregated columns, not aggregation
    * functions themselves). The dimensions are the GROUP BY columns. */
  private def renderGroupBy(
      model:  Model,
      params: scala.collection.mutable.ListBuffer[LiteralValue],
  ): Option[String] = {
    val hasAggregates = model.measures.nonEmpty
    if (!hasAggregates) None
    else if (model.dimensions.isEmpty) None  // no dims, no GROUP BY
    else {
      val groups = model.dimensions.map(d => renderExpr(d.expr, params))
      Some(groups.mkString(", "))
    }
  }

  // -- Aggregate call rendering --

  /** Render a portable `AggregateCall` as a Trino SQL aggregate
    * expression. Maps each `AggregateFn` case to its Trino SQL
    * name. Handles `input` (None for `Count(*)`), `distinct`
    * (Count(DISTINCT x)), and `arguments` (e.g. `0.95` for
    * `ApproxPercentile`).
    *
    * Per scala-data-driven-refactor §3 (sealed ADT over Map):
    * the 16 `AggregateFn` cases are exhaustively matched —
    * adding a new case would require a compile error here.
    *
    * The `params` buffer accumulates literal values for binding. */
  private def renderAggregateCall(
      call:   AggregateCall,
      params: scala.collection.mutable.ListBuffer[LiteralValue],
  ): String = {
    val fnName = call.fn match {
      case AggregateFn.Sum                  => "SUM"
      case AggregateFn.Count                => "COUNT"
      case AggregateFn.CountDistinct        => "COUNT"
      case AggregateFn.Avg                  => "AVG"
      case AggregateFn.Min                  => "MIN"
      case AggregateFn.Max                  => "MAX"
      case AggregateFn.StddevSample         => "STDDEV_SAMP"
      case AggregateFn.StddevPopulation    => "STDDEV_POP"
      case AggregateFn.VarianceSample      => "VAR_SAMP"
      case AggregateFn.VariancePopulation   => "VAR_POP"
      case AggregateFn.Median               => "MEDIAN"
      case AggregateFn.PercentileContinuous => "APPROX_PERCENTILE"
      case AggregateFn.PercentileDiscrete   => "APPROX_PERCENTILE"
      case AggregateFn.ApproxPercentile     => "APPROX_PERCENTILE"
      case AggregateFn.First                => "FIRST_VALUE"
      case AggregateFn.Last                 => "LAST_VALUE"
    }
    val distinct = if (call.distinct) "DISTINCT " else ""
    val input = call.input match {
      case Some(e) => s"$distinct${renderExpr(e, params)}"
      case None    => if (call.distinct) "" else "*"  // COUNT(*) for no-input
    }
    val arguments = if (call.arguments.nonEmpty) {
      s", ${call.arguments.map(v => renderLiteral(v, params)).mkString(", ")}"
    } else ""
    s"$fnName($input$arguments)"
  }

  // -- Expression rendering --

  /** Render a portable `Expr` as a Trino SQL expression.
    * Handles all 21 `Expr` cases (per PR #359).
    *
    * The `params` buffer accumulates literal values for binding.
    * When a `Literal` is encountered, a `?` placeholder is
    * emitted in the SQL and the value is appended to `params`. */
  private def renderExpr(
      e:      Expr,
      params: scala.collection.mutable.ListBuffer[LiteralValue],
  ): String = e match {
    case Expr.Literal(value, _) => renderLiteral(value, params)

    case Expr.FieldRef(name)    => quoteName(name)
    case Expr.MeasureRef(name)  => quoteName(name)  // ref to a base-measure alias

    // Arithmetic
    case Expr.Add(l, r)      => s"(${renderExpr(l, params)} + ${renderExpr(r, params)})"
    case Expr.Subtract(l, r) => s"(${renderExpr(l, params)} - ${renderExpr(r, params)})"
    case Expr.Multiply(l, r) => s"(${renderExpr(l, params)} * ${renderExpr(r, params)})"
    case Expr.Divide(l, r)   => s"(${renderExpr(l, params)} / ${renderExpr(r, params)})"
    case Expr.Modulo(l, r)   => s"(${renderExpr(l, params)} % ${renderExpr(r, params)})"

    // Comparison
    case Expr.Equal(l, r)           => s"(${renderExpr(l, params)} = ${renderExpr(r, params)})"
    case Expr.NotEqual(l, r)        => s"(${renderExpr(l, params)} <> ${renderExpr(r, params)})"
    case Expr.LessThan(l, r)        => s"(${renderExpr(l, params)} < ${renderExpr(r, params)})"
    case Expr.LessOrEqual(l, r)     => s"(${renderExpr(l, params)} <= ${renderExpr(r, params)})"
    case Expr.GreaterThan(l, r)     => s"(${renderExpr(l, params)} > ${renderExpr(r, params)})"
    case Expr.GreaterOrEqual(l, r)  => s"(${renderExpr(l, params)} >= ${renderExpr(r, params)})"

    // Boolean
    case Expr.And(l, r) => s"(${renderExpr(l, params)} AND ${renderExpr(r, params)})"
    case Expr.Or(l, r)  => s"(${renderExpr(l, params)} OR ${renderExpr(r, params)})"
    case Expr.Not(inner) => s"(NOT ${renderExpr(inner, params)})"

    // Null checks
    case Expr.IsNull(inner)    => s"(${renderExpr(inner, params)} IS NULL)"
    case Expr.IsNotNull(inner) => s"(${renderExpr(inner, params)} IS NOT NULL)"

    // Cast (target type is engine-portable; render as Trino type name)
    case Expr.Cast(inner, targetType) => s"CAST(${renderExpr(inner, params)} AS ${renderType(targetType)})"

    // Function call
    case Expr.FunctionCall(name, args) =>
      s"${quoteName(name)}(${args.map(a => renderExpr(a, params)).mkString(", ")})"
  }

  /** Render a portable `LiteralValue` as a `?` placeholder + add
    * the value to the `params` buffer for later binding.
    *
    * ==Why `?` (vs. inlining the value)==
    *
    * Per the design's risk #11: "SQL injection via filter string
    * composition" — user-provided values must NEVER be inlined
    * into the SQL string. Trino's `PreparedStatement` binds them
    * safely. The engine adapter (not the compiler) performs the
    * binding; the compiler just emits `?` + preserves the value
    * for later binding.
    *
    * ==Why `NullValue` stays inline==
    *
    * SQL `NULL` is a literal token in the SQL grammar; it's not a
    * value to bind. The compiler emits `NULL` (no parameter).
    * Same for the structure-only `ArrayValue`/`MapValue`/`StructValue`
    * wrappers — their element VALUES are bound; the array/map/struct
    * wrappers themselves are inline syntax.
    *
    * ==Why aggregate function arguments ARE bound==
    *
    * `ApproxPercentile(x, 0.95)` has a literal argument `0.95`.
    * Binding it as a parameter preserves precision + security. */
  private def renderLiteral(
      v:      LiteralValue,
      params: scala.collection.mutable.ListBuffer[LiteralValue],
  ): String = v match {
    case LiteralValue.IntValue(_)        => params += v; "?"
    case LiteralValue.ByteValue(_)       => params += v; "?"
    case LiteralValue.ShortValue(_)      => params += v; "?"
    case LiteralValue.LongValue(_)       => params += v; "?"
    case LiteralValue.FloatValue(_)      => params += v; "?"
    case LiteralValue.DoubleValue(_)     => params += v; "?"
    case LiteralValue.DecimalValue(_)    => params += v; "?"
    case LiteralValue.StringValue(_)     => params += v; "?"
    case LiteralValue.BoolValue(_)       => params += v; "?"
    case LiteralValue.BinaryValue(_)     => params += v; "?"
    case LiteralValue.TimestampValue(_)  => params += v; "?"
    case LiteralValue.DateValue(_)       => params += v; "?"
    case LiteralValue.ArrayValue(values) =>
      s"ARRAY[${values.map(vv => renderLiteral(vv, params)).mkString(", ")}]"
    case LiteralValue.MapValue(entries) =>
      val rendered = entries.map { case (k, v) =>
        s"${renderLiteral(k, params)} => ${renderLiteral(v, params)}"
      }
      s"MAP(ARRAY[${entries.map(_._1).map(k => renderLiteral(k, params)).mkString(", ")}], ARRAY[${entries.map(_._2).map(vv => renderLiteral(vv, params)).mkString(", ")}])"
    case LiteralValue.StructValue(fields) =>
      val rendered = fields.map { case (n, v) =>
        s"${quoteName(n)} => ${renderLiteral(v, params)}"
      }
      s"ROW(${rendered.mkString(", ")})"
    case LiteralValue.NullValue => "NULL"  // not a parameter — SQL NULL is a token
  }

  /** Render a portable `SealedDataType` as a Trino SQL type name.
    * Used for `CAST(expr AS type)` in `Expr.Cast`. */
  private def renderType(t: io.semanticdf.core.schema.SealedDataType): String = t match {
    case io.semanticdf.core.schema.SealedDataType.BigInt        => "bigint"
    case io.semanticdf.core.schema.SealedDataType.Int           => "integer"
    case io.semanticdf.core.schema.SealedDataType.Double        => "double"
    case io.semanticdf.core.schema.SealedDataType.Varchar       => "varchar"
    case io.semanticdf.core.schema.SealedDataType.Boolean       => "boolean"
    case io.semanticdf.core.schema.SealedDataType.Date          => "date"
    case io.semanticdf.core.schema.SealedDataType.Timestamp     => "timestamp"
    case io.semanticdf.core.schema.SealedDataType.Decimal(_, _) => "decimal"
    case io.semanticdf.core.schema.SealedDataType.Array(_)      => "array"
    case io.semanticdf.core.schema.SealedDataType.Map(_, _)     => "map"
    case io.semanticdf.core.schema.SealedDataType.Row(_)        => "row"
    case io.semanticdf.core.schema.SealedDataType.Json          => "json"
  }

  /** Double-quote an identifier (Trino's standard). Embedded
    * double quotes are escaped by doubling. */
  private def quoteName(name: String): String =
    s""""${name.replace("\"", "\"\"")}""""

  // -- Rollup selection --

  /** Select a rollup that covers the model's query and is fresh
    * per its `RollupFreshnessSpec`.
    *
    * Selection rules:
    *   1. The rollup's `dimensions` must include every model
    *      dimension NAME (the rollup is at least as coarse as the
    *      query).
    *   2. The rollup's `measures` must include every model
    *      measure NAME (the rollup can answer the query's
    *      aggregates).
    *   3. The rollup's `freshness` must permit use:
    *      - `NoTracking` → always fresh
    *      - `Track(maxStaleness, onStale)` → fresh iff
    *        `now - maxStaleness <= watermark[rollup.name]`.
    *        If stale, follow `onStale` (FallBackToBase → skip
    *        this rollup; Error → skip and emit a placeholder
    *        at compile time).
    *
    * @return `Some((rollup, source))` for the first covering
    *         fresh rollup; `None` if no rollup covers the query
    *         or all covering rollups are stale. */
  private def selectRollup(
      model:            Model,
      rollupSources:    Map[String, SourceRef],
      rollupWatermarks: Map[String, Instant],
      now:              Instant,
  ): Option[(RollupSpec, SourceRef)] = {
    val queryDimNames  = model.dimensions.map(_.name).toSet
    val queryMeasNames = model.measures.map(_.name).toSet

    model.rollups.iterator.flatMap { rollup =>
      // coverage checks
      val coversDims  = queryDimNames.forall(rollup.dimensions.contains)
      val coversMeas  = queryMeasNames.forall(m => rollup.measures.exists(_.name == m))
      if (!coversDims || !coversMeas) None
      else if (isFresh(rollup, rollupWatermarks, now))
        rollupSources.get(rollup.name).map(source => (rollup, source))
      else None
    }.toList.headOption
  }

  /** Check if a rollup is fresh per its `RollupFreshnessSpec`.
    *
    * - `NoTracking` → always fresh
    * - `Track(maxStaleness, onStale)` → fresh iff the watermark
    *   is within `maxStaleness` of `now`. If the watermark is
    *   missing or stale, the rollup is NOT considered fresh
    *   (the caller can fall back to the base source per `onStale`).
    *
    * Per scala-data-driven-refactor §3 (sealed ADT over Map):
    * the 2 `RollupFreshnessSpec` cases are exhaustively matched. */
  private def isFresh(
      rollup:           RollupSpec,
      rollupWatermarks: Map[String, Instant],
      now:              Instant,
  ): Boolean = rollup.freshness match {
    case RollupFreshnessSpec.NoTracking => true
    case RollupFreshnessSpec.Track(maxStaleness, _) =>
      rollupWatermarks.get(rollup.name) match {
        case Some(watermark) => watermark.isAfter(now.minus(maxStaleness))
        case None            => false  // no watermark → can't verify freshness → stale
      }
  }
}

object TrinoQueryCompiler {

  /** Singleton instance — the canonical Trino SQL compiler. */
  val instance: TrinoQueryCompiler = new TrinoQueryCompiler
}