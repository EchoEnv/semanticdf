package io.semanticdf.trino

import io.semanticdf.core.expr.{Expr, LiteralValue}
import io.semanticdf.core.model.{Model, SourceRef}
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

  /** Compile a portable [[Model]] to a Trino SQL string.
    *
    * The output is a single SQL statement composed of:
    *   - SELECT clause (dimensions + measures + calculated measures)
    *   - FROM clause (source from `model.source` + chained JOINs)
    *   - WHERE clause (filters composed with AND)
    *   - GROUP BY clause (all dimensions, if there are any aggregates)
    *
    * Returns SQL with all values INLINED (no parameter binding).
    * Field names are double-quoted (Trino's standard identifier
    * delimiter). String values are single-quoted; embedded single
    * quotes are escaped by doubling.
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
    * @param model        the portable model to compile
    * @param modelSources the resolution of right-model names to
    *                     source refs; empty map skips JOIN emission
    *                     for unresolvable joins
    * @return Trino-compatible SQL string */
  def compile(model: Model, modelSources: Map[String, SourceRef] = Map.empty): String = {
    val selectCols = renderSelectColumns(model)
    val fromClause = renderFrom(model, modelSources)
    val whereClause = renderWhere(model.filters)
    val groupByClause = renderGroupBy(model)

    val parts = List(
      Some(s"SELECT ${selectCols.mkString(", ")}"),
      Some(s"FROM $fromClause"),
      whereClause.map(w => s"WHERE $w"),
      groupByClause.map(g => s"GROUP BY $g"),
    ).flatten

    parts.mkString(" ")
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
    * calc DAG is acyclic per `ModelValidator` (calc DAG check #4). */
  private def renderSelectColumns(model: Model): List[String] = {
    val dimCols = model.dimensions.map { d =>
      s"${renderExpr(d.expr)} AS ${quoteName(d.name)}"
    }
    val measureCols = model.measures.map { m =>
      s"${renderAggregateCall(m.expr)} AS ${quoteName(m.name)}"
    }
    val calcCols = model.calculatedMeasures.map { cm =>
      s"${renderExpr(cm.expr)} AS ${quoteName(cm.name)}"
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
    val mainSource = renderSource(model.source)
    val mainAlias = aliasFor(model.source)

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
    * returns `None` (no WHERE clause at all). */
  private def renderWhere(filters: List[io.semanticdf.core.model.FilterSpec]): Option[String] =
    if (filters.isEmpty) None
    else {
      val rendered = filters.map(f => s"(${renderExpr(f.predicate)})")
      Some(rendered.mkString(" AND "))
    }

  // -- GROUP BY clause --

  /** Render the GROUP BY clause. Only emit if there are any
    * aggregate measures (base measures with `AggregateFn.*`).
    *
    * Calculated measures don't trigger GROUP BY (they ARE
    * expressions over the aggregated columns, not aggregation
    * functions themselves). The dimensions are the GROUP BY columns. */
  private def renderGroupBy(model: Model): Option[String] = {
    val hasAggregates = model.measures.nonEmpty
    if (!hasAggregates) None
    else if (model.dimensions.isEmpty) None  // no dims, no GROUP BY
    else {
      val groups = model.dimensions.map(d => renderExpr(d.expr))
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
    * adding a new case would require a compile error here. */
  private def renderAggregateCall(call: AggregateCall): String = {
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
      case Some(e) => s"$distinct${renderExpr(e)}"
      case None    => if (call.distinct) "" else "*"  // COUNT(*) for no-input
    }
    val arguments = if (call.arguments.nonEmpty) {
      s", ${call.arguments.map(renderLiteral).mkString(", ")}"
    } else ""
    s"$fnName($input$arguments)"
  }

  // -- Expression rendering --

  /** Render a portable `Expr` as a Trino SQL expression.
    * Handles all 21 `Expr` cases (per PR #359). */
  private def renderExpr(e: Expr): String = e match {
    case Expr.Literal(value, _) => renderLiteral(value)

    case Expr.FieldRef(name)    => quoteName(name)
    case Expr.MeasureRef(name)  => quoteName(name)  // ref to a base-measure alias

    // Arithmetic
    case Expr.Add(l, r)      => s"(${renderExpr(l)} + ${renderExpr(r)})"
    case Expr.Subtract(l, r) => s"(${renderExpr(l)} - ${renderExpr(r)})"
    case Expr.Multiply(l, r) => s"(${renderExpr(l)} * ${renderExpr(r)})"
    case Expr.Divide(l, r)   => s"(${renderExpr(l)} / ${renderExpr(r)})"
    case Expr.Modulo(l, r)   => s"(${renderExpr(l)} % ${renderExpr(r)})"

    // Comparison
    case Expr.Equal(l, r)           => s"(${renderExpr(l)} = ${renderExpr(r)})"
    case Expr.NotEqual(l, r)        => s"(${renderExpr(l)} <> ${renderExpr(r)})"
    case Expr.LessThan(l, r)        => s"(${renderExpr(l)} < ${renderExpr(r)})"
    case Expr.LessOrEqual(l, r)     => s"(${renderExpr(l)} <= ${renderExpr(r)})"
    case Expr.GreaterThan(l, r)     => s"(${renderExpr(l)} > ${renderExpr(r)})"
    case Expr.GreaterOrEqual(l, r)  => s"(${renderExpr(l)} >= ${renderExpr(r)})"

    // Boolean
    case Expr.And(l, r) => s"(${renderExpr(l)} AND ${renderExpr(r)})"
    case Expr.Or(l, r)  => s"(${renderExpr(l)} OR ${renderExpr(r)})"
    case Expr.Not(inner) => s"(NOT ${renderExpr(inner)})"

    // Null checks
    case Expr.IsNull(inner)    => s"(${renderExpr(inner)} IS NULL)"
    case Expr.IsNotNull(inner) => s"(${renderExpr(inner)} IS NOT NULL)"

    // Cast (target type is engine-portable; render as Trino type name)
    case Expr.Cast(inner, targetType) => s"CAST(${renderExpr(inner)} AS ${renderType(targetType)})"

    // Function call
    case Expr.FunctionCall(name, args) =>
      s"${quoteName(name)}(${args.map(renderExpr).mkString(", ")})"
  }

  /** Render a portable `LiteralValue` as a Trino SQL literal.
    * String values are single-quoted; embedded single quotes are
    * escaped by doubling (Trino's standard escape). Numeric values
    * are inlined. Timestamps are `TIMESTAMP '...'`. Dates are
    * `DATE '...'`. Booleans are `TRUE` / `FALSE`. Null is `NULL`. */
  private def renderLiteral(v: LiteralValue): String = v match {
    case LiteralValue.IntValue(n)        => n.toString
    case LiteralValue.ByteValue(n)       => n.toString
    case LiteralValue.ShortValue(n)      => n.toString
    case LiteralValue.LongValue(n)       => n.toString
    case LiteralValue.FloatValue(n)      => n.toString
    case LiteralValue.DoubleValue(n)     => n.toString
    case LiteralValue.DecimalValue(n)    => n.toString
    case LiteralValue.StringValue(s)     => s"'${s.replace("'", "''")}'"
    case LiteralValue.BoolValue(b)       => if (b) "TRUE" else "FALSE"
    case LiteralValue.BinaryValue(bytes) =>
      s"X'${bytes.iterator.map(b => f"${b & 0xff}%02x").mkString}'"
    case LiteralValue.TimestampValue(instant) =>
      s"TIMESTAMP '${instant.toString}'"
    case LiteralValue.DateValue(date) =>
      s"DATE '${date.toString}'"
    case LiteralValue.ArrayValue(values) =>
      s"ARRAY[${values.map(renderLiteral).mkString(", ")}]"
    case LiteralValue.MapValue(entries) =>
      val rendered = entries.map { case (k, v) =>
        s"${renderLiteral(k)} => ${renderLiteral(v)}"
      }
      s"MAP(ARRAY[${entries.map(_._1).map(renderLiteral).mkString(", ")}], ARRAY[${entries.map(_._2).map(renderLiteral).mkString(", ")}])"
    case LiteralValue.StructValue(fields) =>
      val rendered = fields.map { case (n, v) =>
        s"${quoteName(n)} => ${renderLiteral(v)}"
      }
      s"ROW(${rendered.mkString(", ")})"
    case LiteralValue.NullValue => "NULL"
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
}

object TrinoQueryCompiler {

  /** Singleton instance — the canonical Trino SQL compiler. */
  val instance: TrinoQueryCompiler = new TrinoQueryCompiler
}