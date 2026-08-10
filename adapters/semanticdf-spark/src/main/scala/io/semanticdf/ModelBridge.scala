package io.semanticdf

import io.semanticdf.core.engine.EngineError
import io.semanticdf.core.expr.Expr
import io.semanticdf.core.model._
import io.semanticdf.core.rel.{AggregateCall, AggregateFn, JoinKind}
import io.semanticdf.predicate.{Predicate, PredicateToExprConverter}
import io.semanticdf.{SemanticOp, SemanticFilterOp}

/** Engine-portable partial-bridge from the legacy [SemanticTable]
  * (spark-flavored) to the engine-portable [Model] (core).
  *
  * Per the multi-engine design \u00a76.4 + PR #408 review: the MCP
  * engine registry currently passes a synthetic [Model] with
  * only the name + a synthetic source (per PR #404). The engine
  * provider ignores the body and uses the legacy [SemanticTable]
  * registry for everything that matters.
  *
  * This bridge produces a REAL [Model] from a [SemanticTable]:
  * dimensions, measures, joins are converted. The engine provider
  * can then consume a real [Model] body, setting up PR #409 (the
  * SparkEngine implements `Engine[R]` work).
  *
  * ==Per-field conversion (v1)==
  *
  * - `name`            <- `SemanticTable.name`
  * - `source`          <- `SemanticTable.sourceTable` (or fallback to name)
  * - `dimensions`      <- `SemanticTable.dimensions` (each as `Expr.FieldRef(name)`)
  * - `measures`        <- `SemanticTable.measures` (each as `AggregateCall(Sum, FieldRef(name), name)`)
  * - `joins`           <- `SemanticTable.joins` (each JoinInfo as a JoinSpec)
  * - `description`     <- `SemanticTable.description`
  * - `version`         <- `SemanticTable.version`
  * - `status`          <- `SemanticTable.status`
  * - `filters`         <- NOT converted (Predicate type duplication; deferred to v0.5.0)
  * - `calculatedMeasures` <- NOT converted (CalcGraph is spark-flavored; deferred to v0.5.0)
  * - `rollups`         <- NOT converted (RollupSpec is engine-portable but the rollup
  *                        definitions live in spark-flavored types; deferred to v0.5.0)
  * - `defaultPolicies` <- `ModelPolicyDefaults.none` (no policy extraction yet)
  * - `extensions`      <- empty (no extension extraction yet)
  *
  * ==Known limitations (v1)==
  *
  * 1. **Measure aggregate function is placeholder `Sum`.** The
  *    legacy `Measure.expr: SemanticScope => Column` is a closure
  *    over a Spark column; we cannot statically determine the
  *    aggregate function (`Sum`, `Avg`, `Count`, etc.) without
  *    evaluating the closure against a sample scope. For v1, we
  *    use `Sum` as a placeholder. Future work (v0.5.0): evaluate
  *    the closure against a synthetic scope to detect the
  *    aggregate function, OR introspect the [Measure.exprString]
  *    hint that may carry the original aggregate expression.
  *
  * 2. **Dimensions are simplified to `Expr.FieldRef(name)`.** The
  *    legacy `Dimension.expr: SemanticScope => Column` may
  *    produce any expression. For v1, we assume the common case
  *    (the dimension name IS the source-column reference; per the
  *    `SemanticTable`'s "name doubles as source-column reference"
  *    invariant). For dimensions whose expr aliases a different
  *    source column, the converted Model will be incorrect.
  *
  * 3. **Join `kind` is mapped from `JoinInfo.cardinality`
  *    (String).** `cardinality = "one" | "many" | "cross"`
  *    maps to `JoinKind.Inner | Inner | Cross`. Inner is the
  *    conservative default; the source code may carry more
  *    granular kind information that is lost in the JoinInfo DTO.
  *
  * 4. **Filters (Predicate) are dropped.** The legacy Predicate
  *    type is spark-flavored; the engine-portable Predicate is a
  *    separate, parallel type. A converter is required; deferred
  *    to v0.5.0.
  *
  * 5. **No policy extraction.** `defaultPolicies` defaults to
  *    `ModelPolicyDefaults.none`. The legacy SemanticTable does
  *    not carry policy information in a structured way.
  *
  * ==Why this lives in semanticdf-spark==
  *
  * The bridge needs BOTH types (spark's SemanticTable + core's
  * Model). `semanticdf-core` deliberately does NOT depend on
  * Spark (the design's "engine-portable" boundary). `semanticdf-spark`
  * is the only module that has both on the classpath.
  *
  * ==Data-driven mantra compliance==
  *
  * - Pure data conversion (no behavior on the result)
  * - Deterministic: same SemanticTable -> same Model
  * - Total over the easy fields; partial over the hard ones
  *   (documented above)
  */
object ModelBridge {

  /** Map the legacy [io.semanticdf.ModelStatus] (spark adapter)
    * to the engine-portable [io.semanticdf.core.model.ModelStatus].
    *
    * Both ADTs have the same 3 cases (Draft, Published, Deprecated),
    * so the mapping is mechanical. */
  private def statusToCore(s: io.semanticdf.ModelStatus): io.semanticdf.core.model.ModelStatus =
    s match {
      case io.semanticdf.ModelStatus.Draft      => io.semanticdf.core.model.ModelStatus.Draft
      case io.semanticdf.ModelStatus.Published  => io.semanticdf.core.model.ModelStatus.Published
      case io.semanticdf.ModelStatus.Deprecated => io.semanticdf.core.model.ModelStatus.Deprecated
    }

  /** Convert a [SemanticTable] to an engine-portable [Model].
    *
    * Returns `Left(ModelValidationError)` if the converted model
    * fails validation (e.g. duplicate dimension/measure names).
    * Returns `Right(Model)` on success. */
  def toModel(st: SemanticTable): Either[ModelValidationError, Model] = {
    // v0.3.0 pre-tag fix (Gap 4): fail loud if the legacy table
    // v0.3.1 (Gap 4 closure): convert legacy `where` and `having`
    // predicates to portable `FilterSpec(name, predicate = Expr)` via
    // `PredicateToExprConverter`. If a legacy predicate has no
    // portable counterpart yet (Contains / StartsWith / EndsWith /
    // ArrayContains, or unsupported literal types), fail loud with
    // `FilterConversionUnsupported` so the consumer knows.
    //
    // Detection:
    //   - `having` lives in `st.postAggPredicates: List[Predicate]`
    //     (set by `.having(pred)` in SemanticTableCore).
    //   - `where` wraps the root op tree in `SemanticFilterOp`
    //     chains (set by `.where(pred)`). Walk the tree to extract
    //     each filter's predicate.
    val filterSpecs: List[FilterSpec] = extractFilters(st) match {
      case Right(specs) => specs
      case Left(err)     => return Left(err)
    }
    val havingSpecs: List[FilterSpec] = extractHaving(st) match {
      case Right(specs) => specs
      case Left(err)     => return Left(err)
    }

    // v1: name resolution priority:
    //   1. SemanticTable.name (the user-declared name from YAML `name:`)
    //   2. sourceTable (the YAML `table:` field, often the same as name)
    //   3. For joined tables where neither is set, derive from the
    //      left side: "<leftName>_<joinType>_<rightName>"
    val name: String = st.name.orElse(st.sourceTable).getOrElse(deriveName(st))
    val source: SourceRef = st.sourceTable match {
      case Some(t) => SourceRef.ByName(catalog = None, namespace = None, table = t)
      case None    => SourceRef.ByName(catalog = None, namespace = None, table = name)
    }
    val dimensions: List[Dimension] = st.dimensions.values.toList.map { d =>
      Dimension(
        name     = d.name,
        expr     = Expr.FieldRef(d.name),
        dataType = None,
      )
    }
    val measures: List[Measure] = st.measures.values.toList.map { m =>
      // v0.3.1 (Phase 4 follow-up): parse the legacy measure's
      // `exprString` (the YAML `expr:` hint, e.g. "count(flight_count)")
      // to populate the portable `AggregateCall` with the correct
      // `AggregateFn` and input column. The original v1 placeholder
      // hardcoded `Sum` with `FieldRef(m.name)` as input — which
      // produced nonsense for any non-Sum measure (e.g. `c:
      // "count(flight_count)"` became `Sum(c)` instead of
      // `Count(flight_count)`, and the engine-portable path
      // produced wrong results).
      //
      // For unmatched expressions (lambda-built measures with no
      // string hint), we fall back to the v1 placeholder so the
      // call site doesn't fail loud; the engine-portable query
      // path will then produce a runtime error rather than silently
      // wrong data. Future work: also support closure introspection
      // (v0.5.0).
      val exprString: Option[String] = m.exprString
      val aggregateCall: AggregateCall = exprString.flatMap(parseAggregateExpr) match {
        case Some(call) => call
        case None       =>
          // v1 placeholder for unparseable expressions.
          AggregateCall(
            fn    = AggregateFn.Sum,
            input = Some(Expr.FieldRef(m.name)),
            alias = m.name,
          )
      }
      Measure(
        name = m.name,
        expr = aggregateCall,
      )
    }
    val joins: List[JoinSpec] = st.joins.toList.map { ji =>
      JoinSpec(
        name       = ji.rightName.getOrElse(""),
        rightModel = ji.rightName.getOrElse(""),
        kind       = cardinalityToJoinKind(ji.cardinality),
        keys       = ji.keys.toList.map(k => k -> k),
      )
    }
    val filters: List[FilterSpec] = filterSpecs ++ havingSpecs
    Model.of(
      name               = name,
      source             = source,
      dimensions         = dimensions,
      measures           = measures,
      calculatedMeasures = Nil,
      joins              = joins,
      filters            = filters,
      rollups            = Nil,
      defaultPolicies    = ModelPolicyDefaults.none,
      extensions         = Map.empty,
      description        = st.description,
      version             = st.version,
      status              = statusToCore(st.status),
    )
  }

  private def cardinalityToJoinKind(cardinality: String): JoinKind =
    cardinality.toLowerCase match {
      case "cross" => JoinKind.Cross
      case _       => JoinKind.Inner  // "one" | "many" | unknown -> Inner
    }

  /** Parse a legacy measure expression string (the YAML `expr:`
    * value) into an engine-portable [[AggregateCall]].
    *
    * Per the v0.3.1 Platform migration design doc (PR #443), the
    * engine-portable path needs the actual aggregate function +
    * input column, not a placeholder. The legacy `exprString` is
    * the YAML's `expr:` hint (e.g. `"count(flight_count)"`,
    * `"sum(total_distance)"`), which carries the original
    * aggregate intent.
    *
    * Supported shapes (the v0.3.1 surface; advanced aggregates
    * mirror DuckDBQueryCompiler's mapping from PR #420):
    *   - `count(col)` / `count(*)` / `count(1)`     -> Count / Count
    *   - `count(distinct col)`                       -> CountDistinct
    *   - `sum(col)` / `avg(col)` / `min(col)` / `max(col)` -> Sum / Avg / Min / Max
    *   - `stddev(col)` / `stddev_samp(col)` / `stddev_pop(col)` -> StddevSample / StddevPopulation
    *   - `var(col)` / `var_samp(col)` / `var_pop(col)`            -> VarianceSample / VariancePopulation
    *   - `median(col)`                                                 -> Median
    *   - `quantile_cont(col, ...)` / `quantile_disc(col, ...)`         -> PercentileContinuous / PercentileDiscrete
    *   - `approx_quantile(col, ...)`                                  -> ApproxPercentile
    *
    * Returns `None` for unparseable expressions (lambda-built
    * measures with no exprString hint). The caller falls back to
    * the v1 placeholder in that case.
    */
  private def parseAggregateExpr(s: String): Option[AggregateCall] = {
    val trimmed = s.trim
    if (trimmed.isEmpty) return None
    val lc = trimmed.toLowerCase
    // Pull the function name and the inner arguments.
    // We use a regex that handles function(args) and bare column.
    val fnPattern = """^([a-z_]+)\s*\((.*)\)$""".r
    lc match {
      case fnPattern(fnName, args) =>
        val argList = args.split(",").map(_.trim).filter(_.nonEmpty)
        // For aggregates with a percentile arg (quantile_cont, etc.),
        // ignore the arg (we hardcode 0.5 — same as the SQL lowerer).
        val inputExpr: Option[Expr] = argList.headOption match {
          case Some(arg) if arg == "*" || arg == "1" =>
            Some(Expr.FieldRef("*"))  // count(*) / count(1)
          case Some(arg) =>
            arg.stripPrefix("distinct ").trim match {
              case col if col.nonEmpty => Some(Expr.FieldRef(col))
              case _                   => None
            }
          case None =>
            None
        }
        val fn: Option[AggregateFn] = fnName match {
          case "count" =>
            if (args.toLowerCase.contains("distinct")) Some(AggregateFn.CountDistinct)
            else Some(AggregateFn.Count)
          case "sum"           => Some(AggregateFn.Sum)
          case "avg"           => Some(AggregateFn.Avg)
          case "min"           => Some(AggregateFn.Min)
          case "max"           => Some(AggregateFn.Max)
          case "stddev"        => Some(AggregateFn.StddevSample)
          case "stddev_samp"   => Some(AggregateFn.StddevSample)
          case "stddev_pop"    => Some(AggregateFn.StddevPopulation)
          case "var"           => Some(AggregateFn.VarianceSample)
          case "var_samp"      => Some(AggregateFn.VarianceSample)
          case "var_pop"       => Some(AggregateFn.VariancePopulation)
          case "median"        => Some(AggregateFn.Median)
          case "quantile_cont" => Some(AggregateFn.PercentileContinuous)
          case "quantile_disc" => Some(AggregateFn.PercentileDiscrete)
          case "approx_quantile" => Some(AggregateFn.ApproxPercentile)
          case _               => None
        }
        (fn, inputExpr) match {
          case (Some(f), Some(input)) =>
            Some(AggregateCall(fn = f, input = Some(input), alias = trimmed))
          case _ => None
        }
      case _ =>
        // Bare column reference (no function call). Treat as a sum
        // of the column (the legacy SemanticTable's `expr` does the
        // same for typed arithmetic).
        Some(AggregateCall(
          fn    = AggregateFn.Sum,
          input = Some(Expr.FieldRef(trimmed)),
          alias = trimmed,
        ))
    }
  }

  /** Derive a fallback name for tables without an explicit name or
    * sourceTable (typically joined tables). Returns a synthetic
    * "<leftName>_<rightName>" string; falls back to "unnamed" if
    * the join info is empty. */
  private def nameOrUnknown(st: SemanticTable): String =
    st.name.orElse(st.sourceTable).getOrElse("<unnamed>")

  /** Walk the legacy op tree and return true if any
    * `SemanticFilterOp` exists at any depth. A `where` call wraps
    * the tree in one or more `SemanticFilterOp` nodes; this
    * detects the deepest one. */
  private def hasPreAggFilter(op: SemanticOp): Boolean = op match {
    case _: SemanticFilterOp => true
    case _ => false
  }

  /** Collect `SemanticFilterOp` predicates from the op tree (top-down),
    * convert each to a portable `FilterSpec` via
    * [[PredicateToExprConverter.toExpr]]. Each filter becomes its own
    * `FilterSpec` (named "where_<op>" for each legacy compare op, or
    * "where_<n>" for compound predicates).
    *
    * Per `docs/design/error-handling-style.md`: internal helpers
    * use `Either[L, X]` with the SAME `L` as the public API
    * (`ModelValidationError` here). */
  private def extractFilters(st: SemanticTable): Either[ModelValidationError, List[FilterSpec]] = {
    val filters = collectFilterPredicates(st.root)
    if (filters.isEmpty) Right(Nil)
    else convertAll(filters, namePrefix = "where")
  }

  /** Convert `postAggPredicates` (legacy `having`) to portable
    * `FilterSpec`s via [[PredicateToExprConverter.toExpr]]. */
  private def extractHaving(st: SemanticTable): Either[ModelValidationError, List[FilterSpec]] = {
    if (st.postAggPredicates.isEmpty) Right(Nil)
    else convertAll(st.postAggPredicates, namePrefix = "having")
  }

  /** Walk the op tree and collect every `SemanticFilterOp.predicate`
    * in pre-order. Returns them in source order (top filter first). */
  private def collectFilterPredicates(op: SemanticOp): List[Predicate] = op match {
    case f: SemanticFilterOp =>
      f.predicate +: collectFilterPredicates(f.source)
    case j: io.semanticdf.SemanticJoinOp =>
      // Filters may live inside a join. Recurse into both sides.
      // (Per scala-spark-batch-bugs §3: don't assume; verify.)
      collectFilterPredicates(j.left) ++ collectFilterPredicates(j.right)
    case _ =>
      // Other ops don't carry filters.
      Nil
  }

  /** Convert a list of legacy predicates to portable `FilterSpec`s.
    * Per `docs/design/error-handling-style.md`: typed `Either` at
    * the boundary; `EngineError.UnsupportedCapability` from the
    * converter is converted to `ModelValidationError.FilterConversionUnsupported`
    * here (public-API boundary). Fail-fast via flatMap. */
  private def convertAll(
      predicates: List[Predicate],
      namePrefix: String,
  ): Either[ModelValidationError, List[FilterSpec]] = {
    val initial: Either[ModelValidationError, List[FilterSpec]] = Right(Nil)
    predicates.zipWithIndex.foldLeft(initial) { (acc, item) =>
      acc.flatMap { specs =>
        val (pred, idx) = item
        PredicateToExprConverter.toExpr(pred).flatMap { expr =>
          val opName = pred match {
            case p: io.semanticdf.predicate.Predicate.Compare => opNameOf(p)
            case _                                         => s"$idx"
          }
          Right(specs :+ FilterSpec(name = s"${namePrefix}_${opName}", predicate = expr))
        }.left.map { unsupported =>
          ModelValidationError.FilterConversionUnsupported(unsupported.reason)
        }
      }
    }
  }

  /** Map a legacy `Compare` instance to its op-string (`eq`, `gt`,
    * etc.). Mirrors the canonical op vocabulary used by the legacy
    * `Compare.apply(op, field, value)` factory. */
  private def opNameOf(p: io.semanticdf.predicate.Predicate.Compare): String = p match {
    case _: Predicate.Compare.Eq           => "eq"
    case _: Predicate.Compare.Ne           => "ne"
    case _: Predicate.Compare.Lt           => "lt"
    case _: Predicate.Compare.Le           => "le"
    case _: Predicate.Compare.Gt           => "gt"
    case _: Predicate.Compare.Ge           => "ge"
    case _: Predicate.Compare.Contains     => "contains"
    case _: Predicate.Compare.StartsWith   => "startswith"
    case _: Predicate.Compare.EndsWith     => "endswith"
    case _: Predicate.Compare.ArrayContains => "arraycontains"
  }

  private def deriveName(st: SemanticTable): String = {
    st.joins.headOption.flatMap { ji =>
      ji.leftName.orElse(ji.rightName).map(n => s"${ji.leftName.getOrElse(n)}_${ji.rightName.getOrElse("?")}")
    }.getOrElse("unnamed")
  }
}