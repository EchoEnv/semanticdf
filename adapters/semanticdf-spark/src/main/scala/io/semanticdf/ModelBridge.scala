package io.semanticdf

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
      case Left(err) =>
        return Left(ModelValidationError.FilterConversionUnsupported(err))
    }
    val havingSpecs: List[FilterSpec] = extractHaving(st) match {
      case Right(specs) => specs
      case Left(err) =>
        return Left(ModelValidationError.FilterConversionUnsupported(err))
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
      // v1 placeholder: Sum. See scaladoc for the limitation.
      Measure(
        name = m.name,
        expr = AggregateCall(
          fn    = AggregateFn.Sum,
          input = Some(Expr.FieldRef(m.name)),
          alias = m.name,
        ),
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
    * Returns `Right(Nil)` if the table has no `where`. Returns
    * `Right(List[FilterSpec])` on success. Returns `Left(reason)` if
    * the converter rejects an unsupported legacy predicate. */
  private def extractFilters(st: SemanticTable): Either[String, List[FilterSpec]] = {
    val filters = collectFilterPredicates(st.root)
    if (filters.isEmpty) Right(Nil)
    else convertAll(filters, namePrefix = "where")
  }

  /** Convert `postAggPredicates` (legacy `having`) to portable
    * `FilterSpec`s via [[PredicateToExprConverter.toExpr]]. */
  private def extractHaving(st: SemanticTable): Either[String, List[FilterSpec]] = {
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
    * On conversion failure, returns `Left(reason)` with the
    * converter's error message. */
  private def convertAll(
      predicates: List[Predicate],
      namePrefix: String,
  ): Either[String, List[FilterSpec]] = {
    val results = predicates.zipWithIndex.map { case (pred, idx) =>
      val expr = try Right(PredicateToExprConverter.toExpr(pred))
      catch {
        case e: UnsupportedOperationException => Left(e.getMessage)
      }
      expr.map { expr =>
        val opName = pred match {
          case p: io.semanticdf.predicate.Predicate.Compare => opNameOf(p)
          case _                                         => s"$idx"
        }
        FilterSpec(name = s"${namePrefix}_${opName}", predicate = expr)
      }
    }
    results.collectFirst { case Left(err) => Left(err) }
      .getOrElse(Right(results.collect { case Right(fs) => fs }))
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