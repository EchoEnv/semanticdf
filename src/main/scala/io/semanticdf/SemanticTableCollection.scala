package io.semanticdf

import io.semanticdf.audit.{AuditSink, QueryRequest => AuditQueryRequest}
import io.semanticdf.result.ResultDecoder
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}

/** Collection / rendering methods of [[SemanticTable]] — everything that
  * walks the op tree to produce a human-readable artifact, runs a typed
  * query into a `Dataset[T]`, or builds a `SemanticGroupBy` / `SemanticTable`
  * via the typed-field-ref entry points.
  *
  * == Why a trait ==
  *
  * Collection is the **largest** concern in `SemanticTable` after the
  * Core/Streaming/Mutation trio (~1000 lines including `SemanticPlanRenderer`).
  * It's tightly cohesive: every method here produces an artifact derived
  * from the existing op tree, not a new op. Putting them in one trait
  * gives "rendering and typed aggregation" its own dedicated home.
  *
  * == What lives here ==
  *
  *   - `collectAs`, `queryAs` (typed-result collection)
  *   - `explain` (no-arg), `explain` (implicit spark), `explainExtended`
  *   - `explainSemantic` (3 overloads)
  *   - `groupBy` (string keys), `groupByDimensions` (4 typed overloads),
  *     `groupByDimensionsAll`
  *   - `filters`, `collectFilters` (private)
  *   - `SemanticGroupBy` class (the builder returned by `groupBy`)
  *   - `SemanticPlanRenderer` private class (the explain-semantic engine)
  *   - `Scope` sealed trait + `Scope.{All, Used}` case objects
  *   - `MeasureProbeScope` private class (used by the renderer)
  *
  * == Cross-trait dependencies ==
  *
  *   - `toDataFrame` is in Core; called via `self.toDataFrame(...)`
  *     (used by `explain(implicit spark)` and `explainExtended`).
  *   - `validate()` is in Core; called via `self.validate()` (used by
  *     `SemanticPlanRenderer.collectWarnings`).
  *   - `name`, `sourceTable`, `root`, `dimensions`, `measures` etc. are
  *     all instance fields; the trait accesses them via `self`.
  *
  * == Why the renderer is in the trait ==
  *
  * `SemanticPlanRenderer` is `private class` — it's an implementation
  * detail of `explainSemantic`. It captures a `SemanticTable` reference
  * and reads its fields. Moving the renderer to the trait keeps the
  * encapsulation: the renderer is no longer visible from outside the
  * trait file.
  *
  * == Why `Scope` is in the trait ==
  *
  * The `Scope` sealed trait (with `All` and `Used` case objects) is
  * referenced by `explainSemantic(spark, scope)` and by
  * `SemanticPlanRenderer`'s constructor. The pre-split comment in the
  * original file noted it was package-level "because the underlying
  * renderer is a private class in this same file and path-dependent
  * types are not visible there." Moving the renderer + `Scope`
  * together preserves the same property: they're all in this file
  * and the trait's self-type makes them accessible.
  *
  * == Public API ==
  *
  * 100% unchanged. The trait is `private[semanticdf]` and is mixed
  * in via `extends SemanticTableCollection`. Consumers see the
  * same `SemanticTable` class.
  */
private[semanticdf] trait SemanticTableCollection { self: SemanticTable =>

  // -------------------------------------------------------------------------
  // Typed result collection
  // -------------------------------------------------------------------------

  /** Typed terminal — compile the op tree, collect the rows, decode each
    * into `T` via the implicit [[ResultDecoder]]. The decoder is the
    * caller's responsibility (supply an implicit instance or use one of
    * the built-in primitives). Returns `Seq[T]`, not `DataFrame` — this
    * is the typed counterpart to `execute(spark).collect().map(_.toSeq)`.
    *
    * Example:
    * {{{
    *   val names: Seq[String] = table.execute(spark).collectAs[String]
    * }}}
    *
    * The decoder must match the DataFrame's schema (column count, types).
    * For multi-column results, write a custom decoder — see
    * [[ResultDecoder]] for the typeclass contract. */
  def collectAs[T](spark: SparkSession)(implicit decoder: ResultDecoder[T], ct: scala.reflect.ClassTag[T]): Seq[T] =
    toDataFrame(spark).collect().toSeq.map(decoder.decode)

  /** Typed one-shot bundled query — `query(...)` that decodes into a
    * Spark `Dataset[T]` (Phase E1, see `docs/phase-E-plan.md`).
    *
    * Builds the op tree, runs it, decodes every row into a `T` via the
    * implicit [[ResultDecoder[T]]], and returns a Spark `Dataset[T]`.
    * `T` is usually a case class; `ResultDecoder.derive[T]` derives
    * the decoder automatically for case classes with primitive fields.
    * All `query(...)` parameters (where, having, orderBy,
    * limit, timeGrain, timeGrains, timeRange) work the same way they
    * do in the string-based `query`.
    *
    * '''Compile-time type safety.''' If the case class field names or
    * types don't match the result schema, you get a compile error
    * rather than a runtime `AnalysisException` or wrong values.
    *
    * {{{
    *   case class CarrierRevenue(carrier: String, total: Long)
    *   implicit val dec: ResultDecoder[CarrierRevenue] = ResultDecoder.derive
    *
    *   val result: Dataset[CarrierRevenue] =
    *     model.queryAs[CarrierRevenue]("carrier", "total")
    *
    *   // Wrong case class field? COMPILE ERROR:
    *   // case class CarrierRevenue(carrier: String, totalPassengerrs: Long)
    *   // → error: value totalPassengerrs is not a member of CarrierRevenue
    *
    *   // Wrong field type? COMPILE ERROR:
    *   // case class CarrierRevenue(carrier: Int, total: Long)
    *   // → error: type mismatch: found Int, required String
    * }}}
    *
    * '''Note on Spark `Encoder[T]`.''' The `Dataset[T]` conversion uses
    * Spark's `.as[T]`, which requires an implicit `Encoder[T]` in scope
    * (usually via `import spark.implicits._`). If the encoder is not
    * in scope, you get a clear "could not find Encoder" compile error
    * pointing at the missing import — not a runtime failure.
    *
    * @tparam T the result row shape (typically a case class)
    * @return a Spark `Dataset[T]` of the typed result rows
    */
  def queryAs[T](
      measures:    Iterable[String],
      dimensions:  Iterable[String] = Nil,
      where:       Option[Predicate] = None,
      having:      Option[Predicate] = None,
      orderBy:     Iterable[SortKey] = Nil,
      limit:       Option[Int] = None,
      timeGrain:   Option[String] = None,
      timeGrains:  Map[String, String] = Map.empty,
      timeRange:   Option[(String, String)] = None,
  )(implicit spark: SparkSession, decoder: ResultDecoder[T], encoder: org.apache.spark.sql.Encoder[T]): Dataset[T] =
    query(measures, dimensions, where, having, orderBy, limit,
          timeGrain, timeGrains, timeRange)
      .execute(spark)
      .as[T](encoder)

  // -------------------------------------------------------------------------
  // Observability (Phase B)
  // -------------------------------------------------------------------------

  /** Summarize the planned execution path without running anything.
    *
    * Shows the op-tree shape, dimensions, measures, joins, filters, and the
    * aggregate plan — everything needed to understand what [[toDataFrame]] will do.
    * Classification decisions (base vs calc, topological layers) are logged by
    * [[SemanticLogger]] and appear in the output when Spark DEBUG logging is enabled
    * for the `io.semanticdf` logger.
    *
    * Use [[explain(spark)]] to see Spark's physical plan after compilation.
    *
    * @return a human-readable plan summary
    */
  def explain(): String = {
    val sb = new StringBuilder
    sb.append("semanticdf plan:\n")
    explainNode(root, sb, "  ")
    sb.toString
  }

  private def explainNode(op: SemanticOp, sb: StringBuilder, indent: String): Unit = {
    // The visitor is regenerated for each recursive call so that `indent`
    // is captured fresh from the current invocation. The accumulator `sb`
    // is shared via closure across the recursion.
    def renderOp(o: SemanticOp, ind: String): Unit = o match {
      case t: SemanticTableOp =>
        sb.append(s"${ind}table: ${t.name.getOrElse("(anonymous)")} " +
          s"[${t.table.columns.size} columns]\n")
        if (t.dimensions.nonEmpty)
          sb.append(s"${ind}  dimensions: ${t.dimensions.keys.mkString(", ")}\n")
        if (t.measures.nonEmpty) {
          sb.append(s"${ind}  measures:\n")
          t.measures.values.foreach(m =>
            sb.append(s"${ind}    ${m.name}: ${m.getClass.getSimpleName.replace("$", "")}"))
          sb.append("\n")
        }

      case j: SemanticJoinOp =>
        sb.append(s"${ind}join(${j.cardinality})\n")
        sb.append(s"${ind}  left:\n")
        renderOp(j.left, ind + "    ")
        sb.append(s"${ind}  right:\n")
        renderOp(j.right, ind + "    ")
        if (j.extraDimensions.nonEmpty)
          sb.append(s"${ind}  extra dimensions: ${j.extraDimensions.keys.mkString(", ")}\n")
        if (j.extraMeasures.nonEmpty)
          sb.append(s"${ind}  extra measures: ${j.extraMeasures.keys.mkString(", ")}\n")

      case a: SemanticAggregateOp =>
        sb.append(s"${ind}aggregate(keys=[${a.keys.mkString(", ")}]\n")
        sb.append(s"${ind}  measures: [${a.measureNames.mkString(", ")}]\n")
        sb.append(s"${ind}  source:\n")
        renderOp(a.source, ind + "    ")

      case SemanticFilterOp(src, pred) =>
        sb.append(s"${ind}filter(${pred.describe})\n")
        sb.append(s"${ind}  source:\n")
        renderOp(src, ind + "    ")

      // Pre-join row filter: applied to the source DataFrame before any join,
      // so it is part of the execution plan (visible in explain output).
      case SemanticRowFilterOp(src, name, _, expr, _) =>
        sb.append(s"${ind}row-filter($name): $expr\n")
        sb.append(s"${ind}  source:\n")
        renderOp(src, ind + "    ")

      case SemanticOrderByOp(src, keys) =>
        sb.append(s"${ind}orderBy(${keys.map(_.toString).mkString(", ")})\n")
        sb.append(s"${ind}  source:\n")
        renderOp(src, ind + "    ")

      case SemanticLimitOp(src, n) =>
        sb.append(s"${ind}limit($n)\n")
        sb.append(s"${ind}  source:\n")
        renderOp(src, ind + "    ")

      // Hint is a Spark planner wrapper; semanticdflly a pass-through for non-compile concerns.
      case SemanticHintOp(src, _, _) => renderOp(src, ind)

      // Transforms are applied at compile time; for explain purposes, walk through to the source.
      case SemanticTransformsOp(src, _) => renderOp(src, ind)
    }
    renderOp(op, indent)
  }


  /** Print the Spark physical plan after compiling the op tree.
    *
    * Calls `toDataFrame(spark).explain()` and returns the explain string.
    * This is the "real" plan — Catalyst-optimized, with actual column names,
    * shuffle/partitions info, and broadcast hints visible.
    *
    * Unlike [[explain()]] which shows the semanticdf op tree without compiling,
    * this method compiles the full plan and asks Spark to explain it.
    *
    * @param spark the active SparkSession
    * @return Spark's explain output string
    */
  def explain(implicit spark: SparkSession): String = {
    val df = toDataFrame(spark)
    df.queryExecution.explainString(
      org.apache.spark.sql.execution.ExplainMode.fromString("simple")
    )
  }

  /** Print the extended Spark plan after compiling the op tree.
    *
    * Equivalent to `df.explain(true)` — `ExplainMode.fromString("extended")`. Includes
    * cost/operator-level output that `[[explain(spark)]]` (simple mode) omits, e.g.
    * whole-stage codegen sections and per-operator formatted detail. Use this when
    * debugging join strategies, shuffle volume, or codegen paths.
    *
    * For a *semanticdf-aware, human-readable* view (WHERE vs HAVING, transitive deps,
    * join intent), see `[[explainSemantic]]` instead.
    *
    * @param spark the active SparkSession
    * @return Spark's extended explain output string
    */
  def explainExtended(implicit spark: SparkSession): String = {
    val df = toDataFrame(spark)
    df.queryExecution.explainString(
      org.apache.spark.sql.execution.ExplainMode.fromString("extended")
    )
  }

  /** Human-readable plan combining semantic intent + Spark's physical plan (Tier 1.5,
    * roadmap §1.5).
    *
    * Unlike [[explain()]] which is the op-tree shape only, or [[explain(spark)]] which
    * is just Catalyst output, this method explains *why* semanticdf routed things the
    * way it did: where each filter went (WHERE vs HAVING), which transitive measures
    * were pulled in, what time grains are valid, which joins compiled pre- vs post-agg.
    *
    * Sections, in order:
    *  1. ROUTING          — every filter, where it compiled, which field it references
    *  2. TRANSITIVE DEPS  — measures requested, transitively-pulled, and skipped
    *  3. DIMENSIONS       — list with time/pii/entity flags
    *  4. MEASURES         — list with base/calc classification
    *  5. JOINS            — cardinality, pre/post-agg strategy
    *  6. WARNINGS         — anything notable (calc cycles, time-grain risk, etc.)
    *  7. SPARK PLAN       — `df.explain()` output (only when `spark` is provided)
    *
    * Compile is forced iff `spark` is provided — pass `None` for a static-only view
    * (the cost is just walking the op tree).
    *
    * @param spark optional SparkSession. Required only for section 7 (Spark plan).
    *              Pass `None` to skip compilation.
    * @return multi-line plan summary
    */
  def explainSemantic(spark: Option[SparkSession]): String = {
    val renderer = new SemanticPlanRenderer(this)
    renderer.render(spark)
  }

  /** Convenience overload: pass a SparkSession directly (or null to skip the
    * Spark plan section). Equivalent to `explainSemantic(Option(spark))`. */
  def explainSemantic(implicit spark: SparkSession): String =
    explainSemantic(Option(spark))

  /** Scope selector for field inventory sections (DIMENSIONS, MEASURES).
    *
    * - `All`   — every dimension and measure declared on the model (default; legacy).
    * - `Used`  — only fields referenced by this query (groupBy / aggregate / orderBy / filter).
    *
    * Models commonly have many more fields than a single query touches, so `Used` lets
    * you produce a focused, query-specific report without exploding the section size.
    *
    * @example
    * {{{
    *   model.groupBy("carrier").aggregate("avg_passengers")
    *     .explainSemantic(spark, Scope.Used)   // collapsed inventory
    * }}} */
  def explainSemantic(implicit spark: SparkSession, scope: Scope): String =
    explainSemantic(Option(spark), scope)


  // -------------------------------------------------------------------------
  // Group-by / aggregate
  // -------------------------------------------------------------------------

  /** Apply a filter predicate with automatic WHERE/HAVING routing.
    *
    * Predicates over **dimensions** are applied pre-aggregation (WHERE — they filter
    * base rows). Predicates over **measures** are applied post-aggregation (HAVING —
    * they filter aggregated rows). `And` compounds are split per-condition; `Or`/
    * `Not` mixing dimension and measure conditions stay whole (post-agg, since they
    * cannot be evaluated independently).
    *
    * Use [[having]] to force a predicate to post-aggregation regardless of routing.
    *
    * @example
    * {{{
    * import Predicate._
    * st.where("carrier" === "AA")                      // WHERE
    * st.where("total_passengers" > 600)                 // HAVING
    * st.where(("carrier" === "AA") and ("total" > 100)) // split: WHERE + HAVING
    * }}}
    */
  def groupBy(keys: String*): SemanticGroupBy =
    new SemanticGroupBy(root, keys, postAggPredicates, version, sourceTable, status, auditSink, auditRequest, resultCache)

  // -------------------------------------------------------------------------
  // Typed field references (SemanticField typeclass)
  // -------------------------------------------------------------------------
  // Each typed overload enforces the right kind at compile time. Mixed
  // dimension-vs-measure calls (e.g. a Measure ref into groupByDimensions)
  // are rejected by the implicit-not-found error. Pure adapters: they read
  // the registered ref's `.name` and delegate to the existing string-based
  // method. Identical runtime cost; identical output.

  /** Group-by with one typed dimension ref. Same runtime as `groupBy(ref.name)`. */
  def groupByDimensions[D1](d1: FieldRef[D1])(implicit ev: SemanticDimension[D1]): SemanticGroupBy =
    groupBy(ev.name)

  /** Group-by with two typed dimension refs (each separately type-checked). */
  def groupByDimensions[D1, D2](d1: FieldRef[D1], d2: FieldRef[D2])(
      implicit ev1: SemanticDimension[D1], ev2: SemanticDimension[D2],
  ): SemanticGroupBy = groupBy(ev1.name, ev2.name)

  /** Group-by with three typed dimension refs. */
  def groupByDimensions[D1, D2, D3](d1: FieldRef[D1], d2: FieldRef[D2], d3: FieldRef[D3])(
      implicit ev1: SemanticDimension[D1], ev2: SemanticDimension[D2], ev3: SemanticDimension[D3],
  ): SemanticGroupBy = groupBy(ev1.name, ev2.name, ev3.name)

  /** Group-by with four typed dimension refs. */
  def groupByDimensions[D1, D2, D3, D4](d1: FieldRef[D1], d2: FieldRef[D2], d3: FieldRef[D3], d4: FieldRef[D4])(
      implicit ev1: SemanticDimension[D1], ev2: SemanticDimension[D2],
               ev3: SemanticDimension[D3], ev4: SemanticDimension[D4],
  ): SemanticGroupBy = groupBy(ev1.name, ev2.name, ev3.name, ev4.name)

  /** Group-by with 5+ typed field refs. Kind-checked at runtime: every ref's
    * `.kind` must equal `Dimension`. Arity > 4 drops compile-time kind
    * enforcement (Scala 2.13 varargs can't carry per-element phantom-kind
    * evidence). The first 4 elements get compile-time enforcement via the
    * arity-specific overloads above. */
  def groupByDimensionsAll(refs: Seq[FieldRef[_]]): SemanticGroupBy = {
    refs.foreach { r =>
      if (r.underlying.kind != FieldKind.Dimension)
        throw new IllegalArgumentException(
          s"${r.underlying.name} is not a dimension — groupBy requires dimensions, " +
            s"got a ${r.underlying.kind} ref"
        )
    }
    groupBy(refs.map(_.underlying.name): _*)
  }

  // -------------------------------------------------------------------------
  // Catalog accessors — filters
  // -------------------------------------------------------------------------

  /** All pre-join row filters declared on this model (via YAML `filters:` block).
    *
    * Source: the op tree (single source of truth). Walks [[SemanticRowFilterOp]]
    * nodes in YAML declaration order (innermost first, outermost last).
    * Empty if no `filters:` block was declared.
    *
    * Distinct from query-time filters added via `.where(predicate)` /
    * `.query(where = ...)` — those become [[SemanticFilterOp]] nodes and are
    * NOT returned here.
    */
  def filters: Seq[SemanticFilter] = collectFilters(root)

  private def collectFilters(op: SemanticOp): Seq[SemanticFilter] = op match {
    case SemanticRowFilterOp(src, name, desc, expr, meta) =>
      collectFilters(src) :+ SemanticFilter(name, desc, expr, meta)
    case j: SemanticJoinOp =>
      collectFilters(j.left) ++ collectFilters(j.right)
    case SemanticAggregateOp(src, _, _) => collectFilters(src)
    case SemanticFilterOp(src, _)       => collectFilters(src)
    case SemanticOrderByOp(src, _)      => collectFilters(src)
    case SemanticLimitOp(src, _)        => collectFilters(src)
    case SemanticHintOp(src, _, _)      => collectFilters(src)
    case SemanticTransformsOp(src, _)   => collectFilters(src)  // transforms are transparent
    case _                              => Nil
  }

  // -------------------------------------------------------------------------
  // SemanticGroupBy — builder returned by groupBy()
  // -------------------------------------------------------------------------

  /** Builder produced by [[SemanticTable.groupBy]]. `aggregate(measure names...)` compiles
    * to a [[SemanticAggregateOp]] wrapped in a [[SemanticTable]].
    *
    * Post-aggregation predicates (HAVING) accumulated via [[SemanticTable.where]] /
    * [[SemanticTable.having]] are applied by wrapping the aggregate result in
    * [[SemanticFilterOp]] nodes. */
  final class SemanticGroupBy private[semanticdf] (
      source: SemanticOp,
      keys: Seq[String],
      postAggPredicates: List[Predicate] = Nil,
      /** Per-model schema version, carried from the originating [[SemanticTable]]
        * so that the result of `groupBy().aggregate(...)` keeps the version.
        * Mirrors the convention used by `withDimensions` / `withMeasures` /
        * `orderBy` / `limit` (which all pass `version`). */
      version: Int = 0,
      /** Source-table name, carried from the originating [[SemanticTable]] for the
        * same reason as `version` — MCP and catalog consumers expect the resulting
        * query table to still report the source DataFrame name. */
      sourceTable: Option[String] = None,
      /** Lifecycle status, carried from the originating [[SemanticTable]] for
        * the same reason as `version` / `sourceTable` — the aggregated result
        * must still report the model's lifecycle so consumers (MCP, agents,
        * dashboards) can warn on deprecated models even after aggregation.
        * Defaults to Published; preserved through `groupBy().aggregate(...)`. */
      status: ModelStatus = ModelStatus.Published,
      /** Audit sink — carried from the originating [[SemanticTable]] so the
        * audit event is still emitted when `query()` ends in
        * `groupBy().aggregate(...)`. See [[SemanticTable.auditSink]]. */
      auditSink: Option[AuditSink] = None,
      /** Audit request — carried from the originating [[SemanticTable]] so the
        * audit event carries the user's original query shape. */
      auditRequest: Option[AuditQueryRequest] = None,
      /** Result cache — carried from the originating [[SemanticTable]] so the
        * cache check fires when `query()` ends in
        * `groupBy().aggregate(...)`. See [[SemanticTable.resultCache]]. */
      resultCache: Option[io.semanticdf.cache.ResultCache] = None,
  ) {
    /** Aggregate with one typed measure ref. Same runtime as `aggregate(ref.name)`. */
    def aggregateMeasures[M1](m1: FieldRef[M1])(implicit ev: SemanticMeasure[M1]): SemanticTable =
      aggregate(ev.name)

    /** Aggregate with two typed measure refs (each separately type-checked). */
    def aggregateMeasures[M1, M2](m1: FieldRef[M1], m2: FieldRef[M2])(
        implicit ev1: SemanticMeasure[M1], ev2: SemanticMeasure[M2],
    ): SemanticTable = aggregate(ev1.name, ev2.name)

    /** Aggregate with three typed measure refs. */
    def aggregateMeasures[M1, M2, M3](m1: FieldRef[M1], m2: FieldRef[M2], m3: FieldRef[M3])(
        implicit ev1: SemanticMeasure[M1], ev2: SemanticMeasure[M2], ev3: SemanticMeasure[M3],
    ): SemanticTable = aggregate(ev1.name, ev2.name, ev3.name)

    /** Aggregate with four typed measure refs. */
    def aggregateMeasures[M1, M2, M3, M4](m1: FieldRef[M1], m2: FieldRef[M2], m3: FieldRef[M3], m4: FieldRef[M4])(
        implicit ev1: SemanticMeasure[M1], ev2: SemanticMeasure[M2],
                 ev3: SemanticMeasure[M3], ev4: SemanticMeasure[M4],
    ): SemanticTable = aggregate(ev1.name, ev2.name, ev3.name, ev4.name)

    /** Aggregate with 5+ typed field refs. Kind-checked at runtime: every ref's
      * `.kind` must equal `Measure`. See [[groupByDimensionsAll]] for why
      * arities beyond 4 use Seq. */
    def aggregateMeasuresAll(refs: Seq[FieldRef[_]]): SemanticTable = {
      refs.foreach { r =>
        if (r.underlying.kind != FieldKind.Measure)
          throw new IllegalArgumentException(
            s"${r.underlying.name} is not a measure — aggregate requires measures, " +
              s"got a ${r.underlying.kind} ref"
          )
      }
      aggregate(refs.map(_.underlying.name): _*)
    }

    def aggregate(measures: String*): SemanticTable = {
      var op: SemanticOp = SemanticAggregateOp(source, keys, measures)
      // Wrap with post-agg filters (HAVING). Each is a SemanticFilterOp on the aggregate.
      postAggPredicates.foreach { p => op = SemanticFilterOp(op, p) }
      new SemanticTable(op, version = version, sourceTable = sourceTable, status = status, auditSink = this.auditSink, auditRequest = this.auditRequest, resultCache = this.resultCache)
    }
  }
}

// =============================================================================
// SemanticPlanRenderer — Tier 1.5 explain-semantic renderer
// =============================================================================

/* Walks a SemanticTable's op tree and produces a multi-section human-readable plan
 * explaining where every predicate went, what dependencies were pulled in, what
 * joins compiled pre/post-agg, and which Spark plan was generated.
 *
 * Why a separate class:
 *   - keeps `SemanticTable` small (already 770+ lines)
 *   - isolates the rendering logic so it can be unit-tested independently
 *   - it's stateless apart from the captured `SemanticTable` reference
 *
 * Output sections (separated by horizontal rules):
 *   1. PLAN SUMMARY      - one-line description of the query
 *   2. SEMANTIC ROUTING  - filter decisions (WHERE/HAVING) with reasons
 *   3. TRANSITIVE DEPS   - measures requested vs pulled in vs skipped
 *   4. DIMENSIONS        - with time/pii/entity flags
 *   5. MEASURES          - with base/calc classification + reasons
 *   6. JOINS             - cardinality + pre/post-agg strategy
 *   7. WARNINGS          - notable concerns (only if non-empty)
 *   8. SPARK PLAN        - df.explain() output (only when spark is provided)
 */
private[semanticdf] class SemanticPlanRenderer(st: SemanticTable, scope: Scope = Scope.All) {

  /** Render the multi-section plan. */
  def render(spark: Option[SparkSession]): String = {
    val sb = new StringBuilder

    sb.append(renderSummary())
    sb.append(hr()).append(renderRouting())
    sb.append(hr()).append(renderTransitiveDeps())
    sb.append(hr()).append(renderDimensions())
    sb.append(hr()).append(renderMeasures())
    sb.append(hr()).append(renderJoins())

    val warnings = collectWarnings()
    if (warnings.nonEmpty) {
      sb.append(hr()).append(renderWarnings(warnings))
    }

    spark.foreach { sp =>
      sb.append(hr()).append(renderSparkPlan(sp))
    }

    sb.toString
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private def hr(): String = "\n" + ("─" * 50) + "\n"
  private def heading(text: String): String = s"$text\n${"─" * text.length}\n"
  private def indent(s: String, n: Int = 2): String =
    s.linesIterator.map(line => if (line.isEmpty) line else (" " * n) + line).mkString("\n")

  /** Walk the op tree and return fields actually referenced by this query.
    *
    * A field is "referenced" if it appears in: aggregate keys, aggregate measure
    * names, sort keys, or filter predicate fields. Transitively-pulled calc deps
    * are added via a [[MeasureProbeScope]] walk on top of the directly-referenced
    * set, so a calc measure also surfaces its base-measure dependencies.
    *
    * Note: a bare [[SemanticTableOp]] does NOT contribute its declared fields —
    * `referencedFields` is about what this query *touches*, not what the model
    * *declares*. Used by [[renderDimensions]] / [[renderMeasures]] under
    * [[Scope.Used]]. */
  private def referencedFields(): Set[String] = {
    val acc = scala.collection.mutable.Set.empty[String]
    // The visitor's visit() auto-recurses; we collect referenced fields
    // exactly once per op (no explicit visit() calls here).
    val collector = new SemanticOpVisitor {
      override def enter(op: SemanticOp): Unit = op match {
        case _: SemanticTableOp        => ()  // leaf: do not enumerate declared fields
        case j: SemanticJoinOp =>
          j.extraDimensions.keys.foreach(acc.add)
          j.extraMeasures.keys.foreach(acc.add)
        case a: SemanticAggregateOp =>
          a.keys.foreach(acc.add)
          a.measureNames.foreach(acc.add)
        case SemanticFilterOp(_, pred) =>
          pred.fields.foreach(acc.add)
        case SemanticOrderByOp(_, keys) =>
          keys.foreach(k => acc.add(SortKey.nameOf(k)))
        case _ => ()  // RowFilter, Limit, Hint, Transforms: no field refs.
      }
    }
    collector.visit(st.root)

    // Expand transitively-pulled calc measures so Scope.Used surfaces auto-pulled bases.
    val allMs = allMeasures().toMap
    val known = allMs.keySet
    val queue  = scala.collection.mutable.Queue.empty[String]
    acc.foreach { n => if (known.contains(n)) queue.enqueue(n) }
    val closed = scala.collection.mutable.Set.empty[String] ++ acc
    while (queue.nonEmpty) {
      val name = queue.dequeue()
      val m    = allMs(name)
      val probe = new MeasureProbeScope(known - name)
      try m.expr(probe) catch { case _: Throwable => }
      probe.referenced.foreach { dep =>
        if (!closed.contains(dep) && known.contains(dep)) {
          closed += dep
          queue.enqueue(dep)
        }
      }
    }
    closed.toSet
  }

  /** All measure names reachable from the root, in stable order. */
  private[semanticdf] def allMeasures(): Seq[(String, Measure)] = {
    val acc = scala.collection.mutable.LinkedHashMap.empty[String, Measure]
    val collector = new SemanticOpVisitor {
      override def enter(op: SemanticOp): Unit = op match {
        case t: SemanticTableOp =>
          t.measures.foreach { case (n, m) => acc.update(n, m) }
        case j: SemanticJoinOp =>
          j.extraMeasures.foreach { case (n, m) => acc.update(n, m) }
        case _ => ()  // wrappers (Aggregate, Filter, RowFilter, OrderBy, Limit, Hint, Transforms) carry no declared measures.
      }
    }
    collector.visit(st.root)
    acc.toSeq
  }

  /** All dimensions reachable from the root, in stable order. */
  private[semanticdf] def allDimensions(): Seq[(String, Dimension)] = {
    val acc = scala.collection.mutable.LinkedHashMap.empty[String, Dimension]
    val collector = new SemanticOpVisitor {
      override def enter(op: SemanticOp): Unit = op match {
        case t: SemanticTableOp =>
          t.dimensions.foreach { case (n, d) => acc.update(n, d) }
        case j: SemanticJoinOp =>
          j.extraDimensions.foreach { case (n, d) => acc.update(n, d) }
        case _ => ()
      }
    }
    collector.visit(st.root)
    acc.toSeq
  }

  /** All join operations in the op tree. */
  private def allJoins(): Seq[SemanticJoinOp] = {
    val acc = scala.collection.mutable.ListBuffer.empty[SemanticJoinOp]
    val collector = new SemanticOpVisitor {
      override def enter(op: SemanticOp): Unit = op match {
        case j: SemanticJoinOp => acc += j
        case _ => ()
      }
    }
    collector.visit(st.root)
    acc.toSeq
  }

  /** All filter operations, in op-tree order (top-down). */
  private[semanticdf] def allFilters(): Seq[(SemanticFilterOp, Boolean)] = {
    // Boolean = "is HAVING (post-agg)" — true iff the filter's own source is an
    // aggregate (matches SemanticFilterOp.compile's runtime check).
    val acc = scala.collection.mutable.ListBuffer.empty[(SemanticFilterOp, Boolean)]
    // Note: we need to inspect the FILTER's own source to determine
    // isHaving. The visitor auto-recurses into f.source AFTER enter(f),
    // so we look at f.source directly here.
    val collector = new SemanticOpVisitor {
      override def enter(op: SemanticOp): Unit = op match {
        case f @ SemanticFilterOp(src, _) =>
          val isHaving = src match {
            case _: SemanticAggregateOp => true
            case _                      => false
          }
          acc += ((f, isHaving))
        case _ => ()
      }
    }
    collector.visit(st.root)
    acc.toSeq
  }

  /** All pre-join row filters (declared via YAML `filters:` block) reachable
    * from the root, in op-tree declaration order (innermost first, outermost
    * last). Distinct from [[allFilters]] which returns query-time filters. */
  private[semanticdf] def allRowFilters(): Seq[SemanticRowFilterOp] = {
    // Use `leave` instead of `enter` so we record innermost-first: the
    // visitor recurses into the source BEFORE calling leave on the wrapper,
    // so the deepest SemanticRowFilterOp's leave runs first.
    val acc = scala.collection.mutable.ListBuffer.empty[SemanticRowFilterOp]
    val collector = new SemanticOpVisitor {
      override def leave(op: SemanticOp): Unit = op match {
        case rf: SemanticRowFilterOp => acc += rf
        case _ => ()
      }
    }
    collector.visit(st.root)
    acc.toSeq
  }

  // ---------------------------------------------------------------------------
  // Section renderers
  // ---------------------------------------------------------------------------

  private def renderSummary(): String = {
    val sb = new StringBuilder
    sb.append(heading("PLAN SUMMARY"))

    // Table name(s) — for joins, all source tables are listed (root first).
    val tableNames = allTableNames
    val tableLabel = if (tableNames.isEmpty) "(unnamed)"
                     else if (tableNames.size == 1) tableNames.head
                     else tableNames.mkString(" + ")
    sb.append(s"  table:   $tableLabel\n")

    // Find the aggregate even if it's wrapped by HAVING/orderBy/limit filters.
    // Uses the SemanticOpVisitor (R1 refactor) with a `stop` flag to
    // short-circuit at the first match. The visitor's exhaustive match
    // in `visit()` is the single point of truth for which ops we walk
    // through — adding a new op to the language updates the visitor
    // base class, not this call site.
    val findAggregateVisitor = new SemanticOpVisitor {
      var found: Option[SemanticAggregateOp] = None
      override def enter(op: SemanticOp): Unit = op match {
        case a: SemanticAggregateOp =>
          found = Some(a)
          stop = true
        case _ => ()
      }
    }
    findAggregateVisitor.visit(st.root)
    findAggregateVisitor.found match {
      case Some(a) =>
        val keys = if (a.keys.isEmpty) "(all rows)" else a.keys.mkString(", ")
        val meas = a.measureNames.mkString(", ")
        sb.append(s"  group by: $keys\n")
        sb.append(s"  compute:  $meas\n")
      case None =>
        st.root match {
          case t: SemanticTableOp =>
            sb.append(s"  type:     scan (no aggregation)\n")
          case _ =>
            sb.append(s"  type:     derived table (subquery or view)\n")
        }
    }

    // (Filters and joins are summarised in their own sections — no duplicate counters here.)
    sb.toString
  }

  private def renderRouting(): String = {
    val measureNames = allMeasures().map(_._1).toSet
    val filters = allFilters()
    val rowFilters = allRowFilters()
    val sb = new StringBuilder
    sb.append(heading("SEMANTIC ROUTING")).append("\n")

    if (filters.isEmpty && rowFilters.isEmpty) {
      sb.append("  (no filters applied)\n")
      return sb.toString
    }

    // Pre-join row filters are listed first (they apply before WHERE), with a
    // distinct "ROW-FILTER" label so the reader can tell them apart from WHERE.
    rowFilters.foreach { rf =>
      val desc = rf.description.fold("")(d => s" \u2014 $d")
      sb.append(s"  ROW-FILTER \u2192 ${rf.name}: ${rf.expr}$desc\n")
      sb.append(s"      \u2514\u2500 runs against source table pre-join (hygiene, not a row selection)\n")
    }

    filters.foreach { case (SemanticFilterOp(_, pred), isHaving) =>
      val label    = if (isHaving) "HAVING \u2192" else "WHERE  \u2192"
      val reason   = if (isHaving) "runs after aggregation (slower)" else "runs before aggregation (fast)"
      val fields   = pred.fields.toSeq.sorted.mkString(", ")
      val (pre, post) = Predicate.splitFilter(pred, measureNames)

      sb.append(s"  $label ${pred.describe}\n")
      sb.append(s"      \u2514\u2500 $reason; touches: $fields\n")

      if (pred.isInstanceOf[Predicate.And] && pre.nonEmpty && post.nonEmpty) {
        sb.append(s"      \u2514\u2500 compound AND split at compile time:\n")
        sb.append(s"         \u2192 WHERE  (pre-agg):  ${pre.map(_.describe).mkString(" AND ")}\n")
        sb.append(s"         \u2192 HAVING (post-agg): ${post.map(_.describe).mkString(" AND ")}\n")
      }
    }
    sb.toString
  }

  private def renderTransitiveDeps(): String = {
    // First pass: collect only what the user *directly* asked for
    // (SemanticAggregateOp.measureNames + OrderBy sort keys). Filter predicates
    // reference dimensions or measures but the dimension/measure router handles
    // WHERE/HAVING placement in the SEMANTIC ROUTING section, so we don't pull
    // filter refs here.
    val requestedDirect = scala.collection.mutable.LinkedHashSet.empty[String]
    val collector = new SemanticOpVisitor {
      override def enter(op: SemanticOp): Unit = op match {
        case a: SemanticAggregateOp => a.measureNames.foreach(requestedDirect.add)
        case SemanticFilterOp(src, _)    => visit(src)
        // Pre-join row filters do not contribute directly to the transitive deps walk.
        case SemanticRowFilterOp(src, _, _, _, _) => visit(src)
        case SemanticOrderByOp(src, keys) =>
          keys.foreach(k => requestedDirect.add(SortKey.nameOf(k)))
          visit(src)
        case SemanticLimitOp(src, _)     => visit(src)
        case SemanticHintOp(src, _, _)   => visit(src)
        case SemanticTransformsOp(src, _) => visit(src)  // transforms are transparent
        case _: SemanticJoinOp           =>
        case _: SemanticTableOp          =>
      }
    }
    collector.visit(st.root)

    val allMs = allMeasures().toMap

    // Transitive closure via MeasureProbeScope: start with directly-requested
    // measures and expand into calc deps using the same classification probe the
    // compile path uses. Mirrors SemanticAggregateOp.transitiveClosure.
    val closed = scala.collection.mutable.LinkedHashMap.empty[String, Measure]
    val queue  = scala.collection.mutable.Queue.empty[String]
    requestedDirect.foreach { n => allMs.get(n).foreach { m => closed(n) = m; queue.enqueue(n) } }
    while (queue.nonEmpty) {
      val name = queue.dequeue()
      val m    = closed(name)
      val probe = new MeasureProbeScope(allMs.keySet - name)
      try m.expr(probe) catch { case _: Throwable => }
      probe.referenced.foreach { dep =>
        if (!closed.contains(dep))
          allMs.get(dep).foreach { dm => closed(dep) = dm; queue.enqueue(dep) }
      }
    }
    val willCompute = closed.keys.toSet
    val autoPulled   = willCompute -- requestedDirect

    val all     = allMs.keySet
    val skipped = all -- willCompute
    val unknown = requestedDirect -- all

    val sb = new StringBuilder
    sb.append(heading("TRANSITIVE DEPENDENCIES"))

    // Three compact blocks: DIRECT / PULLED IN / SKIPPED. No redundant summary
    // line — the union is implicit from the two source lists. When the user has
    // not requested any measures yet, emit a hint instead of empty blocks.
    if (requestedDirect.isEmpty && allMs.isEmpty) {
      sb.append("  (no measures declared)\n")
    } else if (requestedDirect.isEmpty) {
      sb.append(s"  (all ${allMs.size} declared measures available; .aggregate / .orderBy selected none)\n")
    } else {
      sb.append("  DIRECT (you asked for these):\n")
      requestedDirect.toSeq.sorted.foreach(n => sb.append(s"    $n\n"))
      if (autoPulled.nonEmpty) {
        sb.append("  PULLED IN (by your calcs):\n")
        autoPulled.toSeq.sorted.foreach(n => sb.append(s"    $n\n"))
      }
    }
    if (skipped.nonEmpty) {
      sb.append(s"  SKIPPED (declared but unused): ${skipped.toSeq.sorted.mkString(", ")}\n")
    }
    if (unknown.nonEmpty) {
      sb.append(s"  ⚠ UNKNOWN (typo?): ${unknown.toSeq.sorted.mkString(", ")}\n")
    }
    sb.toString
  }

  private def renderDimensions(): String = {
    val all    = allDimensions()
    val used   = referencedFields()
    val (dims, collapsed) = scope match {
      case Scope.Used => all.partition { case (n, _) => used.contains(n) }
      case _                        => (all, Nil)
    }
    val sb = new StringBuilder
    val declared = dims.size + collapsed.size
    val label    = if (collapsed.isEmpty) s"${dims.size}" else s"${dims.size} of $declared"
    sb.append(heading(s"DIMENSIONS ($label)"))
    sb.append("  Columns you can group by, filter on, or use in orderBy.\n")
    sb.append("\n")
    if (dims.isEmpty) {
      sb.append("  (none referenced by this query)\n")
    } else {
      dims.foreach { case (name, d) =>
        val flags = scala.collection.mutable.ListBuffer.empty[String]
        if (d.isTimeDimension)        flags += "time"
        if (d.isEntity)               flags += "entity"
        if (d.isEventTimestamp)       flags += "event_ts"
        if (d.smallestTimeGrain.isDefined) flags += s"grain=${d.smallestTimeGrain.get}"
        if (d.metadata.get("pii").contains("true")) flags += "pii"
        val tag  = if (flags.isEmpty) "" else s"  [${flags.mkString(", ")}]"
        val desc = d.description.fold("")(dd => s"  — $dd")
        sb.append(s"  $name$tag$desc\n")
      }
    }
    if (collapsed.nonEmpty)
      sb.append(s"  (${collapsed.size} more declared — not referenced by this query; use Scope.All to expand)\n")
    sb.toString
  }

  private def renderMeasures(): String = {
    val all        = allMeasures()
    val usedSet    = referencedFields()
    val (measures, collapsedMeasures) = scope match {
      case Scope.Used => all.partition { case (n, _) => usedSet.contains(n) }
      case _                        => (all, Nil)
    }
    val sb = new StringBuilder
    val declared = measures.size + collapsedMeasures.size
    val label    = if (collapsedMeasures.isEmpty) s"${measures.size}" else s"${measures.size} of $declared"
    sb.append(heading(s"MEASURES ($label)"))
    sb.append("  Aggregations: base = direct agg; calc = built from other measures.\n")
    sb.append("\n")
    if (measures.isEmpty) {
      sb.append("  (none referenced by this query)\n")
      if (collapsedMeasures.nonEmpty)
        sb.append(s"  (${collapsedMeasures.size} more declared — not referenced by this query; use Scope.All to expand)\n")
      return sb.toString
    }

    // Probe each measure's expr to detect which other measures it references.
    // Uses a SemanticScope that records refs but never executes — same idea as
    // SemanticOp's ClassificationScope, but DataFrame-free.
    val known = all.map(_._1).toSet
    measures.foreach { case (name, m) =>
      val probe = new MeasureProbeScope(known - name)
      try { m.expr(probe) } catch { case _: Throwable => /* probe failure => base */ }
      val deps     = probe.referenced.toSet
      val totals   = probe.referencedTotals.toSet
      val kind = if (deps.nonEmpty) "calc" else "base"
      val tag  = s"[$kind]"
      val desc = m.description.fold("")(d => s"  — $d")
      sb.append(s"  $name  $tag$desc\n")
      if (deps.nonEmpty) {
        // When `totals` ⊆ `deps`, mark each in-deps entry that's also a grand-total
        // reference inline, so the reader doesn't see the same name twice in two
        // adjacent lines. Standalone totals (those not already in `deps`) keep
        // their own line.
        val annotated = deps.toSeq.sorted.map { d =>
          if (totals.contains(d)) s"$d (as grand total)" else d
        }
        sb.append(s"    pulls in: ${annotated.mkString(", ")}\n")
        val standaloneTotals = totals -- deps
        if (standaloneTotals.nonEmpty) {
          val sorted = standaloneTotals.toSeq.sorted.mkString(", ")
          sb.append(s"    uses grand totals: $sorted  (1-row cross-join)\n")
        } else if (totals.nonEmpty) {
          sb.append(s"    (grand totals via 1-row cross-join)\n")
        }
      } else if (totals.nonEmpty) {
        val sorted = totals.toSeq.sorted.mkString(", ")
        sb.append(s"    uses grand totals: $sorted  (1-row cross-join)\n")
      }
    }
    if (collapsedMeasures.nonEmpty)
      sb.append(s"  (${collapsedMeasures.size} more declared — not referenced by this query; use Scope.All to expand)\n")
    sb.toString
  }

  private def renderJoins(): String = {
    val joins = allJoins()
    val sb = new StringBuilder
    sb.append(heading(s"JOINS (${joins.size})"))
    if (joins.isEmpty) {
      sb.append("  (none)\n")
      return sb.toString
    }
    joins.foreach { j =>
      val card     = j.cardinality.toString.toUpperCase
      val (verb, blurb) = j.cardinality match {
        case JoinCardinality.One   => ("LEFT JOIN",          "each row on the right matches at most one on the left")
        case JoinCardinality.Many  => ("PRE-AGG, then JOIN", "each side pre-aggregated at join-key grain — prevents fan-out explosion")
        case JoinCardinality.Cross => ("CROSS JOIN",         "every row on the left × every row on the right")
      }
      // grainCols is read via DynamicVariable, which falls back to Seq.empty
      // when the calling thread hasn't compiled this op (e.g. explainSemantic
      // called from a different thread than execute). Render the placeholder in
      // that case so the plan is still informative.
      val keys = j.grainCols
      j.cardinality match {
        case JoinCardinality.Cross =>
          sb.append(s"  $card  $verb — $blurb\n")
        case _ =>
          val keysStr = if (keys.isEmpty) "(uncompiled)" else keys.mkString("[", ", ", "]")
          sb.append(s"  $card  $verb on $keysStr\n")
          sb.append(s"       $blurb\n")
      }
      if (j.extraDimensions.nonEmpty) {
        sb.append(s"    brings in dimensions: ${j.extraDimensions.keys.toSeq.sorted.mkString(", ")}\n")
      }
      if (j.extraMeasures.nonEmpty) {
        sb.append(s"    brings in measures:  ${j.extraMeasures.keys.toSeq.sorted.mkString(", ")}\n")
      }
    }
    sb.toString
  }

  // Single source of truth for warnings/errors: the public `validate()` method.
  // The renderer just consumes the warnings half for its WARNINGS section.
  private def collectWarnings(): Seq[String] = st.validate().warnings

  private def renderWarnings(warnings: Seq[String]): String = {
    val sb = new StringBuilder
    sb.append(heading("WARNINGS"))
    warnings.foreach { w => sb.append(s"  ⚠  $w\n") }
    sb.toString
  }

  private def renderSparkPlan(spark: SparkSession): String = {
    val plan = try {
      val df = st.toDataFrame(spark)
      df.queryExecution.explainString(
        org.apache.spark.sql.execution.ExplainMode.fromString("simple")
      )
    } catch {
      case e: Throwable => s"(failed to render Spark plan: ${e.getClass.getSimpleName}: ${e.getMessage})"
    }
    heading("SPARK PLAN (df.explain)") + indent(plan) + "\n"
  }

  /** All table names reachable from the root, in op-tree order (root first, then left-to-right).
    * Used to label the PLAN SUMMARY. Returns an empty list if no [[SemanticTableOp]] is
    * reachable (rare). */
  private def allTableNames: Seq[String] = {
    val acc = scala.collection.mutable.ListBuffer.empty[String]
    val collector = new SemanticOpVisitor {
      override def enter(op: SemanticOp): Unit = op match {
        case t: SemanticTableOp => t.name.foreach(acc += _)
        case _ => ()
      }
    }
    collector.visit(st.root)
    acc.toSeq
  }
}

/** Field-inventory scope selector for `explainSemantic` (see
  * [[SemanticTable.explainSemantic(spark:org.apache.spark.sql.SparkSession, scope:* Scope)]]).
  *
  * - `All`:  every declared dimension and measure (default — legacy behaviour).
  * - `Used`: only fields referenced by this query; the rest are collapsed.
  *
  * Package-level (not nested in `SemanticTable`) because the underlying renderer is a
  * private class in this same file and path-dependent types are not visible there.
  *
  * In the v0.2.0 refactor, the renderer and this `Scope` trait moved together to
  * `SemanticTableCollection.scala` — they form one cohesive concern (the
  * explain-semantic surface) so the trait-mixin pattern keeps them co-located. */
sealed trait Scope
object Scope {
  case object All  extends Scope
  case object Used extends Scope
}

/** Lightweight probe used by [[SemanticPlanRenderer]] to classify a measure as base
  * vs calc without needing a real DataFrame. Records which known-measure names a
  * lambda references via `t(name)` and via `t.all(name)`; returns `lit(0.0)` for
  * everything (never executes). Mirrors the intent of
  * [[SemanticOp.ClassificationScope]] but is self-contained here so the renderer
  * does not depend on a SparkSession. */
private[semanticdf] final class MeasureProbeScope(known: Set[String]) extends SemanticScope {
  private[semanticdf] val referenced       = scala.collection.mutable.Set.empty[String]
  private[semanticdf] val referencedTotals = scala.collection.mutable.Set.empty[String]
  override def apply(name: String): Column = {
    if (known.contains(name)) referenced += name
    org.apache.spark.sql.functions.lit(0.0)
  }
  override def all(name: String): Column = {
    if (known.contains(name)) referencedTotals += name
    org.apache.spark.sql.functions.lit(0.0)
  }
}
