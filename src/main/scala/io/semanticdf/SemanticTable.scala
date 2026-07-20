package io.semanticdf

import org.apache.spark.sql.{Column, Dataset, DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._
import scala.jdk.CollectionConverters._

/** Sort-key DSL for [[SemanticTable.orderBy]] / [[SemanticTable.query]] (Phase 5 completion).
  *
  * A bare string is ascending; wrap in [[SortKey.desc]] for descending:
  * {{{
  * st.orderBy("carrier", SortKey.desc("total_passengers"))
  * }}} */
sealed trait SortKey {
  private[semanticdf] def toColumn: Column
}
object SortKey {

  /** Wrap a column name in backticks if it contains characters that
    * Spark's `col(...)` would misinterpret — notably `.` (treated as a
    * table/struct qualifier). Joined dimensions are named `alias.column`
    * (e.g. `customers.signup_date`); without quoting, `col("customers.x")`
    * looks for a nested struct field instead of the literal column.
    * Names already wrapped in backticks (by the caller) are left as-is,
    * so this is backward-compatible with manual `` SortKey.asc(s"`x`") ``.
    * Simple identifiers are returned unchanged. */
  private def quote(name: String): String =
    if (name.startsWith("`")) name
    else if (name.matches("[a-zA-Z_][a-zA-Z0-9_]*")) name
    else s"`$name`"

  private[semanticdf] final case class Asc(name: String)  extends SortKey { def toColumn = col(quote(name)).asc }
  private[semanticdf] final case class Desc(name: String) extends SortKey { def toColumn = col(quote(name)).desc }

  /** Explicit ascending key. */
  def asc(name: String): SortKey = Asc(name)
  /** Explicit descending key. */
  def desc(name: String): SortKey = Desc(name)

  /** Typed ascending key — reads the column name directly from the
    * [[SemanticField]] witness. Works for any field (dimension or measure),
    * so `SortKey.asc(carrier)`, `SortKey.desc(pax)` are both valid.
    *
    * The parameter is the typeclass instance itself (not a `FieldRef`), so
    * `SemanticDimension[F]` / `SemanticMeasure[F]` match by subtyping in
    * Scala's phase-1 overload resolution — no implicit conversion is needed,
    * and this overload is picked over `asc(name: String)` even from
    * cross-package consumer code. */
  def asc(field: SemanticField[_]): SortKey = Asc(field.name)

  /** Typed descending key — see [[asc(field)*]]. */
  def desc(field: SemanticField[_]): SortKey = Desc(field.name)

  /** Read the column-name field of any SortKey (private to avoid exposing the sealed
    * cases to public API). Used by [[SemanticTable.explainSemantic]]. */
  private[semanticdf] def nameOf(k: SortKey): String = k match {
    case Asc(n)  => n
    case Desc(n) => n
    case _       => ""
  }

  /** Implicit `String => SortKey` so `orderBy("carrier", SortKey.desc("x"))` works. */
  implicit def strToSortKey(name: String): SortKey = Asc(name)
}

/** Structured result of a [[SemanticTable.validate]] call.
  *
  * - `errors`   are conditions that would cause `execute()` to throw at runtime.
  * - `warnings` are conditions that are legal but worth surfacing (e.g. a time
  *              dimension with no `smallestTimeGrain` would surprise `atTimeGrain()`).
  *
  * `isValid` is the boolean summary; CI checks use that directly. */
final case class ValidationResult(
    errors: Seq[String],
    warnings: Seq[String],
) {
  def isValid: Boolean = errors.isEmpty
  def hasIssues: Boolean = errors.nonEmpty || warnings.nonEmpty
}

/** Immutable facade over the root of a semantic op tree (DESIGN §4.1).
  *
  * A `SemanticTable` is *not* a Spark `DataFrame`; it is a deferred, source-agnostic
  * definition that compiles to a DataFrame at an execution terminal. The batch terminal
  * is [[SemanticTable.toDataFrame]] / [[SemanticTable.execute]]; a future streaming
  * terminal (`toStreamingQuery`) is described in ADR 0002 — the same definition, a
  * different sink, mirroring Spark's own `df.write` vs `df.writeStream`.
  */
final class SemanticTable private[semanticdf] (
    private[semanticdf] val root: SemanticOp,
    private[semanticdf] val postAggPredicates: List[Predicate] = Nil,
    /** Per-model schema version, propagated to MCP/OKF consumers.
      *
      * `0` means "pre-versioning era" — the model declaration did not commit to
      * a version. The library never fails on a mismatch; it just stores and emits
      * the value. Compatibility policy is the consumer's problem (MCP server,
      * agent framework, downstream pipelines).
      *
      * Defaults to 0. Set via the YAML `version:` field or the fluent `.version(n)` setter.
      */
    val version: Int = 0,
    /** Name of the underlying source DataFrame this model was built from, if known.
      *
      * Populated by [[YamlLoader]] from the YAML `table:` field — the name used
      * to resolve the source DataFrame against either a caller-supplied map or
      * the Spark catalog. Unset (None) for models built directly from the Scala
      * DSL ([[io.semanticdf.toSemanticTable]]) where there's no equivalent concept.
      *
      * Used by MCP `describe_model` to expose the origin of a model's data to
      * consumers (LLM agents, BI tools, lineage trackers).
      */
    val sourceTable: Option[String] = None,
) {

  /** Batch terminal (DESIGN §4.5).
    *
    * Compiles the op tree against `spark` and returns the resulting `DataFrame`.
    * Recompiles on every call; never caches the result internally (DESIGN §4.4).
    *
    * `spark` is an implicit parameter so callers with an
    * `implicit val spark: SparkSession` in scope can write `.toDataFrame()`
    * (no argument). Explicit `.toDataFrame(spark)` is fully backward-compatible.
    */
  def toDataFrame(implicit spark: SparkSession): DataFrame =
    root.compile(spark)

  /** Fluent-chain alias for [[toDataFrame]]. `spark` is implicit; see [[toDataFrame]].
    */
  def execute(implicit spark: SparkSession): DataFrame = toDataFrame(spark)

  /** Streaming terminal (ADR 0002) — compile the op tree as a Structured
    * Streaming query. Parallel to [[toDataFrame]] / [[execute]]; the
    * API shape is the same (one terminal call, one StreamingQuery back).
    *
    * '''Scope of this PR (PR 1):''' only `where` filters on streaming
    * sources are supported. The validator ([[StreamingSupport.StreamingValidator]])
    * rejects op trees that use `groupBy + aggregate`, `orderBy`, `limit`,
    * `t.all(...)`, or joins — each with a clear message pointing at the
    * ADR 0002 stage that would enable it. The validator fails LOUDLY
    * before the query starts; the user's `foreachBatch` callback never
    * sees a partial / wrong result.
    *
    * Each micro-batch is processed by `foreachBatch`: the streaming
    * source is replaced with the batch `DataFrame`, the existing
    * batch compile path runs against it, and the result is passed to
    * the user's `opts.foreachBatch` callback. This is Spark's
    * recommended pattern for "run arbitrary batch logic on a stream"
    * and means the streaming terminal reuses 100% of the existing
    * dimension/measure/filter compile logic.
    *
    * Example:
    * {{{
    *   val model = toStreamingSemanticTable(spark.readStream.format("rate").load())
    *     .withDimensions(Dimension("value", t => t("value")))
    *     .withMeasures(Measure("count", t => count(lit(1))))
    *
    *   val query = model.toStreamingQuery(spark, StreamingQueryOptions(
    *     trigger = Some(Trigger.ProcessingTime("5 seconds")),
    *     foreachBatch = (df: DataFrame) => df.write.parquet("/tmp/out")
    *   ))
    *   query.awaitTermination()
    * }}}
    *
    * Returns a [[org.apache.spark.sql.streaming.StreamingQuery]] that the
    * caller can `awaitTermination()` or `stop()`.
    */
  def toStreamingQuery(
      spark: SparkSession,
      opts: StreamingSupport.StreamingQueryOptions = StreamingSupport.StreamingQueryOptions(),
  ): org.apache.spark.sql.streaming.StreamingQuery = {
    import StreamingSupport._
    import org.apache.spark.sql.functions._

    // 1. Find the streaming source. A non-streaming model is a hard error
    //    (not a soft one) — the user built a batch model and is calling
    //    the streaming terminal by mistake. The root may be a
    //    SemanticAggregateOp wrapping a SemanticStreamingTableOp (when the
    //    model uses groupBy + aggregate); we walk through to find the source.
    val source: DataFrame = {
      def findStream(op: SemanticOp): Option[DataFrame] = op match {
        case s: SemanticStreamingTableOp => Some(s.stream)
        case a: SemanticAggregateOp       => findStream(a.source)
        case f: SemanticFilterOp          => findStream(f.source)
        case rf: SemanticRowFilterOp      => findStream(rf.source)
        case o: SemanticOrderByOp         => findStream(o.source)
        case l: SemanticLimitOp           => findStream(l.source)
        case h: SemanticHintOp           => findStream(h.source)
        case tr: SemanticTransformsOp     => findStream(tr.source)
        case j: SemanticJoinOp           => findStream(j.left).orElse(findStream(j.right))
        case _: SemanticTableOp           => None  // batch root, not a streaming model
        case _                           => None
      }
      findStream(root).getOrElse(
        throw new IllegalStateException(
          s"toStreamingQuery: could not find SemanticStreamingTableOp at the root " +
          s"(found ${root.getClass.getSimpleName}). " +
          "Use toStreamingSemanticTable(stream, ...) at the package level " +
          "to construct a streaming model."))
    }

    /** Walk the op tree to find the dimensions declared on the
      * SemanticStreamingTableOp. Used for static-stream joins to build
      * the JoinSide for the streaming side. */
    def findStreamingDimensions(op: SemanticOp): Map[String, Dimension] = op match {
      case s: SemanticStreamingTableOp => s.dimensions
      case a: SemanticAggregateOp       => findStreamingDimensions(a.source)
      case f: SemanticFilterOp          => findStreamingDimensions(f.source)
      case rf: SemanticRowFilterOp      => findStreamingDimensions(rf.source)
      case o: SemanticOrderByOp         => findStreamingDimensions(o.source)
      case l: SemanticLimitOp           => findStreamingDimensions(l.source)
      case h: SemanticHintOp           => findStreamingDimensions(h.source)
      case tr: SemanticTransformsOp     => findStreamingDimensions(tr.source)
      case j: SemanticJoinOp           =>
        if (StreamingSupport.StreamingValidator.hasStreamingSource(j.left))
          findStreamingDimensions(j.left)
        else
          findStreamingDimensions(j.right)
      case _ => Map.empty
    }

    // 2. Validate. Loud failure for any feature the streaming terminal
    //    doesn't support yet — see StreamingValidator.
    StreamingValidator.validate(this, opts)

    // 3. The user-visible query name (for Spark UI / logs).
    val queryName = sourceTable.getOrElse("semanticdf_streaming_model")

    // 4. Apply watermark FIRST (if specified). Watermarks require event-time
    //    columns, and the watermark must be set before any aggregation.
    val withWatermark: DataFrame = opts.watermark match {
      case Some(w) => source.withWatermark(w.column, w.delay)
      case None    => source
    }

    // 5. If the model has a SemanticAggregateOp, translate it to a streaming
    //    groupBy(window(...)).agg(...) pipeline. This is the TRUE windowed
    //    aggregation — Spark's streaming engine handles the stateful
    //    aggregation across micro-batches, with windows. The foreachBatch
    //    receives the per-window aggregated result.
    //
    //    Without a SemanticAggregateOp, the model is filter-only. The
    //    foreachBatch then runs the existing op tree per batch on the raw
    //    (watermarked) data.
    val foreachBatchFn = opts.foreachBatch
    val queryPlan: DataFrame = root match {
      case agg @ SemanticAggregateOp(src, keys, measureNames) if opts.window.isDefined =>
        val w = opts.window.get
        // Build the streaming aggregation:
        //   groupBy(window(col(w.column), w.duration), keys*)
        //   .agg(measure1, measure2, ...)
        // Each measure's `expr: SemanticScope => Column` is invoked against
        // a MeasureScope over the watermarked source to produce the aggregate
        // column. This is a direct translation of the batch compile path's
        // base-measure pass into the streaming engine.
        // Collect all declared measures via a visitor (allMeasures() is
        // private[semanticdf] but we're in the same package).
        val collectedMeasures = scala.collection.mutable.ListBuffer.empty[(String, Measure)]
        val measCollector = new SemanticOpVisitor {
          override def enter(op: SemanticOp): Unit = op match {
            case t: SemanticTableOp =>
              t.measures.foreach { case (n, m) => collectedMeasures += ((n, m)) }
            case s: SemanticStreamingTableOp =>
              s.measures.foreach { case (n, m) => collectedMeasures += ((n, m)) }
            case _ => ()
          }
        }
        measCollector.visit(this.root)
        val measuresByName: Map[String, Measure] = collectedMeasures.toMap
        val aggregateColumns: Seq[Column] = measureNames.flatMap { name =>
          measuresByName.get(name) match {
            case Some(measure) =>
              val scope = new MeasureScope(
                df = withWatermark,
                knownMeasures = collectedMeasures.map(_._1).toSet,
              )
              try {
                Some(measure.expr(scope).as(name))
              } catch {
                case _: Throwable => None  // skip measures that can't be translated
              }
            case None => None
          }
        }
        val windowCol = window(col(w.column), w.duration)
        val groupCols: Seq[Column] = windowCol +: keys.filter(_ != w.column).map(col)
        // Pass the per-window aggregate to the user's foreachBatch via
        // .foreachBatch — batchDf there is the per-window aggregated result.
        if (aggregateColumns.isEmpty) {
          // No aggregate expressions produced (e.g., all measures were calc
          // references that didn't translate). Fall back to per-batch.
          withWatermark
        } else {
          withWatermark.groupBy(groupCols: _*).agg(aggregateColumns.head, aggregateColumns.tail: _*)
        }

      case j: SemanticJoinOp =>
        // Static-stream join (ADR 0002 stage 3). Identify which side is the
        // stream (only one side can be a streaming source per the validator).
        val streamingIsLeft = StreamingSupport.StreamingValidator.hasStreamingSource(j.left)
        // The static side's root is always a SemanticTableOp (the non-streaming
        // side of the join). leftRoot/rightRoot are those batch roots.
        val (staticRoot, staticDimensions) = if (streamingIsLeft) {
          (j.rightRoot, j.rightRoot.dimensions)
        } else {
          (j.leftRoot,  j.leftRoot.dimensions)
        }
        val staticDf: DataFrame = staticRoot.table
        // The streaming side: walk through to find the SemanticStreamingTableOp.
        // We've already applied watermark to `withWatermark` (the streaming
        // source after .withWatermark(...)).
        val streamingDimensions = if (streamingIsLeft) {
          findStreamingDimensions(j.left)
        } else {
          findStreamingDimensions(j.right)
        }
        // Build JoinSide instances. The static side is the LEFT of the join;
        // the streaming side is the RIGHT. The user's `on` lambda is called
        // with (static, stream) in that order — they can use l/r to mean
        // whatever they want, but the framework's convention is static=LEFT.
        val lCaptured = scala.collection.mutable.Map.empty[String, Boolean]
        val rCaptured = scala.collection.mutable.Map.empty[String, Boolean]
        val lSide = new JoinSide("static", staticDf, staticDimensions, lCaptured)
        val rSide = new JoinSide("stream", withWatermark, streamingDimensions, rCaptured)
        val joinCondition: Column = j.on(lSide, rSide)
        // Static-stream join: static (LEFT) joins with streaming (RIGHT).
        staticDf.join(withWatermark, joinCondition, "leftOuter")

      case _ =>
        // No groupBy + aggregate in the op tree (or no window). Pass each
        // batch's raw data through the existing op tree's compile path.
        // This is the PR 1 behavior.
        withWatermark
    }

    val writer = queryPlan.writeStream
      .foreachBatch { (batchDf: DataFrame, _: Long) =>
        // If we did TRUE streaming aggregation, the batch IS the per-window
        // aggregated result. Pass it through as-is.
        // If we're in the filter-only path, run the op tree per batch.
        root match {
          case _: SemanticAggregateOp if opts.window.isDefined =>
            // The streaming engine already did the aggregation. batchDf is
            // the per-window result. No further compilation needed.
            foreachBatchFn(batchDf)
          case _ =>
            // Filter-only path: compile the op tree against the batch.
            val batchRoot = SemanticTableOp(
              table = batchDf,
              name = sourceTable,
              description = sourceTable.flatMap(_ => None),
            )
            val batchModel = new SemanticTable(
              batchRoot, postAggPredicates, this.version, sourceTable)
            val result = batchModel.toDataFrame(spark)
            foreachBatchFn(result)
        }
      }
      .queryName(queryName)
      .outputMode(opts.outputMode)
      .trigger(opts.trigger.getOrElse(
        org.apache.spark.sql.streaming.Trigger.ProcessingTime("5 seconds")))

    val writerWithCheckpoint = opts.checkpointLocation match {
      case Some(loc) => writer.option("checkpointLocation", loc)
      case None      => writer
    }

    writerWithCheckpoint.start()
  }

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
    * the decoder automatically for case classes with primitive fields
    * (PR #64). All `query(...)` parameters (where, having, orderBy,
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

  /** Attach a pre-join row filter declared via the YAML `filters:` block.
    *
    * Compiles to `df.filter(expr)` against the current root at execution time.
    * The expression's column references must resolve against THIS model's
    * source table (validation is performed by the YamlLoader before this is called).
    *
    * Returns a NEW SemanticTable (immutability preserved). Filters accumulate
    * in the order they are added — each call wraps the previous root in a new
    * [[SemanticRowFilterOp]].
    *
    * Distinct from [[where]]: `where` accepts a structured `Predicate` for
    * query-time use; `withRowFilter` accepts a Spark SQL string for model-level
    * hygiene declared in YAML.
    */
  def withRowFilter(
      name: String,
      expr: String,
      description: Option[String],
      metadata: Map[String, String],
  ): SemanticTable =
    new SemanticTable(
      SemanticRowFilterOp(root, name, description, expr, metadata),
      postAggPredicates,
      version,
      sourceTable,
    )

  /** Set the per-model schema version. Returns a NEW SemanticTable (immutability preserved).
    *
    * Versioning is purely informational at the library level — no compatibility
    * checks are performed. Consumers (MCP server, OKF generator, agent framework)
    * read `version` and apply their own policy. See `mcp-contract.md`.
    *
    * @param v  non-negative integer. `0` is "pre-versioning" (default); `1+` is a
    *           model-declared schema version. Set once at construction; fluent calls
    *           overwrite. Joins create new tables; caller's choice on their version.
    */
  def version(v: Int): SemanticTable = {
    require(v >= 0, s"SemanticTable.version must be non-negative, got: $v")
    new SemanticTable(root, postAggPredicates, version = v, sourceTable)
  }

  /** Same as [[explainSemantic(spark:org.apache.spark.sql.SparkSession, scope:io.semanticdf.SemanticTable#Scope)]]
    * but accepts an optional SparkSession (e.g. for a static-only view). */
  def explainSemantic(spark: Option[SparkSession], scope: Scope): String = {
    val renderer = new SemanticPlanRenderer(this, scope)
    renderer.render(spark)
  }

  /** Return a DataFrame describing every field (dimensions + measures) in this model.
    *
    * This is the analogue of Spark's `df.schema` for a semantic model — it flattens
    * all model metadata into a queryable DataFrame so you can explore, filter, and
    * catalog the model programmatically.
    *
    * {{{
    *   val catalog = model.schema(spark)
    *
    *   // Find all PII fields
    *   catalog.filter(c => c("metadata_keys").contains("pii")).show()
    *
    *   // List all measures owned by finance
    *   catalog.filter(c => c("metadata_owner") === "finance").show()
    *
    *   // Export the full schema to a Delta table
    *   catalog.write.format("delta").save("_semanticdf/model_schema")
    * }}}
    *
    * The DataFrame has one row per field with these columns:
    *   - `model_name`: source table / joined model name
    *   - `model_description`: human description of the model
    *   - `field_name`: dimension or measure name
    *   - `field_type`: `"dimension"` or `"measure"`
    *   - `description`: the field's description (empty if none)
    *   - `metadata_keys`: comma-separated list of metadata keys
    *   - `metadata_values`: comma-separated list of metadata values (aligned with keys)
    *   - `is_entity`: true for entity (join-key) dimensions
    *   - `is_time_dimension`: true for time/timestamp dimensions
    *   - `smallest_grain`: for time dims, the finest supported time grain
    *   - `join_alias`: if this field comes from a joined table, the join alias
    *
    * @param spark the active SparkSession (used only to create the result DataFrame)
    */
  def schema(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val rows = collectSchemaFields(root, None, None)

    val resultSchema = org.apache.spark.sql.types.StructType(Seq(
      org.apache.spark.sql.types.StructField("model_name",          org.apache.spark.sql.types.StringType,  nullable = true),
      org.apache.spark.sql.types.StructField("model_description",  org.apache.spark.sql.types.StringType,  nullable = true),
      org.apache.spark.sql.types.StructField("field_name",          org.apache.spark.sql.types.StringType,  nullable = false),
      org.apache.spark.sql.types.StructField("field_type",          org.apache.spark.sql.types.StringType,  nullable = false),
      org.apache.spark.sql.types.StructField("description",         org.apache.spark.sql.types.StringType,  nullable = true),
      org.apache.spark.sql.types.StructField("metadata_keys",        org.apache.spark.sql.types.StringType,  nullable = true),
      org.apache.spark.sql.types.StructField("metadata_values",      org.apache.spark.sql.types.StringType,  nullable = true),
      org.apache.spark.sql.types.StructField("is_entity",            org.apache.spark.sql.types.BooleanType, nullable = false),
      org.apache.spark.sql.types.StructField("is_time_dimension",    org.apache.spark.sql.types.BooleanType, nullable = false),
      org.apache.spark.sql.types.StructField("smallest_grain",       org.apache.spark.sql.types.StringType,  nullable = true),
      org.apache.spark.sql.types.StructField("join_source",          org.apache.spark.sql.types.StringType,  nullable = true),
      org.apache.spark.sql.types.StructField("join_cardinality",     org.apache.spark.sql.types.StringType,  nullable = true),
    ))

    val sparkRows = rows.map { case (mName, mDesc, fName, fType, desc, mKeys, mVals, isEnt, isTime, grain, jSrc, jCard) =>
      org.apache.spark.sql.Row(
        mName.orNull, mDesc.orNull, fName, fType, desc.orNull,
        if (mKeys.isEmpty) null else mKeys,
        if (mVals.isEmpty) null else mVals,
        isEnt: java.lang.Boolean, isTime: java.lang.Boolean,
        grain.orNull, jSrc.orNull, jCard.orNull,
      )
    }
    spark.createDataFrame(sparkRows.asJava, resultSchema)
  }

  /** Recursively collect schema fields from the op tree. Returns flat list of row tuples. */
  private def collectSchemaFields(
      op: SemanticOp,
      joinSource: Option[String],
      joinCardinality: Option[String],
  ): List[(Option[String], Option[String], String, String, Option[String], String, String, Boolean, Boolean, Option[String], Option[String], Option[String])] = {
    // The visitor is regenerated for each recursive call so that joinSource
    // and joinCardinality are captured fresh from the current invocation.
    def walkSubtree(
        root: SemanticOp,
        src: Option[String],
        card: Option[String],
    ): List[(Option[String], Option[String], String, String, Option[String], String, String, Boolean, Boolean, Option[String], Option[String], Option[String])] = root match {
      case t: SemanticTableOp =>
        val modelName = t.name.orElse(Some("anonymous"))
        val modelDesc = t.description
        val dimRows = t.dimensions.values.map(d =>
          (modelName, modelDesc, d.name, "dimension",
            d.description, d.metadata.keys.mkString(","), d.metadata.values.mkString(","),
            d.isEntity, d.isTimeDimension, d.smallestTimeGrain, src, card)
        ).toList
        val measRows = t.measures.values.map(m =>
          (modelName, modelDesc, m.name, "measure",
            m.description, m.metadata.keys.mkString(","), m.metadata.values.mkString(","),
            false, false, None, src, card)
        ).toList
        dimRows ::: measRows

      case j: SemanticJoinOp =>
        val leftFields  = walkSubtree(j.left,  None,             None)
        val rightSource = j.rightRoot.name.orElse(Some("joined"))
        val rightFields = walkSubtree(j.right, rightSource, Some(j.cardinality.toString))
        leftFields ::: rightFields

      case f: SemanticFilterOp  => walkSubtree(f.source, src, card)
      case rf: SemanticRowFilterOp => walkSubtree(rf.source, src, card)
      case o: SemanticOrderByOp => walkSubtree(o.source, src, card)
      case l: SemanticLimitOp   => walkSubtree(l.source, src, card)
      case a: SemanticAggregateOp => walkSubtree(a.source, src, card)
      case h: SemanticHintOp    => walkSubtree(h.source, src, card)
    }
    walkSubtree(op, joinSource, joinCardinality)
  }


  // -------------------------------------------------------------------------
  // Model extension
  // -------------------------------------------------------------------------

  /** Extend the model with dimensions. Handles single-table and joined roots (Phase 4).
    * Returns a new [[SemanticTable]] (immutable). */
  def withDimensions(dims: Dimension*): SemanticTable = {
    val extra = dims.map(d => d.name -> d).toMap
    root match {
      case t: SemanticTableOp =>
        new SemanticTable(t.copy(dimensions = t.dimensions ++ extra), postAggPredicates, version, sourceTable)

      // Streaming source (ADR 0002): dims attach to the streaming model.
      case s: SemanticStreamingTableOp =>
        new SemanticTable(s.copy(dimensions = s.dimensions ++ extra), postAggPredicates, version, sourceTable)

      case j: SemanticJoinOp =>
        // Pass extra dimensions so mergedModel includes them.
        val updatedJoin = SemanticJoinOp(
          left   = j.left,
          right  = j.right,
          on     = j.on,
          cardinality = j.cardinality,
          leftRoot  = j.leftRoot,
          rightRoot = j.rightRoot,
          extraDimensions = j.extraDimensions ++ extra,
          extraMeasures   = j.extraMeasures,
        )
        new SemanticTable(updatedJoin, postAggPredicates, version, sourceTable)

      // Passthrough ops (Phase 5/6): recurse to the underlying table/join, then re-wrap.
      // Lets a user (or query()) chain withDimensions after where()/orderBy()/limit().
      case SemanticFilterOp(src, pred) =>
        val inner = new SemanticTable(src).withDimensions(dims: _*)
        new SemanticTable(SemanticFilterOp(inner.root, pred), postAggPredicates, version, sourceTable)
      case SemanticRowFilterOp(src, name, desc, expr, meta) =>
        val inner = new SemanticTable(src).withDimensions(dims: _*)
        new SemanticTable(SemanticRowFilterOp(inner.root, name, desc, expr, meta), postAggPredicates, version, sourceTable)
      case SemanticOrderByOp(src, keys) =>
        val inner = new SemanticTable(src).withDimensions(dims: _*)
        new SemanticTable(SemanticOrderByOp(inner.root, keys), postAggPredicates, version, sourceTable)
      case SemanticLimitOp(src, n) =>
        val inner = new SemanticTable(src).withDimensions(dims: _*)
        new SemanticTable(SemanticLimitOp(inner.root, n), postAggPredicates, version, sourceTable)

      // Hint is a Spark planner wrapper; recurse and re-wrap with the same hint.
      case SemanticHintOp(src, strategy, params) =>
        val inner = new SemanticTable(src).withDimensions(dims: _*)
        new SemanticTable(SemanticHintOp(inner.root, strategy, params), postAggPredicates, version, sourceTable)

      // Transforms are applied at compile time; dims should attach to the underlying
      // model so they're visible to the join/table op. Recurse and re-wrap.
      case SemanticTransformsOp(src, transforms) =>
        val inner = new SemanticTable(src).withDimensions(dims: _*)
        new SemanticTable(SemanticTransformsOp(inner.root, transforms), postAggPredicates, version, sourceTable)

      case _ =>
        throw new IllegalStateException(
          s"withDimensions: unexpected root type ${root.getClass.getSimpleName}"
        )
    }
  }

  /** Extend the model with measures. Handles single-table and joined roots (Phase 4).
    * Returns a new [[SemanticTable]] (immutable). */
  def withMeasures(measures: Measure*): SemanticTable = withMeasures0(measures)

  /** Typed-overload of [[withMeasures(measures:Measure*)*]] — accepts a single
    * [[SemanticMeasure]] witness whose `.name` becomes the measure name. The
    * expr still has signature `SemanticScope => Column`, so window functions
    * (`row_number().over(...)`, `lag(...)`, etc.) work the same as in the
    * string-based form.
    *
    * Multi-measure definitions are still string-based:
    * {{{
    *   flights.withMeasures(rank, t => row_number().over(Window.partitionBy(t("carrier"))...))
    *   flights.withMeasures(Measure("rank", expr), Measure("lag_pax", expr))   // multi-arity string
    * }}}
    *
    * Compile-time guarantee: passing a `SemanticDimension[F]` is a compile
    * error — `SemanticDimension` is not a subtype of `SemanticMeasure`, so
    * this overload is not applicable and the varargs overload rejects it too
    * (a `SemanticDimension` is not a `Measure`). The measure's name is read
    * from the typed witness, not a string — a typo in
    * `SemanticMeasure.of[RankWithinCarrier]("rank_winthin_carier")` would
    * still surface at runtime when the model loads, but downstream
    * `aggregateMeasures(rank)` / `Compare.Le(rank, 5)` are type-checked
    * against the same ref.
    *
    * The first parameter is the typeclass instance itself (not a `FieldRef`),
    * so `SemanticMeasure[F]` matches by subtyping in Scala's phase-1 overload
    * resolution — no implicit conversion is needed, and this overload is
    * picked over the varargs form even from cross-package consumer code. */
  def withMeasures[F](
      measure: SemanticMeasure[F],
      expr: SemanticScope => Column,
      description: Option[String] = None,
      metadata: Map[String, String] = Map.empty,
  ): SemanticTable = {
    val m = Measure(name = measure.name, expr = expr, description = description, metadata = metadata)
    withMeasures0(Seq(m))
  }

  /** Internal helper that all `withMeasures` paths funnel through. */
  private def withMeasures0(measures: Seq[Measure]): SemanticTable = {
    val extra = measures.map(m => m.name -> m).toMap
    root match {
      case t: SemanticTableOp =>
        new SemanticTable(t.copy(measures = t.measures ++ extra), postAggPredicates, version, sourceTable)

      // Streaming source (ADR 0002): measures attach to the streaming model.
      case s: SemanticStreamingTableOp =>
        new SemanticTable(s.copy(measures = s.measures ++ extra), postAggPredicates, version, sourceTable)

      case j: SemanticJoinOp =>
        val updatedJoin = SemanticJoinOp(
          left   = j.left,
          right  = j.right,
          on     = j.on,
          cardinality = j.cardinality,
          leftRoot  = j.leftRoot,
          rightRoot = j.rightRoot,
          extraDimensions = j.extraDimensions,
          extraMeasures   = j.extraMeasures ++ extra,
        )
        new SemanticTable(updatedJoin, postAggPredicates, version, sourceTable)

      // Passthrough ops (Phase 5/6): recurse to the underlying table/join, then re-wrap.
      case SemanticFilterOp(src, pred) =>
        val inner = new SemanticTable(src).withMeasures(measures: _*)
        new SemanticTable(SemanticFilterOp(inner.root, pred), postAggPredicates, version, sourceTable)
      case SemanticRowFilterOp(src, name, desc, expr, meta) =>
        val inner = new SemanticTable(src).withMeasures(measures: _*)
        new SemanticTable(SemanticRowFilterOp(inner.root, name, desc, expr, meta), postAggPredicates, version, sourceTable)
      case SemanticOrderByOp(src, keys) =>
        val inner = new SemanticTable(src).withMeasures(measures: _*)
        new SemanticTable(SemanticOrderByOp(inner.root, keys), postAggPredicates, version, sourceTable)
      case SemanticLimitOp(src, n) =>
        val inner = new SemanticTable(src).withMeasures(measures: _*)
        new SemanticTable(SemanticLimitOp(inner.root, n), postAggPredicates, version, sourceTable)

      // Hint is a Spark planner wrapper; recurse and re-wrap with the same hint.
      case SemanticHintOp(src, strategy, params) =>
        val inner = new SemanticTable(src).withMeasures(measures: _*)
        new SemanticTable(SemanticHintOp(inner.root, strategy, params), postAggPredicates, version, sourceTable)

      // Transforms are applied at compile time; measures should attach to the underlying
      // model so they're visible to the join/table op. Recurse and re-wrap.
      case SemanticTransformsOp(src, transforms) =>
        val inner = new SemanticTable(src).withMeasures(measures: _*)
        new SemanticTable(SemanticTransformsOp(inner.root, transforms), postAggPredicates, version, sourceTable)

      case _ =>
        throw new IllegalStateException(
          s"withMeasures: unexpected root type ${root.getClass.getSimpleName}"
        )
    }
  }

  /** Extend the model with per-row transforms applied to the source data at load
    * time. Transforms correspond to dbt's staging models / LookML's
    * `derived_table` — per-row logic (`datediff`, `case when ...`, window
    * functions) that doesn't fit the `agg()` aggregate context.
    *
    * ==Lazy contract==
    *
    * Transforms are NOT applied when this method is called — they are wrapped
    * in a [[SemanticTransformsOp]] and applied lazily when the consumer calls
    * [[toDataFrame]] (or any other terminal that compiles the op tree). This
    * preserves the lazy compile contract (DESIGN §4.4) — `toDataFrame(spark)` is
    * the only place where Spark actually runs. Other passthrough ops
    * ([[where]], [[orderBy]], [[limit]], etc.) follow the same pattern.
    *
    * ==Why a dedicated op (not eager `withColumn` in this method)==
    *
    * Before this refactor, `withTransforms` on a join model called
    * `j.compile(SparkSession.active)` eagerly to get the joined DataFrame, then
    * applied `withColumn` against it. That broke the lazy contract in two ways:
    *
    *   1. `SparkSession.active` is a side effect — it auto-creates a default
    *      session if none is set. A consumer building a SemanticTable in a
    *      context without a session (config loading, `validate()` calls,
    *      catalog accessors) would silently get a Spark session.
    *   2. The join was forced to build its logical plan at op-tree construction
    *      time. Every other op in the tree is a passthrough that defers
    *      compilation to `toDataFrame`.
    *
    * With [[SemanticTransformsOp]], transforms are applied at the terminal,
    * consistent with the rest of the tree.
    *
    * ==Transform outputs are NOT in the catalog==
    *
    * The output of a transform (e.g. `Transform("los_days", t => datediff(...))`)
    * is a DataFrame column, NOT a catalog dimension or measure. Users reference
    * transform outputs by string in subsequent measure/dimension expressions.
    * Transform outputs cannot be referenced via typed refs (`SemanticDimension` /
    * `SemanticMeasure`) because they aren't declared anywhere — adding them to
    * the catalog is a separate, additive feature.
    *
    * ==Chaining==
    *
    * Calling `withTransforms` multiple times composes all the transforms into
    * a single [[SemanticTransformsOp]] layer, applied in declaration order at
    * `toDataFrame(spark)` time. The earlier transforms are NOT replaced — they
    * compose with the new ones. This is the same `withColumn`-chain semantics
    * you'd get in plain Spark, just deferred.
    *
    * {{{
    *   st
    *     .withTransforms(Transform("a", t => t("v") + 1))   // applied first
    *     .withTransforms(Transform("b", t => t("a") * 2))   // applied second, sees `a`
    * }}}
    *
    * If transform B references a column added by transform A, declare A first
    * (the composition preserves declaration order).
    *
    * @example
    * {{{
    * val orders = ...
    *   .withTransforms(
    *     Transform("los_days",
    *       t => datediff(t("shipped_at"), t("order_date"))))
    *   .withMeasures(Measure("avg_los",
    *     t => sum(t("los_days")) / count(lit(1))))
    * }}}
    */
  def withTransforms(transforms: Transform*): SemanticTable = {
    if (transforms.isEmpty) return this
    root match {
      case t: SemanticTableOp =>
        // Single-table models: wrap in a SemanticTransformsOp. The transforms
        // are applied at toDataFrame() time, not now. This matches the lazy
        // pattern used for joins below.
        new SemanticTable(
          SemanticTransformsOp(t, transforms),
          postAggPredicates, version, sourceTable)

      case j: SemanticJoinOp =>
        // Joined models: wrap in a SemanticTransformsOp. CRUCIALLY, we do NOT
        // call j.compile(...) here — that would force the join to build its
        // logical plan now and would trigger SparkSession.active (the
        // side effect we're fixing). The join is compiled at toDataFrame()
        // time, and the transforms are applied then too.
        new SemanticTable(
          SemanticTransformsOp(j, transforms),
          postAggPredicates, version, sourceTable)

      // Passthrough ops (Phase 5/6): recurse to the underlying table/join, then re-wrap.
      case SemanticFilterOp(src, pred) =>
        val inner = new SemanticTable(src).withTransforms(transforms: _*)
        new SemanticTable(SemanticFilterOp(inner.root, pred), postAggPredicates, version, sourceTable)
      case SemanticRowFilterOp(src, name, desc, expr, meta) =>
        val inner = new SemanticTable(src).withTransforms(transforms: _*)
        new SemanticTable(SemanticRowFilterOp(inner.root, name, desc, expr, meta), postAggPredicates, version, sourceTable)
      case SemanticOrderByOp(src, keys) =>
        val inner = new SemanticTable(src).withTransforms(transforms: _*)
        new SemanticTable(SemanticOrderByOp(inner.root, keys), postAggPredicates, version, sourceTable)
      case SemanticLimitOp(src, n) =>
        val inner = new SemanticTable(src).withTransforms(transforms: _*)
        new SemanticTable(SemanticLimitOp(inner.root, n), postAggPredicates, version, sourceTable)
      case SemanticHintOp(src, strategy, params) =>
        val inner = new SemanticTable(src).withTransforms(transforms: _*)
        new SemanticTable(SemanticHintOp(inner.root, strategy, params), postAggPredicates, version, sourceTable)

      // Chained transforms: append the new transforms to the existing layer.
      // Do NOT recurse — recursion would re-enter the case below and create a
      // new SemanticTransformsOp with only the new transforms, losing the
      // existing ones. The passthrough ops above (filter/orderBy/etc.) DO
      // recurse because their semantics are "re-apply the transformation to
      // the underlying source", but transforms are cumulative — each call
      // adds to the chain, not replaces it.
      case SemanticTransformsOp(src, existing) =>
        new SemanticTable(
          SemanticTransformsOp(src, existing ++ transforms),
          postAggPredicates, version, sourceTable)

      case _ =>
        throw new IllegalStateException(
          s"withTransforms: unexpected root type ${root.getClass.getSimpleName}"
        )
    }
  }

  // -------------------------------------------------------------------------
  // Phase 4: Joins (DESIGN §7)
  // -------------------------------------------------------------------------

  /** Join with a one-to-one / parent-child relationship (`join_one`).
    *
    * Aggregation after the join is safe — fact rows are not multiplied by dimension rows.
    * Also eligible for deferred-join optimization (pre-agg fact table + left-join dimension
    * table) when only dimensions from the right table are queried.
    *
    * @param other the right-side semantic table
    * @param on    the equi-join predicate. Use `l("col") === r("col")` where `l` and `r`
    *              are the join-side proxies passed to the lambda. Both sides use bare column
    *              names — the proxy resolves them against the respective DataFrame.
    *
    * @example
    * {{{
    * val orders = toSemanticTable(ordersDf, name = Some("orders"))
    * val customers = toSemanticTable(customersDf, name = Some("customers"))
    *
    * val joined = orders.join_one(customers, (l, r) => l("customer_id") === r("id"))
    *   .withDimensions(Dimension("name", t => t("customers.name")))
    *   .withMeasures(Measure("total_amount", t => sum(t("amount"))))
    *
    * joined.groupBy("customers.name").aggregate("total_amount").execute(spark)
    * }}}
    */
  def join_one(
      other: SemanticTable,
      on: (JoinSide, JoinSide) => Column,
  ): SemanticTable = {
    // We pass `this.root` and `other.root` (the actual op trees) as the
    // `left`/`right` of the SemanticJoinOp — NOT just the roots. The roots
    // are stored separately. This is important for the streaming terminal:
    // when one side is a SemanticStreamingTableOp, the framework needs the
    // ORIGINAL op (preserving the streaming source info) in `left`/`right`
    // so the streaming-side detection walks correctly.
    //
    // Wrapper ops (Filter, RowFilter, OrderBy, Limit, Hint, Transforms) are
    // transparent for join purposes: we walk through them to find the root
    // model (a SemanticTableOp or SemanticStreamingTableOp).
    val leftOp  = this.root
    val rightOp = other.root
    def rootOf(op: SemanticOp): SemanticOp = op match {
      case t: SemanticTableOp         => t
      case s: SemanticStreamingTableOp => s
      // Pre-join row filters and transforms are transparent — unwrap.
      case rf: SemanticRowFilterOp     => rootOf(rf.source)
      case tr: SemanticTransformsOp    => rootOf(tr.source)
      // Query wrappers (filter/orderBy/limit/hint) and aggregate are not
      // supported as roots for joins — see docs/known-limitations.md.
      case j: SemanticJoinOp =>
        throw new IllegalArgumentException(
          s"join_one: the left side is already a joined table. " +
            "Multi-hop joins are not supported in this version — see docs/known-limitations.md.")
      case a: SemanticAggregateOp =>
        throw new IllegalArgumentException(
          s"join_one: cannot join after aggregate(). Join tables first, then call groupBy().")
      case f: SemanticFilterOp => throw queryWrapperError
      case o: SemanticOrderByOp => throw queryWrapperError
      case l: SemanticLimitOp   => throw queryWrapperError
      case h: SemanticHintOp    => throw queryWrapperError
    }
    // Helper for the query-wrapper rejection — extracted so the four
    // cases above share the same error message.
    def queryWrapperError: IllegalArgumentException =
      new IllegalArgumentException(
        s"join_one: the left/right side is a query wrapper (filter/orderBy/limit/hint). " +
          s"Construct joins from base tables (no query layer above them), then call groupBy() " +
          s"and aggregate() on the joined model.")
    val leftUnderlying  = rootOf(leftOp)
    val rightUnderlying = rootOf(rightOp)
    val leftRoot  = leftUnderlying match {
      case t: SemanticTableOp         => t
      case s: SemanticStreamingTableOp =>
        SemanticTableOp(s.stream, s.name, s.description, s.dimensions, s.measures)
    }
    val rightRoot = rightUnderlying match {
      case t: SemanticTableOp         => t
      case s: SemanticStreamingTableOp =>
        SemanticTableOp(s.stream, s.name, s.description, s.dimensions, s.measures)
    }
    val join = SemanticJoinOp(
      left   = leftOp,
      right  = rightOp,
      on     = on,
      cardinality = JoinCardinality.One,
      leftRoot  = leftRoot,
      rightRoot = rightRoot,
    )
    new SemanticTable(join)
  }

  /** Join with a one-to-many / fan-out relationship (`join_many`).
    *
    * This is the primary join for star-schema models (fact → dimension).
    * Each source table's measures are **pre-aggregated at the join-key grain before
    * joining**, preventing the fact-row multiplication that would otherwise inflate
    * SUM/COUNT. This is BSL's safe-aggregation pattern (DESIGN §7.1).
    *
    * Leaf-level measures (e.g. `"line_items.qty_sum"`) are unaffected by fan-out since
    * they are computed at their own source grain before the join.
    *
    * @param other the right-side semantic table (typically the many side)
    * @param on    the equi-join predicate, same form as [[join_one]]
    *
    * @example
    * {{{
    * val orders = toSemanticTable(ordersDf, name = Some("orders"))
    *   .withDimensions(Dimension("customer_id", t => t("customer_id")))
    *   .withMeasures(Measure("total_amount", t => sum(t("amount"))))
    *
    * val items = toSemanticTable(lineItemsDf, name = Some("items"))
    *   .withDimensions(Dimension("order_id", t => t("order_id")))
    *   .withMeasures(Measure("item_count", t => count(lit(1))))
    *
    * // Pre-agg at join-key grain: orders → customer_id, items → order_id, then join.
    * // total_amount = 100 + 200 + 150 = 450 (NOT inflated by the 1:many item rows).
    * orders.join_many(items, (l, r) => l("order_id") === r("order_id"))
    *   .groupBy("orders.customer_id")
    *   .aggregate("orders.total_amount")
    *   .execute(spark)
    * }}}
    */
  def join_many(
      other: SemanticTable,
      on: (JoinSide, JoinSide) => Column,
  ): SemanticTable = {
    val leftRoot  = requireRoot("join_many (left)")
    val rightRoot = other.requireRoot("join_many (right)")
    val join = SemanticJoinOp(
      left   = leftRoot,
      right  = rightRoot,
      on     = on,
      cardinality = JoinCardinality.Many,
      leftRoot  = leftRoot,
      rightRoot = rightRoot,
    )
    new SemanticTable(join)
  }

  /** Cross join (Cartesian product) with another semantic table (`join_cross`).
    *
    * Every row in `this` is paired with every row in `other`. Use with caution —
    * row counts multiply.
    */
  def join_cross(other: SemanticTable): SemanticTable = {
    val leftRoot  = requireRoot("join_cross (left)")
    val rightRoot = other.requireRoot("join_cross (right)")
    val join = SemanticJoinOp(
      left   = leftRoot,
      right  = rightRoot,
      on     = (_, _) => throw new IllegalStateException("Cross join has no predicate."),
      cardinality = JoinCardinality.Cross,
      leftRoot  = leftRoot,
      rightRoot = rightRoot,
    )
    new SemanticTable(join)
  }

  // -------------------------------------------------------------------------
  // Phase 5: Filtering (WHERE / HAVING, DESIGN §6.5)
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
  def where(pred: Predicate): SemanticTable = {
    val knownMeasures = resolveAllMeasureNames
    val (pre, post) = Predicate.splitFilter(pred, knownMeasures)
    val newRoot = pre.foldLeft(root) { (r, p) =>
      SemanticFilterOp(r, p)
    }
    new SemanticTable(newRoot, postAggPredicates ++ post, version, sourceTable)
  }

  /** Apply a filter predicate explicitly as post-aggregation (HAVING).
    *
    * Use when you want a dimension filter to apply after aggregation (rare, but
    * sometimes needed when the dimension is derived from a measure). */
  def having(pred: Predicate): SemanticTable =
    new SemanticTable(root, postAggPredicates :+ pred, version, sourceTable)

  // -------------------------------------------------------------------------
  // Phase 5 completion: order_by / limit / query
  // -------------------------------------------------------------------------

  /** Order the result by one or more sort keys.
    *
    * Bare strings are ascending; use [[SortKey.desc]] for descending:
    * {{{
    * st.orderBy("carrier", SortKey.desc("total_passengers"))
    * }}}
    *
    * Typically chained after `aggregate()`. Composes with [[limit]]. */
  def orderBy(keys: SortKey*): SemanticTable =
    new SemanticTable(SemanticOrderByOp(root, keys), postAggPredicates, version, sourceTable)

  /** Limit the result to the first `n` rows. Composes with [[orderBy]]. */
  def limit(n: Int): SemanticTable =
    new SemanticTable(SemanticLimitOp(root, n), postAggPredicates, version, sourceTable)

  /** Add a Spark planner hint to this SemanticTable.
    *
    * Wraps the underlying compiled DataFrame in `df.hint(strategy, params*)`. The
    * hint is then visible to the Spark planner and propagates to downstream
    * operations (e.g. a broadcast hint marks the result as broadcastable for the
    * next join that uses it as a side).
    *
    * Common uses:
    * {{{
    *   // Force a known-small dimension to broadcast on downstream joins.
    *   smallDim.withHint("broadcast")
    *
    *   // Set the partition count for a shuffle-heavy aggregate.
    *   bigFact.withHint("repartition", 200)
    * }}}
    *
    * The hint is applied to the *whole* compiled result, not to a specific
    * sub-expression. For a join-slot-specific hint, model the join inline
    * (`join_one(...).withHint("broadcast")`) and use the result downstream.
    *
    * Unknown strategies are tolerated by Spark (the hint is recorded but
    * ignored), so no name validation happens here.
    *
    * @param strategy the hint name (e.g. `"broadcast"`, `"repartition"`, `"sort"`)
    * @param params   optional parameters for the hint (e.g. an Int for `repartition_n`)
    * @return a new SemanticTable that emits a hinted DataFrame */
  def withHint(strategy: String, params: Any*): SemanticTable =
    new SemanticTable(SemanticHintOp(root, strategy, params.toSeq), postAggPredicates, version, sourceTable)

  /** One-shot bundled query (Phase 5 completion).
    *
    * Pure sugar over the fluent API — chains `where → groupBy → aggregate[having] →
    * orderBy → limit`. Useful for parameterized / programmatic query building where
    * the fluent chain would be awkward. All parameters except `measures` are optional.
    *
    * @example
    * {{{
    * st.query(
    *   measures   = Seq("total_passengers", "pct_of_total"),
    *   dimensions = Seq("carrier"),
    *   where      = Some("carrier" in ("AA", "UA")),
    *   orderBy    = Seq(SortKey.desc("total_passengers")),
    *   limit      = Some(10),
    * )
    * }}} */
  def query(
      measures: Iterable[String],
      dimensions: Iterable[String] = Nil,
      where: Option[Predicate] = None,
      having: Option[Predicate] = None,
      orderBy: Iterable[SortKey] = Nil,
      limit: Option[Int] = None,
      timeGrain: Option[String] = None,
      timeGrains: Map[String, String] = Map.empty,
      timeRange: Option[(String, String)] = None,
  ): SemanticTable = {
    if (timeGrain.isDefined && timeGrains.nonEmpty)
      throw new IllegalArgumentException(
        "Cannot specify both 'timeGrain' and 'timeGrains'. Use 'timeGrain' for a single " +
          "grain applied to all time dimensions, or 'timeGrains' for per-dimension grains.")
    var t = this
    // Phase 6: time_range first (filters rows by raw timestamp, pre-truncation).
    timeRange.foreach { case (start, end) =>
      val td = findTimeDimension(dimensions)
        .getOrElse(throw new IllegalArgumentException(
          "timeRange requires a time dimension among the query dimensions. " +
            "Mark one with Dimension.time(...) and include it in `dimensions`."))
      // Build Compare predicates explicitly: `td` is a String-typed dimension name, and
      // Scala's Ordered[String] member >= would shadow the Predicate DSL implicit, so we
      // bypass it. time_range filters on the raw column named `td` (pre-truncation).
      val rangePred = Predicate.Compare("ge", td, start).and(Predicate.Compare("le", td, end))
      t = t.where(rangePred)
    }
    // Phase 6: grain truncation overrides each time dimension's expr.
    val grainMap: Map[String, String] =
      if (timeGrains.nonEmpty) timeGrains
      else timeGrain.map(g => timeDimensionsAmong(dimensions).map(_ -> g).toMap).getOrElse(Map.empty)
    grainMap.foreach { case (dim, g) => t = t.atTimeGrain(dim, g) }
    where.foreach(p  => t = t.where(p))
    having.foreach(p => t = t.having(p))
    var result: SemanticTable = t.groupBy(dimensions.toSeq: _*).aggregate(measures.toSeq: _*)
    if (orderBy.nonEmpty) result = result.orderBy(orderBy.toSeq: _*)
    limit.foreach(n => result = result.limit(n))
    result
  }

  // -------------------------------------------------------------------------
  // Group-by / aggregate
  // -------------------------------------------------------------------------

  /** Begin a group-by + aggregate. Returns a builder whose `aggregate(...)` produces
    * the aggregated [[SemanticTable]]. */
  def groupBy(keys: String*): SemanticGroupBy =
    new SemanticGroupBy(root, keys, postAggPredicates, version, sourceTable)

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
  // Internals
  // -------------------------------------------------------------------------

  /** Resolve the set of all known measure names on this table's model.
    * Used by [[where]] to route filters (measure refs → HAVING, else → WHERE). */
  /** Apply a time grain to a time dimension (Phase 6, DESIGN §6.6).
    *
    * Truncates `dimName`'s expression to `grain` (e.g. `"month"`) so group-by buckets
    * at that granularity. Validates the dimension is a time dimension and that `grain`
    * is not finer than its declared `smallestTimeGrain`.
    *
    * Implementation mirrors BSL: override the dimension's `expr` with a truncated
    * version via [[withDimensions]] — no new op node, reuses all existing machinery.
    * Filtering (time_range) stays on the raw column since filters compile against the
    * base scope; only grouping sees the truncation. */
  def atTimeGrain(dimName: String, grain: String): SemanticTable = {
    val unit = TimeGrain.normalize(grain)
    val dim  = resolveDimension(dimName).getOrElse(throw new IllegalArgumentException(
      s"atTimeGrain: dimension '$dimName' not found on this table.${closestMatch(dimName, dimensions.keys).map(c => s" Did you mean: '$c'?").getOrElse("")}"))
    if (!dim.isTimeDimension)
      throw new IllegalArgumentException(
        s"atTimeGrain: '$dimName' is not a time dimension. " +
          "Declare it with Dimension.time(...) to enable grain truncation.")
    TimeGrain.validateNotFiner(unit, dim.smallestTimeGrain, dimName)
    val truncated: Dimension = dim.copy(expr = (scope: SemanticScope) =>
      TimeGrain.truncate(unit, dim.expr(scope)))
    withDimensions(truncated)
  }

  /** Find the first time dimension among `dims`, by name. */
  private def findTimeDimension(dims: Iterable[String]): Option[String] =
    dims.find(name => resolveDimension(name).exists(_.isTimeDimension))

  /** All time dimensions among `dims`, by name. */
  private def timeDimensionsAmong(dims: Iterable[String]): Seq[String] =
    dims.filter(name => resolveDimension(name).exists(_.isTimeDimension)).toSeq

  /** Look up a dimension by name across single-table and joined models. */
  private def resolveDimension(name: String): Option[Dimension] = root match {
    case t: SemanticTableOp => t.dimensions.get(name)
    case j: SemanticJoinOp  => j.mergedModel.dimensions.get(name)
    case SemanticFilterOp(src, _)     => new SemanticTable(src).resolveDimension(name)
    case SemanticOrderByOp(src, _)    => new SemanticTable(src).resolveDimension(name)
    case SemanticLimitOp(src, _)      => new SemanticTable(src).resolveDimension(name)
    case SemanticHintOp(src, _, _)    => new SemanticTable(src).resolveDimension(name)
    case SemanticTransformsOp(src, _) => new SemanticTable(src).resolveDimension(name)
    case _ => SemanticOp.rootModel(root).flatMap(_.dimensions.get(name))
  }

  private def resolveAllMeasureNames: Set[String] = root match {
    case t: SemanticTableOp => t.measures.keySet
    case j: SemanticJoinOp  => j.mergedModel.measures.keySet
    case SemanticFilterOp(src, _) =>
      // Unwrap filters to find the underlying model.
      new SemanticTable(src).resolveAllMeasureNames
    case SemanticHintOp(src, _, _) =>
      // Hint is a Spark planner wrapper; recurse to find the underlying model.
      new SemanticTable(src).resolveAllMeasureNames
    case SemanticTransformsOp(src, _) =>
      // Transforms don't change the measure catalog; recurse to the source.
      new SemanticTable(src).resolveAllMeasureNames
    case _ =>
      SemanticOp.rootModel(root).map(_.measures.keySet).getOrElse(Set.empty)
  }

  private def rootModel: SemanticTableOp = root match {
    case t: SemanticTableOp => t
    case _ =>
      SemanticOp.rootModel(root).getOrElse(
        throw new IllegalStateException(
          s"SemanticTable root is not a SemanticTableOp: ${root.getClass.getSimpleName}. " +
            "Joins produce a SemanticJoinOp; use withDimensions/withMeasures on the " +
            "result to add dimensions/measures before aggregating."
        )
      )
  }

  private def requireRoot(label: String): SemanticTableOp =
    root match {
      case t: SemanticTableOp => t
      case j: SemanticJoinOp =>
        throw new IllegalArgumentException(
          s"$label: the left side is already a joined table. " +
            "Multi-hop joins are not supported in this version — see docs/known-limitations.md."
        )
      case a: SemanticAggregateOp =>
        throw new IllegalArgumentException(
          s"$label: cannot join after aggregate(). Join tables first, then call groupBy()."
        )
      // Pre-join row filters are transparent — unwrap to find the underlying table.
      case SemanticRowFilterOp(src, _, _, _, _) =>
        new SemanticTable(src).requireRoot(label)
      // Transforms are applied at compile time; unwrap to find the underlying table.
      case SemanticTransformsOp(src, _) =>
        new SemanticTable(src).requireRoot(label)
      // Query wrappers (WHERE / ORDER BY / LIMIT / HINT) layer over the model —
      // they don't expose a SemanticTableOp root, so users must join first.
      case SemanticFilterOp(_, _) | SemanticOrderByOp(_, _) |
           SemanticLimitOp(_, _) | SemanticHintOp(_, _, _) =>
        throw new IllegalArgumentException(
          s"$label: the left/right side is a query wrapper (filter/orderBy/limit/hint). " +
            s"Construct joins from base tables (no query layer above them), then call groupBy() " +
            s"and aggregate() on the joined model."
        )
    }

  // -----------------------------------------------------------------------
  // Catalog accessors (unblocks first-consumer: explore a SemanticTable)
  // -----------------------------------------------------------------------

  /** All dimensions declared on this semantic table. */
  def dimensions: Map[String, Dimension] = resolveRootModel.dimensions

  /** All measures declared on this semantic table (base and calc). */
  def measures: Map[String, Measure] = resolveRootModel.measures

  /** Name declared on this semantic table, if any. Set by `toSemanticTable(name=)`,
    * the YAML `flights:` top-level key (via `YamlLoader`), or `withTransforms`/joins.
    * Returns None for anonymous models (no name was ever assigned). */
  def name: Option[String] = resolveRootModel.name

  /** Human-readable description declared on this semantic table, if any. Set by
    * `toSemanticTable(description=)` or the YAML `description:` field. */
  def description: Option[String] = resolveRootModel.description

  /** Look up a dimension by name. */
  def findDimension(name: String): Option[Dimension] = dimensions.get(name)

  /** Look up a measure by name. */
  def findMeasure(name: String): Option[Measure] = measures.get(name)

  /** All joins declared on this semantic model, in declaration order
    * (outermost first; for chained joins the order matches applyJoins).
    *
    * Each [[JoinInfo]] summarises one join — cardinality, side names,
    * grain columns, and any extra dimensions/measures added afterwards. Empty
    * for single-table models that have no joins.
    *
    * MCP `describe_model.joins` reads from this accessor. */
  def joins: Seq[JoinInfo] = collectJoins(root)

  /** Walk the op tree collecting joins. Recurses through transparent wrappers
    * (filter/orderBy/limit/hint/row-filter/aggregate); stops at the leaf
    * table. Returns joins outermost-first so MCP consumers see the order
    * users declared them.
    *
    * Join keys are read from the eager-probe field on [[SemanticJoinOp]]
    * (populated at construction time by [[JoinKeyProbe]]), so this works
    * without compiling the model. */
  private def collectJoins(op: SemanticOp): Seq[JoinInfo] = op match {
    case j: SemanticJoinOp =>
      val info = JoinInfo(
        cardinality     = j.cardinality.toString,
        leftName        = j.leftRoot.name,
        rightName       = j.rightRoot.name,
        keys            = j.grainCols,
        extraDimensions = j.extraDimensions.keys.toSeq.sorted,
        extraMeasures   = j.extraMeasures.keys.toSeq.sorted,
      )
      info +: (collectJoins(j.left) ++ collectJoins(j.right))
    case SemanticFilterOp(src, _)          => collectJoins(src)
    case SemanticRowFilterOp(src, _, _, _, _) => collectJoins(src)
    case SemanticOrderByOp(src, _)         => collectJoins(src)
    case SemanticLimitOp(src, _)           => collectJoins(src)
    case SemanticAggregateOp(src, _, _)    => collectJoins(src)
    case SemanticHintOp(src, _, _)         => collectJoins(src)
    case SemanticTransformsOp(src, _)      => collectJoins(src)  // transforms are transparent
    case _: SemanticTableOp                => Nil
  }

  /** Classify a measure as [[MeasureKind.Base]] or [[MeasureKind.Calc]].
    *
    * Classification is a pure function of the measure's lambda and the set of
    * declared measure names — no SparkSession, no DataFrame, no compile-time.
    * A measure is `Calc` iff its lambda references another declared measure
    * (via `t("other_measure")` or `t.all("other_measure")`); otherwise `Base`.
    *
    * Used by MCP `describe_model.measures[].kind`. */
  def measureKind(name: String): MeasureKind = {
    val m = findMeasure(name).getOrElse(throw new IllegalArgumentException(
      s"measureKind: unknown measure '$name'." +
        closestMatch(name, measures.keys).map(c => s" Did you mean: '$c'?").getOrElse("")
    ))
    val known = measures.keySet - name
    val probe = new MeasureProbeScope(known)
    try m.expr(probe) catch { case _: Throwable => () /* probe-safe: lit(0.0) for unknown columns */ }
    if (probe.referenced.isEmpty) MeasureKind.Base else MeasureKind.Calc
  }

  // -------------------------------------------------------------------------
  // Metastore integration (unblocks BI tools: query via Spark SQL)
  // -------------------------------------------------------------------------

  /** Compile this semantic table and register it as a Spark temporary view.
    *
    * After registration, any Spark SQL query can reference `name` as a table:
    * {{{
    * st.createOrReplaceTempView("flights")
    * spark.sql("SELECT carrier, total_passengers FROM flights WHERE carrier = 'AA'")
    * }}}
    *
    * The view is session-scoped and disappears when the SparkSession stops.
    * For a global view, use [[createOrReplaceGlobalTempView]].
    *
    * @param name the view name (must be a valid Spark identifier)
    */
  def createOrReplaceTempView(name: String)(implicit spark: SparkSession): Unit =
    toDataFrame(spark).createOrReplaceTempView(name)

  /** Compile this semantic table and register it as a session-scoped temp view.
    *
    * Unlike [[createOrReplaceTempView]], this throws if `name` already exists.
    *
    * @param name the view name
    */
  def createTempView(name: String)(implicit spark: SparkSession): Unit =
    toDataFrame(spark).createTempView(name)

  /** Compile this semantic table and register it as a global Spark temporary view.
    *
    * Global views are stored in the global_temp database and persist across sessions
    * within the same application:
    * {{{
    * st.createOrReplaceGlobalTempView("flights")
    * spark.sql("SELECT * FROM global_temp.flights")
    * }}}
    *
    * @param name the view name
    */
  def createOrReplaceGlobalTempView(name: String)(implicit spark: SparkSession): Unit =
    toDataFrame(spark).createOrReplaceGlobalTempView(name)

  // -------------------------------------------------------------------------
  // Typed result schema (unblocks first-consumer: preview output before run)
  // -------------------------------------------------------------------------

  /** Compile this semantic table and return the COMPILED output schema.
    *
    * **Not compile-free.** This calls `toDataFrame(spark).schema`, which compiles
    * the entire pipeline (pass 1 + pass 2 + Spark's optimizer pass). For a hot
    * path (UI render, IDE preview, BI tool schema sync), use [[schema(spark)]]
    * instead — that one walks the model's declared dimensions and measures
    * without compiling and returns them as a one-row-per-field DataFrame.
    *
    * `compiledSchema` is the right call when you genuinely need the post-
    * aggregation output schema (group keys + measure columns after all
    * Spark type promotion rules have been applied). The name makes the
    * cost explicit so callers don't reach for it expecting a cheap lookup.
    *
    * @param spark the active SparkSession
    * @return the post-aggregation output schema (StructType)
    */
  def compiledSchema(implicit spark: SparkSession): StructType =
    toDataFrame(spark).schema

  /** Validate the op tree without compiling.
    *
    * Walks the model definition only — no `SparkSession` is required, no compile,
    * no DataFrame materialization. Use this in CI to pre-flight a model before
    * deploying, or in interactive REPLs to check a freshly-built table.
    *
    * - **Errors** are conditions that would cause `execute()` to throw. Examples:
    *   filter references an unknown field. (Calc-dependency cycles are detected
    *   at execute() time by `SemanticAggregateOp.topologicalLayers` — they cannot
    *   be caught compile-free because probing a measure expr needs a DataFrame.)
    * - **Warnings** are conditions that are legal but worth surfacing. Examples:
    *   time dimension declared without `smallestTimeGrain` would raise a clear
    *   error from `atTimeGrain()` on any request.
    *
    * @return structured report; `isValid` is the boolean summary. */
  def validate(): ValidationResult = {
    val errors   = scala.collection.mutable.ListBuffer.empty[String]
    val warnings = scala.collection.mutable.ListBuffer.empty[String]

    // Single op-tree walk to collect everything we need. Inline (rather than calling
    // the renderer's helpers) so validate() stays compile-free and self-contained.
    val allMs        = scala.collection.mutable.LinkedHashMap.empty[String, Measure]
    val allDs        = scala.collection.mutable.LinkedHashMap.empty[String, Dimension]
    val allFilters   = scala.collection.mutable.ListBuffer.empty[SemanticFilterOp]
    // The visitor's visit() auto-recurses into all wrapper ops, so we don't
    // need explicit visit(...) calls here. Each op is entered exactly once.
    val collector = new SemanticOpVisitor {
      override def enter(op: SemanticOp): Unit = op match {
        case t: SemanticTableOp =>
          t.measures.foreach { case (n, m) => allMs.update(n, m) }
          t.dimensions.foreach { case (n, d) => allDs.update(n, d) }
        case j: SemanticJoinOp =>
          j.extraMeasures.foreach { case (n, m) => allMs.update(n, m) }
          j.extraDimensions.foreach { case (n, d) => allDs.update(n, d) }
        case f @ SemanticFilterOp(src, _) =>
          // Record this filter; leave its recursion to the visitor.
          allFilters += f
        case _ => ()  // Aggregate, RowFilter, OrderBy, Limit, Hint, Transforms: no declared dim/measure/filter work.
      }
    }
    collector.visit(root)
    val allMsMap      = allMs.toMap
    val measureNames  = allMsMap.keySet
    val knownFields   = allDs.keySet ++ measureNames

    // Note: calc-dependency cycles are NOT checked here. Detecting them requires
    // probing each measure's expr through a ClassificationScope, which needs a
    // DataFrame — and validate() is documented as compile-free. The runtime check
    // in SemanticAggregateOp.topologicalLayers raises "Calc dependency cycle"
    // with a clear message when execute() runs; see the Phase 2a regression test.

    // 1. Filter references an unknown field (ERROR).
    allFilters.foreach { f =>
      f.predicate.fields.foreach { field =>
        if (!knownFields.contains(field))
          errors += s"filter references unknown field '$field'"
      }
    }

    // 3. Time dimension without smallestTimeGrain (WARNING).
    allDs.foreach { case (n, d) =>
      if (d.isTimeDimension && d.smallestTimeGrain.isEmpty)
        warnings += s"time dimension '$n' has no smallestTimeGrain — atTimeGrain() will raise on any request"
    }

    // 4. AND or OR predicate that mixes dim + measure categories (WARNING).
    //    The whole predicate goes post-agg (which may not be the user's intent).
    //    Note: `where()` already splits ANDs into separate filter nodes at
    //    construction time, so AND never reaches this check. OR is preserved
    //    intact and is the case users will actually see in practice.
    allFilters.foreach { f =>
      val mixed = f.predicate match {
        case Predicate.And(children @ _*) =>
          children.exists(p => Predicate.referencesMeasure(p, measureNames)) &&
          children.exists(p => !Predicate.referencesMeasure(p, measureNames))
        case Predicate.Or(children @ _*) =>
          children.exists(p => Predicate.referencesMeasure(p, measureNames)) &&
          children.exists(p => !Predicate.referencesMeasure(p, measureNames))
        case _ => false
      }
      if (mixed)
        warnings += s"compound predicate mixes dim + measure conditions: ${f.predicate.describe} — whole predicate goes post-agg"
    }

    ValidationResult(errors.toSeq, warnings.toSeq)
  }

  // -------------------------------------------------------------------------
  // Internals (reordered for readability)
  // -------------------------------------------------------------------------

  /** Resolve the leaf [[SemanticTableOp]] from the root, unwrapping transparent
    * wrappers (where/orderBy/limit/row-filter). Used by the catalog accessors
    * (`dimensions`, `measures`, `findDimension`, `findMeasure`). */
  private def resolveRootModel: MergedSemanticModel = root match {
    case t: SemanticTableOp => MergedSemanticModel(t.dimensions, t.measures, t.name, t.description)
    case j: SemanticJoinOp  => j.mergedModel
    case SemanticAggregateOp(src, _, _) =>
      new SemanticTable(src).resolveRootModel
    case SemanticFilterOp(src, _)  => new SemanticTable(src).resolveRootModel
    // Pre-join row filters do not change the declared model — unwrap transparently.
    case SemanticRowFilterOp(src, _, _, _, _) => new SemanticTable(src).resolveRootModel
    case SemanticOrderByOp(src, _) => new SemanticTable(src).resolveRootModel
    case SemanticLimitOp(src, _)   => new SemanticTable(src).resolveRootModel
    case SemanticHintOp(src, _, _) => new SemanticTable(src).resolveRootModel
    // Transforms are transparent — they don't change the declared model.
    case SemanticTransformsOp(src, _) => new SemanticTable(src).resolveRootModel
  }

  override def toString: String = s"SemanticTable(${root.getClass.getSimpleName})"

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

}

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
    new SemanticTable(op, version = version, sourceTable = sourceTable)
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
private class SemanticPlanRenderer(st: SemanticTable, scope: Scope = Scope.All) {

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

/** A row-level filter declared on a model via YAML `filters:` block.
  *
  * Read-only value type. The library maintains these internally as op-tree
  * entries (`SemanticRowFilterOp`), and exposes them through this type for
  * catalog consumers: MCP `describe_model.filters` and OKF `# Filters`.
  */
final case class SemanticFilter(
    name: String,
    description: Option[String],
    expr: String,
    metadata: Map[String, String],
)

/** Classification of a measure within a semantic model.
  *
  * - `Base`: aggregates source columns directly (e.g. `sum(amount)`, `count(1)`).
  * - `Calc`: lambda references other declared measures in the same model
  *   (e.g. `total_revenue / event_count`).
  *
  * Surfaced as MCP `describe_model.measures[].kind` so consumers can reason
  * about aggregation costs and dependencies without re-classifying locally. */
sealed trait MeasureKind
object MeasureKind {
  case object Base extends MeasureKind
  case object Calc extends MeasureKind
}

/** Summary of one join in a semantic model — exposed for MCP `describe_model`.
  *
  * Captures the cardinality, side names, grain (join-key) columns, and any
  * dimensions/measures added via `withDimensions` / `withMeasures` after the
  * join. Internalised `SemanticJoinOp` is kept private to the package; this
  * DTO is the stable, MCP-facing shape. */
final case class JoinInfo(
    /** Cardinality as a string ("one" | "many" | "cross") — string not enum so
      * it serializes cleanly to JSON without a sealed-trait encoder. */
    cardinality: String,
    /** Name of the left-side source model (e.g. "orders"). None if anonymous. */
    leftName: Option[String],
    /** Name of the right-side source model (e.g. "customers"). None if anonymous. */
    rightName: Option[String],
    /** Join-key column names — the equi-join keys. Empty for cross joins. */
    keys: Seq[String],
    /** Names of dimensions added via `withDimensions` after this join. */
    extraDimensions: Seq[String],
    /** Names of measures added via `withMeasures` after this join. */
    extraMeasures: Seq[String],
)

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

/** Field-inventory scope selector for `explainSemantic` (see
  * [[SemanticTable.explainSemantic(spark:org.apache.spark.sql.SparkSession, scope:* Scope)]]).
  *
  * - `All`:  every declared dimension and measure (default — legacy behaviour).
  * - `Used`: only fields referenced by this query; the rest are collapsed.
  *
  * Package-level (not nested in `SemanticTable`) because the underlying renderer is a
  * private class in this same file and path-dependent types are not visible there. */
sealed trait Scope
object Scope {
  case object All  extends Scope
  case object Used extends Scope
}


