package io.semanticdf

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}

/** Streaming-terminal support.
  *
  * Provides:
  *  - [[StreamingQueryOptions]] — user-facing options for the streaming
  *    terminal (parallel to `writeStream` options).
  *  - [[StreamingSource]] — a typed wrapper for a streaming `DataFrame`
  *    (the result of `spark.readStream`).
  *  - [[StreamingUnsupportedError]] — the loud failure when a model uses
  *    features the streaming terminal can't handle (yet).
  *  - [[StreamingValidator]] — walks the op tree and accumulates the
  *    list of features that the streaming terminal rejects.
  *
  * Supported in streaming: `where` filters, dimensions, base measures,
  * windowed aggregation, static-stream joins, windowed totals via `t.all(...)`,
  * default watermark + checkpoint location.
  *
  * The validator fails LOUDLY for unsupported features — the user gets a
  * clear error naming the offending pattern, not a silent wrong result.
  */
object StreamingSupport {

  /** A typed wrapper for a streaming `DataFrame` (the result of
    * `spark.readStream`).
    *
    * Used by [[toStreamingSemanticTable]] at the package level to
    * construct a streaming model. The streaming terminal method
    * [[SemanticTable.toStreamingQuery]] requires a model whose root
    * is a [[SemanticStreamingTableOp]] (built from this source). */
  final case class StreamingSource(stream: DataFrame) {
    require(stream.isStreaming,
      "StreamingSource requires a streaming DataFrame (from spark.readStream).")
  }

  /** Options for [[SemanticTable.toStreamingQuery]].
    *
    * Parallel to Spark's `DataStreamWriter` options. The `foreachBatch`
    * callback receives each micro-batch result as a `DataFrame` — this
    * is where the user does sink writes (parquet, console, kafka, etc.).
    *
    * @param trigger Spark trigger (defaults to 5-second ProcessingTime).
    * @param outputMode "append" (default), "update", or "complete".
    * @param checkpointLocation optional checkpoint path for fault tolerance.
    * @param foreachBatch callback for each micro-batch result. Default no-op.
    */
  /** Time window spec for windowed aggregation in the streaming terminal.
    * `column` is the time column to window on; `duration` is a Spark
    * duration string (e.g., "5 minutes", "1 hour"). The terminal applies
    * this as `groupBy(window(col(column), duration))` on the streaming
    * source. */
  final case class WindowSpec(column: String, duration: String)

  /** Watermark spec for event-time processing in the streaming terminal.
    * `column` is the event-time column; `delay` is the allowed lateness
    * (Spark duration string, e.g., "10 minutes"). */
  final case class WatermarkSpec(column: String, delay: String)

  final case class StreamingQueryOptions(
    trigger: Option[Trigger] = None,
    outputMode: String = "append",
    checkpointLocation: Option[String] = None,
    foreachBatch: DataFrame => Unit = _ => (),
    /** Time window for windowed aggregation. Required when the model uses
      * `groupBy + aggregate`; otherwise the validator rejects the query. */
    window: Option[WindowSpec] = None,
    /** Watermark (event-time lateness). Optional but recommended for any
      * streaming query that uses event-time windows. */
    watermark: Option[WatermarkSpec] = None,
  )

  /** Thrown when a model uses features the streaming terminal doesn't support
    * (yet). The message names the offending feature so the user can
    * adjust the model without consulting an internal spec. */
  class StreamingUnsupportedError(msg: String)
    extends RuntimeException(s"Streaming terminal: $msg")

  /** Validates that the op tree is compatible with the streaming terminal.
    *
    * The streaming terminal supports `where` filters on streaming sources
    * (no groupBy, no aggregate, no orderBy, no limit, no `t.all`, no joins).
    * Any of these in the op tree accumulate as violations; the first
    * failure throws a [[StreamingUnsupportedError]] listing all violations.
    *
    * Detection of `t.all(...)` requires probing each measure's lambda
    * with a [[MeasureProbeScope]] (which records calls to `t.all`). This
    * is the same machinery the framework uses internally to track
    * percent-of-total references during classification.
    */
  object StreamingValidator {
    /** Full validation including t.all detection. Preferred entry point.
      * The op-tree walk catches groupBy/aggregate/limit/orderBy/joins;
      * the per-measure probe catches t.all(...) usage. */
    def validate(model: SemanticTable, options: StreamingQueryOptions = StreamingQueryOptions()): Unit = {
      val violations = scala.collection.mutable.ListBuffer.empty[String]
      walk(model.root, violations, options)
      // Collect measures via the SemanticTable's allMeasures() (private[semanticdf]).
      // The call is made from inside the same package, so visibility is fine.
      val measures = scala.collection.mutable.ListBuffer.empty[(String, Measure)]
      val collector = new SemanticOpVisitor {
        override def enter(op: SemanticOp): Unit = op match {
          case t: SemanticTableOp =>
            t.measures.foreach { case (n, m) => measures += ((n, m)) }
          case s: SemanticStreamingTableOp =>
            s.measures.foreach { case (n, m) => measures += ((n, m)) }
          case _ => ()
        }
      }
      collector.visit(model.root)
      val totalUsers = findTotalUsers(measures.toSeq)
      if (totalUsers.nonEmpty) {
        // t.all(...) is allowed in streaming IF a window is specified —
        // the framework computes per-window totals in that case.
        if (options.window.isEmpty) {
          violations += s"t.all(...) used in measures: ${totalUsers.mkString(", ")} " +
            "(requires windowed-totals — set StreamingQueryOptions.window)"
        }
      }
      if (violations.nonEmpty) {
        throw new StreamingUnsupportedError(
          violations.mkString("; ") +
          " — only `where` filters on streaming sources are supported without " +
          "a WindowSpec; `groupBy`/`aggregate` require a WindowSpec, " +
          "`orderBy`/`limit` are not supported in streaming, " +
          "stream-stream joins are not supported yet")
      }
    }

    private def walk(
      op: SemanticOp,
      v: scala.collection.mutable.ListBuffer[String],
      options: StreamingQueryOptions,
    ): Unit = op match {
      case _: SemanticTableOp         => () // batch root: not a streaming model
      case _: SemanticStreamingTableOp => () // streaming root: ok
      case f: SemanticFilterOp        => walk(f.source, v, options)  // where: ok
      case rf: SemanticRowFilterOp    => walk(rf.source, v, options) // row filter: ok
      case h: SemanticHintOp          => walk(h.source, v, options)  // hint: ok
      case tr: SemanticTransformsOp   => walk(tr.source, v, options) // transforms: ok
      case a: SemanticAggregateOp =>
        // Windowed aggregation is OK if a WindowSpec is provided.
        if (options.window.isEmpty) {
          v += "groupBy(...).aggregate(...) requires a window spec in StreamingQueryOptions (set StreamingQueryOptions.window)"
        }
        // If a window IS specified, we accept the aggregate — the terminal will
        // translate the groupBy+aggregate into a streaming groupBy(window(...)).agg(...).
        // We still walk children so any nested ops get checked.
        walk(a.source, v, options)
      case o: SemanticOrderByOp =>
        v += "orderBy is not supported in streaming"
      case l: SemanticLimitOp =>
        v += "limit is not supported in streaming"
      case j: SemanticJoinOp =>
        // Static-stream join is supported: one side is a batch source
        // (SemanticTableOp), the other is a streaming source
        // (SemanticStreamingTableOp). Stream-stream joins are still rejected.
        val leftIsStream  = hasStreamingSource(j.left)
        val rightIsStream = hasStreamingSource(j.right)
        if (!leftIsStream && !rightIsStream) {
          v += "join: neither side is a streaming source — the model is fully batch"
        } else if (leftIsStream && rightIsStream) {
          v += "stream-stream joins are not supported in streaming " +
            "(only static-stream joins work today — stream-stream needs " +
            "time bounds + watermarks on both sides)"
        }
        // Static-stream: OK, the terminal will handle it.
        // Still walk children for any nested issues.
        walk(j.left, v, options)
        walk(j.right, v, options)
    }

    /** Does the op subtree contain a `SemanticStreamingTableOp`? */
    private[semanticdf] def hasStreamingSource(op: SemanticOp): Boolean = op match {
      case _: SemanticStreamingTableOp => true
      case a: SemanticAggregateOp       => hasStreamingSource(a.source)
      case f: SemanticFilterOp          => hasStreamingSource(f.source)
      case rf: SemanticRowFilterOp      => hasStreamingSource(rf.source)
      case o: SemanticOrderByOp         => hasStreamingSource(o.source)
      case l: SemanticLimitOp           => hasStreamingSource(l.source)
      case h: SemanticHintOp           => hasStreamingSource(h.source)
      case tr: SemanticTransformsOp     => hasStreamingSource(tr.source)
      case j: SemanticJoinOp           => hasStreamingSource(j.left) || hasStreamingSource(j.right)
      case _: SemanticTableOp           => false
      case _                           => false
    }

    /** Probe each measure's lambda with a [[MeasureProbeScope]] to detect
      * `t.all(...)` (percent-of-total) references. Returns the set of
      * measure names that use `t.all`. Empty set means the model is safe
      * for streaming. */
    def findTotalUsers(measures: Seq[(String, Measure)]): Set[String] = {
      val known = measures.map(_._1).toSet
      val users = scala.collection.mutable.Set.empty[String]
      measures.foreach { case (name, m) =>
        val probe = new MeasureProbeScope(known)
        try { m.expr(probe) } catch { case _: Throwable => () }
        if (probe.referencedTotals.nonEmpty) users += name
      }
      users.toSet
    }
  }
}
