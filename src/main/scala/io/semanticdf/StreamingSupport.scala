package io.semanticdf

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}

/** Streaming-terminal support (ADR 0002, PR 1 of the streaming build).
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
  * Scope of this PR (terminal + validator + minimum viable streaming):
  *  Supported: `where` filters on streaming sources, dimensions, base measures.
  *  Deferred (per ADR 0002's "Revive trigger" order): windowed aggregation,
  *  stream-static joins, windowed totals, checkpoint defaults.
  *
  * The validator fails LOUDLY for the deferred features — the user gets a
  * clear "this needs ADR 0002 stage 2/3/4" error, not a silent wrong result.
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
  final case class StreamingQueryOptions(
    trigger: Option[Trigger] = None,
    outputMode: String = "append",
    checkpointLocation: Option[String] = None,
    foreachBatch: DataFrame => Unit = _ => (),
  )

  /** Thrown when a model uses features the streaming terminal doesn't support
    * (yet). The message names the offending feature and the relevant
    * ADR 0002 stage that would enable it. */
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
    def validate(root: SemanticOp): Unit = {
      val violations = scala.collection.mutable.ListBuffer.empty[String]
      walk(root, violations)
      if (violations.nonEmpty) {
        throw new StreamingUnsupportedError(
          violations.mkString("; ") +
          " (see ADR 0002 — only where filters on streaming sources are " +
          "currently supported; groupBy/aggregate/joints/limits/orderBy/t.all " +
          "need their respective ADR stages)")
      }
    }

    /** Full validation including t.all detection. Preferred entry point.
      * The op-tree walk catches groupBy/aggregate/limit/orderBy/joins;
      * the per-measure probe catches t.all(...) usage. */
    def validate(model: SemanticTable): Unit = {
      val violations = scala.collection.mutable.ListBuffer.empty[String]
      walk(model.root, violations)
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
        violations += s"t.all(...) used in measures: ${totalUsers.mkString(", ")} " +
          "(requires windowed-totals — ADR 0002 stage 4)"
      }
      if (violations.nonEmpty) {
        throw new StreamingUnsupportedError(
          violations.mkString("; ") +
          " (see ADR 0002 — only where filters on streaming sources are " +
          "currently supported; groupBy/aggregate/joints/limits/orderBy/t.all " +
          "need their respective ADR stages)")
      }
    }

    private def walk(
      op: SemanticOp,
      v: scala.collection.mutable.ListBuffer[String],
    ): Unit = op match {
      case _: SemanticTableOp         => () // batch root: not a streaming model
      case _: SemanticStreamingTableOp => () // streaming root: ok
      case f: SemanticFilterOp        => walk(f.source, v)  // where: ok
      case rf: SemanticRowFilterOp    => walk(rf.source, v) // row filter: ok
      case h: SemanticHintOp          => walk(h.source, v)  // hint: ok
      case tr: SemanticTransformsOp   => walk(tr.source, v) // transforms: ok
      case a: SemanticAggregateOp =>
        v += "groupBy(...).aggregate(...) requires time windows + watermark (ADR 0002 stage 2)"
      case o: SemanticOrderByOp =>
        v += "orderBy not supported in streaming (ADR 0002 stage 2)"
      case l: SemanticLimitOp =>
        v += "limit not supported in streaming (ADR 0002 stage 2)"
      case j: SemanticJoinOp =>
        v += "joins not yet supported in streaming (ADR 0002 stage 3)"
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
