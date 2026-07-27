package io.semanticdf

import io.semanticdf.audit.{AuditEvent, QueryRequest => AuditQueryRequest}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

/** Streaming methods of [[SemanticTable]] — `toStreamingQuery` (4 overloads)
  * plus the 3 private helpers (`substituteStreamingLeaf`,
  * `findStreamRecursive`, `emitStreamingAudit`).
  *
  * == Why a trait ==
  *
  * Streaming is the most self-contained concern in `SemanticTable`:
  * the methods are loaded together (you call `toStreamingQuery` and
  * its private helpers kick in), they share a tight internal contract
  * (the op-tree walker shape), and they have no overlap with batch
  * query / mutation / collection logic. Pulling them out keeps
  * `SemanticTable` focused on the batch path and gives the streaming
  * internals a dedicated home.
  *
  * == What lives here ==
  *
  *   - `toStreamingQuery` (4 overloads: 2 explicit-SparkSession + 2
  *     implicit-SparkSession; the explicit ones are the canonical
  *     implementation, the implicit ones are forwarders).
  *   - `substituteStreamingLeaf` — private; replaces every
  *     `SemanticStreamingTableOp` leaf with a `SemanticTableOp`
  *     wrapping the batch DataFrame.
  *   - `findStreamRecursive` — private; is there a streaming leaf
  *     anywhere under `op`? Used by `substituteStreamingLeaf`'s
  *     join branch.
  *   - `emitStreamingAudit` — private; emits an audit event with the
  *     actual per-batch rowCount (per-batch rowCount
  *     fix). Used by both foreachBatch branches.
  *
  * == Cross-trait dependencies ==
  *
  *   - `compileWindowedAggregation` lives in `SemanticTableCore`
  *     (private[semanticdf]); the streaming trait calls it via
  *     `self.compileWindowedAggregation(...)`.
  *   - `toDataFrame` (Core) is called from the non-aggregated
  *     foreachBatch path; same pattern: `self.toDataFrame(spark)`.
  *   - `AuditEvent`, `QueryRequest` are imported from the audit
  *     package — same as Core.
  *   - `StreamingSupport` (the validator, JoinSide, WatermarkSpec,
  *     WindowSpec types) is imported from its own file.
  *
  * == Public API ==
  *
  * 100% unchanged. The trait is `private[semanticdf]` and is mixed
  * in via `extends SemanticTableStreaming`. Consumers see the
  * same `SemanticTable` class.
  */
private[semanticdf] trait SemanticTableStreaming { self: SemanticTable =>

  // -------------------------------------------------------------------------
  // Streaming terminal — explicit SparkSession (canonical implementation)
  // -------------------------------------------------------------------------

  def toStreamingQuery(
      spark: SparkSession,
      opts: StreamingSupport.StreamingQueryOptions,
  ): org.apache.spark.sql.streaming.StreamingQuery = {
    import StreamingSupport._
    import org.apache.spark.sql.functions._

    // 1. Find the streaming source. A non-streaming model is a hard error
    //    (not a soft one) — the user built a batch model and is calling
    //    the streaming terminal by mistake. The root may be a
    //    SemanticAggregateOp wrapping a SemanticStreamingTableOp (when the
    //    model uses groupBy + aggregate); we walk through to find the source.
    val source: DataFrame = findStream(root).getOrElse(
      throw new IllegalStateException(
        s"toStreamingQuery: could not find SemanticStreamingTableOp at the root " +
        s"(found ${root.getClass.getSimpleName}). " +
        "Use toStreamingSemanticTable(stream, ...) at the package level " +
        "to construct a streaming model."))

    // 2. Validate. Loud failure for any feature the streaming terminal
    //    doesn't support yet — see StreamingValidator.
    StreamingValidator.validate(this, opts)

    // 3. The user-visible query name (for Spark UI / logs).
    val queryName = sourceTable.getOrElse("semanticdf_streaming_model")

    // 4. Apply watermark FIRST (if specified or defaulted). Watermarks
    //    require event-time columns, and the watermark must be set before
    //    any aggregation. When a window is set but the user did not
    //    provide a watermark, default it to the window column with a
    //    10-minute delay. This bounds streaming state and keeps the
    //    pipeline well-formed.
    val resolvedWatermark: Option[WatermarkSpec] = opts.watermark match {
      case w @ Some(_) => w
      case None        =>
        opts.window.map(w => WatermarkSpec(w.column, "10 minutes"))
    }
    val withWatermark: DataFrame = resolvedWatermark match {
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
      case SemanticAggregateOp(_, keys, _) if opts.window.isDefined =>
        // Existing path: user wrote `.groupBy(...).aggregate(...)` in the
        // Scala DSL. Aggregate-op becomes the windowed agg root; keys come
        // from the op tree.
        val collected = scala.collection.mutable.ListBuffer.empty[(String, Measure)]
        new SemanticOpVisitor {
          override def enter(op: SemanticOp): Unit = op match {
            case t: SemanticTableOp          => t.measures.foreach { case (n, m) => collected += ((n, m)) }
            case s: SemanticStreamingTableOp => s.measures.foreach { case (n, m) => collected += ((n, m)) }
            case _                           => ()
          }
        }.visit(this.root)
        self.compileWindowedAggregation(
          sourceWatermarked = withWatermark,
          groupKeys         = keys,
          collectedMeasures = collected.toSeq,
          measuresByName    = collected.toMap,
          w                 = opts.window.get,
        )

      case s: SemanticStreamingTableOp if opts.window.isDefined && opts.groupKeys.nonEmpty =>
        // New path: a streaming source root with group keys declared in the
        // operator's `StreamingConfig`. Same pipeline as the aggregate-op
        // case — only the source of (groupKeys, collectedMeasures) differs.
        self.compileWindowedAggregation(
          sourceWatermarked = withWatermark,
          groupKeys         = opts.groupKeys,
          collectedMeasures = s.measures.toSeq,
          measuresByName    = s.measures,
          w                 = opts.window.get,
        )

      case j: SemanticJoinOp =>
        // Static-stream join. Identify which side is the
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
        //
        // COST NOTE (v0.2.0 doc fix): when `withAuditSink(...)` is set,
        // both the windowed and the filter-only branches call
        // `batchDf.count()` per microbatch to populate the audit
        // event's `rowCount`. This is a Spark action — it materializes
        // the batch partitions. The user's `foreachBatchFn` typically
        // also does an action on the same batch, so for batches that
        // are not already materialized, the same partitions may be
        // computed twice. If the per-microbatch cost matters, drop the
        // audit sink on hot streaming queries.
        root match {
          case _: SemanticAggregateOp if opts.window.isDefined =>
            // The streaming engine already did the aggregation. batchDf is
            // the per-window result. No further compilation needed.
            //
            // The normal `toDataFrame` audit emit (which fires rowCount
            // from the cache row count) is bypassed in this short-circuit
            // branch, so we have to emit the audit event here with the
            // actual batch row count. Without this, a windowed streaming
            // query with `withAuditSink(...)` produces ZERO audit events
            // — a silent break of the v0.1.17 observability surface.
            val t0 = System.nanoTime()
            emitStreamingAudit(batchDf, t0)(spark)
            foreachBatchFn(batchDf)
          case _ =>
            // Non-aggregated path: walk the original op tree and
            // substitute the streaming leaf with the batch DataFrame.
            // This catch-all also receives a `SemanticStreamingTableOp`
            // root with no window (filter-only models), a streaming
            // root with `window + groupKeys` (the per-micros-batch
            // group-by path), and the static side of a static-stream
            // join. It preserves the user's `.where(...)`,
            // `.withTransforms(...)`, `.withRowFilter(...)` and
            // similar transformations — they were silently dropped in
            // v0.1.16 and earlier.
            //
            // The pre-fix code constructed a bare `SemanticTableOp(batchDf)`
            // and discarded the rest of the op tree, so filters/transforms
            // applied to a streaming model never reached the
            // `foreachBatch` callback. This is the data-correctness fix.
            val batchRoot = substituteStreamingLeaf(root, batchDf)
            // Pass `auditSink = None` to the batchModel so its
            // `toDataFrame` takes the fast path and does NOT emit an
            // audit event. We emit manually below with the actual
            // rowCount; otherwise we'd double-emit and the first event
            // would carry rowCount=0 (the `case None` branch's
            // documented "caller will collect" sentinel). Without
            // auditSink=None, the audit log would show every batch
            // as "0 rows" — a silent wrong-answer.
            val batchModel = new SemanticTable(
              batchRoot, postAggPredicates, this.version, sourceTable, status,
              auditSink = None,
              auditRequest = auditRequest,
              // Disable the result cache inside foreachBatch. The
              // cache key does not include batch identity, so the
              // first micro-batch's result could be returned for
              // every subsequent batch. Disabling the cache here
              // means each batch is computed fresh.
              resultCache = None)
            val t0 = System.nanoTime()
            val result = batchModel.toDataFrame(spark)
            emitStreamingAudit(result, t0)(spark)
            foreachBatchFn(result)
        }
      }
      .queryName(queryName)
      .outputMode(opts.outputMode)
      .trigger(opts.trigger.getOrElse(
        org.apache.spark.sql.streaming.Trigger.ProcessingTime("5 seconds")))

    val writerWithCheckpoint = opts.checkpointLocation match {
      case Some(loc) => writer.option("checkpointLocation", loc)
      // Default the checkpoint to a per-query temp dir.
      // Production callers should always pass `checkpointLocation`
      // explicitly to a durable path; this default is for prototyping.
      case None      =>
        val dir = java.io.File.createTempFile(
          "semanticdf-checkpoints-", "").getAbsoluteFile
        dir.delete()
        dir.mkdirs()
        writer.option("checkpointLocation", dir.getAbsolutePath)
    }

    writerWithCheckpoint.start()
  }

  // -------------------------------------------------------------------------
  // Streaming terminal — declarative `StreamingConfig` overload
  // -------------------------------------------------------------------------

  /** Declarative overload of [[toStreamingQuery]] — takes a
    * [[StreamingSupport.StreamingConfig]] (the shape the YAML /
    * MCP / CLI surfaces use) and translates it into the lower-level
    * [[StreamingSupport.StreamingQueryOptions]] before delegating.
    *
    * Same return type and same runtime semantics. The two methods share
    * the underlying implementation — this overload is just sugar for the
    * declarative shape.
    */
  def toStreamingQuery(
      spark: SparkSession,
      config: StreamingSupport.StreamingConfig,
  ): org.apache.spark.sql.streaming.StreamingQuery =
    toStreamingQuery(spark, config.toQueryOptions)

  // -------------------------------------------------------------------------
  // Streaming terminal — implicit-SparkSession overloads
  // -------------------------------------------------------------------------

  /** Implicit-SparkSession variant of [[toStreamingQuery]] — mirrors
    * `toDataFrame(implicit spark)` so the same DSL ergonomics (call
    * sites without the explicit `spark` arg) work for both terminals.
    *
    * Callsite:
    * {{{
    *   implicit val spark: SparkSession = ...
    *   val query = model.toStreamingQuery(StreamingConfig(...))   // spark picked up implicitly
    *   val query2 = model.toStreamingQuery()                       // defaults + implicit spark
    * }}}
    *
    * Forwarded to the canonical explicit-SparkSession overload, so
    * behavior is identical. The forward is important — it keeps the
    * validator / pipeline logic in exactly one place.
    */
  def toStreamingQuery(
      opts: StreamingSupport.StreamingQueryOptions = StreamingSupport.StreamingQueryOptions(),
  )(implicit spark: SparkSession): org.apache.spark.sql.streaming.StreamingQuery =
    toStreamingQuery(spark, opts)

  /** Implicit-SparkSession variant of the declarative-`StreamingConfig`
    * overload — same ergonomics as above for the typed-config path. */
  def toStreamingQuery(
      config: StreamingSupport.StreamingConfig,
  )(implicit spark: SparkSession): org.apache.spark.sql.streaming.StreamingQuery =
    toStreamingQuery(spark, config.toQueryOptions)

  // -------------------------------------------------------------------------
  // Streaming private helpers
  // -------------------------------------------------------------------------

  /** Find the streaming source. A non-streaming model is a hard error
    * (not a soft one) — the user built a batch model and is calling
    * the streaming terminal by mistake. The root may be a
    * `SemanticAggregateOp` wrapping a `SemanticStreamingTableOp` (when
    * the model uses groupBy + aggregate); we walk through to find the
    * source.
    */
  private def findStream(op: SemanticOp): Option[DataFrame] = op match {
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

  /** Walk the op tree to find the dimensions declared on the
    * SemanticStreamingTableOp. Used for static-stream joins to build
    * the JoinSide for the streaming side. */
  private def findStreamingDimensions(op: SemanticOp): Map[String, Dimension] = op match {
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

  /** Recursive walk that replaces every [[SemanticStreamingTableOp]]
    * leaf in `op` with a [[SemanticTableOp]] wrapping `batchDf`. Used
    * by [[toStreamingQuery]]'s `foreachBatch` path so the user's
    * `.where(...)`, `.withTransforms(...)`, `.withRowFilter(...)`
    * and other transformations on a streaming model actually
    * execute per micro-batch.
    *
    * Each intermediate op (filter, transform, hint, etc.) is
    * preserved with the substituted source. The `name` and
    * `description` from the original streaming leaf are copied
    * onto the replacement.
    */
  private def substituteStreamingLeaf(
      op: SemanticOp,
      batchDf: DataFrame,
  ): SemanticOp = op match {
    case s: SemanticStreamingTableOp =>
      SemanticTableOp(
        table = batchDf,
        name = s.name,
        description = s.description,
      )
    case a: SemanticAggregateOp =>
      a.copy(source = substituteStreamingLeaf(a.source, batchDf))
    case f: SemanticFilterOp =>
      f.copy(source = substituteStreamingLeaf(f.source, batchDf))
    case rf: SemanticRowFilterOp =>
      rf.copy(source = substituteStreamingLeaf(rf.source, batchDf))
    case o: SemanticOrderByOp =>
      o.copy(source = substituteStreamingLeaf(o.source, batchDf))
    case l: SemanticLimitOp =>
      l.copy(source = substituteStreamingLeaf(l.source, batchDf))
    case h: SemanticHintOp =>
      h.copy(source = substituteStreamingLeaf(h.source, batchDf))
    case tr: SemanticTransformsOp =>
      tr.copy(source = substituteStreamingLeaf(tr.source, batchDf))
    case j: SemanticJoinOp =>
      // Joins in streaming models are static-stream only; the
      // streaming side is on either j.left or j.right. Substitute
      // the side that holds the streaming leaf.
      val newLeft  = findStreamRecursive(j.left)
      val newRight = findStreamRecursive(j.right)
      val substLeft  = if (newLeft)  substituteStreamingLeaf(j.left,  batchDf) else j.left
      val substRight = if (newRight) substituteStreamingLeaf(j.right, batchDf) else j.right
      j.copy(left = substLeft, right = substRight)
    case other =>
      // Other ops (e.g. SemanticTableOp at a non-streaming root, or
      // a node we don't recognise) — pass through. The compile()
      // will fail loudly if the resulting tree is malformed.
      other
  }

  /** Inner helper: is there a streaming leaf anywhere under `op`?
    * Walks the same set of node types as [[substituteStreamingLeaf]]. */
  private def findStreamRecursive(op: SemanticOp): Boolean = op match {
    case _: SemanticStreamingTableOp => true
    case a: SemanticAggregateOp       => findStreamRecursive(a.source)
    case f: SemanticFilterOp          => findStreamRecursive(f.source)
    case rf: SemanticRowFilterOp      => findStreamRecursive(rf.source)
    case o: SemanticOrderByOp         => findStreamRecursive(o.source)
    case l: SemanticLimitOp           => findStreamRecursive(l.source)
    case h: SemanticHintOp           => findStreamRecursive(h.source)
    case tr: SemanticTransformsOp     => findStreamRecursive(tr.source)
    case j: SemanticJoinOp            => findStreamRecursive(j.left) || findStreamRecursive(j.right)
    case _                           => false
  }

  /** Emit an audit event for a streaming micro-batch.
    *
    * The standard [[toDataFrame]] audit emit returns `rowCount=0`
    * when there is no cache key (the `case None` branch — the
    * caller is expected to collect). For the streaming terminal's
    * `foreachBatch` path, the data is in `batchDf` RIGHT NOW; the
    * user does not call `collect()` separately. Count it here so
    * the audit log reflects the real per-batch row count.
    *
    * `t0` is the start of the BATCH-HANDLING work (right before
    * this emit), not the start of the streaming batch trigger.
    * The captured `elapsedMs` therefore measures the
    * emit-time work, not the upstream streaming aggregation or
    * the user's `foreachBatchFn` callback. This matches the
    * existing `toDataFrame` audit semantics (`t0` captured right
    * before the cache/compile work). Both streaming and batch
    * terminals report "framework-side terminal work" time, not
    * end-to-end query latency.
    *
    * The error path is `NonFatal`-swallowed — the audit sink is
    * best-effort and must never break the streaming pipeline. */
  private def emitStreamingAudit(
      batchDf: DataFrame,
      t0: Long,
  )(implicit spark: SparkSession): Unit = {
    if (auditSink.isDefined) {
      try {
        val rowCount   = batchDf.count()
        val elapsedMs  = (System.nanoTime() - t0) / 1000000L
        val model      = this.name.getOrElse(sourceTable.getOrElse("unknown"))
        val req        = auditRequest.getOrElse(AuditQueryRequest(model = model, version = this.version))
        val whereHash  = req.where.map(where => io.semanticdf.audit.PredicateHasher.hash(where))
        val havingHash = req.having.map(having => io.semanticdf.audit.PredicateHasher.hash(having))
        // dedupHash is the replay-safe / dedup-safe contract key.
        // The streaming foreachBatch runs in the Spark driver, not
        // inside a Restate handler (per
        // docs/design/platform-determinism-audit.md), so this is for
        // general dedup-safety, not Restate replay-safety. Same
        // query-shape dedups; different timestamp or batch does not.
        auditSink.get.emit(AuditEvent(
          ts         = java.time.Instant.now(),
          model      = model,
          version    = req.version,
          measures   = req.measures,
          dimensions = req.dimensions,
          whereHash  = whereHash,
          havingHash = havingHash,
          rowCount   = rowCount,
          elapsedMs  = elapsedMs,
          status     = "ok",
          dedupHash  = AuditEvent.dedupHashOf(
                        model, req.version, req.measures, req.dimensions,
                        whereHash, havingHash),
        ))
      } catch { case scala.util.control.NonFatal(_) => () }
    }
  }
}
