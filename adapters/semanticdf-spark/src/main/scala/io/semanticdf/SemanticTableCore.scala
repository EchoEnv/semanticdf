package io.semanticdf
import io.semanticdf.predicate._

import io.semanticdf.audit.{AuditEvent, AuditSink, QueryRequest => AuditQueryRequest}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType

import scala.jdk.CollectionConverters._

/** Core methods of [[SemanticTable]] — terminals, simple query builders,
  * introspection, schema, validation, and the audit/cache setters.
  *
  * == Why a trait ==
  *
  * `SemanticTable` carries 80+ methods spanning 6 concerns (streaming,
  * mutation, collection, terminal, introspection, schema). Putting them
  * all in one file makes the file huge (was 3341 lines) and the
  * responsibilities hard to navigate. This trait groups the "core"
  * concern — anything that doesn't belong to a more specific concern.
  *
  * The trait is mixed in via `extends SemanticTableCore`, so consumers
  * see exactly the same `SemanticTable` API as before. The split is a
  * file-organization concern, not a class-organization concern.
  *
  * == What lives here ==
  *
  *   - Terminals: [[toDataFrame]], [[execute]]
  *   - Query capture: [[query]], [[where]], [[having]], [[orderBy]],
  *     [[limit]], [[withHint]], [[atTimeGrain]]
  *   - Setters: [[version]], [[status]], [[withAuditSink]],
  *     [[withResultCache]], [[withRowFilter]]
  *   - Introspection: [[dimensions]], [[measures]], [[name]],
  *     [[description]], [[isJoined]], [[findDimension]],
  *     [[findMeasure]], [[joins]], [[measureKind]]
  *   - Schema: [[schema]], [[compiledSchema]], [[validate]]
  *   - Catalog: [[createOrReplaceTempView]], [[createTempView]],
  *     [[createOrReplaceGlobalTempView]]
  *
  * Streaming-specific methods (toStreamingQuery + helpers) live in
  * `SemanticTableStreaming.scala`. Mutation methods
  * (withDimensions, withMeasures, withTransforms, joins) live in
  * `SemanticTableMutation.scala`. Collection methods (collectAs,
  * queryAs, render, explain*, SemanticPlanRenderer) live in
  * `SemanticTableCollection.scala`.
  *
  * == Cross-trait helpers ==
  *
  * Several private helpers are called by both Core and other traits:
  *   - `resolveRootModel`, `resolveAllMeasureNames`, `requireRoot`,
  *     `collectSchemaFields` — called by introspection and any future
  *     validation surface. Stay private to the trait (visible to itself).
  *   - `compileWindowedAggregation` — private so far, called by
  *     streaming internals. Will move to `SemanticTableStreaming`
  *     when the streaming trait lands. For now, both Core and the
  *     future streaming trait can access it via `self`.
  *   - `resolveDimension`, `findTimeDimension`, `timeDimensionsAmong`
  *     — Core-only.
  *   - `rootModel` — private to Core; called by `requireRoot`.
  */
private[semanticdf] trait SemanticTableCore { self: SemanticTable =>

  // -------------------------------------------------------------------------
  // Terminals
  // -------------------------------------------------------------------------

  /** Batch terminal (DESIGN §4.5).
    *
    * Compiles the op tree against `spark` and returns the resulting `DataFrame`.
    * Recompiles on every call; never caches the result internally (DESIGN §4.4).
    *
    * `spark` is an implicit parameter so callers with an
    * `implicit val spark: SparkSession` in scope can write `.toDataFrame()`
    * (no argument). Explicit `.toDataFrame(spark)` is fully backward-compatible.
    */
  def toDataFrame(implicit spark: SparkSession): DataFrame = toDataFrameInternal(spark, io.semanticdf.audit.Clock.systemDefault)
  def toDataFrame()(implicit spark: SparkSession, clock: () => java.time.Instant = io.semanticdf.audit.Clock.systemDefault): DataFrame = toDataFrameInternal(spark, clock)
  private def toDataFrameInternal(spark: SparkSession, clock: () => java.time.Instant): DataFrame = {
    // Translate `salt = Some(n)` into Spark AQE skew handling. The
    // hint requires THREE conf settings to take effect:
    //   1. `spark.sql.adaptive.enabled = true` — the parent AQE
    //      config. Without it, `skewJoin.enabled` is a no-op.
    //      This is a deliberate override: the user opted into
    //      skew handling via `withSalt`, which implies AQE must
    //      run for the hint to be effective. If the operator has
    //      explicitly disabled AQE for compliance/audit reasons,
    //      they should not call `withSalt` (or call `conf.set(
    //      spark.sql.adaptive.enabled, false)` AFTER the query
    //      that uses salt).
    //   2. `spark.sql.adaptive.skewJoin.enabled = true` — the
    //      skew-join child of AQE.
    //   3. `spark.sql.adaptive.skewJoin.skewedPartitionFactor = n`
    //      — the threshold multiplier.
    //
    // Spark's actual behavior (per OptimizeSkewedJoin.scala):
    // AQE divides each skewed shuffle partition into smaller
    // sub-partitions AND replicates the matching partition on the
    // other side of the join, so they can run in parallel tasks.
    // It is NOT auto-broadcast semantics — that's a common
    // misconception. The library's previous Scaladoc said
    // "broadcasts them automatically", which was wrong; this
    // comment is the corrected version.
    //
    // Config is session-global. Setting it once enables skew
    // handling for all subsequent joins in the SparkSession. The
    // `conf.set` calls are inexpensive (no plan invalidation).
    // Validate the audit/cache invariant BEFORE mutating session config,
    // so a misuse fails fast without leaving the session polluted.
    if (auditRequest.isEmpty && (auditSink.isDefined || resultCache.isDefined)) {
      val offenders = List(
        Option.when(auditSink.isDefined)("auditSink"),
        Option.when(resultCache.isDefined)("resultCache"),
      ).flatten
      val verb = if (offenders.length > 1) "are" else "is"
      throw new IllegalStateException(
        s"withAuditSink and/or withResultCache require a query request. " +
        s"${offenders.mkString(" and ")} $verb set but query() was never called " +
        s"to capture the request shape. Try " +
        s"model.query(measures = Seq(\"your_measure\"), dimensions = Seq(\"your_dim\"))." +
        s"toDataFrame(spark) instead.")
    }
    applyAqeSkewConfig(spark)

    if (auditSink.isEmpty && resultCache.isEmpty) {
      // Fast path: no audit, no cache. Apply `materializeLevel` here
      // if set — subsequent actions on the returned DataFrame reuse
      // the persisted storage instead of re-executing the Spark
      // plan. The audit/cache branch (below) does NOT honour
      // `materializeLevel`: it returns a `parallelize`-based
      // DataFrame built from cached rows, which is effectively
      // MEMORY_ONLY for the call's duration. Persisting the compiled
      // DataFrame there would leak cluster storage (the user never
      // sees the compiled DataFrame). See `SemanticTable.materializeLevel`.
      val compiled = root.compile(spark)
      materializeLevel match {
        case Some(level) => compiled.persist(level)
        case None       => compiled
      }
    } else {
      // Audit + cache path. auditRequest is guaranteed non-empty by
      // the pre-check above (cleared along with resultCache by
      // invalidateAuditRequest, which any post-query shape-changer
      // calls).
      val t0 = System.nanoTime()
      val model = this.name.getOrElse(sourceTable.getOrElse("unknown"))
      val req   = auditRequest.get
      val whereHash  = req.where.map(where => io.semanticdf.audit.PredicateHasher.hash(where))
      val havingHash = req.having.map(having => io.semanticdf.audit.PredicateHasher.hash(having))

      // Cache key: only computed when a cache is configured. The
      // auditRequest is non-empty here (enforced above).
      val cacheKeyOpt: Option[String] =
        resultCache.flatMap(_ => io.semanticdf.cache.CacheKey.forRequest(req, maxRows))

      // Cache check: on hit, rebuild a DataFrame from the cached rows
      // and skip Spark's planner entirely. This is the "best performance"
      // path — no Spark job, no compile, just a `parallelize` of
      // already-materialised rows.
      val cachedOpt: Option[io.semanticdf.cache.CachedResult] =
        cacheKeyOpt.flatMap(key => resultCache.get.get(key))

      // Captures the actual Spark execution plan that ran. Set by
      // both the cache-hit and cache-miss paths (after `compile()`)
      // so the audit event can surface the real plan operators
      // ran, not the requested shape. Remains `None` on the cache
      // HIT path (no compile happens) and on the error path.
      var executedPlanCapture: Option[String] = None

      try {
        val (df, rowCount) =
          if (cachedOpt.isDefined) {
            val c = cachedOpt.get
            val rebuilt = if (c.rows.isEmpty) {
              // Empty result: avoid parallelize on an empty Seq.
              spark.createDataFrame(
                spark.sparkContext.emptyRDD[org.apache.spark.sql.Row], c.schema)
            } else {
              spark.createDataFrame(
                spark.sparkContext.parallelize(c.rows.toSeq), c.schema)
            }
            (rebuilt, c.rows.length.toLong)
          } else {
            cacheKeyOpt match {
              case Some(key) =>
                // Cache miss with a cache key. Collect once for the
                // cache, then rebuild the DataFrame from those exact
                // rows + schema — the same pattern the cache hit path
                // uses above. Before #184, the miss path returned the
                // lazy `fresh` DataFrame, so the caller's `collect()`
                // triggered the Spark job again — twice on every miss,
                // once on every hit. That violated the "no overhead"
                // contract.
                //
                // The fix is self-evidently correct by inspection: it
                // mirrors the hit path (`parallelize + schema`). Six
                // independent signals (job count, optimized plan,
                // executed plan, root RDD class, full RDD chain, CSV
                // source) all collapse to the same value for buggy and
                // fixed code because Spark's `QueryExecution` layer
                // abstracts the source — so there's no clean regression
                // test for this branch. The code review comment is the
                // documentation; the behaviour is identical to the hit
                // path, which IS tested.
                val fresh = root.compile(spark)
                // Step 1: cap + collect. If the query itself fails, let the
                // failure propagate through the outer audit handler.
                // `maxRows > 0` mirrors CacheBridge.executeQuery: apply
                // df.limit(maxRows) BEFORE collect to bound driver memory.
                // `maxRows == 0` disables the cap (escape hatch only).
                // `maxRows < 0` is rejected upstream by withMaxRows's `require`.
                val capped =
                  if (maxRows > 0) fresh.limit(maxRows) else fresh
                val rows = capped.collect()
                // Step 2: try to populate the cache. Cache failures
                // must NOT break the query, so we use NonFatal only
                // (catching OOM/SOE was wrong — it would silently
                // report a successful query).
                try {
                  resultCache.get.putWithModelAndVersion(key,
                    io.semanticdf.cache.CachedResult(rows, fresh.schema),
                    model, req.version)
                } catch { case scala.util.control.NonFatal(_) => () }
                // Step 3: rebuild the DataFrame from the collected
                // rows so the caller is decoupled from the source.
                val rebuilt = if (rows.isEmpty) {
                  spark.createDataFrame(
                    spark.sparkContext.emptyRDD[org.apache.spark.sql.Row], fresh.schema)
                } else {
                  spark.createDataFrame(
                    spark.sparkContext.parallelize(rows.toSeq), fresh.schema)
                }
                executedPlanCapture = Some(fresh.queryExecution.executedPlan.toString())
                (rebuilt, rows.length.toLong)
              case None =>
                // No cache key (audit-only path: resultCache is None, but
                // auditSink is set so the audit event needs an accurate
                // rowCount). Apply the maxRows cap here too so the
                // driver's collect() doesn't OOM on a 10M-row query
                // when the cache isn't configured.
                val fresh = root.compile(spark)
                val capped =
                  if (maxRows > 0) fresh.limit(maxRows) else fresh
                val rows = capped.collect()
                val rebuilt =
                  spark.createDataFrame(
                    spark.sparkContext.parallelize(rows.toSeq), fresh.schema)
                executedPlanCapture = Some(fresh.queryExecution.executedPlan.toString())
                (rebuilt, rows.length.toLong)
            }
          }

        val elapsedMs = (System.nanoTime() - t0) / 1000000L
        if (auditSink.isDefined) {
          // Fire-and-forget; the sink is documented as non-throwing,
          // but we wrap defensively so a misbehaving sink can't
          // break the query.
          val okEvent = AuditEvent(
            ts         = clock(),
            model      = model,
            version    = req.version,
            measures   = req.measures,
            dimensions = req.dimensions,
            whereHash  = whereHash,
            havingHash = havingHash,
            rowCount   = rowCount,
            elapsedMs  = elapsedMs,
            status     = "ok",
            executedPlan = executedPlanCapture,
            // dedupHash is the replay-safe contract key; computed
            // from the 6 query-shape fields only (excludes ts,
            // elapsedMs, rowCount, status, error, requester, requestId).
            // See docs/design/platform-determinism-audit.md.
            dedupHash  = AuditEvent.dedupHashOf(
                          model, req.version, req.measures, req.dimensions,
                          whereHash, havingHash),
          )
          try auditSink.get.emit(okEvent) catch { case scala.util.control.NonFatal(_) => () }
        }
        df
      } catch {
        case e: Throwable =>
          val elapsedMs = (System.nanoTime() - t0) / 1000000L
          if (auditSink.isDefined) {
            val errorEvent = AuditEvent(
              ts         = clock(),
              model      = model,
              version    = req.version,
              measures   = req.measures,
              dimensions = req.dimensions,
              whereHash  = whereHash,
              havingHash = havingHash,
              rowCount   = 0L,
              elapsedMs  = elapsedMs,
              status     = "error",
              error      = Some(s"${e.getClass.getSimpleName}: ${e.getMessage}"),
              // dedupHash includes status; the same error + same
              // query shape dedups to one row across replays.
              dedupHash  = AuditEvent.dedupHashOf(
                            model, req.version, req.measures, req.dimensions,
                            whereHash, havingHash),
            )
            try auditSink.get.emit(errorEvent) catch { case scala.util.control.NonFatal(_) => () }
          }
          throw e
      }
    }
  }

  /** Fluent-chain alias for [[toDataFrame]]. `spark` is implicit; see [[toDataFrame]].
    */
  def execute(implicit spark: SparkSession): DataFrame = toDataFrame()
  def execute()(implicit spark: SparkSession, clock: () => java.time.Instant = io.semanticdf.audit.Clock.systemDefault): DataFrame = toDataFrame()

  // -------------------------------------------------------------------------
  // Setters (withRowFilter lives here because it's a query-time filter,
  // not a model mutation like withDimensions/withMeasures)
  // -------------------------------------------------------------------------

  /** Declare a pre-join row filter on the model. */
  def withRowFilter(
      name: String,
      expr: String,
      description: Option[String],
      metadata: Map[String, String],
  ): SemanticTable = {
    val next = new SemanticTable(
      SemanticRowFilterOp(root, name, description, expr, metadata),
      postAggPredicates,
      version,
      sourceTable,
      status, auditSink, auditRequest, resultCache, maxRows = maxRows,
      broadcastJoinThreshold = broadcastJoinThreshold,
      materializeLevel = materializeLevel,
      salt = salt,
      rollups = this.rollups,
    )
    // Adding a row filter changes the rows returned — invalidate.
    if (next.auditRequest.isDefined || next.resultCache.isDefined) next.invalidateAuditRequest() else next
  }

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
    new SemanticTable(root, postAggPredicates, version = v, sourceTable, status, auditSink, auditRequest, resultCache, maxRows = maxRows, broadcastJoinThreshold = broadcastJoinThreshold,
          materializeLevel = materializeLevel,
salt = salt, rollups = this.rollups)
  }

  /** Set the model's lifecycle status. Returns a NEW SemanticTable (immutability
    * preserved). Same pattern as [[version(v:Int)* version]] — the field name and
    * setter name are the same; Scala disambiguates by argument count.
    *
    * Typical uses:
    *   - `model.status(ModelStatus.Draft)` while authoring
    *   - `model.status(ModelStatus.Deprecated)` before a planned removal
    *
    * See [[ModelStatus]] for the lifecycle contract. */
  def status(s: ModelStatus): SemanticTable = {
    require(s != null, "SemanticTable.status: ModelStatus must be non-null")
    new SemanticTable(root, postAggPredicates, version, sourceTable, s, auditSink, auditRequest, resultCache, maxRows = maxRows, broadcastJoinThreshold = broadcastJoinThreshold,
          materializeLevel = materializeLevel,
salt = salt, rollups = this.rollups)
  }

  /** Install an [[io.semanticdf.audit.AuditSink]] on this table.
    *
    * Every subsequent `query()` + `toDataFrame()` / `execute()` round trip
    * will emit an [[io.semanticdf.audit.AuditEvent]] to the sink. The sink
    * survives the fluent chain (`.query(...).limit(...).toDataFrame(...)`
    * keeps the sink) so a single setter call at the model level covers
    * every downstream query.
    *
    * Default: `None` (no audit). Pass `Some(sink)` to enable, `None` to
    * disable. For a JSONL-on-stdout sink, use
    * `Some(io.semanticdf.audit.AuditSink.JsonlStdout)`. */
  def withAuditSink(sink: AuditSink): SemanticTable =
    new SemanticTable(root, postAggPredicates, version, sourceTable, status, Some(sink), auditRequest, resultCache, maxRows = maxRows, broadcastJoinThreshold = broadcastJoinThreshold,
          materializeLevel = materializeLevel,
salt = salt, rollups = this.rollups)

  /** Install a [[io.semanticdf.cache.ResultCache]] on this table.
    *
    * Every subsequent `query()` + `toDataFrame()` / `execute()` round
    * trip checks the cache first; on hit, the cached rows are
    * returned without re-executing the Spark plan. On miss, the
    * result is stored before the DataFrame is returned. The cache
    * survives the fluent chain the same way [[withAuditSink]] does.
    *
    * Default: `None` (no cache). Pass `Some(cache)` to enable. For
    * an LRU-bounded in-memory cache, use
    * `Some(io.semanticdf.cache.ResultCache.inMemory(256))`. */
  def withResultCache(cache: io.semanticdf.cache.ResultCache): SemanticTable =
    new SemanticTable(root, postAggPredicates, version, sourceTable, status, auditSink, auditRequest, Some(cache), maxRows = maxRows, broadcastJoinThreshold = broadcastJoinThreshold,
          materializeLevel = materializeLevel,
salt = salt, rollups = this.rollups)

  /** Set the driver-memory safety cap applied on the cache-miss collect path.
    *
    * Mirrors the platform's `CacheBridge.executeQuery` row cap: a
    * positive value applies `df.limit(maxRows)` before `.collect()` so the
    * materialised row array is bounded. The default
    * ([[io.semanticdf.cache.CacheKey.DefaultMaxRows]] = 100,000)
    * protects against OOM on unexpectedly large results.
    *
    * `n == 0` disables the cap (escape hatch only — not recommended for
    * production). `n < 0` throws `IllegalArgumentException` since
    * `Dataset.limit` itself rejects negative values.
    *
    * The cap survives the fluent chain the same way [[resultCache]] does.
    * The cap also participates in the result-cache key (see
    * [[io.semanticdf.cache.CacheKey.forRequest]]) so two queries that
    * differ only in `maxRows` produce different cache entries. */
  def withMaxRows(n: Int): SemanticTable = {
    require(n >= 0, s"SemanticTable.withMaxRows: n must be non-negative, got: $n")
    new SemanticTable(root, postAggPredicates, version, sourceTable, status, auditSink, auditRequest, resultCache, maxRows = n, broadcastJoinThreshold = broadcastJoinThreshold,
          materializeLevel = materializeLevel,
salt = salt, rollups = this.rollups)
  }

  /** Opt-in auto-broadcast threshold for joins (size-based, bytes).
    *
    * When set to a positive value, the equi-join compile path applies
    * `broadcast(rightSide)` automatically if
    * `rightSide.queryExecution.optimizedPlan.stats.sizeInBytes < bytes`.
    * This overrides Spark's cost-based `autoBroadcastJoinThreshold`
    * decision for this specific query.
    *
    * When `None` (the default), no library override — Spark decides.
    * The threshold survives the fluent chain the same way [[resultCache]]
    * and [[maxRows]] do. Manifest round-trip is lossy (see
    * [[broadcastJoinThreshold]] Scaladoc).
    *
    * `bytes <= 0` disables broadcast (escape hatch); `bytes < 0` throws.
    *
    * @param bytes size threshold in bytes; pass a positive value to opt
    *              in to size-based auto-broadcast
    */
  def withBroadcastJoinThreshold(bytes: Long): SemanticTable = {
    require(bytes >= 0,
      s"SemanticTable.withBroadcastJoinThreshold: bytes must be non-negative, got: $bytes")
    new SemanticTable(
      root, postAggPredicates, version, sourceTable, status,
      auditSink, auditRequest, resultCache, maxRows,
      broadcastJoinThreshold = if (bytes == 0L) None else Some(bytes),
      materializeLevel = materializeLevel,
salt = salt, rollups = this.rollups)
  }

  /** Opt-in skew-handling hint for equi-joins. When set, the
    * next `toDataFrame` call configures Spark AQE to handle
    * skewed partitions: sets
    * `spark.sql.adaptive.enabled = true` (the parent AQE config
    * — required for the skew child to take effect),
    * `spark.sql.adaptive.skewJoin.enabled = true`, and
    * `spark.sql.adaptive.skewJoin.skewedPartitionFactor = n`.
    *
    * Spark's actual behavior (per `OptimizeSkewedJoin.scala`):
    * AQE **divides each skewed shuffle partition into smaller
    * sub-partitions and replicates the matching partition on the
    * other side of the join** — they run as parallel tasks. It
    * is NOT auto-broadcast semantics (the previous Scaladoc said
    * "broadcasts them automatically", which was wrong; this is
    * the corrected version).
    *
    * The `n` parameter is the skew-handling factor (Spark's
    * `skewedPartitionFactor`): a partition is considered skewed if
    * its size exceeds `max(n * median_size, thresholdBytes)`.
    * Spark's default is `5`. A larger `n` makes skew detection
    * more conservative (only very-skewed partitions are split);
    * a smaller `n` is more aggressive. `n = 0` disables the hint
    * (mirrors the `broadcastJoinThreshold = 0` convention — the
    * field becomes `None`); `n >= 1` enables skew handling.
    *
    * Trade-off: `adaptive.enabled = true` is a session-global
    * override. If the operator has explicitly disabled AQE for
    * compliance/audit reasons, calling `withSalt` would silently
    * re-enable it. The library's position: `withSalt` is an
    * explicit opt-in by the user; the override is necessary for
    * the hint to activate. Operators who need AQE disabled can
    * call `conf.set("spark.sql.adaptive.enabled", "false")` AFTER
    * the query that uses salt.
    *
    * See [[SemanticTable.salt]] for the full contract: skew
    * motivation, sentinel conventions, propagation rules, and
    * why a custom `rand() * n` salt column would be wrong (it
    * doesn't match across sides for shuffled joins). */
  def withSalt(n: Int): SemanticTable = {
    require(n >= 0,
      s"SemanticTable.withSalt: n must be non-negative, got: $n")
    new SemanticTable(
      root, postAggPredicates, version, sourceTable, status,
      auditSink, auditRequest, resultCache, maxRows,
      broadcastJoinThreshold = broadcastJoinThreshold,
      materializeLevel = materializeLevel,
      salt = if (n == 0) None else Some(n),
      rollups = this.rollups)
  }

  /** Opt-in DataFrame persistence. When set, the fast path of
    * [[toDataFrame]] (no audit, no result cache) calls
    * `df.persist(level)` on the compiled DataFrame so subsequent
    * actions on the returned `DataFrame` reuse the cached storage
    * instead of re-executing the Spark plan. See [[SemanticTable.materializeLevel]]
    * for the full contract: lifecycle, audit/cache interaction,
    * streaming no-op, join propagation, and the reason the library
    * does NOT retain a `DataFrame` ref or expose `unpersist()` on
    * the table.
    *
    * The `level` is the standard Spark `StorageLevel` enum
    * (`MEMORY_ONLY`, `MEMORY_AND_DISK`, `MEMORY_ONLY_SER`, etc.).
    * Storage level choice is the operator's responsibility — the
    * library does not curate a subset and cannot protect the
    * cluster from a user-chosen `MEMORY_ONLY` on a 10M-row query.
    *
    * Note: `StorageLevel.NONE` is a meaningful Spark value that
    * means "no persist" — passing it via `Some(NONE)` is
    * indistinguishable from the default `None` at runtime. Callers
    * who want "no persist" should not call this setter at all. */
  def withMaterialize(level: org.apache.spark.storage.StorageLevel): SemanticTable = {
    require(level != null,
      "SemanticTable.withMaterialize: level must not be null. " +
      "To disable persistence, omit the .withMaterialize() call.")
    new SemanticTable(
      root, postAggPredicates, version, sourceTable, status,
      auditSink, auditRequest, resultCache, maxRows,
      broadcastJoinThreshold,
      materializeLevel = Some(level),
salt = salt, rollups = this.rollups)
  }

  /** Apply the `salt` field as Spark AQE skew-handling config.
    * Called from BOTH the batch terminal (`toDataFrameInternal`)
    * and the streaming terminal (`SemanticTableStreaming.toStreamingQuery`).
    *
    * Why both: the static-stream join plan is built when the user
    * calls `writeStream.start()` — Spark planning reads SQLConf at
    * that moment. If AQE isn't set before `start()`, the join plan
    * is created WITHOUT skew handling (the foreachBatch path's
    * `batchModel.toDataFrame` is too late — it only affects the
    * per-microbatch DataFrame, not the streaming query plan itself).
    *
    * Per the [[SemanticTable.salt]] Scaladoc:
    *   1. `spark.sql.adaptive.enabled = true` (parent AQE config)
    *   2. `spark.sql.adaptive.skewJoin.enabled = true` (skew child)
    *   3. `spark.sql.adaptive.skewJoin.skewedPartitionFactor = n`
    *
    * No-op when `salt = None`. Idempotent — Spark optimizes repeated
    * `conf.set` calls on the same key. */
  private[semanticdf] def applyAqeSkewConfig(spark: SparkSession): Unit =
    salt.foreach { n =>
      spark.conf.set("spark.sql.adaptive.enabled", "true")
      spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
      spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", n.toString)
    }

  /** Internal: stamp the captured request shape for audit emission.
    * Called by [[query]] once the chain is built. Not part of the public
    * API — callers should let [[query]] capture the request. */
  private[semanticdf] def copyAuditRequest(req: AuditQueryRequest): SemanticTable =
    new SemanticTable(root, postAggPredicates, version, sourceTable, status, auditSink, Some(req), resultCache, maxRows = maxRows, broadcastJoinThreshold = broadcastJoinThreshold,
          materializeLevel = materializeLevel,
salt = salt, rollups = this.rollups)

  /** Drop the captured audit request AND clear `resultCache`. Either
    * field alone is unsound: a stale cache key without a request, or
    * a cache key computed against a new shape. Used by result-shaping
    * chainable methods (orderBy, limit, where, having, atTimeGrain)
    * when they're called AFTER `query()`. */
  private[semanticdf] def invalidateAuditRequest(): SemanticTable =
    new SemanticTable(root, postAggPredicates, version, sourceTable, status, auditSink, None, None, maxRows = maxRows, broadcastJoinThreshold = broadcastJoinThreshold,
          materializeLevel = materializeLevel,
salt = salt, rollups = this.rollups)

  /** Same as [[explainSemantic(spark:org.apache.spark.sql.SparkSession, scope:io.semanticdf.SemanticTable#Scope)]]
    * but accepts an optional SparkSession (e.g. for a static-only view). */
  def explainSemantic(spark: Option[SparkSession], scope: Scope): String = {
    val renderer = new SemanticPlanRenderer(this, scope)
    renderer.render(spark)
  }

  // -------------------------------------------------------------------------
  // Schema
  // -------------------------------------------------------------------------

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
  private[semanticdf] def collectSchemaFields(
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
  // Query builders (where / having / orderBy / limit / withHint / query)
  // -------------------------------------------------------------------------

  def where(pred: Predicate): SemanticTable = {
    val knownMeasures = resolveAllMeasureNames
    val (pre, post) = Predicate.splitFilter(pred, knownMeasures)
    val newRoot = pre.foldLeft(root) { (r, p) =>
      SemanticFilterOp(r, p)
    }
    val next = new SemanticTable(newRoot, postAggPredicates ++ post, version, sourceTable, status, auditSink, auditRequest, resultCache, maxRows = maxRows, broadcastJoinThreshold = broadcastJoinThreshold,
          materializeLevel = materializeLevel,
salt = salt, rollups = this.rollups)
    // If auditRequest was set by an earlier query() call, the new
    // filter changes the result. Drop the audit request so the
    // cache key doesn't match the pre-filter query.
    if (auditRequest.isDefined) next.invalidateAuditRequest() else next
  }

  /** Apply a filter predicate explicitly as post-aggregation (HAVING).
    *
    * Use when you want a dimension filter to apply after aggregation (rare, but
    * sometimes needed when the dimension is derived from a measure). */
  def having(pred: Predicate): SemanticTable = {
    val next = new SemanticTable(root, postAggPredicates :+ pred, version, sourceTable, status, auditSink, auditRequest, resultCache, maxRows = maxRows, broadcastJoinThreshold = broadcastJoinThreshold,
          materializeLevel = materializeLevel,
salt = salt, rollups = this.rollups)
    // If auditRequest was set by an earlier query() call, the new
    // post-agg filter changes the result. Drop the audit request so
    // the cache key doesn't match the pre-filter query.
    if (auditRequest.isDefined) next.invalidateAuditRequest() else next
  }

  /** Order the result by one or more sort keys.
    *
    * Bare strings are ascending; use [[SortKey.desc]] for descending:
    * {{{
    * st.orderBy("carrier", SortKey.desc("total_passengers"))
    * }}}
    *
    * Typically chained after `aggregate()`. Composes with [[limit]]. */
  def orderBy(keys: SortKey*): SemanticTable = {
    // If auditRequest is set (came from query()), drop it so the
    // cache key reflects the new shape. Otherwise preserve.
    val next = new SemanticTable(SemanticOrderByOp(root, keys), postAggPredicates, version, sourceTable, status, auditSink, auditRequest, resultCache, maxRows = maxRows, broadcastJoinThreshold = broadcastJoinThreshold,
          materializeLevel = materializeLevel,
salt = salt, rollups = this.rollups)
    if (auditRequest.isDefined) next.invalidateAuditRequest() else next
  }

  /** Limit the result to the first `n` rows. Composes with [[orderBy]]. */
  def limit(n: Int): SemanticTable = {
    val next = new SemanticTable(SemanticLimitOp(root, n), postAggPredicates, version, sourceTable, status, auditSink, auditRequest, resultCache, maxRows = maxRows, broadcastJoinThreshold = broadcastJoinThreshold,
          materializeLevel = materializeLevel,
salt = salt, rollups = this.rollups)
    if (auditRequest.isDefined) next.invalidateAuditRequest() else next
  }

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
    new SemanticTable(SemanticHintOp(root, strategy, params.toSeq), postAggPredicates, version, sourceTable, status, auditSink, auditRequest, resultCache, maxRows = maxRows, broadcastJoinThreshold = broadcastJoinThreshold,
          materializeLevel = materializeLevel,
salt = salt, rollups = this.rollups)

  /** One-shot bundled query.
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
    // time_range first (filters rows by raw timestamp, pre-truncation).
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
    // grain truncation overrides each time dimension's expr.
    val grainMap: Map[String, String] =
      if (timeGrains.nonEmpty) timeGrains
      else timeGrain.map(g => timeDimensionsAmong(dimensions).map(_ -> g).toMap).getOrElse(Map.empty)
    grainMap.foreach { case (dim, g) => t = t.atTimeGrain(dim, g) }
    where.foreach(p  => t = t.where(p))
    having.foreach(p => t = t.having(p))
    var result: SemanticTable = t.groupBy(dimensions.toSeq: _*).aggregate(measures.toSeq: _*)
    if (orderBy.nonEmpty) result = result.orderBy(orderBy.toSeq: _*)
    limit.foreach(n => result = result.limit(n))
    // Stamp the captured request shape so the audit event emitted at
    // toDataFrame carries the user's original query, not the post-chain
    // op tree. The chainable methods preserved auditRequest; this is
    // the final touch — set it on the result.
    val model = this.name.getOrElse(sourceTable.getOrElse("unknown"))
    val captured = AuditQueryRequest(
      model      = model,
      version    = this.version,
      measures   = measures.toSeq,
      dimensions = dimensions.toSeq,
      // Phase 1 consolidation: convert at the audit/cache boundary.
      // QueryRequest.where/having are typed as the engine-portable core
      // ADT (so the hasher + cache-key chain have no converter on the hot
      // path). The user-facing `query()` parameter is still the original
      // Spark-bearing Predicate, so we convert once here at capture time.
      where      = where.map(io.semanticdf.predicate.PredicateConverter.toCore),
      having     = having.map(io.semanticdf.predicate.PredicateConverter.toCore),
      orderBy    = orderBy.toSeq.map { case SortKey.Asc(name)  => (name, "asc")
                                       case SortKey.Desc(name) => (name, "desc") },
      limit      = limit,
      timeGrain  = timeGrain,
      timeGrains = timeGrains,
      timeRange  = timeRange,
    )
    result.auditRequest match {
      case Some(_) => result  // already stamped by a nested query() call
      case None    => result.copyAuditRequest(captured)
    }
  }

  // -------------------------------------------------------------------------
  // Time-grain convenience
  // -------------------------------------------------------------------------

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
  private[semanticdf] def resolveDimension(name: String): Option[Dimension] = root match {
    case t: SemanticTableOp => t.dimensions.get(name)
    case j: SemanticJoinOp  => j.mergedModel.dimensions.get(name)
    case SemanticFilterOp(src, _)     => new SemanticTable(src, auditSink = this.auditSink, auditRequest = this.auditRequest, resultCache = this.resultCache, maxRows = maxRows, broadcastJoinThreshold = broadcastJoinThreshold,
          materializeLevel = materializeLevel,
salt = salt, rollups = this.rollups).resolveDimension(name)
    case SemanticOrderByOp(src, _)    => new SemanticTable(src, auditSink = this.auditSink, auditRequest = this.auditRequest, resultCache = this.resultCache, maxRows = maxRows, broadcastJoinThreshold = broadcastJoinThreshold,
          materializeLevel = materializeLevel,
salt = salt, rollups = this.rollups).resolveDimension(name)
    case SemanticLimitOp(src, _)      => new SemanticTable(src, auditSink = this.auditSink, auditRequest = this.auditRequest, resultCache = this.resultCache, maxRows = maxRows, broadcastJoinThreshold = broadcastJoinThreshold,
          materializeLevel = materializeLevel,
salt = salt, rollups = this.rollups).resolveDimension(name)
    case SemanticHintOp(src, _, _)    => new SemanticTable(src, auditSink = this.auditSink, auditRequest = this.auditRequest, resultCache = this.resultCache, maxRows = maxRows, broadcastJoinThreshold = broadcastJoinThreshold,
          materializeLevel = materializeLevel,
salt = salt, rollups = this.rollups).resolveDimension(name)
    case SemanticTransformsOp(src, _) => new SemanticTable(src, auditSink = this.auditSink, auditRequest = this.auditRequest, resultCache = this.resultCache, maxRows = maxRows, broadcastJoinThreshold = broadcastJoinThreshold,
          materializeLevel = materializeLevel,
salt = salt, rollups = this.rollups).resolveDimension(name)
    case _ => SemanticOp.rootModel(root).flatMap(_.dimensions.get(name))
  }

  private[semanticdf] def resolveAllMeasureNames: Set[String] = root match {
    case t: SemanticTableOp => t.measures.keySet
    case j: SemanticJoinOp  => j.mergedModel.measures.keySet
    case SemanticFilterOp(src, _) =>
      // Unwrap filters to find the underlying model.
      new SemanticTable(src, auditSink = this.auditSink, auditRequest = this.auditRequest, resultCache = this.resultCache, maxRows = maxRows, broadcastJoinThreshold = broadcastJoinThreshold,
          materializeLevel = materializeLevel,
salt = salt, rollups = this.rollups).resolveAllMeasureNames
    case SemanticHintOp(src, _, _) =>
      // Hint is a Spark planner wrapper; recurse to find the underlying model.
      new SemanticTable(src, auditSink = this.auditSink, auditRequest = this.auditRequest, resultCache = this.resultCache, maxRows = maxRows, broadcastJoinThreshold = broadcastJoinThreshold,
          materializeLevel = materializeLevel,
salt = salt, rollups = this.rollups).resolveAllMeasureNames
    case SemanticTransformsOp(src, _) =>
      // Transforms don't change the measure catalog; recurse to the source.
      new SemanticTable(src, auditSink = this.auditSink, auditRequest = this.auditRequest, resultCache = this.resultCache, maxRows = maxRows, broadcastJoinThreshold = broadcastJoinThreshold,
          materializeLevel = materializeLevel,
salt = salt, rollups = this.rollups).resolveAllMeasureNames
    case _ =>
      SemanticOp.rootModel(root).map(_.measures.keySet).getOrElse(Set.empty)
  }

  private[semanticdf] def rootModel: SemanticTableOp = root match {
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

  private[semanticdf] def requireRoot(label: String): SemanticTableOp =
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
        new SemanticTable(src, auditSink = this.auditSink, auditRequest = this.auditRequest, resultCache = this.resultCache, maxRows = maxRows, broadcastJoinThreshold = broadcastJoinThreshold,
          materializeLevel = materializeLevel,
salt = salt, rollups = this.rollups).requireRoot(label)
      // Transforms are applied at compile time; unwrap to find the underlying table.
      case SemanticTransformsOp(src, _) =>
        new SemanticTable(src, auditSink = this.auditSink, auditRequest = this.auditRequest, resultCache = this.resultCache, maxRows = maxRows, broadcastJoinThreshold = broadcastJoinThreshold,
          materializeLevel = materializeLevel,
salt = salt, rollups = this.rollups).requireRoot(label)
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

  // -------------------------------------------------------------------------
  // Catalog accessors
  // -------------------------------------------------------------------------

  /** All dimensions declared on this semantic table. */
  def dimensions: Map[String, Dimension] = resolveRootModel.dimensions

  /** All measures declared on this semantic table (base and calc). */
  def measures: Map[String, Measure] = resolveRootModel.measures

  /** Name declared on this semantic table, if any. Set by `toSemanticTable(name=)`,
    * the YAML `flights:` top-level key (via `YamlLoader`), or `withTransforms`/joins.
    * Returns None for anonymous models (no name was ever assigned). */
  def name: Option[String] = resolveRootModel.name

  /** True if the table's root op is a join (i.e. the model was built by
    * one or more `join_inner` / `join_cross` / `join_outer` calls, or
    * carries a YAML `joins:` block). Used by writers and CLI tools to
    * decide between single-table vs joined manifests.
    *
    * Mirrors `SemanticManifest.toJson`'s writer behavior: emitting a
    * manifest for a joined table throws `IllegalStateException` (recipe
    * §10 anti-scope). Callers should branch on `isJoined` BEFORE
    * calling `toJson` so the failure mode is `if/else`, not `try/catch`. */
  def isJoined: Boolean = root match {
    case _: SemanticJoinOp => true
    case _                  => false
  }

  /** Human-readable description declared on this semantic table, if any. Set by
    * `toSemanticTable(description=)` or the YAML `description:` field. */
  def description: Option[String] = resolveRootModel.description

  /** Look up a dimension by name. */
  def findDimension(name: String): Option[Dimension] = dimensions.get(name)

  /** Look up a measure by name. */
  def findMeasure(name: String): Option[Measure] = measures.get(name)

  /** Return the declared `smallestTimeGrain` of a time dimension,
    * if `dimName` is a time dimension with one declared. Returns None
    * for non-time dimensions or unnamed dimensions.
    *
    * The grain is normalized to the canonical Spark `date_trunc` unit
    * (e.g. "month" -> "MONTH").
    */
  def findDimensionTimeGrain(dimName: String): Option[String] =
    findDimension(dimName).flatMap { d =>
      if (d.isTimeDimension) d.smallestTimeGrain.map(TimeGrain.normalize)
      else None
    }

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
  private[semanticdf] def collectJoins(op: SemanticOp): Seq[JoinInfo] = op match {
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
  // Metastore integration
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
  // Typed result schema
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
    // with a clear message when execute() runs; see the regression test.

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

    // 4. OR predicate that mixes dim + measure categories (WARNING).
    //    The whole predicate goes post-agg (which may not be the user's intent).
    //    Note: `where()` already splits ANDs into separate filter nodes at
    //    construction time, so AND never reaches this check. OR is preserved
    //    intact and is the case users will actually see in practice.
    //
    //    (Previously this match had a `Predicate.And` arm that was
    //    unreachable: `splitFilter` either splits AND into separate
    //    post-agg filter nodes, or re-groups them as a single AND with
    //    `mixed = false`. The arm was dead code with a misleading
    //    comment — fixed in the data-design audit S2-6. See
    //    [[Predicate.splitFilter]] for the partitioning rule that makes
    //    the AND arm unreachable.)
    allFilters.foreach { f =>
      val mixed = f.predicate match {
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
  // Internals (used by catalog accessors + future traits)
  // -------------------------------------------------------------------------

  /** Resolve the leaf [[SemanticTableOp]] from the root, unwrapping transparent
    * wrappers (where/orderBy/limit/row-filter). Used by the catalog accessors
    * (`dimensions`, `measures`, `findDimension`, `findMeasure`). */
  private[semanticdf] def resolveRootModel: MergedSemanticModel = root match {
    case t: SemanticTableOp          => MergedSemanticModel(t.dimensions, t.measures, t.name, t.description)
    case s: SemanticStreamingTableOp => MergedSemanticModel(s.dimensions, s.measures, s.name, s.description)
    case j: SemanticJoinOp  => j.mergedModel
    case SemanticAggregateOp(src, _, _) =>
      new SemanticTable(src, auditSink = this.auditSink, auditRequest = this.auditRequest, resultCache = this.resultCache, maxRows = maxRows, broadcastJoinThreshold = broadcastJoinThreshold,
          materializeLevel = materializeLevel,
salt = salt, rollups = this.rollups).resolveRootModel
    case SemanticFilterOp(src, _)  => new SemanticTable(src, auditSink = this.auditSink, auditRequest = this.auditRequest, resultCache = this.resultCache, maxRows = maxRows, broadcastJoinThreshold = broadcastJoinThreshold,
          materializeLevel = materializeLevel,
salt = salt, rollups = this.rollups).resolveRootModel
    // Pre-join row filters do not change the declared model — unwrap transparently.
    case SemanticRowFilterOp(src, _, _, _, _) => new SemanticTable(src, auditSink = this.auditSink, auditRequest = this.auditRequest, resultCache = this.resultCache, maxRows = maxRows, broadcastJoinThreshold = broadcastJoinThreshold,
          materializeLevel = materializeLevel,
salt = salt, rollups = this.rollups).resolveRootModel
    case SemanticOrderByOp(src, _) => new SemanticTable(src, auditSink = this.auditSink, auditRequest = this.auditRequest, resultCache = this.resultCache, maxRows = maxRows, broadcastJoinThreshold = broadcastJoinThreshold,
          materializeLevel = materializeLevel,
salt = salt, rollups = this.rollups).resolveRootModel
    case SemanticLimitOp(src, _)   => new SemanticTable(src, auditSink = this.auditSink, auditRequest = this.auditRequest, resultCache = this.resultCache, maxRows = maxRows, broadcastJoinThreshold = broadcastJoinThreshold,
          materializeLevel = materializeLevel,
salt = salt, rollups = this.rollups).resolveRootModel
    case SemanticHintOp(src, _, _) => new SemanticTable(src, auditSink = this.auditSink, auditRequest = this.auditRequest, resultCache = this.resultCache, maxRows = maxRows, broadcastJoinThreshold = broadcastJoinThreshold,
          materializeLevel = materializeLevel,
salt = salt, rollups = this.rollups).resolveRootModel
    // Transforms are transparent — they don't change the declared model.
    case SemanticTransformsOp(src, _) => new SemanticTable(src, auditSink = this.auditSink, auditRequest = this.auditRequest, resultCache = this.resultCache, maxRows = maxRows, broadcastJoinThreshold = broadcastJoinThreshold,
          materializeLevel = materializeLevel,
salt = salt, rollups = this.rollups).resolveRootModel
  }

  /** Build the streaming windowed-aggregation pipeline that the
    * `foreachBatch` callback receives as `batchDf`.
    *
    * Used by both the `SemanticAggregateOp + window` case (existing
    * path: the user wrote `.groupBy(...).aggregate(...)` in the Scala DSL)
    * AND the new `SemanticStreamingTableOp + window + groupKeys` case
    * (new path: a YAML-declared streaming model whose group keys live in
    * the operator's `StreamingConfig`). Both paths share the same windowed
    * aggregation + windowed-totals cross-join algorithm so behavior is
    * identical regardless of whether grouping was declared in the model
    * or in the operator config.
    *
    * Lives in `SemanticTableCore` for now because it's a small,
    * compile-time helper. When the Streaming trait lands
    * (`SemanticTableStreaming`), it will move there because it's only
    * called from streaming internals. Widening to `private[semanticdf]`
    * ensures the future trait can still call it.
    *
    * @param sourceWatermarked the streaming source with `.withWatermark(...)`
    *                          applied (if a watermark is configured).
    * @param groupKeys         additional source-column group keys to add to
    *                          `groupBy` alongside the window column. The
    *                          window column is filtered out if it appears
    *                          here (deduplication).
    * @param collectedMeasures all measures declared on the model that
    *                          need to be evaluated (already collected by
    *                          the caller — visitor-walked for the
    *                          aggregate-op case, direct from the streaming
    *                          op's `measures` field for the streaming-root
    *                          case).
    * @param measuresByName    lookup of measure name → measure. Used by
    *                          the totals-probe step that distinguishes
    *                          base measures from calc measures that
    *                          reference `t.all(...)`.
    * @param w                 the window spec (column + duration).
    * @return the compiled streaming aggregation `DataFrame` whose columns
    *         include `window`, the additional group keys, and the
    *         computed measures (including any `t.all(...)-using calc
    *         measures`).
    */
  private[semanticdf] def compileWindowedAggregation(
      sourceWatermarked: DataFrame,
      groupKeys: Seq[String],
      collectedMeasures: Seq[(String, Measure)],
      measuresByName: Map[String, Measure],
      w: StreamingSupport.WindowSpec,
  ): DataFrame = {
    import org.apache.spark.sql.functions._
    // 1. Compute one aggregate column per declared measure. Each measure's
    //    `expr: SemanticScope => Column` is invoked against a MeasureScope
    //    over the watermarked source to produce the aggregate column.
    val aggregateColumns: Seq[Column] = collectedMeasures.flatMap { case (name, measure) =>
      val scope = new MeasureScope(
        df = sourceWatermarked,
        knownMeasures = collectedMeasures.map(_._1).toSet,
      )
      try {
        Some(measure.expr(scope).as(name))
      } catch {
        case _: Throwable => None  // skip measures that can't be translated against the source
      }
    }
    val windowCol = window(col(w.column), w.duration)
    val groupCols: Seq[Column] = windowCol +: groupKeys.filter(_ != w.column).map(col)
    val mainAgg = if (aggregateColumns.isEmpty) {
      // No aggregate expressions produced (e.g., all measures were calc
      // references that didn't translate). Fall back to per-batch.
      sourceWatermarked
    } else {
      sourceWatermarked.groupBy(groupCols: _*).agg(aggregateColumns.head, aggregateColumns.tail: _*)
    }

    // 2. Windowed-totals support. For calc measures that use t.all(...),
    //    build a second aggregation per window (no other group keys) that
    //    gives the per-window grand totals. Cross-join this with the main
    //    aggregation, then evaluate the t.all-using calc measures against
    //    the cross-joined DataFrame.
    val totalUsers = StreamingSupport.StreamingValidator.findTotalUsers(collectedMeasures)
    if (totalUsers.isEmpty) {
      mainAgg
    } else {
      // For per-window totals: the t.all-using calc measures reference
      // BASE measures like "sum_value". Compute the same base measures,
      // but per window only (no other group keys), giving the per-window
      // grand totals. Cross-join with mainAgg to expose totals to the
      // calc measures via totalsScope.
      def asBaseColumn(name: String, m: Measure): Option[Column] = {
        val scope = new MeasureScope(
          df = sourceWatermarked,
          knownMeasures = collectedMeasures.map(_._1).toSet,
        )
        try Some(m.expr(scope).as(name))
        catch { case _: Throwable => None }
      }
      val allNames = collectedMeasures.map(_._1).toSet
      val baseMeasures = collectedMeasures.filterNot { case (name, _) =>
        val probe = new MeasureProbeScope(allNames)
        try { measuresByName(name).expr(probe) } catch { case _: Throwable => () }
        probe.referencedTotals.nonEmpty
      }
      val calcUsingAll = collectedMeasures.filter { case (name, _) =>
        val probe = new MeasureProbeScope(allNames)
        try { measuresByName(name).expr(probe) } catch { case _: Throwable => () }
        probe.referencedTotals.nonEmpty
      }
      if (baseMeasures.isEmpty || calcUsingAll.isEmpty) {
        mainAgg
      } else {
        // Per-window totals: same base measures, grouped by window only.
        val totalsCols: Seq[Column] = baseMeasures.flatMap { case (n, m) =>
          asBaseColumn(n, m).map(c => c.as("__total__" + n))
        }
        // Rename the totalsDf window column to avoid clash with mainAgg.
        val totalsDf = sourceWatermarked
          .groupBy(windowCol.as("__window__"))
          .agg(totalsCols.head, totalsCols.tail: _*)
        // Equi-join on window (both sides come from the same stream,
        // so the window always matches). We avoid crossJoin here because
        // two streaming aggregations cannot be cross-joined; an equi-join
        // is allowed under Spark's stream-stream join semantics.
        val withTotals = mainAgg.join(
          broadcast(totalsDf),
          mainAgg("window") === totalsDf("__window__"),
          "inner")
        val totalsScope = new MeasureScope(
          df = withTotals,
          knownMeasures = allNames,
          totalsResolver = Some((name: String) => withTotals("__total__" + name)),
        )
        val derived: Seq[Column] = calcUsingAll.map { case (name, measure) =>
          measure.expr(totalsScope).as(name)
        }
        withTotals.select(
          (withTotals.columns.map(col) ++ derived): _*
        )
      }
    }
  }

  override def toString: String = s"SemanticTable(${root.getClass.getSimpleName})"
}
