package io.semanticdf.mcp.handlers

import io.semanticdf.predicate._

import io.modelcontextprotocol.json.McpJsonMapper
import io.modelcontextprotocol.server.McpServerFeatures.SyncToolSpecification
import io.modelcontextprotocol.spec.McpSchema.{CallToolResult, Tool}
import io.semanticdf.mcp.{Envelope, Handlers, Models, OkfCache}
import io.semanticdf.core.engine.EngineError
import io.semanticdf.{SortKey, SemanticTable}
import org.apache.spark.sql.{Row => SparkRow, SparkSession, DataFrame}
import org.apache.spark.sql.types.{DataType, StringType, LongType, IntegerType, DoubleType, FloatType, BooleanType, DateType, TimestampType, DecimalType}

import java.util.{List => JList}
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future, TimeoutException}
import scala.jdk.CollectionConverters._

/** `query` tool — the workhorse. Runs a query against the loaded models and
  * returns the resulting rows.
  *
  * Per `mcp-contract.md` v2 §"Tool 3: query":
  *
  *   Request: `{model, dimensions, measures, where, having, order_by, limit,
  *              time_grain, time_range}`
  *   Response: `{columns: [{name, type}], rows: [[..]], row_count: N}`
  *
  * And §"Tool 4: explain" — same request shape, returns the
  * `SemanticTable.explainSemantic(spark)` string verbatim (no execution). */
final class Query(
    spark: SparkSession,
    val maxRows: Int = Query.maxRowsFromEnv(),
    val timeoutMs: Long = Query.timeoutMsFromEnv(),
    /** Audit sink — when set, every successful and failed query
      * emits an [[io.semanticdf.audit.AuditEvent]] through this sink
      * (via the underlying `SemanticTable.withAuditSink(...)` path).
      * Default `None` so the audit path is opt-in; the MCP server
      * passes its shared `InMemoryAuditSink` to enable the
      * `audit_log` retrieval tool. */
    val auditSink: Option[io.semanticdf.audit.AuditSink] = None,
    /** Engine registry (per design §6.4). When
      * `request.engine.nonEmpty` AND this is `Some(_)`,
      * `handle()`/`explain()` route through the engine provider;
      * otherwise the legacy `Models` + `SemanticTable` path is
      * used (backward-compat with pre-PR-5c callers).
      *
      * Per karpathy §2 ("minimum code that solves the problem"):
      * default `None` preserves the existing constructor signature.
      * The companion object exposes an additive constructor
      * overload that accepts the registry. */
    val engineRegistry: Option[io.semanticdf.core.engine.MCPEngineRegistry] = None,
) {

  private val log = Query.log

  /** Handle `query`: run the query, return rows. */
  def handle(registry: Models, request: QueryRequest): Envelope[Query.Data] = {
    // v0.3.1 (engine-default flip): when the engine registry is
    // configured, route through it by DEFAULT. Falls back to the
    // legacy `Models` + `SemanticTable` path if:
    //   (a) no engine registry is configured (backward compat for
    //       setups that haven't yet wired the registry), OR
    //   (b) the engine path fails (e.g. the legacy `SemanticTable`
    //       has no `SourceRef` the engine can resolve) — the legacy
    //       path is the fallback of last resort.
    //
    // Per the user audit (post-v0.3.1): the legacy path was the
    // default for too long, hiding the engine-portable path.
    // Flipping the default makes the new path the standard.
    if (engineRegistry.isDefined) {
      val t0 = System.currentTimeMillis()
      handleViaRegistry(registry, request) match {
        case Right(pqr) =>
          val elapsed = System.currentTimeMillis() - t0
          return Envelope.ok(
            portableToData(pqr),
            warnings = Handlers.lifecycleWarnings(request.model, io.semanticdf.ModelStatus.Draft),
            meta = io.semanticdf.mcp.Meta(elapsed_ms = elapsed, model = Some(request.model)),
          )
        case Left(err) =>
          // Engine path failed. Per the standard: don't swallow
          // the error silently (scala-chaos-testing §2). We fall
          // back to the legacy path so the legacy `SemanticTable`
          // (which the engine can't resolve via SourceRef) still
          // works, but we log the engine-path failure for
          // observability.
          log.warn(
            s"engine path failed (${err.getClass.getSimpleName}: ${err.toErrorDetail.message}); falling back to legacy path"
          )
          // Fall through to the legacy path below.
      }
    }

    val raw = registry(request.model)
    // Attach the audit sink (if any) so the underlying `query()` +
    // `toDataFrame()` flow emits an event. `withAuditSink` is a pure
    // copy — it does not mutate the registry.
    val t = auditSink.fold(raw)(s => raw.withAuditSink(s))

    // AMBIGUOUS_MEASURE / AMBIGUOUS_DIMENSION: detect when a requested
    // name matches multiple fields in the merged model. Spark is
    // case-insensitive at the Column level, but the library's name
    // resolution is case-sensitive — so a query like {dimensions: ["Carrier"]}
    // would silently resolve to "carrier" even when "carriers.carrier" also
    // exists. Catch this BEFORE the library call so the agent gets a
    // typed error with candidates instead of a silent ambiguity.
    Query.checkAmbiguity(request, t)

    // Build the library-level semantic-table chain.
    val st =
      t.query(
        measures   = request.measures,
        dimensions = request.dimensions.getOrElse(Nil),
        where      = Query.mergedWhere(request),
        having     = Query.mergedHaving(request),
        orderBy    = request.order_by.map(Query.toSortKey),
        limit      = request.limit,
        timeGrain  = request.time_grain,
        timeGrains = request.time_grains.map(_.toMap).getOrElse(Map.empty),
        timeRange  = request.time_range,
      )

    val t0 = System.currentTimeMillis()
    // QUERY_TIMEOUT: run the query under a Spark job group with a deadline.
    // On timeout, the job group is cancelled (which interrupts the Spark
    // task via `interruptOnCancel = true`) and a QueryTimeout is thrown.
    // Per mcp-contract.md §7, the agent must add `limit`/`where` or raise
    // MCP_QUERY_TIMEOUT_MS.
    val groupId = s"mcp-query-${request.model}-${System.nanoTime()}"
    val (df, collected) = Query.withTimeout(spark, timeoutMs, groupId, s"MCP query: ${request.model}") {
      val frame = st.toDataFrame(spark)
      (frame, frame.collect())
    }
    val elapsed = System.currentTimeMillis() - t0

    // RESULT_TOO_LARGE: if the request omitted `limit` and the result
    // exceeds the safety cap, reject it. Per mcp-contract.md §7, this is
    // a fast rejection — the agent must add `limit` or narrow filters.
    if (collected.length > maxRows && request.limit.isEmpty)
      throw QueryErrors.ResultTooLarge(collected.length, maxRows)

    val columns = df.schema.fields.toList.map(f => Query.ColumnInfo(f.name, Query.typeName(f.dataType)))
    val rows    = collected.toList.map(Query.rowToSeq)

    val data = Query.Data(
      columns    = columns,
      rows       = rows,
      row_count  = rows.length,
      truncated  = false,
    )

    Envelope.ok(
      data,
      warnings = Handlers.lifecycleWarnings(request.model, t.status),
      meta = io.semanticdf.mcp.Meta(elapsed_ms = elapsed, model = Some(request.model)),
    )
  }

  /** Handle `explain`: same request shape, but no execution. The library
    * emits the full semantic plan via `explainSemantic(spark)`. */
  def explain(registry: Models, request: QueryRequest): Envelope[String] = {
    val raw = registry(request.model)
    val t = auditSink.fold(raw)(s => raw.withAuditSink(s))
    val st =
      t.query(
        measures   = request.measures,
        dimensions = request.dimensions.getOrElse(Nil),
        where      = Query.mergedWhere(request),
        having     = Query.mergedHaving(request),
        orderBy    = request.order_by.map(Query.toSortKey),
        limit      = request.limit,
        timeGrain  = request.time_grain,
        timeGrains = request.time_grains.map(_.toMap).getOrElse(Map.empty),
        timeRange  = request.time_range,
      )
    val planText = st.explainSemantic(spark)
    Envelope.ok(
      planText,
      warnings = Handlers.lifecycleWarnings(request.model, t.status),
      meta = io.semanticdf.mcp.Meta(model = Some(request.model)),
    )
  }

  /** Route a query through the engine registry (per design §6.4).
    * Called from `handle()` when
    * `request.engine.nonEmpty && engineRegistry.isDefined`.
    *
    * The legacy `Models` registry carries `SemanticTable`s
    * (spark-side, per the spark adapter). The engine registry
    * expects `Model` (engine-portable, per `core.model.Model`).
    * For v1, we build a synthetic `Model.of(name, ...)` with just
    * the model name (the engine provider uses this for routing only;
    * per-attribute lookup still goes through the legacy `Models`
    * registry). The semantic `EngineContext` is the canonical shape
    * per design §4.5.4. */
  private def handleViaRegistry(
      legacy:  Models,
      request: io.semanticdf.mcp.handlers.QueryRequest,
  ): Either[io.semanticdf.core.engine.EngineError, io.semanticdf.core.engine.PortableQueryResult] = {
    val engineReg = engineRegistry.get  // safe: gated by handle() caller
    // v0.3.1 Phase C2: convert the wire-DTO `where` / `ast_where`
    // to engine-portable typed filters. We reuse the legacy
    // mergePredicates (Option[Predicate]) then convert via
    // PredicateToExprConverter (legacy Predicate -> core Expr).
    // On conversion failure (unsupported predicate shape), we
    // surface as a typed EngineError.UnsupportedCapability per
    // the standard.
    //
    // Per scala-error-handling §1: Either[L, X] at the IO boundary,
    // not throw. Per scala-impact-analysis: the field name matches
    // MCPQueryRequest.filters (the typed engine-portable filter spec).
    val requestFilters: List[io.semanticdf.core.model.FilterSpec] =
      Query.mergedWhere(request) match {
        case None => Nil
        case Some(legacyPred) =>
          io.semanticdf.predicate.PredicateToExprConverter
            .toExpr(legacyPred)
            .map { expr =>
              List(io.semanticdf.core.model.FilterSpec(
                name      = "where",
                predicate = expr))
            }
            .fold(
              err => return Left(err),
              fs => fs)
      }

    val mcpReq = io.semanticdf.core.engine.MCPQueryRequest(
      model      = request.model,
      dimensions = request.dimensions.getOrElse(Nil),
      measures   = request.measures,
      limit      = request.limit.map(_.toLong),
      timeGrain  = request.time_grain,
      filters    = requestFilters,
    )
    // Default to the registry's default engine if request.engine
    // is empty. Per the user audit: this is the "engine-default
    // flip" — makes the new path the default.
    val engineName = if (request.engine.nonEmpty) request.engine else engineReg.default
    val provider = engineReg.select(engineName) match {
      case Right(p) => p
      case Left(err) => return Left(err)
    }
    // Build a REAL core.Model from the legacy SemanticTable via
    // ModelBridge (PR #412). Falls back to a minimal model if the
    // table is missing or the conversion fails (e.g. legacy
    // predicate has no portable counterpart yet).
    // Models.apply throws ModelNotFound if missing; we use the
    // raw `registry` map for the Option-style lookup.
    val model: io.semanticdf.core.model.Model = legacy.registry.get(request.model) match {
      case Some(st) =>
        io.semanticdf.ModelBridge.toModel(st) match {
          case Right(m) => m
          case Left(_)  => minimalModel(request.model)
        }
      case None => minimalModel(request.model)
    }
    provider.query(model, mcpReq, io.semanticdf.core.engine.EngineContext.defaultContext)
  }

  /** Build a minimal `core.Model` as a fallback for handleViaRegistry.
    * Same as the `handle()` fallback's semantics.
    *
    * Per the standard's "Internal helper rule": ONE call site, caller
    * does `match` on the result immediately → plain function
    * returning `Model` (NOT `Either[L, Model]`).
    *
    * The `Model.of(...).fold(...)` pattern is a throw-across-Either
    * pattern, which the standard says is deprecated. We use
    * `IllegalArgumentException` (not `RuntimeException`) because the
    * failure here is a PROGRAMMER ERROR (we constructed a Model
    * with hardcoded empty fields; if `Model.of` rejects it, our
    * code is broken, not the user's data). Per the standard's
    * "Programmer error" rule: throw `IllegalArgumentException` at
    * boundary, NOT `RuntimeException` / `Either`. */
  private def minimalModel(name: String): io.semanticdf.core.model.Model = {
    io.semanticdf.core.model.Model.of(
      name            = name,
      source          = io.semanticdf.core.model.SourceRef.ByName(
        catalog = None, namespace = None, table = name,
      ),
      dimensions         = Nil,
      measures           = Nil,
      calculatedMeasures = Nil,
      joins              = Nil,
      defaultPolicies    = io.semanticdf.core.model.ModelPolicyDefaults.none,
      status             = io.semanticdf.core.model.ModelStatus.Draft,
    ).fold(
      err => throw new IllegalArgumentException(
        s"semanticdf-mcp: minimal model for '$name' is invalid by construction: $err"
      ),
      identity,
    )
  }

  /** Translate a `PortableQueryResult` (the engine-portable shape,
    * from PR #400) to the legacy `Query.Data` shape (the MCP
    * envelope's existing column + row + row_count structure).
    *
    * Translation: the `ResultSchema.fields: List[Field]` becomes
    * `Query.Data.columns: List[ColumnInfo]`, and the rows are
    * converted to `List[Any]`. For v1 we don't preserve the
    * typed `ResultValue` shape — the MCP envelope is engine-
    * portable but type-erased. A future PR can carry the typed
    * shape through. */
  private def portableToData(
      pqr: io.semanticdf.core.engine.PortableQueryResult,
  ): io.semanticdf.mcp.handlers.Query.Data = {
    val columns = pqr.schema.fields.toList.map { f =>
      Query.ColumnInfo(name = f.name, `type` = f.dataType.toString)
    }
    val rows: List[List[Any]] = pqr.rows.toList.map { row =>
      row.values.toList.map {
        case io.semanticdf.core.engine.ResultValue.NullV              => null
        case io.semanticdf.core.engine.ResultValue.BoolV(b)           => b
        case io.semanticdf.core.engine.ResultValue.IntV(n)            => n
        case io.semanticdf.core.engine.ResultValue.DoubleV(d)         => d
        case io.semanticdf.core.engine.ResultValue.DecimalV(bd)       => bd
        case io.semanticdf.core.engine.ResultValue.StringV(s)        => s
        case io.semanticdf.core.engine.ResultValue.TimestampV(instant) => instant
        case io.semanticdf.core.engine.ResultValue.DateV(date)       => date
      }
    }
    Query.Data(
      columns    = columns,
      rows       = rows,
      row_count  = pqr.rowCount,
      truncated  = false,
    )
  }
}

/** Companion: DTOs and helpers shared by both `Query` handlers. */
object Query {

  private[handlers] val log = org.slf4j.LoggerFactory.getLogger(classOf[Query])

  /** Default row-count safety cap for `query` results. Overridden by the
    * `MCP_MAX_ROWS` env var (parsed as a positive integer). Per
    * `mcp-contract.md`, queries that omit `limit` and exceed this cap are
    * rejected with `RESULT_TOO_LARGE`. */
  private val DefaultMaxRows = 10000

  /** Read `MCP_MAX_ROWS` from the environment. Values that are missing,
    * non-numeric, or <= 0 fall back to [[DefaultMaxRows]]. */
  def maxRowsFromEnv(): Int =
    sys.env.get("MCP_MAX_ROWS")
      .flatMap(v => scala.util.Try(v.toInt).toOption)
      .filter(_ > 0)
      .getOrElse(DefaultMaxRows)

  /** Default query-execution deadline for `query` (millis). Overridden by
    * the `MCP_QUERY_TIMEOUT_MS` env var (parsed as a positive integer).
    * Values <= 0 disable the timeout (no deadline enforced). */
  private val DefaultTimeoutMs = 30000L

  /** Read `MCP_QUERY_TIMEOUT_MS` from the environment. Values that are
    * missing or non-numeric fall back to [[DefaultTimeoutMs]]. A value
    * of 0 or negative is passed through (it disables the timeout). */
  def timeoutMsFromEnv(): Long =
    sys.env.get("MCP_QUERY_TIMEOUT_MS")
      .flatMap(v => scala.util.Try(v.toLong).toOption)
      .filter(_ >= 0)
      .getOrElse(DefaultTimeoutMs)

  /** Merge the two predicate sources into one [[io.semanticdf.predicate.Predicate]]
    * for the library's `where` parameter. The structured `ast_where`
    * field (if present) takes precedence over the flat `where` array; if
    * both are present, they are AND-combined (and a `nil` AST plus an
    * empty array → `None`). Exposed `private[handlers]` so the
    * [[QuerySpec]] can verify the merge logic without a full request
    * round-trip.
    *
    * Shape precedence:
    *   1. ast_where present  -> it's the primary predicate.
    *   2. where  array present -> AND-combined into the AST (or alone
    *                              if the AST is absent).
    *   3. Neither present    -> None.
    *
    * Symmetric for `having`. */
  private[handlers] def mergedWhere(req: QueryRequest): Option[io.semanticdf.predicate.Predicate] =
    Query.mergePredicates(
      ast  = req.ast_where,
      flat = req.where,
    )

  private[handlers] def mergedHaving(req: QueryRequest): Option[io.semanticdf.predicate.Predicate] =
    Query.mergePredicates(
      ast  = req.ast_having,
      flat = req.having,
    )

  private def mergePredicates(
      ast:  Option[Any],
      flat: Option[Seq[Any]],
  ): Option[io.semanticdf.predicate.Predicate] = {
    // Phase 1 consolidation: BOTH the AST and flat paths now produce
    // engine-portable core predicates directly. The merge step (when
    // both are present) AND-combines them at the core level. The final
    // convert-back to the Spark-bearing original happens at the very
    // end — once per query, not once per AST node.
    val astPred  = ast.map(AstPredicates.parseCore)
    val flatPred = flat.flatMap(JsonPredicates.parseAllCore)
    (astPred, flatPred) match {
      case (None,    None)    => None
      case (Some(a), None)    => Some(io.semanticdf.predicate.PredicateConverter.fromCore(a))
      case (None,    Some(f)) => Some(io.semanticdf.predicate.PredicateConverter.fromCore(f))
      case (Some(a), Some(f)) => Some(io.semanticdf.predicate.PredicateConverter.fromCore(
        io.semanticdf.core.predicate.Predicate.And(a, f),
      ))
    }
  }

  /** Detect AMBIGUOUS_DIMENSION / AMBIGUOUS_MEASURE before the library
    * silently resolves a name. For each requested dimension/measure name,
    * check whether it matches more than one field in the merged model's
    * dimensions or measures (case-insensitive comparison, matching
    * Spark's column resolution). The first match wins if there's exactly
    * one; if there are multiple, throw with the candidates list.
    *
    * Why MCP-layer (not library): the library's resolution is
    * case-sensitive exact-match. Adding case-insensitive resolution to
    * the library would change behavior for direct (non-MCP) callers.
    * The MCP layer is the primary consumer and the right place for a
    * safety check. */
  private[handlers] def checkAmbiguity(request: QueryRequest, t: SemanticTable): Unit = {
    val allNames = request.measures ++ request.dimensions.getOrElse(Nil)
    allNames.foreach { name =>
      val lc = name.toLowerCase
      val dimMatches    = t.dimensions.keys.filter(_.toLowerCase == lc).toSeq
      val measureMatches = t.measures.keys.filter(_.toLowerCase == lc).toSeq
      val totalMatches  = (dimMatches ++ measureMatches).distinct
      if (totalMatches.size > 1) {
        // If all matches are in measures, throw AMBIGUOUS_MEASURE;
        // otherwise (any dimension match), throw AMBIGUOUS_DIMENSION
        // — dimensions are queried first in the library's resolveDim.
        if (measureMatches.nonEmpty && dimMatches.isEmpty)
          throw QueryErrors.AmbiguousMeasure(name, totalMatches)
        else
          throw QueryErrors.AmbiguousDimension(name, totalMatches)
      }
    }
  }

  /** Run `body` under a Spark job group with a deadline. On timeout the
    * job group is cancelled (which interrupts the Spark task via
    * `interruptOnCancel = true`) and a [[QueryErrors.QueryTimeout]] is
    * thrown. A `timeoutMs <= 0` disables the deadline (waits forever).
    *
    * The job group ensures cancellation is scoped to this query only —
    * other Spark operations on the same SparkContext are unaffected.
    *
    * Exposed as `private[handlers]` so the [[QuerySpec]] test can verify
    * timeout behavior deterministically (via Thread.sleep) without
    * depending on Spark query wall-clock timing. */
  private[handlers] def withTimeout[T](
      spark: SparkSession,
      timeoutMs: Long,
      groupId: String,
      description: String,
  )(body: => T): T = {
    // Spark 3.5+ deprecates `setJobGroup / cancelJobGroup / clearJobGroup`
    // in favor of the tag-based API. The tag API is also the one that
    // works under Spark Connect (job groups are client-side; tags
    // propagate to the server). See SPARK-37928.
    val sc = spark.sparkContext
    sc.addJobTag(groupId)
    sc.setJobDescription(description)
    try {
      val future = Future { body }(ExecutionContext.global)
      try {
        val deadline = if (timeoutMs <= 0) Duration.Inf else timeoutMs.millis
        Await.result(future, deadline)
      } catch {
        case _: TimeoutException =>
          sc.cancelJobsWithTag(groupId)
          throw QueryErrors.QueryTimeout(timeoutMs)
      }
    } finally {
      // Pop our tag from the thread-local tag stack so the next request
      // on this thread starts clean. We can't `clearJobTags()` because
      // that wipes ALL tags (including the Spark Connect server's
      // session tag), which would break concurrent requests.
      sc.removeJobTag(groupId)
    }
  }

  /** Result data shape. Mirrors the contract. */
  final case class Data(
      columns: List[ColumnInfo],
      rows: List[List[Any]],
      row_count: Int,
      truncated: Boolean = false,
  )

  final case class ColumnInfo(name: String, `type`: String)

  /** `order_by: [{field, direction}]` → `SortKey` list. Direction defaults
    * to `"asc"` (mirrors the library's bare-string shorthand). */
  def toSortKey(ob: OrderBy): SortKey = ob.direction match {
    case "desc" => SortKey.desc(ob.field)
    case _      => SortKey.asc(ob.field)
  }

  /** A Spark `Row` to a JSON-friendly `List[Any]`. */
  def rowToSeq(r: SparkRow): List[Any] = {
    val n   = r.length
    val arr = new Array[Any](n)
    var i = 0
    while (i < n) {
      arr(i) = toJsonValue(r.get(i))
      i += 1
    }
    arr.toList
  }

  /** Encode a Spark cell into a JSON-friendly value. Timestamps → ISO-8601;
    * everything else passes through. */
  def toJsonValue(v: Any): Any = v match {
    case null                       => null
    case ts: java.sql.Timestamp     => ts.toInstant.toString
    case ts: java.time.Instant       => ts.toString
    case other                      => other
  }

  /** Map a Spark `DataType` to a stable string. DecimalType is an object
    * in Spark 3.5+ (not a case class), so it can't be pattern-extracted; the
    * fallback `dt.typeName` already returns `decimal(p,s)`. */
  def typeName(dt: DataType): String = dt match {
    case _: StringType     => "string"
    case _: LongType       => "long"
    case _: IntegerType    => "int"
    case _: DoubleType     => "double"
    case _: FloatType      => "float"
    case _: BooleanType    => "boolean"
    case _: DateType       => "date"
    case _: TimestampType  => "timestamp"
    case other             => other.typeName
  }

  // ---------------------------------------------------------------------------
  // SDK adapter for the `query` tool
  // ---------------------------------------------------------------------------

  /** JSON Schema for the `query` / `explain` tool input. The SDK validates
    * the agent's request against this shape; extra properties are allowed
    * (we ignore what we don't need).
    *
    * The McpSchema record's `properties` value is a `Map[String, Object]`
    * that the SDK only supports `{"type": "<scalar>"}` shapes — no
    * per-property `description` or `items` are surfaced to the LLM
    * agent. The full field semantics are documented here so the
    * server can answer schema-aware MCP clients that ask for
    * descriptions via the `tools/list` endpoint's `description`
    * field (handled at the tool level, not at the property level):
    *
    *   model        : required. The model name to query.
    *   measures     : required. Array of measure names.
    *   dimensions   : optional. Array of dimension names.
    *   where        : optional. Array of flat predicates (legacy).
    *   having       : optional. Array of flat predicates (legacy).
    *   ast_where    : optional. Structured predicate (preferred).
    *   ast_having   : optional. Structured predicate (preferred).
    *   order_by     : optional. Array of {field, direction} objects.
    *   limit        : optional. Top-N cap (integer).
    *   time_grain   : optional. Single time grain applied to all
    *                  time dimensions (string: "day" / "month" / etc.).
    *   time_grains  : optional. Per-dimension time grains. Array
    *                  of [dimension, grain] PAIRS. Malformed pairs
    *                  (length != 2) are dropped silently. Duplicate
    *                  keys collapse to the last value (Map
    *                  semantics in the library).
    *   time_range   : optional. Half-open [start, end] string pair
    *                  (array of 2 strings). */
  val queryToolSchema: io.modelcontextprotocol.spec.McpSchema.JsonSchema = {
    val props = new java.util.LinkedHashMap[String, Object]()
    def strProp(t: String) = java.util.Map.of[String, Object]("type", t): java.util.Map[String, Object]
    props.put("model",      strProp("string"))
    props.put("measures",   strProp("array"))
    props.put("dimensions", strProp("array"))
    props.put("where",      strProp("array"))
    props.put("having",     strProp("array"))
    props.put("ast_where",  strProp("object"))
    props.put("ast_having", strProp("object"))
    props.put("order_by",   strProp("array"))
    props.put("limit",      strProp("integer"))
    props.put("time_grain", strProp("string"))
    props.put("time_grains", strProp("array"))
    props.put("time_range", strProp("array"))
    // The 12th property (the design's "13th property" when counting
    // from 1). Empty = legacy path; non-empty = route through
    // MCPEngineRegistry.
    props.put("engine",     strProp("string"))
    new io.modelcontextprotocol.spec.McpSchema.JsonSchema(
      "object",
      props,
      JList.of("model", "measures"),
      java.lang.Boolean.TRUE,
      java.util.Map.of(),
      java.util.Map.of(),
    )
  }

  /** Register the `query` tool with the MCP server. */
  def registerQuerySpec(
      models: Models,
      handler: Query,
      mapper: McpJsonMapper,
  ): SyncToolSpecification = {
    val tool = new Tool.Builder()
      .name("query")
      .description("Run a query and return the resulting rows.")
      .inputSchema(queryToolSchema)
      .build()

    new SyncToolSpecification(
      tool,
      (_exchange: io.modelcontextprotocol.server.McpSyncServerExchange, args: java.util.Map[String, Object]) => {
        runWithError(models, mapper, () => {
          val req = Query.parseRequest(args)
          handler.handle(models, req)
        })
      },
    )
  }

  /** Register the `explain` tool — same request shape, no execution. */
  def registerExplainSpec(
      models: Models,
      handler: Query,
      mapper: McpJsonMapper,
  ): SyncToolSpecification = {
    val tool = new Tool.Builder()
      .name("explain")
      .description("Return the semantic plan (op tree + filter routing + transitive deps) for a query, without executing it.")
      .inputSchema(queryToolSchema)
      .build()

    new SyncToolSpecification(
      tool,
      (_exchange: io.modelcontextprotocol.server.McpSyncServerExchange, args: java.util.Map[String, Object]) => {
        runWithError(models, mapper, () => {
          val req = Query.parseRequest(args)
          handler.explain(models, req)
        })
      },
    )
  }

  /** Run `f` and return either the success envelope or an error envelope
    * (formatted into a JSON string in [[Handlers.textResult]]). Catches
    * all known domain exceptions and maps them to the MCP error-code list. */
  private[handlers] def runWithError[T](
      models: Models,
      mapper: McpJsonMapper,
      f: () => Envelope[T],
  ): CallToolResult = {
    import scala.util.{Try, Success, Failure}
    val reqModel: Option[String] = None   // reserved for a future "model" extraction helper
    try {
      val env = f()
      Handlers.textResult(env, mapper)
    } catch {
      case e: io.semanticdf.mcp.ModelNotFound =>
        val hint = io.semanticdf.closestMatch(
          /* need the model name — extract it lazily from the request */ reqModel.getOrElse(""),
          models.all.map(_._1),
        ).map(c => s"Did you mean '$c'?")
        Handlers.textResult(
          io.semanticdf.mcp.ErrorEnvelope.of("MODEL_NOT_FOUND", e.getMessage, hint = hint),
          mapper,
        )
      case e: JsonPredicates.InvalidPredicate =>
        Handlers.textResult(
          io.semanticdf.mcp.ErrorEnvelope.of("INVALID_PREDICATE", stripPrefix(e.getMessage, "INVALID_PREDICATE: ")),
          mapper,
        )
      case e: JsonPredicates.UnsupportedOp =>
        val msg = stripPrefix(e.getMessage, "UNSUPPORTED_OP: ")
        Handlers.textResult(
          io.semanticdf.mcp.ErrorEnvelope.of(
            "UNSUPPORTED_OP",
            s"'$msg'",
            hint = Some("allowed: eq, ne, lt, le, gt, ge, in, not_in, is_null, is_not_null, and, or, not"),
          ),
          mapper,
        )
      case e: QueryErrors.ResultTooLarge =>
        Handlers.textResult(
          io.semanticdf.mcp.ErrorEnvelope(
            status = "error",
            error = io.semanticdf.mcp.ErrorDetail(
              code = "RESULT_TOO_LARGE",
              message = s"Query returned ${e.rowCount} rows; safety cap is ${e.limit}. Add a \"limit\" to your request or narrow your filters.",
              hint = Some(s"Add \"limit\": ${e.limit} to your request, or narrow your filters with \"where\"."),
              details = Map("suggested_limit" -> e.limit.toString),
            ),
          ),
          mapper,
        )
      case e: QueryErrors.QueryTimeout =>
        Handlers.textResult(
          io.semanticdf.mcp.ErrorEnvelope(
            status = "error",
            error = io.semanticdf.mcp.ErrorDetail(
              code = "QUERY_TIMEOUT",
              message = e.getMessage,
              hint = Some("Add a narrower \"where\" or \"limit\" clause, or raise MCP_QUERY_TIMEOUT_MS."),
              details = Map("timeout_ms" -> e.timeoutMs.toString),
            ),
          ),
          mapper,
        )
      case e: QueryErrors.AmbiguousDimension =>
        Handlers.textResult(
          io.semanticdf.mcp.ErrorEnvelope(
            status = "error",
            error = io.semanticdf.mcp.ErrorDetail(
              code = "AMBIGUOUS_DIMENSION",
              message = e.getMessage,
              hint = Some(s"Disambiguate with one of: ${e.candidates.mkString(", ")}"),
              details = Map("candidates" -> e.candidates.mkString(",")),
            ),
          ),
          mapper,
        )
      case e: QueryErrors.AmbiguousMeasure =>
        Handlers.textResult(
          io.semanticdf.mcp.ErrorEnvelope(
            status = "error",
            error = io.semanticdf.mcp.ErrorDetail(
              code = "AMBIGUOUS_MEASURE",
              message = e.getMessage,
              hint = Some(s"Disambiguate with one of: ${e.candidates.mkString(", ")}"),
              details = Map("candidates" -> e.candidates.mkString(",")),
            ),
          ),
          mapper,
        )
      case e: IllegalArgumentException =>
        Handlers.textResult(
          io.semanticdf.mcp.ErrorEnvelope.of("EXECUTION_ERROR", e.getMessage),
          mapper,
        )
    }
  }

  private def stripPrefix(s: String, prefix: String): String =
    if (s.startsWith(prefix)) s.stripPrefix(prefix) else s

  /** Parse the `Map[String, Object]` from the MCP SDK into a typed
    * [[QueryRequest]]. Most fields are optional; only `model` and `measures`
    * are required (SDK validates those via the schema). */
  def parseRequest(args: java.util.Map[String, Object]): QueryRequest = {
    import scala.jdk.CollectionConverters._
    val map = args.asScala.toMap.asInstanceOf[Map[String, Any]]

    def asStr(name: String): String = map.get(name) match {
      case Some(s: String) => s
      case _               => throw new IllegalArgumentException(s"`$name` is required (string)")
    }
    def asSeq(name: String): Seq[Any] = map.get(name) match {
      case None                  => Seq.empty
      case Some(s: Seq[_])       => s
      case Some(jl: java.util.List[_]) => jl.asScala.toSeq
      case Some(other)          => throw new IllegalArgumentException(s"`$name` must be an array, got ${other.getClass.getSimpleName}")
    }
    def asOpt[T](name: String): Option[T] = map.get(name).collect { case v: T => v }

    QueryRequest(
      model      = asStr("model"),
      measures   = asSeq("measures").map { case s: String => s },
      dimensions = Some(asSeq("dimensions").map { case s: String => s }),
      where      = Some(asSeq("where")),
      having     = Some(asSeq("having")),
      ast_where  = map.get("ast_where"),
      ast_having = map.get("ast_having"),
      order_by   = asSeq("order_by").map(OrderByParser.parse),
      limit      = asOpt[java.lang.Integer]("limit").map(_.intValue),
      time_grain = asOpt[String]("time_grain"),
      time_grains = map.get("time_grains") match {
        case Some(arr: Seq[_]) =>
          // Each entry is `[dimension, grain]`. Skip malformed pairs.
          Some(arr.collect { case pair: Seq[_] if pair.length >= 2 =>
            (pair.head.toString, pair(1).toString)
          case pair: java.util.List[_] if pair.size() >= 2 =>
            (pair.get(0).toString, pair.get(1).toString)
          })
        case Some(jl: java.util.List[_]) =>
          Some(jl.asScala.toSeq.collect { case pair: java.util.List[_] if pair.size() >= 2 =>
            (pair.get(0).toString, pair.get(1).toString)
          })
        case _ => None
      },
      time_range = map.get("time_range") match {
        case Some(arr: Seq[_]) if arr.length >= 2 =>
          Some((arr.head.asInstanceOf[String], arr(1).asInstanceOf[String]))
        case Some(jl: java.util.List[_]) if jl.size() >= 2 =>
          Some((jl.get(0).asInstanceOf[String], jl.get(1).asInstanceOf[String]))
        case _ => None
      },
      engine = asOpt[String]("engine").getOrElse(""),
    )
  }
}

/** Top-level request DTO. Parsed from the MCP arguments map by the SDK
  * adapter (registered via `Query.registerSpec`).
  *
  * ==The `engine` field (12th queryToolSchema property)==
  *
  * When `engine.nonEmpty`, the Query handler routes through the
  * `MCPEngineRegistry` (per design §6.4). When `engine.isEmpty`,
  * the handler falls back to the legacy `Models` + `SemanticTable`
  * path (backward-compat with PRs #1-#401).
  *
  * Per design §6.4 + the v0.3.0 design review's "MCP engine
  * registry" finding: the engine field is the 12th property in
  * `queryToolSchema` (the design's "13th property" is the
  * `engine` field added in this PR — counting from 1). */
final case class QueryRequest(
    model: String,
    measures: Seq[String],
    dimensions: Option[Seq[String]] = None,
    where: Option[Seq[Any]] = None,
    having: Option[Seq[Any]] = None,
    ast_where: Option[Any] = None,
    ast_having: Option[Any] = None,
    order_by: Seq[OrderBy] = Seq.empty,
    limit: Option[Int] = None,
    time_grain: Option[String] = None,
    /** Per-dimension time-grain map. MCP accepts an array of
      * `[dimension, grain]` pairs and converts to the
      * `Map[String, String]` the library expects. */
    time_grains: Option[Seq[(String, String)]] = None,
    time_range: Option[(String, String)] = None,
    /** Engine to route the query through (per design §6.4). Empty
      * means "use the legacy `Models` + `SemanticTable` path"
      * (backward-compat with pre-PR-5b callers). Non-empty routes
      * through the `MCPEngineRegistry` (this PR's path). */
    engine: String = "",
)

/** One `order_by` entry. Direction defaults to `asc`. */
final case class OrderBy(field: String, direction: String)

/** Parser for one `order_by` JSON entry. Lives outside `Query` so the SDK
  * adapter can reach it. */
object OrderByParser {
  def parse(json: Any): OrderBy = {
    // Accept BOTH java.util.Map (legacy SDK adapter callers) AND Scala Map
    // (Jackson-with-DefaultScalaModule callers — the REST transport, after
    // PR #54). Before #54, nested JSON objects over REST deserialised as
    // java.util.LinkedHashMap and matched the original branch; after #54
    // they arrive as Scala Map2 (the Scala module's default for untyped
    // nested objects). See the regression tests in `QuerySpec` for the
    // exact JSON path that triggered this.
    val map: Map[String, Any] = json match {
      case m: java.util.Map[_, _] => m.asScala.toMap.asInstanceOf[Map[String, Any]]
      case m: Map[_, _]          => m.asInstanceOf[Map[String, Any]]
      case other =>
        throw new IllegalArgumentException(s"order_by entry must be a JSON object, got ${other.getClass.getSimpleName}")
    }
    val field = map.get("field") match {
      case Some(s: String) => s
      case _ => throw new IllegalArgumentException("order_by[].field is required (string)")
    }
    val direction = map.get("direction") match {
      case Some("desc") => "desc"
      case _            => "asc"
    }
    OrderBy(field, direction)
  }
}

/** Error types raised during query construction. The SDK adapter catches
  * these by type and serialises them as MCP error envelopes. Each name maps
  * directly to the closed error-code list in `mcp-contract.md` v2. */
object QueryErrors {
  final case class UnknownField(name: String, available: String)
      extends RuntimeException(s"UNKNOWN_FIELD: '$name'. Available: $available")

  final case class AmbiguousMeasure(name: String, candidates: Seq[String])
      extends RuntimeException(s"AMBIGUOUS_MEASURE: '$name' matches multiple measures: ${candidates.mkString(", ")}")

  final case class AmbiguousDimension(name: String, candidates: Seq[String])
      extends RuntimeException(s"AMBIGUOUS_DIMENSION: '$name' matches multiple dimensions: ${candidates.mkString(", ")}")

  final case class ResultTooLarge(rowCount: Int, limit: Int)
      extends RuntimeException(
        s"RESULT_TOO_LARGE: query returned $rowCount rows; safety cap is $limit. " +
        s"Add a \"limit\" to your request or narrow your filters.")

  final case class QueryTimeout(timeoutMs: Long)
      extends RuntimeException(
        s"QUERY_TIMEOUT: query exceeded ${timeoutMs}ms deadline (MCP_QUERY_TIMEOUT_MS).")
}
