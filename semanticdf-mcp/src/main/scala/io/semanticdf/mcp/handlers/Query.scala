package io.semanticdf.mcp.handlers

import io.modelcontextprotocol.json.McpJsonMapper
import io.modelcontextprotocol.server.McpServerFeatures.SyncToolSpecification
import io.modelcontextprotocol.spec.McpSchema.{CallToolResult, Tool}
import io.semanticdf.mcp.{Envelope, Handlers, Models, OkfCache}
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
) {

  private val log = Query.log

  /** Handle `query`: run the query, return rows. */
  def handle(registry: Models, request: QueryRequest): Envelope[Query.Data] = {
    val t = registry(request.model)

    // Build the library-level semantic-table chain.
    val st =
      t.query(
        measures   = request.measures,
        dimensions = request.dimensions.getOrElse(Nil),
        where      = request.where.flatMap(JsonPredicates.parseAll),
        having     = request.having.flatMap(JsonPredicates.parseAll),
        orderBy    = request.order_by.map(Query.toSortKey),
        limit      = request.limit,
        timeGrain  = request.time_grain,
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
      warnings = Nil,
      meta = io.semanticdf.mcp.Meta(elapsed_ms = elapsed, model = Some(request.model)),
    )
  }

  /** Handle `explain`: same request shape, but no execution. The library
    * emits the full semantic plan via `explainSemantic(spark)`. */
  def explain(registry: Models, request: QueryRequest): Envelope[String] = {
    val t = registry(request.model)
    val st =
      t.query(
        measures   = request.measures,
        dimensions = request.dimensions.getOrElse(Nil),
        where      = request.where.flatMap(JsonPredicates.parseAll),
        having     = request.having.flatMap(JsonPredicates.parseAll),
        orderBy    = request.order_by.map(Query.toSortKey),
        limit      = request.limit,
        timeGrain  = request.time_grain,
        timeRange  = request.time_range,
      )
    val planText = st.explainSemantic(spark)
    Envelope.ok(planText, meta = io.semanticdf.mcp.Meta(model = Some(request.model)))
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
    spark.sparkContext.setJobGroup(groupId, description, interruptOnCancel = true)
    try {
      val future = Future { body }(ExecutionContext.global)
      try {
        val deadline = if (timeoutMs <= 0) Duration.Inf else timeoutMs.millis
        Await.result(future, deadline)
      } catch {
        case _: TimeoutException =>
          spark.sparkContext.cancelJobGroup(groupId)
          throw QueryErrors.QueryTimeout(timeoutMs)
      }
    } finally {
      spark.sparkContext.clearJobGroup()
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
    * (we ignore what we don't need). */
  val queryToolSchema: io.modelcontextprotocol.spec.McpSchema.JsonSchema =
    new io.modelcontextprotocol.spec.McpSchema.JsonSchema(
      "object",
      java.util.Map.of(
        "model",      java.util.Map.of("type", "string"),
        "measures",   java.util.Map.of("type", "array"),
        "dimensions", java.util.Map.of("type", "array"),
        "where",      java.util.Map.of("type", "array"),
        "having",     java.util.Map.of("type", "array"),
        "order_by",   java.util.Map.of("type", "array"),
        "limit",      java.util.Map.of("type", "integer"),
        "time_grain", java.util.Map.of("type", "string"),
        "time_range", java.util.Map.of("type", "array"),
      ),
      JList.of("model", "measures"),
      java.lang.Boolean.TRUE,
      java.util.Map.of(),
      java.util.Map.of(),
    )

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
      order_by   = asSeq("order_by").map(OrderByParser.parse),
      limit      = asOpt[java.lang.Integer]("limit").map(_.intValue),
      time_grain = asOpt[String]("time_grain"),
      time_range = map.get("time_range") match {
        case Some(arr: Seq[_]) if arr.length >= 2 =>
          Some((arr.head.asInstanceOf[String], arr(1).asInstanceOf[String]))
        case Some(jl: java.util.List[_]) if jl.size() >= 2 =>
          Some((jl.get(0).asInstanceOf[String], jl.get(1).asInstanceOf[String]))
        case _ => None
      },
    )
  }
}

/** Top-level request DTO. Parsed from the MCP arguments map by the SDK
  * adapter (registered via `Query.registerSpec`). */
final case class QueryRequest(
    model: String,
    measures: Seq[String],
    dimensions: Option[Seq[String]] = None,
    where: Option[Seq[Any]] = None,
    having: Option[Seq[Any]] = None,
    order_by: Seq[OrderBy] = Seq.empty,
    limit: Option[Int] = None,
    time_grain: Option[String] = None,
    time_range: Option[(String, String)] = None,
)

/** One `order_by` entry. Direction defaults to `asc`. */
final case class OrderBy(field: String, direction: String)

/** Parser for one `order_by` JSON entry. Lives outside `Query` so the SDK
  * adapter can reach it. */
object OrderByParser {
  def parse(json: Any): OrderBy = json match {
    case m: java.util.Map[_, _] =>
      // `asScala.toMap` loses the key/value types — cast back explicitly so
      // Scala 2 can resolve `Option[String]` below.
      val map: Map[String, Any] = m.asScala.toMap.asInstanceOf[Map[String, Any]]
      val field = map.get("field") match {
        case Some(s: String) => s
        case _ => throw new IllegalArgumentException("order_by[].field is required (string)")
      }
      val direction = map.get("direction") match {
        case Some("desc") => "desc"
        case _            => "asc"
      }
      OrderBy(field, direction)
    case other =>
      throw new IllegalArgumentException(s"order_by entry must be a JSON object, got ${other.getClass.getSimpleName}")
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
