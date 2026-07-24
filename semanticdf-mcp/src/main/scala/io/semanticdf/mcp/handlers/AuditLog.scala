package io.semanticdf.mcp.handlers

import io.modelcontextprotocol.json.McpJsonMapper
import io.modelcontextprotocol.server.McpServerFeatures.SyncToolSpecification
import io.modelcontextprotocol.server.McpSyncServerExchange
import io.modelcontextprotocol.spec.McpSchema.{CallToolResult, Tool}
import io.semanticdf.audit.AuditEvent
import io.semanticdf.mcp.{Envelope, Handlers}

import java.time.Instant
import java.util.{List => JList}
import scala.jdk.CollectionConverters._

/** `audit_log` handler — exposes the audit-event buffer to agents.
  *
  * Per the post-v0.1.16 audit-log design (`docs/design/audit-log.md`):
  * every `query` call emits an `AuditEvent` into a sink. When the MCP
  * server is configured with an in-memory sink, an agent can read the
  * recent event stream back via this tool — useful for self-introspection
  * ("what did I just query?"), diffing across runs, and confirming that
  * a query actually executed.
  *
  * The handler is read-only; it never mutates the buffer.
  *
  * == Request ==
  *
  * {{{
  *   { "limit": 100 }     // optional, default 100, capped at 1000
  * }}}
  *
  * == Response ==
  *
  * {{{
  *   {
  *     "events": [ /* AuditEvent JSON, oldest first */ ],
  *     "count": <int>,            // events in this response
  *     "total": <int>,            // events in the buffer
  *     "truncated": <bool>        // true if `total > count`
  *   }
  * }}}
  *
  * The events list is the most recent `limit` events, oldest first. */
final class AuditLog(sink: io.semanticdf.audit.AuditSink) {

  /** Hard cap on the number of events returned in a single call.
    * Prevents a misbehaving caller from pulling the whole buffer. */
  private val MaxLimit = 1000

  /** Default limit when the caller omits the parameter. */
  private val DefaultLimit = 100

  def handle(request: AuditLogRequest): Envelope[AuditLog.Data] = {
    val limit = request.limit.getOrElse(DefaultLimit).min(MaxLimit).max(1)
    val all   = sink.snapshot()
    val total = all.size
    // Take the most recent `limit` events; order is oldest first to
    // match the buffer's arrival order.
    val windowed =
      if (all.size <= limit) all
      else all.slice(all.size - limit, all.size)
    val data = AuditLog.Data(
      events    = windowed.map(toWire).toList,
      count     = windowed.size,
      total     = total,
      truncated = total > windowed.size,
    )
    Envelope.ok(data)
  }

  private def toWire(e: AuditEvent): AuditLog.Event = AuditLog.Event(
    ts          = e.ts.toString,
    model       = e.model,
    measures    = e.measures.toList,
    dimensions  = e.dimensions.toList,
    where_hash  = e.whereHash,
    having_hash = e.havingHash,
    row_count   = e.rowCount,
    elapsed_ms  = e.elapsedMs,
    status      = e.status,
    error       = e.error,
    requester   = e.requester,
    request_id  = e.requestId,
  )
}

/** Request DTO — parsed from the MCP arguments map. */
final case class AuditLogRequest(
    limit: Option[Int] = None,
)

object AuditLog {

  /** Wire-side response shape. Lives on the companion so tests and
    * other handlers can reference it as `AuditLog.Data` without
    * holding a handler instance. */
  final case class Data(
      events:    List[Event],
      count:     Int,
      total:     Int,
      truncated: Boolean,
  )

  /** Wire-side rendering of an [[io.semanticdf.audit.AuditEvent]]. The
    * library's case class uses `Option[String]` for nullable fields;
    * the wire shape uses absent-vs-present (no `null` in the JSON). */
  final case class Event(
      ts:           String,
      model:        String,
      measures:     List[String],
      dimensions:   List[String],
      where_hash:   Option[String],
      having_hash:  Option[String],
      row_count:    Long,
      elapsed_ms:   Long,
      status:       String,
      error:        Option[String],
      requester:    Option[String],
      request_id:   Option[String],
  )


  /** Wire schema for the `audit_log` tool. `limit` is optional; the
    * handler applies its own default + cap. */
  val schema: io.modelcontextprotocol.spec.McpSchema.JsonSchema = {
    val props = new java.util.LinkedHashMap[String, Object]()
    val limitProp: java.util.Map[String, Object] = {
      val m = new java.util.LinkedHashMap[String, Object]()
      m.put("type", "integer")
      m.put("minimum", java.lang.Integer.valueOf(1))
      m.put("maximum", java.lang.Integer.valueOf(1000))
      m
    }
    props.put("limit", limitProp)
    new io.modelcontextprotocol.spec.McpSchema.JsonSchema(
      "object",
      props,
      JList.of(),
      java.lang.Boolean.TRUE,
      java.util.Map.of(),
      java.util.Map.of(),
    )
  }

  /** Parse the SDK arguments map into a typed request. Unknown fields
    * are ignored (the schema's `additionalProperties: true` is the
    * same policy the other handlers use). */
  def parseRequest(args: java.util.Map[String, Object]): AuditLogRequest = {
    val map = args.asScala.toMap.asInstanceOf[Map[String, Any]]
    AuditLogRequest(
      limit = map.get("limit").collect { case n: java.lang.Number => n.intValue },
    )
  }

  def registerSpec(sink: io.semanticdf.audit.AuditSink, mapper: McpJsonMapper): SyncToolSpecification = {
    val handler = new AuditLog(sink)
    val tool = new Tool.Builder()
      .name("audit_log")
      .description("Return the most recent audit events emitted by this server's query handler. " +
        "Useful for agent self-introspection (\"what did I just query?\"), diffing across runs, " +
        "and confirming a query actually executed.")
      .inputSchema(schema)
      .build()

    new SyncToolSpecification(
      tool,
      (_exchange: McpSyncServerExchange, args: java.util.Map[String, Object]) => {
        try {
          val req = parseRequest(args)
          Handlers.textResult(handler.handle(req), mapper)
        } catch {
          case e: IllegalArgumentException =>
            Handlers.textResult(
              io.semanticdf.mcp.ErrorEnvelope.of("INVALID_REQUEST", e.getMessage),
              mapper,
            )
        }
      },
    )
  }
}
