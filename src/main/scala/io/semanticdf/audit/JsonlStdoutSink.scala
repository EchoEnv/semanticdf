package io.semanticdf.audit

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.databind.SerializationFeature

import java.time.format.DateTimeFormatter
import java.util.logging.{ConsoleHandler, Level, Logger, SimpleFormatter}

/** JSON Lines writer on stdout. Each event is a single-line JSON object
  * with a fixed key order so the output is greppable.
  *
  * Implementation notes:
  *   - Uses Jackson with the Scala module (Seq => JSON array).
  *   - Timestamps are ISO-8601 with millisecond precision.
  *   - Output goes through a dedicated `java.util.logging.Logger`
  *     (`io.semanticdf.audit.jsonl`) so it can be redirected via
  *     standard logging config without conflicting with the user's
  *     application logs.
  *   - Catches and swallows all exceptions — a sink failure must
  *     never break a query.
  *
  * Exposed only via [[AuditSink.JsonlStdout]]; no public constructor
  * so the logger config stays internal. */
private[audit] final class JsonlStdoutSink extends AuditSink {

  private val mapper: ObjectMapper = new ObjectMapper()
    .registerModule(DefaultScalaModule)
    .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)

  private val log: Logger = {
    val l = Logger.getLogger("io.semanticdf.audit.jsonl")
    l.setUseParentHandlers(false)
    if (l.getHandlers.length == 0) {
      val h = new ConsoleHandler()
      h.setLevel(Level.INFO)
      h.setFormatter(new SimpleFormatter())
      l.addHandler(h)
      l.setLevel(Level.INFO)
    }
    l
  }

  private val tsFormatter: DateTimeFormatter =
    DateTimeFormatter.ISO_INSTANT

  def emit(event: AuditEvent): Unit = {
    try {
      val payload = scala.collection.mutable.LinkedHashMap[String, Any](
        "ts"         -> tsFormatter.format(event.ts),
        "model"      -> event.model,
        "measures"   -> event.measures.toList,
        "dimensions" -> event.dimensions.toList,
        "where_hash"   -> event.whereHash.orNull,
        "having_hash"  -> event.havingHash.orNull,
        "row_count"  -> event.rowCount,
        "elapsed_ms" -> event.elapsedMs,
        "status"     -> event.status,
      )
      event.error.foreach(payload.put("error", _))
      event.requester.foreach(payload.put("requester", _))
      event.requestId.foreach(payload.put("request_id", _))
      log.info(mapper.writeValueAsString(payload))
    } catch {
      // Sink must never break the query.
      case _: Throwable => ()
    }
  }
}
