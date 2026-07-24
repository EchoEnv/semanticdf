package io.semanticdf.mcp.handlers

import io.semanticdf.audit.{AuditEvent, AuditSink}
import io.semanticdf.mcp.SparkFixture
import io.semanticdf.mcp.handlers.AuditLog


import java.time.Instant
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Tests for the `audit_log` MCP handler.
  *
  * The handler is read-only; the unit tests below verify the JSON
  * shape, the limit / truncation behavior, and the wiring (events
  * emitted into the shared sink by the Query handler are visible
  * through the AuditLog handler).
  *
  * End-to-end coverage (Query -> AuditLog) lives in the integration
  * spec; the unit tests here isolate the handler. */
class AuditLogSpec extends AnyFunSuite with SparkFixture {

  /** Build an in-memory sink pre-loaded with N synthetic events. */
  private def sinkWith(n: Int): AuditSink = {
    val s = AuditSink.inMemory(maxEvents = n + 10)
    val now = Instant.now()
    for (i <- 0 until n) {
      s.emit(AuditEvent(
        ts = now.plusMillis(i.toLong),
        model = s"m$i",
        measures = Seq(s"meas_$i"),
        dimensions = Seq(s"dim_$i"),
        whereHash = Some(f"hash_$i%016x"),
        havingHash = None,
        rowCount = i.toLong,
        elapsedMs = i.toLong,
        status = "ok",
      ))
    }
    s
  }

  test("handle: empty sink returns an empty list") {
    val h = new AuditLog(AuditSink.inMemory())
    val env = h.handle(AuditLogRequest())
    assert(env.status == "ok")
    val data = env.data.asInstanceOf[AuditLog.Data]
    assert(data.events.isEmpty)
    assert(data.count == 0)
    assert(data.total == 0)
    assert(data.truncated == false)
  }

  test("handle: returns all events when count <= limit") {
    val h = new AuditLog(sinkWith(5))
    val env = h.handle(AuditLogRequest(limit = Some(10)))
    val data = env.data.asInstanceOf[AuditLog.Data]
    assert(data.count == 5)
    assert(data.total == 5)
    assert(data.truncated == false)
    assert(data.events.map(_.model) == Seq("m0","m1","m2","m3","m4"))
  }

  test("handle: respects limit and marks truncated") {
    val h = new AuditLog(sinkWith(20))
    val env = h.handle(AuditLogRequest(limit = Some(5)))
    val data = env.data.asInstanceOf[AuditLog.Data]
    // Most recent 5: m15..m19 (oldest first)
    assert(data.count == 5)
    assert(data.total == 20)
    assert(data.truncated == true)
    assert(data.events.map(_.model) == Seq("m15","m16","m17","m18","m19"))
  }

  test("handle: default limit is 100 when no limit passed") {
    val h = new AuditLog(sinkWith(150))
    val env = h.handle(AuditLogRequest())
    val data = env.data.asInstanceOf[AuditLog.Data]
    assert(data.count == 100)
    assert(data.total == 150)
    assert(data.truncated == true)
  }

  test("handle: limit is capped at 1000") {
    val h = new AuditLog(sinkWith(5))
    val env = h.handle(AuditLogRequest(limit = Some(9999)))
    val data = env.data.asInstanceOf[AuditLog.Data]
    // All 5 events fit; the cap doesn't truncate when the buffer is small.
    assert(data.count == 5)
    assert(data.truncated == false)
  }

  test("handle: limit of 0 is clamped to 1") {
    val h = new AuditLog(sinkWith(5))
    val env = h.handle(AuditLogRequest(limit = Some(0)))
    val data = env.data.asInstanceOf[AuditLog.Data]
    assert(data.count == 1)
  }

  test("handle: event JSON shape matches the wire contract") {
    val now = Instant.parse("2026-07-24T10:00:00Z")
    val sink = AuditSink.inMemory()
    sink.emit(AuditEvent(
      ts         = now,
      model      = "flights",
      measures   = Seq("flight_count"),
      dimensions = Seq("carrier"),
      whereHash  = Some("abc123"),
      havingHash = None,
      rowCount   = 3L,
      elapsedMs  = 42L,
      status     = "ok",
    ))
    val h = new AuditLog(sink)
    val data = h.handle(AuditLogRequest()).data.asInstanceOf[AuditLog.Data]
    val e = data.events.head
    assert(e.ts == "2026-07-24T10:00:00Z")
    assert(e.model == "flights")
    assert(e.measures == List("flight_count"))
    assert(e.dimensions == List("carrier"))
    assert(e.where_hash == Some("abc123"))
    assert(e.having_hash == None)
    assert(e.row_count == 3L)
    assert(e.elapsed_ms == 42L)
    assert(e.status == "ok")
    assert(e.error == None)
  }

  test("handle: error event carries the error string") {
    val sink = AuditSink.inMemory()
    sink.emit(AuditEvent(
      ts = Instant.now(), model = "flights",
      measures = Nil, dimensions = Nil,
      whereHash = None, havingHash = None,
      rowCount = 0L, elapsedMs = 5L,
      status = "error",
      error = Some("IllegalArgumentException: nope"),
    ))
    val data = new AuditLog(sink).handle(AuditLogRequest()).data.asInstanceOf[AuditLog.Data]
    val e = data.events.head
    assert(e.status == "error")
    assert(e.error == Some("IllegalArgumentException: nope"))
  }

  test("parseRequest: limit is parsed from the args map") {
    val args = new java.util.HashMap[String, Object]()
    args.put("limit", java.lang.Integer.valueOf(50))
    val req = AuditLog.parseRequest(args)
    assert(req.limit == Some(50))
  }

  test("parseRequest: no limit => None") {
    val args = new java.util.HashMap[String, Object]()
    val req = AuditLog.parseRequest(args)
    assert(req.limit.isEmpty)
  }

  test("parseRequest: unknown fields are ignored") {
    val args = new java.util.HashMap[String, Object]()
    args.put("limit", java.lang.Integer.valueOf(10))
    args.put("weird", "ignored")
    val req = AuditLog.parseRequest(args)
    assert(req.limit == Some(10))
  }

  test("the Query handler emits events visible through audit_log") {
    // This is the integration check: the Query handler is constructed
    // with the same sink that AuditLog reads from. Run a query, then
    // read the audit log.
    import io.semanticdf.{Dimension, Measure, Predicate, toSemanticTable}
    import org.apache.spark.sql.functions.{count, lit}

    val sink = AuditSink.inMemory()
    val flights = toSemanticTable(
      spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(
          ("AA", 1), ("UA", 1), ("DL", 1),
        ))
      ).toDF("carrier", "x"),
      name = Some("flights"))
      .withDimensions(Dimension("carrier", t => t("carrier")))
      .withMeasures(Measure("flight_count", t => count(lit(1))))
    val registry = new io.semanticdf.mcp.Models(Map("flights" -> flights), io.semanticdf.mcp.DataConfig(Map.empty))

    val queryHandler = new Query(spark, auditSink = Some(sink))
    queryHandler.handle(registry, QueryRequest(
      model = "flights",
      measures = Seq("flight_count"),
      dimensions = Some(Seq("carrier")),
      where = Some(Seq.empty),
      having = Some(Seq.empty),
    ))

    val data = new AuditLog(sink).handle(AuditLogRequest()).data.asInstanceOf[AuditLog.Data]
    assert(data.count == 1, s"expected 1 event, got ${data.count}")
    val e = data.events.head
    assert(e.model == "flights")
    assert(e.measures == List("flight_count"))
    assert(e.dimensions == List("carrier"))
    assert(e.status == "ok")
  }
}
