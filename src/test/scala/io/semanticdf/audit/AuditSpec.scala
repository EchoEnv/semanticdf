package io.semanticdf.audit
import io.semanticdf.predicate._

import io.semanticdf.{Dimension, FlightsFixture, Measure, SparkSessionFixture, toSemanticTable}
import org.apache.spark.sql.functions.{count, lit}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Tests for the audit-log machinery.
  *
  * The audit path has four moving parts:
  *   1. `PredicateHasher` — stable canonical form for predicates
  *   2. `InMemoryAuditSink` — test sink that retains recent events
  *   3. `SemanticTable.query()` — captures the request shape
  *   4. `SemanticTable.toDataFrame()` — emits the event
  *
  * These tests cover all four parts. The PredicateHasher is the
  * most subtle — same predicate, two ways (Predicate vs. AST), must
  * hash the same. */
class AuditSpec extends AnyFunSuite with SparkSessionFixture with FlightsFixture {

  private def baseModel: io.semanticdf.SemanticTable =
    toSemanticTable(flightsDf, name = Some("flights"))
      .withDimensions(io.semanticdf.Dimension("carrier", t => t("carrier")))
      .withMeasures(io.semanticdf.Measure("flight_count", t => count(lit(1))))

  // ----------------------------------------------------------------
  // PredicateHasher
  // ----------------------------------------------------------------

  test("hash: same Compare gives the same hash") {
    val a = Predicate.Compare.Eq("carrier", "AA")
    val b = Predicate.Compare.Eq("carrier", "AA")
    assert(PredicateHasher.hash(a) == PredicateHasher.hash(b))
  }

  test("hash: different value gives a different hash") {
    val a = Predicate.Compare.Eq("carrier", "AA")
    val b = Predicate.Compare.Eq("carrier", "UA")
    assert(PredicateHasher.hash(a) != PredicateHasher.hash(b))
  }

  test("hash: different op gives a different hash") {
    val a = Predicate.Compare.Eq("carrier", "AA")
    val b = Predicate.Compare.Ne("carrier", "AA")
    assert(PredicateHasher.hash(a) != PredicateHasher.hash(b))
  }

  test("hash: And is commutative (A and B == B and A)") {
    val a = Predicate.And(
      Predicate.Compare.Eq("carrier", "AA"),
      Predicate.Compare.Gt("distance", 500),
    )
    val b = Predicate.And(
      Predicate.Compare.Gt("distance", 500),
      Predicate.Compare.Eq("carrier", "AA"),
    )
    assert(PredicateHasher.hash(a) == PredicateHasher.hash(b))
  }

  test("hash: Or is commutative") {
    val a = Predicate.Or(
      Predicate.Compare.Eq("carrier", "AA"),
      Predicate.Compare.Eq("carrier", "UA"),
    )
    val b = Predicate.Or(
      Predicate.Compare.Eq("carrier", "UA"),
      Predicate.Compare.Eq("carrier", "AA"),
    )
    assert(PredicateHasher.hash(a) == PredicateHasher.hash(b))
  }

  test("hash: And vs Or give different hashes") {
    val left  = Predicate.Compare.Eq("carrier", "AA")
    val right = Predicate.Compare.Eq("carrier", "UA")
    assert(PredicateHasher.hash(Predicate.And(left, right)) !=
           PredicateHasher.hash(Predicate.Or(left, right)))
  }

  test("hash: nested And/Or is stable") {
    val nested = Predicate.And(
      Predicate.Compare.Gt("distance", 500),
      Predicate.Or(
        Predicate.Compare.Eq("carrier", "AA"),
        Predicate.Compare.Eq("carrier", "UA"),
      ),
    )
    val a = PredicateHasher.hash(nested)
    val b = PredicateHasher.hash(nested)
    assert(a == b)
    assert(a.length == 64)  // SHA-256 hex
  }

  test("hash: In is order-insensitive (it's a set)") {
    val a = Predicate.In("carrier", Seq("AA", "UA", "DL"), negate = false)
    val b = Predicate.In("carrier", Seq("DL", "AA", "UA"), negate = false)
    assert(PredicateHasher.hash(a) == PredicateHasher.hash(b))
  }

  test("hash: In vs not_in are different") {
    val a = Predicate.In("carrier", Seq("AA"), negate = false)
    val b = Predicate.In("carrier", Seq("AA"), negate = true)
    assert(PredicateHasher.hash(a) != PredicateHasher.hash(b))
  }

  test("hash: Contains is distinct from Eq") {
    // `carrier = 'AA'` and `carrier contains 'AA'` produce different
    // SQL — the cache must distinguish them.
    val a = Predicate.Compare.Eq("carrier", "AA")
    val b = Predicate.Compare.Contains("carrier", "AA")
    assert(PredicateHasher.hash(a) != PredicateHasher.hash(b))
  }

  test("hash: Contains does not crash (was a MatchError before the fix)") {
    val p = Predicate.Compare.Contains("carrier", "AA")
    val h = PredicateHasher.hash(p)
    assert(h.length == 64) // SHA-256 hex
  }

  test("hash: StartsWith does not crash") {
    val p = Predicate.Compare.StartsWith("carrier", "AA")
    val h = PredicateHasher.hash(p)
    assert(h.length == 64)
  }

  test("hash: EndsWith does not crash") {
    val p = Predicate.Compare.EndsWith("carrier", "AA")
    val h = PredicateHasher.hash(p)
    assert(h.length == 64)
  }

  test("hash: ArrayContains does not crash") {
    val p = Predicate.Compare.ArrayContains("tags", "promo")
    val h = PredicateHasher.hash(p)
    assert(h.length == 64)
  }

  test("hash: And with 3+ children does not crash") {
    val a = Predicate.Compare.Eq("a", 1)
    val b = Predicate.Compare.Eq("b", 2)
    val c = Predicate.Compare.Eq("c", 3)
    val p = Predicate.And(a, b, c)
    val h = PredicateHasher.hash(p)
    assert(h.length == 64)
  }

  test("hash: Or with 3+ children does not crash") {
    val a = Predicate.Compare.Eq("a", 1)
    val b = Predicate.Compare.Eq("b", 2)
    val c = Predicate.Compare.Eq("c", 3)
    val p = Predicate.Or(a, b, c)
    val h = PredicateHasher.hash(p)
    assert(h.length == 64)
  }

  test("hash: And with 3+ children is commutative") {
    // 3+ children can be reordered; the hash should be stable.
    val a = Predicate.Compare.Eq("a", 1)
    val b = Predicate.Compare.Eq("b", 2)
    val c = Predicate.Compare.Eq("c", 3)
    val left  = Predicate.And(a, b, c)
    val right = Predicate.And(c, b, a)
    assert(PredicateHasher.hash(left) == PredicateHasher.hash(right))
  }

  test("canonicalize: stable string form") {
    val p = Predicate.And(
      Predicate.Compare.Eq("carrier", "AA"),
      Predicate.Compare.Gt("distance", 500),
    )
    val s = PredicateHasher.canonicalize(p)
    // Order-insensitive: both "and(eq,gt)" and "and(gt,eq)" are valid.
    assert(s == "and(eq(carrier,'AA'),gt(distance,500))" ||
           s == "and(gt(distance,500),eq(carrier,'AA'))")
  }

  // ----------------------------------------------------------------
  // AuditSink
  // ----------------------------------------------------------------

  test("NoOp sink: accepts events without storing") {
    val sink = AuditSink.NoOp
    sink.emit(AuditEvent(
      ts = java.time.Instant.now(), model = "x", version = 0,
      measures = Nil, dimensions = Nil,
      whereHash = None, havingHash = None,
      rowCount = 0, elapsedMs = 0, status = "ok"))
    // No assertion needed — NoOp must not throw.
  }

  test("InMemory sink: retains events in arrival order") {
    val sink = AuditSink.inMemory(maxEvents = 10).asInstanceOf[InMemoryAuditSink]
    val now  = java.time.Instant.now()
    sink.emit(AuditEvent(now, "m1", 0, Nil, Nil, None, None, 0, 0, "ok"))
    sink.emit(AuditEvent(now, "m2", 0, Nil, Nil, None, None, 0, 0, "ok"))
    sink.emit(AuditEvent(now, "m3", 0, Nil, Nil, None, None, 0, 0, "ok"))
    val snap = sink.snapshot()
    assert(snap.length == 3)
    assert(snap.map(_.model) == Seq("m1", "m2", "m3"))
  }

  test("InMemory sink: drops oldest on overflow") {
    val sink = AuditSink.inMemory(maxEvents = 2).asInstanceOf[InMemoryAuditSink]
    val now  = java.time.Instant.now()
    sink.emit(AuditEvent(now, "m1", 0, Nil, Nil, None, None, 0, 0, "ok"))
    sink.emit(AuditEvent(now, "m2", 0, Nil, Nil, None, None, 0, 0, "ok"))
    sink.emit(AuditEvent(now, "m3", 0, Nil, Nil, None, None, 0, 0, "ok"))
    val snap = sink.snapshot()
    assert(snap.length == 2)
    assert(snap.map(_.model) == Seq("m2", "m3"))
  }

  test("InMemory sink: clear() drops everything") {
    val sink = AuditSink.inMemory().asInstanceOf[InMemoryAuditSink]
    val now  = java.time.Instant.now()
    sink.emit(AuditEvent(now, "m1", 0, Nil, Nil, None, None, 0, 0, "ok"))
    sink.clear()
    assert(sink.snapshot().isEmpty)
  }

  // ----------------------------------------------------------------
  // SemanticTable.query + toDataFrame -> AuditEvent
  // ----------------------------------------------------------------

  test("query + toDataFrame: emits an event with the captured request shape") {
    val sink = AuditSink.inMemory().asInstanceOf[InMemoryAuditSink]
    val t = baseModel.withAuditSink(sink)
    t.query(
      measures   = Seq("flight_count"),
      dimensions = Seq("carrier"),
    ).toDataFrame(spark)

    val snap = sink.snapshot()
    assert(snap.length == 1)
    val e = snap.head
    assert(e.model == "flights")
    assert(e.measures == Seq("flight_count"))
    assert(e.dimensions == Seq("carrier"))
    assert(e.whereHash.isEmpty)
    assert(e.status == "ok")
    assert(e.elapsedMs >= 0)
  }

  test("query + toDataFrame: where predicate is captured as a stable hash") {
    val sink = AuditSink.inMemory().asInstanceOf[InMemoryAuditSink]
    val t = baseModel.withAuditSink(sink)
    t.query(
      measures   = Seq("flight_count"),
      dimensions = Seq("carrier"),
      where      = Some(Predicate.Compare.Eq("carrier", "AA")),
    ).toDataFrame(spark)

    val e = sink.snapshot().head
    val expected = PredicateHasher.hash(Predicate.Compare.Eq("carrier", "AA"))
    assert(e.whereHash == Some(expected))
  }

  test("query + toDataFrame: chain (limit, orderBy) preserves the audit sink") {
    val sink = AuditSink.inMemory().asInstanceOf[InMemoryAuditSink]
    val t = baseModel.withAuditSink(sink)
    t.query(
      measures   = Seq("flight_count"),
      dimensions = Seq("carrier"),
      orderBy    = Seq(io.semanticdf.SortKey.desc("flight_count")),
      limit      = Some(3),
    ).toDataFrame(spark)

    // Sink survived the chain; exactly one event emitted.
    assert(sink.snapshot().length == 1)
  }

  test("query + toDataFrame: error path emits an error event with status='error'") {
    val sink = AuditSink.inMemory().asInstanceOf[InMemoryAuditSink]
    val t = baseModel.withAuditSink(sink)
    // Reference a non-existent measure to force an error.
    val ex = intercept[IllegalArgumentException] {
      t.query(
        measures   = Seq("nonexistent_measure"),
        dimensions = Seq("carrier"),
      ).toDataFrame(spark)
    }
    assert(ex != null)  // the exception propagated
    val e = sink.snapshot().head
    assert(e.status == "error")
    assert(e.error.isDefined)
  }

  test("query + toDataFrame: audit emission does not swallow fatal errors") {
    val fatalSink = new AuditSink {
      override def emit(event: AuditEvent): Unit = throw new LinkageError("fatal audit sink")
    }
    val t = baseModel.withAuditSink(fatalSink)

    val ex = intercept[LinkageError] {
      t.query(
        measures   = Seq("flight_count"),
        dimensions = Seq("carrier"),
      ).toDataFrame(spark)
    }
    assert(ex.getMessage == "fatal audit sink")
  }

  test("JsonlStdoutSink: emits an event whose JSON payload includes the model version") {
    // The standing recommendation says the audit log is the natural
    // trigger for downstream invalidation consumers. For that to be
    // true at the wire level, the JSONL stdout sink must include the
    // model version. This test wires a custom JUL handler to capture
    // the sink's output and verify the payload.
    import java.util.logging.{Handler, LogRecord, Logger}

    val log = Logger.getLogger("io.semanticdf.audit.jsonl")
    val captured = scala.collection.mutable.ArrayBuffer.empty[LogRecord]
    val handler = new Handler {
      override def publish(record: LogRecord): Unit = synchronized { captured += record }
      override def flush(): Unit = ()
      override def close(): Unit = ()
    }
    log.addHandler(handler)
    try {
      val sink = AuditSink.JsonlStdout
      // Build an event with a non-default version.
      val event = AuditEvent(
        ts         = java.time.Instant.parse("2026-07-26T11:00:00Z"),
        model      = "flights",
        version    = 42,
        measures   = Seq("flight_count"),
        dimensions = Seq("carrier"),
        whereHash  = Some("abc123"),
        havingHash = None,
        rowCount   = 3L,
        elapsedMs  = 42L,
        status     = "ok",
      )
      sink.emit(event)
      assert(captured.length == 1, s"expected exactly 1 log record; got ${captured.length}")
      val json = captured.head.getMessage
      // The wire surface must carry the model version so operators
      // tail-ing the JSONL can correlate events with model versions.
      assert(json.contains(""""version":42"""),
        s"JSONL output must include the model version; got: $json")
      // Regression: the existing fields still arrive.
      assert(json.contains(""""model":"flights""""), s"model missing: $json")
      assert(json.contains(""""row_count":3"""), s"row_count missing: $json")
    } finally {
      log.removeHandler(handler)
    }
  }

  test("query + toDataFrame: no sink = no audit (default off, zero overhead)") {
    // Default model has no sink; toDataFrame must still succeed.
    val df = baseModel.query(
      measures   = Seq("flight_count"),
      dimensions = Seq("carrier"),
    ).toDataFrame(spark)
    assert(df != null)
  }
}
