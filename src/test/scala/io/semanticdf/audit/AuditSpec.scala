package io.semanticdf.audit

import io.semanticdf.{Dimension, FlightsFixture, Measure, Predicate, SparkSessionFixture, toSemanticTable}
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
      ts = java.time.Instant.now(), model = "x",
      measures = Nil, dimensions = Nil,
      whereHash = None, havingHash = None,
      rowCount = 0, elapsedMs = 0, status = "ok"))
    // No assertion needed — NoOp must not throw.
  }

  test("InMemory sink: retains events in arrival order") {
    val sink = AuditSink.inMemory(maxEvents = 10).asInstanceOf[InMemoryAuditSink]
    val now  = java.time.Instant.now()
    sink.emit(AuditEvent(now, "m1", Nil, Nil, None, None, 0, 0, "ok"))
    sink.emit(AuditEvent(now, "m2", Nil, Nil, None, None, 0, 0, "ok"))
    sink.emit(AuditEvent(now, "m3", Nil, Nil, None, None, 0, 0, "ok"))
    val snap = sink.snapshot()
    assert(snap.length == 3)
    assert(snap.map(_.model) == Seq("m1", "m2", "m3"))
  }

  test("InMemory sink: drops oldest on overflow") {
    val sink = AuditSink.inMemory(maxEvents = 2).asInstanceOf[InMemoryAuditSink]
    val now  = java.time.Instant.now()
    sink.emit(AuditEvent(now, "m1", Nil, Nil, None, None, 0, 0, "ok"))
    sink.emit(AuditEvent(now, "m2", Nil, Nil, None, None, 0, 0, "ok"))
    sink.emit(AuditEvent(now, "m3", Nil, Nil, None, None, 0, 0, "ok"))
    val snap = sink.snapshot()
    assert(snap.length == 2)
    assert(snap.map(_.model) == Seq("m2", "m3"))
  }

  test("InMemory sink: clear() drops everything") {
    val sink = AuditSink.inMemory().asInstanceOf[InMemoryAuditSink]
    val now  = java.time.Instant.now()
    sink.emit(AuditEvent(now, "m1", Nil, Nil, None, None, 0, 0, "ok"))
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

  test("query + toDataFrame: no sink = no audit (default off, zero overhead)") {
    // Default model has no sink; toDataFrame must still succeed.
    val df = baseModel.query(
      measures   = Seq("flight_count"),
      dimensions = Seq("carrier"),
    ).toDataFrame(spark)
    assert(df != null)
  }
}
