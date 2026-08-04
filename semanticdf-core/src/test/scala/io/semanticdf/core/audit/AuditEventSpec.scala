package io.semanticdf.core.audit

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.time.Instant

/** Phase 1 increment 9: prove `io.semanticdf.core.audit.AuditEvent` is a
  * usable, self-contained, Spark-free data record + pure factory.
  *
  * ==Why this test file exists==
  *
  * The new package — `io.semanticdf.core.audit` — contains the data-only
  * shape that future engine adapters (Trino, Databricks) will use to
  * emit audit events. It must compile and run with NO Spark on the
  * classpath. This test verifies both:
  *
  *   1. The data record holds the right values for every field.
  *   2. The `dedupHashOf` factory is a pure function — same input
  *      always returns the same hash, and commutative inputs
  *      (e.g. measures in different orders) produce the same hash.
  *
  * ==Data-driven mantra compliance==
  *
  * Every assertion checks data shape: case class construction, field
  * equality, and `dedupHashOf` determinism. No `Map`-based dispatch,
  * no closures, no Spark imports. Per `scala-data-driven-refactor`
  * step 1, `dedupHashOf` is data (a String), and the function is pure.
  */
class AuditEventSpec extends AnyFunSuite with Matchers {

  // -------------------------------------------------------------------------
  // Data record construction
  // -------------------------------------------------------------------------

  test("AuditEvent holds all 15 fields") {
    val ts = Instant.parse("2026-08-04T10:00:00Z")
    val ev = AuditEvent(
      ts         = ts,
      model      = "flights",
      version    = 1,
      measures   = Seq("flight_count"),
      dimensions = Seq("carrier"),
      whereHash  = Some("hash_w"),
      havingHash = Some("hash_h"),
      rowCount   = 42L,
      elapsedMs  = 100L,
      status     = "ok",
      error      = None,
      requester  = None,
      requestId = None,
      dedupHash  = "abc123",
      executedPlan = None,
    )
    ev.ts shouldBe ts
    ev.model shouldBe "flights"
    ev.version shouldBe 1
    ev.measures shouldBe Seq("flight_count")
    ev.dimensions shouldBe Seq("carrier")
    ev.whereHash shouldBe Some("hash_w")
    ev.havingHash shouldBe Some("hash_h")
    ev.rowCount shouldBe 42L
    ev.elapsedMs shouldBe 100L
    ev.status shouldBe "ok"
    ev.error shouldBe None
    ev.requester shouldBe None
    ev.requestId shouldBe None
    ev.dedupHash shouldBe "abc123"
    ev.executedPlan shouldBe None
  }

  test("AuditEvent default values: error/requester/requestId/executedPlan/version") {
    val ts = Instant.parse("2026-08-04T10:00:00Z")
    val ev = AuditEvent(
      ts         = ts,
      model      = "flights",
      measures   = Nil,
      dimensions = Nil,
      whereHash  = None,
      havingHash = None,
      rowCount   = 0L,
      elapsedMs  = 0L,
      status     = "ok",
      dedupHash  = "abc",
    )
    ev.version shouldBe 0           // default
    ev.error shouldBe None          // default
    ev.requester shouldBe None      // default
    ev.requestId shouldBe None      // default
    ev.executedPlan shouldBe None    // default
  }

  test("AuditEvent with error status carries the error string") {
    val ev = AuditEvent(
      ts = Instant.now(), model = "flights",
      measures = Nil, dimensions = Nil,
      whereHash = None, havingHash = None,
      rowCount = 0L, elapsedMs = 5L,
      status = "error",
      error = Some("IllegalArgumentException: nope"),
      dedupHash = "x",
    )
    ev.status shouldBe "error"
    ev.error shouldBe Some("IllegalArgumentException: nope")
  }

  // -------------------------------------------------------------------------
  // Equality: case class auto-derive
  // -------------------------------------------------------------------------

  test("Two AuditEvents with same data are equal") {
    val ts = Instant.parse("2026-08-04T10:00:00Z")
    val a = AuditEvent(ts, "flights", 0, Seq("m"), Seq("d"),
                        Some("wh"), None, 1L, 10L, "ok", dedupHash = "h")
    val b = AuditEvent(ts, "flights", 0, Seq("m"), Seq("d"),
                        Some("wh"), None, 1L, 10L, "ok", dedupHash = "h")
    a shouldBe b
    a.hashCode shouldBe b.hashCode
  }

  test("Different dedupHash makes them unequal") {
    val ts = Instant.now()
    val a = AuditEvent(ts, "flights", 0, Nil, Nil, None, None, 0L, 0L, "ok", dedupHash = "h1")
    val b = AuditEvent(ts, "flights", 0, Nil, Nil, None, None, 0L, 0L, "ok", dedupHash = "h2")
    a should not be b
  }

  // -------------------------------------------------------------------------
  // dedupHashOf: pure function with commutative property
  // -------------------------------------------------------------------------

  test("dedupHashOf is deterministic: same input always returns same hash") {
    val h1 = AuditEvent.dedupHashOf("flights", 0,
                                     Seq("flight_count"), Seq("carrier"),
                                     Some("wh"), None)
    val h2 = AuditEvent.dedupHashOf("flights", 0,
                                     Seq("flight_count"), Seq("carrier"),
                                     Some("wh"), None)
    h1 shouldBe h2
  }

  test("dedupHashOf is commutative in measures/dimensions: order doesn't matter") {
    val a = AuditEvent.dedupHashOf("flights", 1,
                                    Seq("a", "b", "c"), Seq("x", "y"),
                                    Some("wh"), None)
    val b = AuditEvent.dedupHashOf("flights", 1,
                                    Seq("c", "a", "b"), Seq("y", "x"),
                                    Some("wh"), None)
    a shouldBe b
  }

  test("dedupHashOf changes when model changes") {
    val a = AuditEvent.dedupHashOf("flights", 0, Nil, Nil, None, None)
    val b = AuditEvent.dedupHashOf("cars",    0, Nil, Nil, None, None)
    a should not be b
  }

  test("dedupHashOf changes when version changes") {
    val a = AuditEvent.dedupHashOf("flights", 0, Nil, Nil, None, None)
    val b = AuditEvent.dedupHashOf("flights", 1, Nil, Nil, None, None)
    a should not be b
  }

  test("dedupHashOf changes when whereHash changes") {
    val a = AuditEvent.dedupHashOf("flights", 0, Nil, Nil, Some("wh1"), None)
    val b = AuditEvent.dedupHashOf("flights", 0, Nil, Nil, Some("wh2"), None)
    a should not be b
  }

  test("dedupHashOf changes when havingHash changes") {
    val a = AuditEvent.dedupHashOf("flights", 0, Nil, Nil, None, Some("hh1"))
    val b = AuditEvent.dedupHashOf("flights", 0, Nil, Nil, None, Some("hh2"))
    a should not be b
  }

  test("dedupHashOf returns a 64-char lowercase hex string (SHA-256)") {
    val h = AuditEvent.dedupHashOf("flights", 0, Nil, Nil, None, None)
    h.length shouldBe 64
    h should fullyMatch regex "[0-9a-f]{64}"
  }

  test("dedupHashOf handles None for whereHash/havingHash identically to empty string") {
    val a = AuditEvent.dedupHashOf("flights", 0, Nil, Nil, None, None)
    val b = AuditEvent.dedupHashOf("flights", 0, Nil, Nil, Some(""), Some(""))
    a shouldBe b
  }

  // -------------------------------------------------------------------------
  // Data-driven: contract invariants
  // -------------------------------------------------------------------------

  test("dedupHash does NOT include ts (wall-clock is non-deterministic)") {
    val ts1 = Instant.parse("2026-08-04T10:00:00Z")
    val ts2 = Instant.parse("2026-08-04T11:00:00Z")
    val a = AuditEvent.dedupHashOf("flights", 0, Nil, Nil, None, None)
    // dedupHash is computed from query-shape only — ts is not an input.
    val b = AuditEvent.dedupHashOf("flights", 0, Nil, Nil, None, None)
    a shouldBe b
    // Verify: same dedupHash despite different ts in the AuditEvent record.
    val ev1 = AuditEvent(ts1, "flights", 0, Nil, Nil, None, None, 0L, 0L, "ok", dedupHash = a)
    val ev2 = AuditEvent(ts2, "flights", 0, Nil, Nil, None, None, 0L, 0L, "ok", dedupHash = b)
    ev1.dedupHash shouldBe ev2.dedupHash
  }
}