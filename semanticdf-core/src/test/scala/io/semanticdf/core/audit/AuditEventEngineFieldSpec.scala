package io.semanticdf.core.audit

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.semanticdf.core.engine.EngineIdentity

import java.time.Instant

/** Tests for the `engine: Option[EngineIdentity]` field on
  * `AuditEvent` (added in v0.3.0).
  *
  * Per design \u00a74.5.5 + design §11 closure: the
  * audit event MUST include the engine identity so the dedup
  * key distinguishes a Spark request from a Trino request for
  * the same model. Without it, a single dedup-hash key would
  * collapse both engine's events into the same audit log entry.
  *
  * This spec pins the field's behavior:
  *   1. Default value is `None` (backward-compat with pre-PR-4
  *      events)
  *   2. `Some(...)` round-trips through Java serialization
  *   3. `dedupHashOf(...)` produces DIFFERENT hashes for the
  *      same model + measures + dimensions with DIFFERENT
  *      engine identities */
class AuditEventEngineFieldSpec extends AnyFunSuite with Matchers {

  private val sparkEngine = EngineIdentity(
    name                 = "spark",
    nativeVersion        = "3.5.8",
    engineAdapterVersion = "0.3.0",
  )
  private val trinoEngine = EngineIdentity(
    name                 = "trino",
    nativeVersion        = "0.286",
    engineAdapterVersion = "0.3.0",
  )

  // -- engine field default + round-trip --

  test("AuditEvent defaults engine to None (backward-compat)") {
    val event = AuditEvent(
      ts           = Instant.parse("2024-01-15T10:30:00Z"),
      model        = "orders",
      measures     = Seq("amount"),
      dimensions   = Seq("region"),
      whereHash    = None,
      havingHash   = None,
      rowCount     = 10,
      elapsedMs    = 100,
      status       = "ok",
      dedupHash    = "abc",
    )
    event.engine shouldBe None
  }

  test("AuditEvent round-trips engine = Some(spark) through Java serialization") {
    val event = AuditEvent(
      ts           = Instant.now(),
      model        = "orders",
      measures     = Seq("amount"),
      dimensions   = Seq("region"),
      whereHash    = None,
      havingHash   = None,
      rowCount     = 10,
      elapsedMs    = 100,
      status       = "ok",
      dedupHash    = "abc",
      engine       = Some(sparkEngine),
    )
    val out = new java.io.ByteArrayOutputStream()
    val oos = new java.io.ObjectOutputStream(out)
    oos.writeObject(event)
    oos.close()
    val ois = new java.io.ObjectInputStream(new java.io.ByteArrayInputStream(out.toByteArray))
    val back = ois.readObject().asInstanceOf[AuditEvent]
    ois.close()
    back shouldBe event
    back.engine shouldBe Some(sparkEngine)
  }

  // -- dedup hash includes engine identity --

  test("dedupHashOf produces DIFFERENT hashes for the same model + different engines") {
    val baseArgs = ("orders", 1, Seq("amount"), Seq("region"), None, None)
    val sparkHash = AuditEvent.dedupHashOf(
      model = baseArgs._1, version = baseArgs._2,
      measures = baseArgs._3, dimensions = baseArgs._4,
      whereHash = baseArgs._5, havingHash = baseArgs._6,
      engine    = Some(sparkEngine),
    )
    val trinoHash = AuditEvent.dedupHashOf(
      model = baseArgs._1, version = baseArgs._2,
      measures = baseArgs._3, dimensions = baseArgs._4,
      whereHash = baseArgs._5, havingHash = baseArgs._6,
      engine    = Some(trinoEngine),
    )
    sparkHash should not be trinoHash
  }

  test("dedupHashOf produces SAME hash for the same engine (different call instances)") {
    val hash1 = AuditEvent.dedupHashOf(
      model = "orders", version = 1,
      measures = Seq("amount"), dimensions = Seq("region"),
      whereHash = None, havingHash = None,
      engine    = Some(sparkEngine),
    )
    val hash2 = AuditEvent.dedupHashOf(
      model = "orders", version = 1,
      measures = Seq("amount"), dimensions = Seq("region"),
      whereHash = None, havingHash = None,
      engine    = Some(sparkEngine),
    )
    hash1 shouldBe hash2
  }

  test("dedupHashOf with engine = None is forward-compatible with old (pre-PR-4) hash format") {
    val oldStyleHash = AuditEvent.dedupHashOf(
      model = "orders", version = 1,
      measures = Seq("amount"), dimensions = Seq("region"),
      whereHash = None, havingHash = None,
      // engine omitted (= default None)
    )
    // Different from the new (engine = Some(spark)) hash
    val newStyleHash = AuditEvent.dedupHashOf(
      model = "orders", version = 1,
      measures = Seq("amount"), dimensions = Seq("region"),
      whereHash = None, havingHash = None,
      engine    = Some(sparkEngine),
    )
    oldStyleHash should not be newStyleHash
  }
}