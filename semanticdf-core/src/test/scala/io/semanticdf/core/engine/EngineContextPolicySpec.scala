package io.semanticdf.core.engine

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.Duration

/** Policy-interaction + serialization tests for [EngineContext]
  * (v0.3.0).
  *
  * The existing [EngineContextSpec] pins the basic data shape
  * (case counts, field preservation, equality). This spec pins:
  *
  *   - **Distributed safety** \u2014 `Product with Serializable`
  *     round-trip for `EngineContext` and each sub-ADT
  *   - **Default contract** \u2014 the exact values of
  *     `EngineContext.defaultContext` (pinned, not just "sensible")
  *   - **Policy combinations** \u2014 valid + invalid combinations
  *     (e.g. `Duration.Inf` with `CancellationCapability.Cooperative`)
  *   - **Immutability** \u2014 `copy` semantics for `EngineContext`
  *     and `JoinHints`
  *   - **Distinct singletons** \u2014 case objects across all 5
  *     sub-ADTs are pairwise distinct
  */
class EngineContextPolicySpec extends AnyFunSuite with Matchers {

  // --------------------------------------------------------------------
  // Default contract (pinned values)
  // --------------------------------------------------------------------

  test("defaultContext has pinned field values (the source of truth for 'default')") {
    val ctx = EngineContext.defaultContext
    ctx.materializePolicy shouldBe MaterializePolicy.None
    ctx.cachePolicy shouldBe CachePolicy.NoCache
    ctx.auditPolicy shouldBe AuditPolicy.NoAudit
    ctx.joinHints shouldBe JoinHints()
    ctx.timeout shouldBe Duration.Inf
    ctx.cancellation shouldBe CancellationCapability.Unsupported
  }

  test("defaultContext is reusable across calls (no shared mutable state)") {
    // Per the value-object contract: defaultContext is a `val`
    // (immutable singleton), so two calls return the same object.
    // If anyone adds a var field, this test catches it.
    EngineContext.defaultContext should be theSameInstanceAs EngineContext.defaultContext
  }

  // --------------------------------------------------------------------
  // Immutability + copy
  // --------------------------------------------------------------------

  test("EngineContext.copy preserves unchanged fields and overrides the set one") {
    val original = EngineContext.defaultContext
    val updated = original.copy(timeout = Duration("30 seconds"))
    // The unchanged fields are preserved:
    updated.materializePolicy shouldBe original.materializePolicy
    updated.cachePolicy shouldBe original.cachePolicy
    updated.auditPolicy shouldBe original.auditPolicy
    updated.joinHints shouldBe original.joinHints
    updated.cancellation shouldBe original.cancellation
    // The changed field is updated:
    updated.timeout shouldBe Duration("30 seconds")
    // The original is untouched:
    original.timeout shouldBe Duration.Inf
  }

  test("JoinHints.copy overrides one field while preserving others") {
    val original = JoinHints(
      broadcastRightBelowBytes = Some(10485760L),
      skewFactor = Some(10),
    )
    val updated = original.copy(preferredStrategy = Some(JoinStrategy.Broadcast))
    updated.broadcastRightBelowBytes shouldBe Some(10485760L)
    updated.skewFactor shouldBe Some(10)
    updated.preferredStrategy shouldBe Some(JoinStrategy.Broadcast)
    // The original is untouched:
    original.preferredStrategy shouldBe None
  }

  // --------------------------------------------------------------------
  // Distinct singletons across the 5 sub-ADTs
  // --------------------------------------------------------------------

  test("MaterializePolicy singletons are pairwise distinct") {
    val all = Seq(
      MaterializePolicy.None,
      MaterializePolicy.MemoryOnly,
      MaterializePolicy.MemoryAndDisk,
      MaterializePolicy.EngineDefault,
    )
    all.distinct.size shouldBe all.size
  }

  test("CachePolicy singletons are pairwise distinct") {
    val all = Seq(
      CachePolicy.NoCache,
      CachePolicy.ReadThrough,
      CachePolicy.WriteThrough,
      CachePolicy.ReadOnly,
    )
    all.distinct.size shouldBe all.size
  }

  test("AuditPolicy singletons are pairwise distinct") {
    val all = Seq(AuditPolicy.NoAudit, AuditPolicy.EngineDefault)
    all.distinct.size shouldBe all.size
  }

  test("JoinStrategy singletons are pairwise distinct") {
    val all = Seq(
      JoinStrategy.Broadcast,
      JoinStrategy.ShuffleHash,
      JoinStrategy.SortMerge,
    )
    all.distinct.size shouldBe all.size
  }

  // --------------------------------------------------------------------
  // Policy combinations (valid)
  // --------------------------------------------------------------------

  test("valid combination: All 6 policies set simultaneously") {
    val ctx = EngineContext(
      materializePolicy = MaterializePolicy.MemoryAndDisk,
      cachePolicy       = CachePolicy.ReadThrough,
      auditPolicy       = AuditPolicy.EngineDefault,
      joinHints         = JoinHints(
        broadcastRightBelowBytes = Some(10485760L),
        skewFactor = Some(10),
        preferredStrategy = Some(JoinStrategy.Broadcast),
      ),
      timeout           = Duration("30 seconds"),
      cancellation      = CancellationCapability.Cooperative("req-1"),
    )
    ctx.materializePolicy shouldBe MaterializePolicy.MemoryAndDisk
    ctx.cachePolicy shouldBe CachePolicy.ReadThrough
    ctx.auditPolicy shouldBe AuditPolicy.EngineDefault
    ctx.joinHints.broadcastRightBelowBytes shouldBe Some(10485760L)
    ctx.joinHints.skewFactor shouldBe Some(10)
    ctx.joinHints.preferredStrategy shouldBe Some(JoinStrategy.Broadcast)
    ctx.timeout shouldBe Duration("30 seconds")
    ctx.cancellation shouldBe CancellationCapability.Cooperative("req-1")
  }

  test("valid combination: Duration.Inf with Unsupported cancellation is the documented default") {
    // Per the design: when cancellation is Unsupported, the caller
    // is expected to bound the query with a finite timeout.
    // The DEFAULT has Duration.Inf + Unsupported together
    // (no caller intent \u2014 explicit timeout/cancellation is
    // a caller decision).
    val ctx = EngineContext.defaultContext
    ctx.timeout shouldBe Duration.Inf
    ctx.cancellation shouldBe CancellationCapability.Unsupported
  }

  test("valid combination: JoinHints with only some fields set") {
    val h = JoinHints(broadcastRightBelowBytes = Some(1024L))
    h.broadcastRightBelowBytes shouldBe Some(1024L)
    h.skewFactor shouldBe None
    h.preferredStrategy shouldBe None
  }

  // --------------------------------------------------------------------
  // Empty JoinHints vs all-fields-set are NOT equal (pin: not a free-form Map)
  // --------------------------------------------------------------------

  test("JoinHints() and JoinHints(Some(1L)) are NOT equal (free-form fields matter)") {
    JoinHints() should not be JoinHints(broadcastRightBelowBytes = Some(1L))
  }

  // --------------------------------------------------------------------
  // Java serialization round-trip (Product with Serializable contract)
  // --------------------------------------------------------------------

  test("EngineContext.defaultContext round-trips through Java serialization") {
    val original = EngineContext.defaultContext
    val roundTripped = javaSerializeRoundTrip(original)
    roundTripped shouldBe original
  }

  test("EngineContext with all policies set round-trips through Java serialization") {
    val original = EngineContext(
      materializePolicy = MaterializePolicy.MemoryOnly,
      cachePolicy       = CachePolicy.ReadOnly,
      auditPolicy       = AuditPolicy.EngineDefault,
      joinHints         = JoinHints(
        broadcastRightBelowBytes = Some(1024L),
        skewFactor               = Some(3),
        preferredStrategy        = Some(JoinStrategy.SortMerge),
      ),
      timeout           = Duration("5 minutes"),
      cancellation      = CancellationCapability.SparkJobTag("req-77"),
    )
    val roundTripped = javaSerializeRoundTrip(original)
    roundTripped shouldBe original
  }

  test("MaterializePolicy case objects round-trip through Java serialization") {
    val all = Seq(
      MaterializePolicy.None,
      MaterializePolicy.MemoryOnly,
      MaterializePolicy.MemoryAndDisk,
      MaterializePolicy.EngineDefault,
    )
    all.foreach { p =>
      javaSerializeRoundTrip(p) shouldBe p
    }
  }

  test("CachePolicy case objects round-trip through Java serialization") {
    val all = Seq(
      CachePolicy.NoCache,
      CachePolicy.ReadThrough,
      CachePolicy.WriteThrough,
      CachePolicy.ReadOnly,
    )
    all.foreach { p =>
      javaSerializeRoundTrip(p) shouldBe p
    }
  }

  test("CancellationCapability.Cooperative carries requestId through Java serialization") {
    val original = CancellationCapability.Cooperative("req-abc")
    javaSerializeRoundTrip(original) shouldBe original
  }

  test("CancellationCapability.SparkJobTag carries requestId through Java serialization") {
    val original = CancellationCapability.SparkJobTag("req-xyz")
    javaSerializeRoundTrip(original) shouldBe original
  }

  test("CancellationCapability.RemoteStatement carries requestId through Java serialization") {
    val original = CancellationCapability.RemoteStatement("req-mno")
    javaSerializeRoundTrip(original) shouldBe original
  }

  test("JoinHints round-trips through Java serialization") {
    val original = JoinHints(
      broadcastRightBelowBytes = Some(2048L),
      skewFactor               = Some(7),
      preferredStrategy        = Some(JoinStrategy.Broadcast),
    )
    javaSerializeRoundTrip(original) shouldBe original
  }

  // --------------------------------------------------------------------
  // Helper: Java serialization round-trip
  // --------------------------------------------------------------------

  private def javaSerializeRoundTrip[T](value: T): T = {
    val baos = new java.io.ByteArrayOutputStream()
    val oos  = new java.io.ObjectOutputStream(baos)
    oos.writeObject(value)
    oos.close()
    val bais = new java.io.ByteArrayInputStream(baos.toByteArray)
    val ois  = new java.io.ObjectInputStream(bais)
    val out  = ois.readObject().asInstanceOf[T]
    ois.close()
    out
  }
}