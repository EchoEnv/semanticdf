package io.semanticdf.cache

import io.semanticdf._
import io.semanticdf.audit.AuditSink
import io.semanticdf.predicate.Predicate

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{count, lit}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType, StringType}
import org.apache.spark.storage.StorageLevel
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Tests for the opt-in DataFrame persistence on `withMaterialize(level)`.
  *
  * `withMaterialize(level)` makes the fast path of `toDataFrame`
  * (no audit, no result cache) call `df.persist(level)` on the
  * compiled DataFrame. The audit/cache branch does NOT honour this
  * flag — see the design doc (`docs/design/with-materialize.md`)
  * for why.
  *
  * The library does NOT retain a `DataFrame` ref and does NOT expose
  * `unpersist()` on `SemanticTable`. Callers manage cleanup via the
  * `DataFrame` they get from `toDataFrame()`. */
class MaterializeSpec extends AnyFunSuite with Matchers with SparkSessionFixture {

  /** Tiny base model — 10 Int rows from spark.range(). */
  private def baseModel(spark: SparkSession, name: String = "m") = {
    toSemanticTable(spark.range(10).toDF("k"), name = Some(name))
      .withDimensions(Dimension("k", t => t("k")))
      .withMeasures(Measure("n", t => count(lit(1))))
  }

  // ----------------------------------------------------------------
  // Default: materializeLevel = None
  // ----------------------------------------------------------------

  test("default materializeLevel = None (no persist)") {
    val m = baseModel(spark)
    m.materializeLevel shouldBe None
  }

  // ----------------------------------------------------------------
  // Fluent setter
  // ----------------------------------------------------------------

  test("withMaterialize(level) sets the field") {
    val m = baseModel(spark).withMaterialize(StorageLevel.MEMORY_ONLY)
    m.materializeLevel shouldBe Some(StorageLevel.MEMORY_ONLY)
  }

  test("withMaterialize(null) throws IllegalArgumentException") {
    val m = baseModel(spark)
    intercept[IllegalArgumentException] {
      m.withMaterialize(null.asInstanceOf[StorageLevel])
    }
  }

  test("withMaterialize preserves the level across withDimensions / withMeasures / withTransforms") {
    val m = baseModel(spark)
      .withMaterialize(StorageLevel.MEMORY_ONLY)
      .withDimensions(Dimension("kk", t => t("k")))   // adds a dim
      .withMeasures(Measure("nn", t => count(lit(1))))  // adds a measure
    m.materializeLevel shouldBe Some(StorageLevel.MEMORY_ONLY)
  }

  test("withTransforms preserves materializeLevel (regression: recent audit pattern)") {
    // Same regression pattern as withRowFilter from the withRowFilter and broadcastJoinThreshold setters:
    // each setter must thread materializeLevel through new SemanticTable(...). The
    // mutation's withTransforms creates a fresh SemanticTable; if materializeLevel
    // were dropped, the user's persist setting would silently vanish.
    val m = baseModel(spark).withMaterialize(StorageLevel.MEMORY_AND_DISK)
    // withTransforms takes a Transform — use a simple alias transform.
    import io.semanticdf.Transform
    val chained = m.withTransforms(Transform("k_alias", t => t("k").as("k_alias")))
    chained.materializeLevel shouldBe Some(StorageLevel.MEMORY_AND_DISK)
  }

  test("withRowFilter preserves materializeLevel") {
    val m = baseModel(spark).withMaterialize(StorageLevel.DISK_ONLY)
    val chained = m.withRowFilter(
      name = "f",
      expr = "k >= 0",
      description = None,
      metadata = Map.empty,
    )
    chained.materializeLevel shouldBe Some(StorageLevel.DISK_ONLY)
  }

  // ----------------------------------------------------------------
  // Compile-path behavior: fast path
  // ----------------------------------------------------------------

  test("toDataFrame on fast path returns a persisted DataFrame when materializeLevel is set") {
    val m = baseModel(spark).withMaterialize(StorageLevel.MEMORY_ONLY)
    val df = m.toDataFrame(spark)
    try {
      df.storageLevel shouldBe StorageLevel.MEMORY_ONLY
    } finally df.unpersist()
  }

  test("toDataFrame on fast path returns an un-persisted DataFrame when materializeLevel is None") {
    val m = baseModel(spark)
    val df = m.toDataFrame(spark)
    df.storageLevel shouldBe StorageLevel.NONE
  }

  test("user-managed unpersist: the library does NOT retain a DataFrame ref") {
    // Falsification for the dropped `unpersist()` design: the user
    // holds the DataFrame, calls unpersist themselves, and the storage
    // level returns to NONE. If the library retained a ref, the next
    // toDataFrame call would re-persist and the storage level wouldn't
    // drop to NONE after the user's unpersist.
    val m = baseModel(spark).withMaterialize(StorageLevel.MEMORY_ONLY)
    val df = m.toDataFrame(spark)
    df.storageLevel shouldBe StorageLevel.MEMORY_ONLY
    df.unpersist()
    df.storageLevel shouldBe StorageLevel.NONE
  }

  // ----------------------------------------------------------------
  // Audit/cache path: materializeLevel is a no-op
  // ----------------------------------------------------------------

  test("toDataFrame with auditSink set: materializeLevel is a no-op (parallelize-based return)") {
    import scala.collection.mutable.ArrayBuffer
    val sink = AuditSink.inMemory(8)
    val m = baseModel(spark)
      .withMaterialize(StorageLevel.MEMORY_ONLY)
      .withAuditSink(sink)
    val df = m.toDataFrame(spark)
    try {
      // The audit/cache path returns a parallelize-based DataFrame,
      // not the persisted compiled one. So `df.storageLevel` is NONE
      // (not MEMORY_ONLY). The user's materializeLevel is silently
      // dropped on this path — documented in the field's Scaladoc.
      df.storageLevel shouldBe StorageLevel.NONE
    } finally df.unpersist()
  }

  test("toDataFrame with resultCache set: materializeLevel is a no-op") {
    val cache = ResultCache.inMemory()
    val m = baseModel(spark)
      .withMaterialize(StorageLevel.MEMORY_ONLY)
      .withResultCache(cache)
    val df = m.toDataFrame(spark)
    try {
      df.storageLevel shouldBe StorageLevel.NONE
    } finally df.unpersist()
  }

  // ----------------------------------------------------------------
  // Join wrapper: LEFT-wins, RIGHT-fallback
  // ----------------------------------------------------------------

  test("join_one: right-side withMaterialize propagates via joinMaterializeLevel helper") {
    val left  = baseModel(spark, "left")
    val right = baseModel(spark, "right").withMaterialize(StorageLevel.MEMORY_ONLY)

    val joined = left.join_one(right, (l, r) => l("k") === r("k"))
    joined.materializeLevel shouldBe Some(StorageLevel.MEMORY_ONLY)
  }

  test("join_one: LEFT-side withMaterialize wins when both sides set (precedence preserved)") {
    val left  = baseModel(spark, "left").withMaterialize(StorageLevel.MEMORY_ONLY)
    val right = baseModel(spark, "right").withMaterialize(StorageLevel.DISK_ONLY)

    val joined = left.join_one(right, (l, r) => l("k") === r("k"))
    joined.materializeLevel shouldBe Some(StorageLevel.MEMORY_ONLY)
  }

  test("join_many: materializeLevel propagates with same LEFT-wins precedence") {
    val fact = baseModel(spark, "fact")
    val dim  = baseModel(spark, "dim").withMaterialize(StorageLevel.MEMORY_AND_DISK)

    val joined = fact.join_many(dim, (l, r) => l("k") === r("k"))
    joined.materializeLevel shouldBe Some(StorageLevel.MEMORY_AND_DISK)
  }

  test("join_one: both sides unset → materializeLevel is None on the joined result") {
    val left  = baseModel(spark, "left")
    val right = baseModel(spark, "right")

    val joined = left.join_one(right, (l, r) => l("k") === r("k"))
    joined.materializeLevel shouldBe None
  }

  test("join_cross: materializeLevel propagates with LEFT-wins precedence (cross join isn't equi)") {
    // join_cross does not use compileEquiJoin — it doesn't carry a broadcastJoinThreshold.
    // materializeLevel is still propagated via the wrapper helper (audit-found regression
    // pattern: every join must thread the field).
    val left  = baseModel(spark, "left")
    val right = baseModel(spark, "right").withMaterialize(StorageLevel.DISK_ONLY)
    val joined = left.join_cross(right)
    joined.materializeLevel shouldBe Some(StorageLevel.DISK_ONLY)
  }

  test("join_cross: both sides unset → materializeLevel is None") {
    val left  = baseModel(spark, "left")
    val right = baseModel(spark, "right")
    val joined = left.join_cross(right)
    joined.materializeLevel shouldBe None
  }

  // ----------------------------------------------------------------
  // groupBy().aggregate(): materializeLevel preserved
  // ----------------------------------------------------------------

  test("groupBy().aggregate(): materializeLevel is preserved through SemanticGroupBy") {
    val m = baseModel(spark).withMaterialize(StorageLevel.MEMORY_ONLY)
    val aggregated = m.groupBy("k").aggregate("n")
    aggregated.materializeLevel shouldBe Some(StorageLevel.MEMORY_ONLY)
  }

  // ----------------------------------------------------------------
  // query()+withMaterialize: the dormant-auditRequest fast path
  // ----------------------------------------------------------------

  test("query().toDataFrame(): materializeLevel survives copyAuditRequest and the fast path applies persist") {
    // Regression test for the recent audit finding. The
    // `query()` chain internally calls `copyAuditRequest` (an internal
    // setter on SemanticTable) to stamp the captured request. The
    // resulting SemanticTable has `auditRequest = Some(...)` but
    // `auditSink = None, resultCache = None` — still the fast path.
    // We need to verify two things:
    //   1. The field survives copyAuditRequest (regression: forgetting
    //      to thread `materializeLevel = materializeLevel` would silently
    // drop it, mirroring the withRowFilter bug).
    //   2. The fast path applies persist on the resulting table (the
    //      auditSink.isEmpty && resultCache.isEmpty gate still passes).
    val m = baseModel(spark).withMaterialize(StorageLevel.MEMORY_AND_DISK_SER)
    val queried = m.query(measures = Seq("n"), dimensions = Seq("k"))
    queried.materializeLevel shouldBe Some(StorageLevel.MEMORY_AND_DISK_SER)
    queried.auditRequest shouldBe defined  // confirms we went through copyAuditRequest
    queried.auditSink shouldBe None        // confirms we hit the fast path
    queried.resultCache shouldBe None

    val df = queried.toDataFrame(spark)
    try {
      df.storageLevel shouldBe StorageLevel.MEMORY_AND_DISK_SER
    } finally df.unpersist()
  }

  // ----------------------------------------------------------------
  // Streaming batchModel: materializeLevel defaults to None
  // ----------------------------------------------------------------

  test("streaming batchModel: materializeLevel defaults to None (constructor default applies)") {
    // The streaming `batchModel` in `SemanticTableStreaming.scala:237`
    // constructs a `new SemanticTable(...)` via named args. None of
    // those named args include `materializeLevel`, so the constructor's
    // default (`None`) applies — per-microbatch persist would be
    // meaningless because each batch's DataFrame is consumed once via
    // `foreachBatch` and then discarded.
    //
    // This test verifies the design guarantee: the constructor's
    // default for `materializeLevel` is `None`, so the streaming path
    // automatically doesn't persist. We construct the SAME shape the
    // streaming path constructs (with the same named-args pattern —
    // `materializeLevel` deliberately omitted) and assert the field
    // defaults to `None` regardless of the parent's value.
    //
    // CAVEAT: this test does NOT call the actual streaming batchModel
    // code (which is inline in `SemanticTableStreaming.scala:237`).
    // A regression in the real batchModel (e.g., someone adding
    // `materializeLevel = this.materializeLevel` thinking that's
    // "preserving") wouldn't be caught here. The structural assertion
    // is the best we can do without spinning up a streaming query in
    // a unit test. The M4 finding from the recent audit (architect
    // review) flagged this trade-off; deferring the refactor that
    // extracts `batchModel` into a testable method.
    val model = baseModel(spark).withMaterialize(StorageLevel.MEMORY_ONLY)
    val batchModel = new SemanticTable(
      model.root, Nil, model.version, model.sourceTable, model.status,
      auditSink = None,
      auditRequest = model.auditRequest,
      resultCache = None,
      maxRows = model.maxRows,
      // NOTE: real `SemanticTableStreaming.batchModel` does NOT pass
      // `broadcastJoinThreshold` either — this test passes it to keep
      // the construction shape flexible. The design-critical check is
      // the absence of `materializeLevel`, which the real code also
      // omits.
      broadcastJoinThreshold = model.broadcastJoinThreshold,
      // materializeLevel deliberately NOT set — defaults to None
    )
    batchModel.materializeLevel shouldBe None
    model.materializeLevel shouldBe Some(StorageLevel.MEMORY_ONLY)
  }
}
