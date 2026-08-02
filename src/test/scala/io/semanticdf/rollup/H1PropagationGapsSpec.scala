package io.semanticdf.rollup

import io.semanticdf.{Dimension, Measure, SparkSessionFixture, toSemanticTable}
import org.scalatest.funsuite.AnyFunSuite

/** Targeted tests for the 9 H1 propagation sites missed by the
  * initial fix. Each test demonstrates that the rollup metadata
  * is silently dropped by a fluent-chain operation that doesn't
  * propagate it.
  *
  * Sites covered:
  *   - H1-α: withRowFilter  (SemanticTableCore.scala:325)
  *   - H1-β: withSalt       (SemanticTableCore.scala:497)
  *   - H1-γ: join_on        (SemanticTableMutation.scala ~755)
  *   - H1-δ: groupBy+aggregate (SemanticTableCollection.scala:531)
  *   - H1-ε: manifest round-trip (SemanticManifest.scala:836, 1093)
  */
class H1PropagationGapsSpec extends AnyFunSuite with SparkSessionFixture {

  private def buildModel(spark: org.apache.spark.sql.SparkSession, rollupName: String = "r1") = {
    import org.apache.spark.sql.functions.{col, sum}
    val src = spark.range(5).toDF("k").groupBy("k").agg(sum("k").as("sum_k"))
    val rollup = Rollup(
      rollupName, "orders", Seq("k"),
      Seq(RollupMeasure("total", "sum", "sum_k")),
      () => src,
    )
    val model = toSemanticTable(spark.range(5).toDF("k"), name = Some("orders"))
      .withDimensions(Dimension("k", t => t("k")))
      .withMeasures(Measure("total", _ => sum(col("k"))))
      .withRollup(rollup)
    (model, rollup)
  }

  test("H1-α: withRowFilter preserves rollup") {
    val spark = this.spark
    val (model, _) = buildModel(spark)
    val after = model.withRowFilter("r1", "k > 0", None, Map.empty)
    assert(after.listRollups().map(_.name) == List("r1"),
      s"H1-α REAL: withRowFilter dropped rollup, got ${after.listRollups().map(_.name)}")
  }

  test("H1-β: withSalt preserves rollup") {
    val spark = this.spark
    val (model, _) = buildModel(spark)
    val after = model.withSalt(5)
    assert(after.listRollups().map(_.name) == List("r1"),
      s"H1-β REAL: withSalt dropped rollup, got ${after.listRollups().map(_.name)}")
  }

  test("H1-γ: join_on drops rollup (Path-2 contract — see H1Path2JoinDropsRollupSpec)") {
    // Per v0.2.4 redesign (Post329RedesignSpec.scala H5):
    // rollups are bound to a specific base model. Joins on SemanticTable
    // are forbidden when a rollup is active. After join_on, the joined
    // model has empty rollups. Re-register on the joined model if needed.
    val spark = this.spark
    val (model, _) = buildModel(spark)
    val dim = toSemanticTable(spark.range(5).toDF("k"), name = Some("dim"))
      .withDimensions(Dimension("k", t => t("k")))
    val after = model.join_on(dim, ("k", "k"))
    assert(after.listRollups().isEmpty,
      s"H1-γ Path-2: rollup should be dropped by join, got ${after.listRollups().map(_.name)}")
  }

  test("H1-δ: groupBy+aggregate preserves rollup") {
    val spark = this.spark
    val (model, _) = buildModel(spark)
    val after = model.groupBy("k").aggregate("total")
    assert(after.listRollups().map(_.name) == List("r1"),
      s"H1-δ REAL: groupBy+aggregate dropped rollup, got ${after.listRollups().map(_.name)}")
  }

  test("H1-ε: manifest round-trip drops rollups (documented lossy contract)") {
    // Per the H1-ε fix: rollups don't survive manifest round-trip
    // because manifest can't carry the sourceProvider function.
    // After fromJson, the model has empty rollups — user must re-register.
    val spark = this.spark
    import org.apache.spark.sql.functions.{col, sum}
    import io.semanticdf.adapters.SemanticManifest
    val src = spark.range(5).toDF("k").groupBy("k").agg(sum("k").as("sum_k"))
    val rollup = Rollup(
      "r1", "orders", Seq("k"),
      Seq(RollupMeasure("total", "sum", "sum_k")),
      () => src,
    )
    val (model, _) = buildModel(spark)
    assert(model.listRollups().map(_.name) == List("r1"))
    val json = SemanticManifest.toJson(model)
    val restored = SemanticManifest.fromJson(json, src)
    assert(restored.listRollups().isEmpty,
      s"H1-ε: manifest round-trip should drop rollups, got ${restored.listRollups().map(_.name)}")
  }
}
