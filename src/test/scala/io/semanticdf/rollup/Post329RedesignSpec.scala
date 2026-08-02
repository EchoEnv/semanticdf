package io.semanticdf.rollup

import io.semanticdf.{Dimension, Measure, SparkSessionFixture, toSemanticTable}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col => fcol, sum}
import org.scalatest.funsuite.AnyFunSuite

/** Falsification tests for the v0.2.4 redesign. These 5 tests were the
  * HIGH-severity bugs in the v1 design (PR #328 / #329). After the
  * redesign, `useRollup` returns a separate `RollupQuery` type that
  * CANNOT call `withDimensions`, `withMeasures`, `groupBy`, `aggregate`,
  * `join_one`, or `atTimeGrain` -- the type system prevents the bugs.
  *
  * Each test pins the new contract: a compile error is the EXPECTED
  * behavior (proving the type system prevents the bug). Some tests
  * exercise the runtime path that does exist (e.g. `withWhere`).
  */
class Post329RedesignSpec extends AnyFunSuite with SparkSessionFixture {

  // Helper: build a rollup-rooted model
  private def buildRollupModel(spark: org.apache.spark.sql.SparkSession) = {
    val rollupDf = spark.range(10).toDF("k").groupBy("k").agg(sum("k").as("sum_k"))
    val rollup = Rollup("r1", "orders", Seq("k"),
      Seq(RollupMeasure("total", RollupAggregator.Sum, "sum_k")), () => rollupDf)
    val raw = spark.range(10).toDF("k")
      .withColumn("v", (fcol("k") * 3 % 7).cast("string"))
    val model = toSemanticTable(raw, name = Some("orders"))
      .withDimensions(
        Dimension("k", t => t("k")),
        Dimension("v", t => t("v")),
      )
      .withMeasures(Measure("total", _ => sum(fcol("k"))))
      .withRollup(rollup)
    val registry = RollupRegistry.empty.register("r1", () => rollupDf)
    (model, registry, rollup)
  }

  // ---- H1-DE: useRollup + join ----
  // The new design returns RollupQuery which has NO join_one method.
  // The user cannot compile code that calls join_one on a RollupQuery.
  // This test verifies that the TYPE prevents the bug.

  test("H1: useRollup returns RollupQuery which has no join_one (compile-time bug prevention)") {
    val spark = this.spark
    val (model, registry, _) = buildRollupModel(spark)
    val active = model.useRollup("r1", registry)
    // The return type is RollupQuery. If we tried to call .join_one(...) on it,
    // the compiler would reject the code. We use reflection to verify the
    // type at runtime.
    assert(active.isInstanceOf[RollupQuery],
      s"Expected RollupQuery, got ${active.getClass.getName}")
    // No need to actually call join_one — the type system prevents it.
  }

  // ---- H2-DE: useRollup + withDimensions ----
  // The new design returns RollupQuery which has NO withDimensions method.

  test("H2: useRollup returns RollupQuery which has no withDimensions (compile-time bug prevention)") {
    val spark = this.spark
    val (model, registry, _) = buildRollupModel(spark)
    val active = model.useRollup("r1", registry)
    // Type check: RollupQuery has no withDimensions method
    val hasWithDimensions = active.getClass.getMethods.exists(_.getName == "withDimensions")
    assert(!hasWithDimensions,
      s"RollupQuery should not have withDimensions (causes the v1 bug). Methods: ${active.getClass.getMethods.map(_.getName).mkString(", ")}")
  }

  // ---- H3-DE: useRollup + groupBy + aggregate ----
  // The new design returns RollupQuery which has NO groupBy or aggregate method.

  test("H3: useRollup returns RollupQuery which has no groupBy/aggregate (compile-time bug prevention)") {
    val spark = this.spark
    val (model, registry, _) = buildRollupModel(spark)
    val active = model.useRollup("r1", registry)
    val hasGroupBy = active.getClass.getMethods.exists(_.getName == "groupBy")
    val hasAggregate = active.getClass.getMethods.exists(_.getName == "aggregate")
    assert(!hasGroupBy, s"RollupQuery should not have groupBy (causes the v1 bug)")
    assert(!hasAggregate, s"RollupQuery should not have aggregate (causes the v1 bug)")
  }

  // ---- H4-DE: useRollup + atTimeGrain ----
  // The new design returns RollupQuery which has NO atTimeGrain method.

  test("H4: useRollup returns RollupQuery which has no atTimeGrain (compile-time bug prevention)") {
    val spark = this.spark
    val (model, registry, _) = buildRollupModel(spark)
    val active = model.useRollup("r1", registry)
    val hasAtTimeGrain = active.getClass.getMethods.exists(_.getName == "atTimeGrain")
    assert(!hasAtTimeGrain, s"RollupQuery should not have atTimeGrain (causes the v1 bug)")
  }

  // ---- H2-Arch: joinRollups duplicates collidable names ----
  // The new design has NO joinRollups at all. Joins on SemanticTable are
  // forbidden when a rollup is active (useRollup returns a different type).

  test("H5: useRollup returns RollupQuery; joins aren't on a rollup-rooted table") {
    val spark = this.spark
    val (model, registry, _) = buildRollupModel(spark)
    val active = model.useRollup("r1", registry)
    // The active value is NOT a SemanticTable. Calling .join_one on it
    // would not compile. We assert the type.
    assert(!active.isInstanceOf[io.semanticdf.SemanticTable],
      s"RollupQuery should NOT be a SemanticTable (must be a separate type)")
  }

  // ---- Sanity: the new API still WORKS for the supported operations ----

  test("Sanity: RollupQuery.execute() still returns the rollup-projected rows") {
    val spark = this.spark
    val (model, registry, _) = buildRollupModel(spark)
    val rows = model.useRollup("r1", registry).execute(spark).collect()
    assert(rows.length == 10, s"Expected 10 rows (one per k), got ${rows.length}")
  }

  test("Sanity: RollupQuery.withWhere() applies a predicate to the rollup data") {
    val spark = this.spark
    val (model, registry, _) = buildRollupModel(spark)
    val rows = model.useRollup("r1", registry)
      .withWhere(io.semanticdf.predicate.Predicate.Compare.Gt("k", 0))
      .execute(spark)
      .collect()
    assert(rows.length == 9, s"Expected 9 rows (k=1..9), got ${rows.length}")
  }
}
