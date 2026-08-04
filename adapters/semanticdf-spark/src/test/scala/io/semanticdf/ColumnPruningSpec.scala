package io.semanticdf

import org.apache.spark.sql.functions._
import org.scalatest.funsuite.AnyFunSuite

/** Tier 1.1 — column pruning (lazy measure evaluation).
  *
  * Regression tests that lock in the behavior: requesting fewer measures
  * in `aggregate(...)` must result in fewer columns in the Spark plan.
  * The infrastructure (`compileWithBase` filtering by `measuresToCompute`)
  * has existed since Phase 2a but was never tested directly.
  *
  * If someone removes the filter, these tests catch it. */
class ColumnPruningSpec extends AnyFunSuite with SparkSessionFixture {

  /** Build a wide model with 6 measures for the column-pruning tests. */
  private def wideModel(spark: org.apache.spark.sql.SparkSession) = {
    import spark.implicits._
    val df = Seq(
      ("AA", 100, 1.0, "x"), ("UA", 200, 2.0, "y"), ("DL", 300, 3.0, "z")
    ).toDF("carrier", "pax", "dist", "tag")
    toSemanticTable(df, name = Some("wide"))
      .withDimensions(Dimension("carrier", t => t("carrier")))
      .withMeasures(
        Measure("m_pax",       t => sum(t("pax"))),
        Measure("m_dist",      t => sum(t("dist"))),
        Measure("m_cnt",       t => count(t("pax"))),
        Measure("m_avg_pax",   t => avg(t("pax"))),
        Measure("m_max_dist",  t => max(t("dist"))),
        Measure("m_min_pax",   t => min(t("pax"))),
        Measure("m_revenue",   t => sum(t("pax") * t("dist"))),
        Measure("m_calc_ratio",t => t("m_pax") / t("m_cnt")),  // depends on m_pax, m_cnt
      )
  }

  test("PRUNE: requesting 1 measure → Spark plan computes only that measure") {
    val model = wideModel(spark)
    val df = model.groupBy("carrier").aggregate("m_pax").toDataFrame(spark)

    // Spark qualifies columns with #N, so use regex to match either `sum(pax)` or
    // `sum(pax#123)`. Use (?i) for case-insensitive (function names are sometimes uppercased).
    val plan = df.queryExecution.explainString(
      org.apache.spark.sql.execution.ExplainMode.fromString("simple")
    )
    assert(plan.matches("(?is).*sum\\s*\\(pax(\\b|#\\d+).*"),
      s"Expected sum(pax) in plan, got:\n$plan")
    assert(!plan.matches("(?is).*sum\\s*\\(dist(\\b|#\\d+).*"),
      s"Did NOT expect sum(dist) in plan, got:\n$plan")
    assert(!plan.matches("(?is).*count\\s*\\(pax(\\b|#\\d+).*"),
      s"Did NOT expect count(pax) in plan, got:\n$plan")
  }

  test("PRUNE: requesting 2 measures → Spark plan computes only those 2") {
    val model = wideModel(spark)
    val df = model.groupBy("carrier")
      .aggregate("m_pax", "m_dist")
      .toDataFrame(spark)
    val plan = df.queryExecution.explainString(
      org.apache.spark.sql.execution.ExplainMode.fromString("simple")
    )
    assert(plan.matches("(?is).*sum\\s*\\(pax(\\b|#\\d+).*"),
      s"Expected sum(pax) in plan, got:\n$plan")
    assert(plan.matches("(?is).*sum\\s*\\(dist(\\b|#\\d+).*"),
      s"Expected sum(dist) in plan, got:\n$plan")
    assert(!plan.matches("(?is).*count\\s*\\(pax(\\b|#\\d+).*"),
      s"Did NOT expect count(pax) in plan, got:\n$plan")
    assert(!plan.matches("(?is).*avg\\s*\\("),
      s"Did NOT expect avg() in plan, got:\n$plan")
  }

  test("PRUNE: requesting a calc pulls its transitive deps (m_pax, m_cnt)") {
    // m_calc_ratio depends on m_pax and m_cnt. When we request m_calc_ratio,
    // the framework should auto-include m_pax and m_cnt in the aggregation.
    val model = wideModel(spark)
    val df = model.groupBy("carrier").aggregate("m_calc_ratio").toDataFrame(spark)
    val plan = df.queryExecution.explainString(
      org.apache.spark.sql.execution.ExplainMode.fromString("simple")
    )
    // m_pax, m_cnt, and m_calc_ratio should all appear. Spark may optimize
    // count(pax) → count(1) when pax has no nulls, so we accept either form.
    assert(plan.matches("(?is).*sum\\s*\\(pax(\\b|#\\d+).*"),
      s"Expected sum(pax) in plan (transitive dep), got:\n$plan")
    assert(plan.matches("(?is).*count\\s*(\\(1\\)|\\(pax(\\b|#\\d+)\\)).*"),
      s"Expected count() in plan (transitive dep), got:\n$plan")
    // Unrelated measures should NOT appear.
    assert(!plan.matches("(?is).*sum\\s*\\(dist(\\b|#\\d+).*"),
      s"Did NOT expect sum(dist) in plan, got:\n$plan")
    assert(!plan.matches("(?is).*avg\\s*\\("),
      s"Did NOT expect avg() in plan, got:\n$plan")
    assert(!plan.matches("(?is).*max\\s*\\("),
      s"Did NOT expect max() in plan, got:\n$plan")
  }

  test("PRUNE: requesting calc + unrelated measure — both transitively-required base measures come along") {
    // Request m_calc_ratio (needs m_pax, m_cnt) and m_max_dist (independent).
    // All 4 (m_pax, m_cnt, m_max_dist, m_calc_ratio) should be in the plan.
    // Unrelated (m_dist, m_avg_pax, m_min_pax) should NOT.
    val model = wideModel(spark)
    val df = model.groupBy("carrier")
      .aggregate("m_calc_ratio", "m_max_dist")
      .toDataFrame(spark)
    val plan = df.queryExecution.explainString(
      org.apache.spark.sql.execution.ExplainMode.fromString("simple")
    )
    assert(plan.matches("(?is).*sum\\s*\\(pax(\\b|#\\d+).*"),
      s"Expected sum(pax) in plan, got:\n$plan")
    assert(plan.matches("(?is).*count\\s*(\\(1\\)|\\(pax(\\b|#\\d+)\\)).*"),
      s"Expected count() in plan, got:\n$plan")
    assert(plan.matches("(?is).*max\\s*\\(dist(\\b|#\\d+).*"),
      s"Expected max(dist) in plan, got:\n$plan")
    assert(!plan.matches("(?is).*sum\\s*\\(dist(\\b|#\\d+).*"),
      s"Did NOT expect sum(dist) in plan, got:\n$plan")
    assert(!plan.matches("(?is).*avg\\s*\\(pax(\\b|#\\d+).*"),
      s"Did NOT expect avg(pax) in plan, got:\n$plan")
    assert(!plan.matches("(?is).*min\\s*\\(pax(\\b|#\\d+).*"),
      s"Did NOT expect min(pax) in plan, got:\n$plan")
  }

  test("PRUNE: requesting NO measures throws IllegalArgumentException") {
    val model = wideModel(spark)
    val ex = intercept[IllegalArgumentException] {
      model.groupBy("carrier").aggregate().toDataFrame(spark)
    }
    assert(ex.getMessage.contains("at least one measure"),
      s"Expected 'at least one measure' error, got: ${ex.getMessage}")
  }

  test("PRUNE: requesting a non-existent measure throws IllegalArgumentException") {
    val model = wideModel(spark)
    val ex = intercept[IllegalArgumentException] {
      model.groupBy("carrier").aggregate("m_pax", "nonexistent_measure").toDataFrame(spark)
    }
    assert(ex.getMessage.contains("nonexistent_measure"),
      s"Expected error mentioning the bad name, got: ${ex.getMessage}")
  }

  test("PRUNE: result schema only has requested columns + group keys") {
    val model = wideModel(spark)
    val df = model.groupBy("carrier").aggregate("m_pax").toDataFrame(spark)
    val cols = df.columns.toSet
    // Should have carrier (group key) and m_pax (requested measure).
    assert(cols == Set("carrier", "m_pax"),
      s"Expected only carrier + m_pax, got: $cols")
  }

  test("PRUNE: result schema includes transitively-required base measures too") {
    // m_calc_ratio needs m_pax and m_cnt. The result should include all three
    // (m_calc_ratio as the requested measure, m_pax and m_cnt because they're
    // inputs to the calc — the calc layer needs them in scope).
    val model = wideModel(spark)
    val df = model.groupBy("carrier").aggregate("m_calc_ratio").toDataFrame(spark)
    val cols = df.columns.toSet
    assert(cols.contains("carrier"), s"Expected carrier in $cols")
    assert(cols.contains("m_calc_ratio"), s"Expected m_calc_ratio in $cols")
    // m_pax and m_cnt should be present in the output (the calc layer uses them).
    assert(cols.contains("m_pax"), s"Expected m_pax in $cols (needed by m_calc_ratio)")
    assert(cols.contains("m_cnt"), s"Expected m_cnt in $cols (needed by m_calc_ratio)")
  }
}
