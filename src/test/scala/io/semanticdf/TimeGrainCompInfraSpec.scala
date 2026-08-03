package io.semanticdf

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Companion-infrastructure tests for v0.2.4 manual rollups companion infrastructure.
  *
  * Pins the contracts of `TimeGrain.finer` / `finerOrEqual` and
  * `SemanticTable.findDimensionTimeGrain`. These are reused by
  * auto-routing (v0.4.0) and exist so that the manual-rollups feature
  * and the future auto-routing feature share the same time-grain
  * comparison semantics.
  */
class TimeGrainCompInfraSpec extends AnyFunSuite with Matchers {

  // ---- TimeGrain.finer / finerOrEqual ----

  test("finerOrEqual: same grain is at-least-as-fine as itself") {
    TimeGrain.finerOrEqual("DAY", "DAY") shouldBe true
  }

  test("finerOrEqual: DAY is at-least-as-fine as MONTH") {
    TimeGrain.finerOrEqual("DAY", "MONTH") shouldBe true
  }

  test("finerOrEqual: MONTH is NOT at-least-as-fine as DAY (coarser)") {
    TimeGrain.finerOrEqual("MONTH", "DAY") shouldBe false
  }

  test("finerOrEqual: HOUR is at-least-as-fine as YEAR (4 levels finer)") {
    TimeGrain.finerOrEqual("HOUR", "YEAR") shouldBe true
  }

  test("finer: DAY is strictly finer than MONTH") {
    TimeGrain.finer("DAY", "MONTH") shouldBe true
  }

  test("finer: same grain is NOT strictly finer") {
    TimeGrain.finer("DAY", "DAY") shouldBe false
  }

  test("finer: coarser is NOT strictly finer") {
    TimeGrain.finer("MONTH", "DAY") shouldBe false
  }

  test("finerOrEqual: unknown grain throws IllegalArgumentException (parse don't validate)") {
    intercept[IllegalArgumentException] {
      TimeGrain.finerOrEqual("houer", "DAY")  // typo
    }
  }

  test("finerOrEqual: lowercase grain normalizes correctly") {
    // normalize happens at registration; finerOrEqual operates on canonical.
    // Verified separately via normalize round-trip.
    val n1 = TimeGrain.normalize("day")
    val n2 = TimeGrain.normalize("month")
    TimeGrain.finerOrEqual(n1, n2) shouldBe true
  }

  // ---- SemanticTable.findDimensionTimeGrain ----

  test("findDimensionTimeGrain: time dimension returns its declared smallest grain (normalized)") {
    val df = spark.range(10).toDF("k").withColumn("ts", org.apache.spark.sql.functions.lit("2024-01-01"))
    val model = toSemanticTable(df, name = Some("m"))
      .withDimensions(
        Dimension("k", t => t("k")),
        Dimension.time("ts", t => t("ts"), smallestTimeGrain = Some("day")),
      )
    model.findDimensionTimeGrain("ts") shouldBe Some("DAY")
  }

  test("findDimensionTimeGrain: lowercase grain input is normalized") {
    val df = spark.range(10).toDF("k").withColumn("ts", org.apache.spark.sql.functions.lit("2024-01-01"))
    val model = toSemanticTable(df, name = Some("m"))
      .withDimensions(
        Dimension("k", t => t("k")),
        Dimension.time("ts", t => t("ts"), smallestTimeGrain = Some("month")),
      )
    model.findDimensionTimeGrain("ts") shouldBe Some("MONTH")
  }

  test("findDimensionTimeGrain: non-time dimension returns None") {
    val df = spark.range(10).toDF("k")
    val model = toSemanticTable(df, name = Some("m"))
      .withDimensions(Dimension("k", t => t("k")))
    model.findDimensionTimeGrain("k") shouldBe None
  }

  test("findDimensionTimeGrain: unknown dimension returns None") {
    val df = spark.range(10).toDF("k")
    val model = toSemanticTable(df, name = Some("m"))
      .withDimensions(Dimension("k", t => t("k")))
    model.findDimensionTimeGrain("nonexistent") shouldBe None
  }

  // Access to spark fixture via mixin
  private def spark: org.apache.spark.sql.SparkSession = {
    import org.apache.spark.sql.SparkSession
    SparkSession.builder().master("local[1]").config("spark.ui.enabled", "false").getOrCreate()
  }
}