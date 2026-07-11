package io.semantica

import org.apache.spark.sql.functions._
import org.scalatest.funsuite.AnyFunSuite

/** Stability probe: exercise the edge cases a first consumer would hit.
  * Each test documents whether the behavior is correct, helpful, or broken. */
class StabilityProbeSpec extends AnyFunSuite with SparkSessionFixture with FlightsFixture {

  test("EDGE: calc referencing non-existent measure — clear error?") {
    val model = toSemanticTable(flightsDf, name = Some("flights"))
      .withDimensions(Dimension("carrier", t => t("carrier")))
      .withMeasures(
        Measure("total", t => sum(t("passengers"))),
        Measure("bad_calc", t => t("nonexistent_measure") / t("total")),
      )
    val ex = intercept[Exception] {
      model.groupBy("carrier").aggregate("bad_calc").toDataFrame(spark).collect()
    }
    // Does the error tell the user what went wrong?
    val msg = ex.getMessage
    info(s"Error class: ${ex.getClass.getSimpleName}")
    info(s"Error message: $msg")
    assert(msg != null, "Error should have a message")
  }

  test("EDGE: groupBy with no dimensions — global aggregate WORKS") {
    val model = toSemanticTable(flightsDf, name = Some("flights"))
      .withMeasures(Measure("total", t => sum(t("passengers"))))
    // groupBy() with no keys — global aggregate. Should produce 1 row.
    val rows = model.groupBy().aggregate("total").toDataFrame(spark).collect()
    assert(rows.length == 1, s"Global aggregate should produce 1 row, got ${rows.length}")
    info(s"Global aggregate total: ${rows.head.getAs[Long](0)}")
  }

  test("EDGE: aggregate non-existent measure — clear error?") {
    val model = toSemanticTable(flightsDf, name = Some("flights"))
      .withDimensions(Dimension("carrier", t => t("carrier")))
      .withMeasures(Measure("total", t => sum(t("passengers"))))
    val ex = intercept[Exception] {
      model.groupBy("carrier").aggregate("nonexistent").toDataFrame(spark).collect()
    }
    info(s"aggregate('nonexistent'): ${ex.getClass.getSimpleName}: ${ex.getMessage}")
  }

  test("EDGE: groupBy non-existent dimension — clear error?") {
    val model = toSemanticTable(flightsDf, name = Some("flights"))
      .withMeasures(Measure("total", t => sum(t("passengers"))))
    val ex = intercept[Exception] {
      model.groupBy("nonexistent_dim").aggregate("total").toDataFrame(spark).collect()
    }
    info(s"groupBy('nonexistent_dim'): ${ex.getClass.getSimpleName}: ${ex.getMessage}")
  }

  test("EDGE: NULL in join key — does left join handle it?") {
    val session = spark
    import session.implicits._
    val left = Seq(("AA", 100), ("BB", 200), (null, 300)).toDF("carrier", "val")
    val right = Seq(("AA", "American"), (null, "Unknown")).toDF("carrier", "name")

    val leftModel = toSemanticTable(left, name = Some("left"))
      .withDimensions(Dimension("carrier", t => t("carrier")))
      .withMeasures(Measure("val", t => sum(t("val"))))
    val rightModel = toSemanticTable(right, name = Some("right"))
      .withDimensions(Dimension("carrier", t => t("carrier")))

    val joined = leftModel.join_one(rightModel, (l, r) => l("carrier") === r("carrier"))
    val rows = joined.groupBy("carrier").aggregate("val").toDataFrame(spark).collect()
    info(s"NULL join key produced ${rows.length} rows")
    rows.foreach(r => info(s"  $r"))
  }

  test("EDGE: wide model — 30 calc measures, dependency chain") {
    val session = spark
    import session.implicits._
    val df = Seq((1, "a", 10), (2, "b", 20), (3, "a", 30)).toDF("id", "k", "v")
    var model = toSemanticTable(df, name = Some("wide"))
      .withDimensions(Dimension("k", t => t("k")))
      .withMeasures(Measure("m0", t => sum(t("v"))))

    // Chain: m1 = m0 / m0, m2 = m1 + m0, ... m29
    (1 to 29).foreach { i =>
      model = model.withMeasures(Measure(s"m$i", t => t(s"m${i-1}") + t("m0")))
    }
    val t0 = System.nanoTime()
    val rows = model.groupBy("k").aggregate("m29").toDataFrame(spark).collect()
    val elapsed = (System.nanoTime() - t0) / 1e6
    info(f"30-measure calc chain: ${rows.length} rows in ${elapsed}%.1fms")
    assert(rows.nonEmpty)
  }

  test("EDGE: column name collision after join — KNOWN ISSUE (ambiguous reference)") {
    val session = spark
    import session.implicits._
    // Both tables have a column named 'shared' — collision risk.
    // Known issue: Spark throws AMBIGUOUS_REFERENCE at execution time, not a helpful
    // semantica error. Consumers should alias conflicting columns or reference them
    // by their prefixed join name (e.g. "right.shared").
    val left = Seq(("AA", "L1"), ("BB", "L2")).toDF("carrier", "shared")
    val right = Seq(("AA", "R1"), ("BB", "R2")).toDF("carrier", "shared")

    val leftModel = toSemanticTable(left, name = Some("left"))
      .withDimensions(
        Dimension("carrier", t => t("carrier")),
        Dimension("shared", t => t("shared")),
      )
      .withMeasures(Measure("cnt", t => count(t("carrier"))))
    val rightModel = toSemanticTable(right, name = Some("right"))
      .withDimensions(Dimension("carrier", t => t("carrier")))

    val joined = leftModel.join_one(rightModel, (l, r) => l("carrier") === r("carrier"))
    val ex = intercept[Exception] {
      joined.groupBy("carrier", "shared").aggregate("cnt").toDataFrame(spark).collect()
    }
    info(s"Collision error: ${ex.getClass.getSimpleName}: ${ex.getMessage}")
    assert(ex.getMessage.contains("ambiguous") || ex.getMessage.contains("AMBIGUOUS"),
      s"Expected ambiguous reference error, got: ${ex.getMessage}")
  }
}
