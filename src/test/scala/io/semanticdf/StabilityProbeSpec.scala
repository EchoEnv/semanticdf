package io.semanticdf

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

  test("EDGE: column name collision after join — clear semanticdf error before query") {
    val session = spark
    import session.implicits._
    // Both tables have a dimension named 'shared' — collision risk.
    // semanticdf should detect this at JOIN time and throw a helpful error,
    // not let Spark throw AMBIGUOUS_REFERENCE at query execution time.
    val left = Seq(("AA", "L1"), ("BB", "L2")).toDF("carrier", "shared")
    val right = Seq(("AA", "R1"), ("BB", "R2")).toDF("carrier", "shared")

    val leftModel = toSemanticTable(left, name = Some("left"))
      .withDimensions(
        Dimension("carrier", t => t("carrier")),
        Dimension("shared", t => t("shared")),
      )
      .withMeasures(Measure("cnt", t => count(t("carrier"))))
    val rightModel = toSemanticTable(right, name = Some("right"))
      .withDimensions(
        Dimension("carrier", t => t("carrier")),
        Dimension("shared", t => t("shared")),  // also on right → real collision
      )

    val joined = leftModel.join_one(rightModel, (l, r) => l("carrier") === r("carrier"))

    // The collision is detected when the joined model is COMPILED, not at groupBy/aggregate.
    val ex = intercept[IllegalArgumentException] {
      joined.toDataFrame(spark).collect()  // compile happens here
    }
    info(s"Collision error: ${ex.getMessage}")
    assert(ex.getMessage.contains("collision"),
      s"Expected semanticdf 'collision' error, got: ${ex.getMessage}")
    assert(ex.getMessage.contains("shared"),
      s"Error should mention the conflicting dimension name: ${ex.getMessage}")
    assert(ex.getMessage.contains("'shared'") || ex.getMessage.contains("shared"),
      s"Error should name the conflicting dim: ${ex.getMessage}")
  }

  test("EDGE: collision check ALLOWS the join key itself to collide (necessary)") {
    // The join key 'carrier' exists on both sides by design. That collision
    // must NOT trigger our check — it's the whole point of the join.
    val session = spark
    import session.implicits._
    val left = Seq(("AA", 100), ("BB", 200)).toDF("carrier", "val")
    val right = Seq(("AA", "American"), ("BB", "United")).toDF("carrier", "name")

    val leftModel = toSemanticTable(left, name = Some("flights"))
      .withDimensions(Dimension("carrier", t => t("carrier")))
      .withMeasures(Measure("val", t => sum(t("val"))))
    val rightModel = toSemanticTable(right, name = Some("carriers"))
      .withDimensions(
        Dimension("carrier", t => t("carrier")),  // join key — allowed to collide
        Dimension("name", t => t("name")),
      )

    val joined = leftModel.join_one(rightModel, (l, r) => l("carrier") === r("carrier"))
    val rows = joined.groupBy("carrier").aggregate("val").toDataFrame(spark).collect()
    assert(rows.length == 2, s"Join should succeed, got ${rows.length} rows")
    info(s"Join-key collision case produced ${rows.length} rows")
  }

  test("EDGE: where().groupBy().aggregate() on a JOINED model works") {
    // Previously failed: SemanticAggregateOp.resolveModel didn't walk through
    // SemanticFilterOp before looking for SemanticJoinOp, so a filter wrapping
    // a join would throw IllegalStateException. This regression test ensures
    // the standard query pattern works end-to-end.
    import Predicate._
    val session = spark
    import session.implicits._
    val left = Seq(("AA", "JFK", 100), ("AA", "LAX", 200), ("BB", "SFO", 50))
      .toDF("carrier", "origin", "val")
    val right = Seq(("AA", "American"), ("BB", "BigBrand"))
      .toDF("carrier", "name")
    val leftModel = toSemanticTable(left, name = Some("left"))
      .withDimensions(Dimension("carrier", t => t("carrier")), Dimension("origin", t => t("origin")))
      .withMeasures(Measure("val", t => org.apache.spark.sql.functions.sum(t("val"))))
    val rightModel = toSemanticTable(right, name = Some("right"))
      .withDimensions(Dimension("carrier", t => t("carrier")), Dimension("name", t => t("name")))
    val joined = leftModel.join_one(rightModel, (l, r) => l("carrier") === r("carrier"))

    val rows = joined
      .where("carrier" === "AA")
      .groupBy("origin")
      .aggregate("val")
      .toDataFrame(spark)
      .collect()
    assert(rows.nonEmpty, "Filter + groupBy + aggregate on joined model should produce rows")
    info(s"Filter+groupBy+agg on joined model: ${rows.length} rows")
  }
}
