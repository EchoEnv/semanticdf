package io.semanticdf

import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{count, lit, sum}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Phase A — correctness hardening regression tests.
  *
  * These were written by first *reproducing* each edge case (debug-mantra #1) and
  * observing Spark's actual behavior, then converting the observations into permanent
  * assertions. Two real framework bugs surfaced and were fixed in the process:
  *
  *   1. The calc classifier mis-handled `Measure("x", t => sum(t("x")))` — the common
  *      "measure aggregates a same-named base column" pattern — as a self-dependency,
  *      producing a false "calc cycle" error. Fixed in `classifyOne` / `transitiveClosure`
  *      by excluding the measure's own name from its dependency probe.
  *
  * Everything else here documents *correct* pass-through Spark SQL semantics: nulls,
  * decimals, and join-key null handling all behave correctly without framework help.
  * Only divide-by-zero is guarded, opt-in, via [[CalcHelpers.safeDivide]].
  */
class HardeningSpec extends AnyFunSuite with Matchers with SparkSessionFixture {

  private def df(rows: (String, Integer)*): DataFrame = {
    val schema = new StructType().add("carrier", "string").add("distance", "integer")
    val data = rows.map { case (c, d) => Row(c, d) }
    spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
  }

  // ---- Classifier self-reference fix (the real bug) --------------------------

  test("Phase A: Measure(name, sum(t(name))) — same-name aggregation is NOT a self-cycle") {
    // The common real-world pattern: a measure aggregates a base column that shares its
    // name. Before the fix this raised "calc cycle" because the classifier recorded the
    // measure as depending on itself.
    val d = {
      val session = spark
      import session.implicits._
      Seq(("AA", 100, 2), ("UA", 200, 0)).toDF("carrier", "distance", "flight_count")
    }
    val st = toSemanticTable(d, name = Some("f"))
      .withDimensions(Dimension("carrier", t => t("carrier")))
      .withMeasures(
        Measure("distance",      t => sum(t("distance"))),       // same name as base col
        Measure("flight_count",  t => sum(t("flight_count"))),   // same name as base col
        Measure("avg",           t => t("distance") / t("flight_count")),  // real calc
      )
    val rows = st.groupBy("carrier").aggregate("distance", "avg").execute(spark).collect()
      .map(r => r.getAs[String]("carrier") -> r.getAs[Double]("avg"))
      .toMap
    rows("AA") shouldBe 50.0 +- 1e-9   // 100/2
    // UA: 200/0 — Spark returns null (correct SQL semantics). Documented below.
    rows.get("UA") shouldBe Some(null)
  }

  // ---- Divide-by-zero: default null, opt-in safeDivide -----------------------

  test("Phase A: plain division by zero yields null (Spark SQL default, documented)") {
    val d = {
      val session = spark
      import session.implicits._
      Seq(("AA", 100, 0)).toDF("carrier", "distance", "flight_count")
    }
    val st = toSemanticTable(d, name = Some("f"))
      .withDimensions(Dimension("carrier", t => t("carrier")))
      .withMeasures(
        Measure("distance",     t => sum(t("distance"))),
        Measure("flight_count", t => sum(t("flight_count"))),
        Measure("per_flight",   t => t("distance") / t("flight_count")),
      )
    val row = st.groupBy("carrier").aggregate("per_flight").execute(spark).collect().head
    row.getAs[AnyRef]("per_flight") shouldBe null
  }

  test("Phase A: safeDivide returns the default instead of null on zero denominator") {
    val d = {
      val session = spark
      import session.implicits._
      Seq(("AA", 100, 0), ("UA", 200, 2)).toDF("carrier", "distance", "flight_count")
    }
    val st = toSemanticTable(d, name = Some("f"))
      .withDimensions(Dimension("carrier", t => t("carrier")))
      .withMeasures(
        Measure("distance",     t => sum(t("distance"))),
        Measure("flight_count", t => sum(t("flight_count"))),
        Measure("per_flight",   t => CalcHelpers.safeDivide(t("distance"), t("flight_count"), defaultValue = 0.0)),
      )
    val rows = st.groupBy("carrier").aggregate("per_flight").execute(spark).collect()
      .map(r => r.getAs[String]("carrier") -> r.getAs[Double]("per_flight"))
      .toMap
    rows("AA") shouldBe 0.0   // 100/0 guarded → 0.0 (not null)
    rows("UA") shouldBe 100.0 // 200/2 normal
  }

  // ---- Nulls: correct SQL semantics pass through (no framework bug) ----------

  test("Phase A: sum over nulls — nulls ignored (Spark SQL semantics)") {
    val st = toSemanticTable(df(("AA", null), ("AA", 100), ("UA", null)), name = Some("f"))
      .withDimensions(Dimension("carrier", t => t("carrier")))
      .withMeasures(Measure("total_distance", t => sum(t("distance"))))
    val rows = st.groupBy("carrier").aggregate("total_distance").execute(spark).collect()
      .map(r => r.getAs[String]("carrier") -> r.getAs[Any]("total_distance"))
      .toMap
    rows("AA") shouldBe 100   // null ignored
    Option(rows("UA")) shouldBe None  // sum of all-null → null (Option(null) == None)
  }

  test("Phase A: null group-by keys coalesce into one group (Spark SQL semantics)") {
    val st = toSemanticTable(df((null, 100), (null, 50), ("UA", 200)), name = Some("f"))
      .withDimensions(Dimension("carrier", t => t("carrier")))
      .withMeasures(Measure("total_distance", t => sum(t("distance"))))
    val rows = st.groupBy("carrier").aggregate("total_distance").execute(spark).collect()
      .map(r => r.getAs[String]("carrier") -> r.getAs[Int]("total_distance"))
      .toMap
    rows(null) shouldBe 150   // two null keys grouped together
    rows("UA") shouldBe 200
  }

  test("Phase A: null join keys do not match (SQL: null != null), left join keeps rows") {
    val left = {
      val session = spark
      import session.implicits._
      Seq(("k1", 100), (null, 200)).toDF("k", "v_left")
    }
    val right = {
      val session = spark
      import session.implicits._
      Seq(("k1", 10), (null, 20)).toDF("k", "v_right")
    }
    val l = toSemanticTable(left, name = Some("l"))
      .withDimensions(Dimension("k", t => t("k"))).withMeasures(Measure("lv", t => sum(t("v_left"))))
    val r = toSemanticTable(right, name = Some("r"))
      .withDimensions(Dimension("k", t => t("k"))).withMeasures(Measure("rv", t => sum(t("v_right"))))
    val rows = l.join_one(r, (a, b) => a("k") === b("k"))
      .groupBy("k").aggregate("lv", "rv").execute(spark).collect()
      .map(r => r.getAs[String]("k") -> (r.getAs[Int]("lv"), r.getAs[Any]("rv")))
      .toMap
    rows("k1") shouldBe ((100, 10))   // matched
    rows(null) shouldBe ((200, null)) // null != null → no match; left join keeps left row, rv null
  }

  // ---- Decimals: precision preserved natively by Spark (no framework bug) ----

  test("Phase A: decimal measures + calcs preserve precision (Spark-native)") {
    val schema = new StructType()
      .add("carrier", "string")
      .add("revenue", org.apache.spark.sql.types.DecimalType(18, 4))
      .add("qty", IntegerType)
    val data = Seq(
      Row("AA", new java.math.BigDecimal("100.0001"), 3),
      Row("AA", new java.math.BigDecimal("0.0001"), 1),
      Row("UA", new java.math.BigDecimal("200.5000"), 2),
    )
    val d = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    val st = toSemanticTable(d, name = Some("f"))
      .withDimensions(Dimension("carrier", t => t("carrier")))
      .withMeasures(
        Measure("revenue",     t => sum(t("revenue"))),
        Measure("qty",         t => sum(t("qty"))),
        Measure("avg_price",   t => t("revenue") / t("qty")),
        Measure("pct_revenue", t => t("revenue") / t.all("revenue")),
      )
    val rows = st.groupBy("carrier").aggregate("revenue", "avg_price", "pct_revenue")
      .execute(spark).collect().map(r => r.getAs[String]("carrier") -> r).toMap

    // AA: revenue = 100.0002 (4 decimals preserved), avg = 100.0002/4 = 25.00005,
    // pct = 100.0002 / 300.5002 ≈ 0.3328.
    rows("AA").getAs[java.math.BigDecimal]("revenue").doubleValue() shouldBe 100.0002 +- 1e-9
    rows("AA").getAs[java.math.BigDecimal]("avg_price").doubleValue() shouldBe 25.00005 +- 1e-9
    rows("AA").getAs[java.math.BigDecimal]("pct_revenue").doubleValue() shouldBe 0.3327791462 +- 1e-6

    // Type promotion is Spark's: sum(18,4)→(28,4); division promotes scale.
    val revenueType = rows("AA").schema("revenue").dataType.toString
    revenueType should include ("DecimalType(28,4)")
  }

  // ---- "Did you mean" suggestions (Tier 1.3) ----------------------------------

  test("Phase A: Unknown measure error suggests closest match") {
    val d = df("AA" -> 100, "UA" -> 200)
    val st = toSemanticTable(d, name = Some("f")).withDimensions(
      Dimension("carrier", t => t("carrier"))
    ).withMeasures(
      Measure("total_revenue",   t => sum(t("distance"))),
      Measure("avg_passengers",  t => sum(t("distance"))),
      Measure("total_count",     t => count(t("distance"))),
    )

    val ex = the[IllegalArgumentException] thrownBy st.groupBy("carrier").aggregate("total_revneue").execute(spark)
    ex.getMessage should include("Unknown measure 'total_revneue'")
    ex.getMessage should include("Did you mean: 'total_revenue'")
  }

  test("Phase A: Unknown measure suggestion works for close-but-wrong names") {
    val d = df("AA" -> 100)
    val st = toSemanticTable(d, name = Some("f")).withDimensions(
      Dimension("carrier", t => t("carrier"))
    ).withMeasures(
      Measure("total_count",  t => count(t("distance"))),
      Measure("avg_passengers", t => sum(t("distance"))),
    )

    // "totla" → "total_count" (edit distance 2: insert 'a', swap 'l')
    val ex = the[IllegalArgumentException] thrownBy st.groupBy("carrier").aggregate("totla_count").execute(spark)
    ex.getMessage should include("totla_count")
    ex.getMessage should include("Did you mean: 'total_count'")
  }

  test("Phase A: Unknown measure shows no suggestion when nothing is close") {
    val d = df("AA" -> 100)
    val st = toSemanticTable(d, name = Some("f")).withDimensions(
      Dimension("carrier", t => t("carrier"))
    ).withMeasures(
      Measure("total_revenue", t => sum(t("distance"))),
    )

    val ex = the[IllegalArgumentException] thrownBy st.groupBy("carrier").aggregate("xyz_measure").execute(spark)
    ex.getMessage should include("xyz_measure")
    // No suggestion when name is completely unrelated
    ex.getMessage should not include("Did you mean")
  }

  test("Phase A: atTimeGrain: unknown dimension suggests closest match") {
    val d = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row("AA", java.sql.Timestamp.valueOf("2024-01-01 08:00:00")),
      )),
      new StructType()
        .add("carrier", "string")
        .add("flight_date", org.apache.spark.sql.types.TimestampType),
    )
    val st = toSemanticTable(d, name = Some("f")).withDimensions(
      Dimension("carrier",    t => t("carrier")),
      Dimension.time("flight_date", t => t("flight_date"), smallestTimeGrain = Some("day")),
    ).withMeasures(
      Measure("total_revenue", t => sum(t("distance"))),
    )

    // "fligt_date" → "flight_date" (edit distance 1: missing 'h')
    val ex = the[IllegalArgumentException] thrownBy st.atTimeGrain("fligt_date", "month")
    ex.getMessage should include("fligt_date")
    ex.getMessage should include("Did you mean: 'flight_date'")
  }

  // ---- Percent-of-total cross-join: explicit broadcast hint --------------------

  test("Phase A: percent-of-total cross-join is broadcast even when auto-broadcast is disabled") {
    // The percent-of-total path cross-joins a 1-row totals table against the
    // aggregated result. With the explicit broadcast() hint the side is
    // always shipped to executors. Without the hint Catalyst usually
    // auto-broadcasts (because the side is 1 row) but isn't guaranteed.
    //
    // This test sets autoBroadcastJoinThreshold = -1 to disable Catalyst's
    // auto-broadcast decision, then asserts the physical plan still uses
    // broadcast. If the explicit hint is ever removed, this test fails.
    val schema = new org.apache.spark.sql.types.StructType()
      .add("carrier", org.apache.spark.sql.types.StringType)
      .add("passengers", org.apache.spark.sql.types.IntegerType)
    val rows = spark.sparkContext.parallelize(Seq(
      Row("AA", 100), Row("AA", 200), Row("UA", 50), Row("DL", 75),
    ))
    val d = spark.createDataFrame(rows, schema)
    val st = toSemanticTable(d, name = Some("f"))
      .withDimensions(Dimension("carrier", t => t("carrier")))
      .withMeasures(
        Measure("total_passengers",  t => sum(t("passengers"))),
        Measure("pct_of_passengers", t => t("total_passengers") / t.all("total_passengers")),
      )
      .groupBy("carrier").aggregate("pct_of_passengers")

    val originalThreshold = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
    try {
      spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
      val plan = st.execute(spark).queryExecution.explainString(
        org.apache.spark.sql.execution.ExplainMode.fromString("simple")
      )
      // Either of these physical operators is proof the side was broadcast:
      //   BroadcastExchange          — the side shipped to executors
      //   BroadcastNestedLoopJoin    — the cross-join used a broadcast side
      //   BroadcastHashJoin          — the join strategy that consumes a broadcast input
      // (Catalyst may rewrite the plan; any of these is acceptable evidence.)
      val broadcasted = plan.contains("BroadcastExchange") ||
                        plan.contains("BroadcastNestedLoopJoin") ||
                        plan.contains("BroadcastHashJoin")
      assert(broadcasted, s"Expected a broadcast in physical plan, got:\n$plan")
    } finally {
      spark.conf.set("spark.sql.autoBroadcastJoinThreshold", originalThreshold)
    }
  }
}
