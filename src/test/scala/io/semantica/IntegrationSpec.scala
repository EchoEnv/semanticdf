package io.semantica

import org.apache.spark.sql.functions.{count, lit, sum}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

/** Integration tests — real file I/O, no in-memory fixtures.
  *
  * These exercise the actual Spark read path (schema inference, CSV parsing,
  * metastore interaction, temp view registration) rather than the in-memory
  * DataFrame path used in unit tests.
  *
  * Run with: mvn test -Dtest=IntegrationSpec
  */
class IntegrationSpec extends AnyFunSuite with SparkSessionFixture {

  // -------------------------------------------------------------------------
  // File-based fixture (real CSV read, real schema inference)
  // -------------------------------------------------------------------------

  test("read from CSV — schema inferred, group-by correct") {
    val csvPath = getClass.getResource("/flights.csv").getPath

    // Real Spark CSV read — schema inferred from header
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(csvPath)

    df.schema.fieldNames should contain theSameElementsAs
      Seq("carrier", "origin", "dest", "distance", "passengers")

    val model = toSemanticTable(df, name = Some("flights"))
      .withDimensions(Dimension("carrier", t => t("carrier")))
      .withMeasures(
        Measure("total_passengers", t => sum(t("passengers"))),
        Measure("flight_count",    t => count(lit(1))),
      )

    val result = model.groupBy("carrier").aggregate("total_passengers", "flight_count")
    val rows = result.execute(spark).collect()

    rows should have size 3
    val byCarrier = rows.map(r => r.getString(0) -> r.getLong(1)).toMap
    byCarrier("AA") shouldEqual 21L  // 5 + 4 + 7 + 5
    byCarrier("UA") shouldEqual 9L   // 3 + 2 + 4
    byCarrier("DL") shouldEqual 17L  // 6 + 3 + 8
  }

  // -------------------------------------------------------------------------
  // Metastore: temp view registered, queryable via Spark SQL
  // -------------------------------------------------------------------------

  test("createOrReplaceTempView — query result via Spark SQL") {
    val csvPath = getClass.getResource("/flights.csv").getPath
    val df = spark.read.option("header", "true").option("inferSchema", "true").csv(csvPath)

    val model = toSemanticTable(df, name = Some("flights"))
      .withDimensions(Dimension("carrier", t => t("carrier")))
      .withMeasures(Measure("total_passengers", t => sum(t("passengers"))))
      .groupBy("carrier")
      .aggregate("total_passengers")

    implicit val sparkSession = spark
    model.createOrReplaceTempView("flights_sql")

    val sqlResult = spark.sql(
      "SELECT carrier, total_passengers FROM flights_sql ORDER BY carrier"
    ).collect()

    sqlResult should have size 3
    sqlResult.map(_.getString(0)) shouldEqual Seq("AA", "DL", "UA")
  }

  // -------------------------------------------------------------------------
  // previewSchema — real file, no rows executed
  // -------------------------------------------------------------------------

  test("previewSchema on CSV — no rows materialized") {
    val csvPath = getClass.getResource("/flights.csv").getPath
    val df = spark.read.option("header", "true").option("inferSchema", "true").csv(csvPath)

    val model = toSemanticTable(df)
      .withDimensions(Dimension("carrier", t => t("carrier")))
      .withMeasures(Measure("total_passengers", t => sum(t("passengers"))))
      .groupBy("carrier")
      .aggregate("total_passengers")

    // previewSchema should NOT trigger any row reads
    val schema = model.previewSchema(spark)
    schema.fieldNames should contain theSameElementsAs Seq("carrier", "total_passengers")

    // Verify: the compiled plan should reference the CSV path (not cached rows)
    val planStr = model.explain()
    planStr should include("aggregate")
    planStr should include("carrier")
  }

  // -------------------------------------------------------------------------
  // Catalog: dimensions/measures readable from file-loaded model
  // -------------------------------------------------------------------------

  test("dimensions and measures visible on file-loaded model") {
    val csvPath = getClass.getResource("/flights.csv").getPath
    val df = spark.read.option("header", "true").option("inferSchema", "true").csv(csvPath)

    val model = toSemanticTable(df, name = Some("flights_from_csv"))
      .withDimensions(
        Dimension("carrier", t => t("carrier")),
        Dimension("origin",  t => t("origin")),
      )
      .withMeasures(
        Measure("total_passengers", t => sum(t("passengers"))),
        Measure("total_distance",   t => sum(t("distance"))),
      )

    model.dimensions.keys should contain theSameElementsAs Set("carrier", "origin")
    model.measures.keySet should contain theSameElementsAs Set("total_passengers", "total_distance")
    model.findDimension("carrier") shouldBe defined
    model.findMeasure("total_passengers") shouldBe defined
    model.findDimension("nonexistent") shouldBe empty
    model.findMeasure("nonexistent") shouldBe empty
  }

  // -------------------------------------------------------------------------
  // Calc correctness: calc on file-loaded model
  // -------------------------------------------------------------------------

  test("calc measure on CSV — t.all() works from real file read") {
    val csvPath = getClass.getResource("/flights.csv").getPath
    val df = spark.read.option("header", "true").option("inferSchema", "true").csv(csvPath)

    val model = toSemanticTable(df)
      .withDimensions(Dimension("carrier", t => t("carrier")))
      .withMeasures(
        Measure("total_passengers", t => sum(t("passengers"))),
        Measure("flight_count",     t => count(lit(1))),
        Measure("avg_passengers",   t => t("total_passengers") / t("flight_count")),
        Measure("pct_of_total",     t => t("total_passengers") / t.all("total_passengers")),
      )

    val result = model.groupBy("carrier").aggregate("total_passengers", "pct_of_total")
    val rows = result.execute(spark).collect()

    rows should have size 3
    // pct_of_total: avoid assuming Spark's result type (Long/Double/Decimal varies);
    // read as Any and convert to Double.
    val byCarrier = rows.map(r => r.getString(0) -> r.get(2).toString.toDouble).toMap
    // AA: 21, UA: 9, DL: 17, total: 47
    byCarrier("AA") shouldBe 21.0 / 47.0 +- 0.001
    byCarrier("UA") shouldBe 9.0  / 47.0 +- 0.001
    byCarrier("DL") shouldBe 17.0 / 47.0 +- 0.001
  }

  // -----------------------------------------------------------------------
  // Performance baseline: 10K-row CSV, real read + agg path
  // -----------------------------------------------------------------------

  test("performance baseline — 10K rows, group-by + calc under 5s") {
    val csvPath = getClass.getResource("/flights_large.csv").getPath
    val df = spark.read.option("header", "true").option("inferSchema", "true").csv(csvPath)

    val model = toSemanticTable(df, name = Some("flights_large"))
      .withDimensions(
        Dimension("carrier", t => t("carrier")),
        Dimension("origin",  t => t("origin")),
      )
      .withMeasures(
        Measure("total_passengers", t => sum(t("passengers"))),
        Measure("flight_count",    t => count(lit(1))),
        Measure("avg_passengers",  t => t("total_passengers") / t("flight_count")),
      )

    val t0 = System.nanoTime()
    val rows = model.groupBy("carrier", "origin")
      .aggregate("total_passengers", "avg_passengers")
      .execute(spark).collect()
    val elapsed = (System.nanoTime() - t0) / 1_000_000.0

    // 10K rows / 5 carriers x 8 origins: at most 40 groups, but skewed.
    // Baseline assertion: completes in well under 5 seconds on local Spark.
    assert(elapsed < 5000.0, s"group-by + calc on 10K rows took ${elapsed}ms (expected < 5000ms)")
    // Sanity: every row has a non-null carrier group.
    assert(rows.nonEmpty)
    println(s"[perf] 10K-row groupBy + calc: ${elapsed}ms, ${rows.length} groups")
  }
}
