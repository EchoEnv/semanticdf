package io.semanticdf

import org.apache.spark.sql.functions._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Smoke test for the `examples/telco-analytics/` consumer template.
  *
  * Loads the template's data + YAML and exercises the same 3 queries
  * the template's `Main.scala` runs. Verifies the queries execute cleanly
  * and produce non-empty results.
  */
class TelcoTemplateSpec extends AnyFunSuite with Matchers with SparkSessionFixture {

  /** Resolve a path under `<project_root>/examples/...` from the working dir. */
  private def examplesPath(parts: String*): String = {
    val cwd = System.getProperty("user.dir")
    val candidates = Seq(cwd, new java.io.File(cwd).getParent).distinct
    candidates.iterator.map { c =>
      val p = new java.io.File(c, "examples/" + parts.mkString("/"))
      if (p.exists) p.getAbsolutePath else null
    }.find(_ != null).getOrElse {
      fail(s"Could not locate examples/${parts.mkString("/")} from cwd=$cwd")
      ""
    }
  }

  private def loadUsageModel() = {
    val dir = examplesPath("telco-analytics")
    val plansCsv = spark.read.option("header", "true").option("inferSchema", "true")
      .csv(s"$dir/data/plans.csv")
    val promotionsCsv = spark.read.option("header", "true").option("inferSchema", "true")
      .csv(s"$dir/data/promotions.csv")
    val usageCsv = spark.read.option("header", "true").option("inferSchema", "true")
      .csv(s"$dir/data/usage.csv")
      .withColumn("event_date", col("event_date").cast("date"))
    val tables = Map(
      "plans_csv"      -> plansCsv,
      "promotions_csv" -> promotionsCsv,
      "usage_csv"      -> usageCsv,
    )
    YamlLoader.loadDir(s"$dir/models/", tables)("usage")
  }

  test("telco-analytics: Q1 monthly ARPU per plan produces non-empty results") {
    val usage = loadUsageModel()
    val withArpu = usage.withMeasures(
      Measure("active_customers", t => countDistinct(t("customer_id"))),
      Measure("arpu",
        t => CalcHelpers.safeDivide(
          t("total_revenue"), t("active_customers"), defaultValue = 0.0)),
    )
    val rows = withArpu
      .atTimeGrain("event_date", "month")
      .groupBy("plan_name")
      .aggregate("total_revenue", "active_customers", "arpu")
      .execute(spark).collect()
    assert(rows.length > 0, "ARPU query should produce at least one row")
    assert(rows.length <= 4, s"expected ≤4 plans, got ${rows.length}")
    val arpus = rows.map(_.getAs[Double]("arpu"))
    assert(arpus.forall(_ >= 0), s"all ARPU values must be ≥ 0, got $arpus")
  }

  test("telco-analytics: Q2 promotion effectiveness produces non-empty results") {
    val usage = loadUsageModel()
    val withPromoPct = usage.withMeasures(
      Measure("pct_of_revenue", t => t("total_revenue") / t.all("total_revenue")),
      Measure("customers_on_promo", t => countDistinct(t("customer_id"))),
    )
    val rows = withPromoPct
      .groupBy("promo_code")
      .aggregate("total_revenue", "customers_on_promo", "pct_of_revenue")
      .execute(spark).collect()
    // 4 unique promo codes: 3 explicit + null (no promo)
    assert(rows.length >= 3, s"expected ≥3 promo codes, got ${rows.length}")
    val pcts = rows.map(_.getAs[Double]("pct_of_revenue"))
    // pcts may include a NaN if the row's total_revenue is 0; filter those.
    val finitePcts = pcts.filter(!_.isNaN)
    assert(finitePcts.nonEmpty, s"expected at least one finite pct, got $pcts")
  }

  test("telco-analytics: Q3 roaming revenue contribution produces non-empty results") {
    val usage = loadUsageModel()
    val withRoamingPct = usage.withMeasures(
      Measure("pct_roaming", t => t("total_roaming_revenue") / t.all("total_revenue")),
    )
    val rows = withRoamingPct
      .groupBy()
      .aggregate("total_revenue", "total_roaming_revenue", "pct_roaming")
      .execute(spark).collect()
    assert(rows.length == 1, s"expected 1 aggregate row, got ${rows.length}")
    val totalRevenue = rows.head.getAs[Double]("total_revenue")
    val roamingRevenue = rows.head.getAs[Double]("total_roaming_revenue")
    val pctRoaming = rows.head.getAs[Double]("pct_roaming")
    assert(totalRevenue > 0, s"total revenue should be > 0, got $totalRevenue")
    assert(roamingRevenue >= 0, s"roaming revenue should be ≥ 0, got $roamingRevenue")
    assert(roamingRevenue <= totalRevenue,
      s"roaming $roamingRevenue should be ≤ total $totalRevenue")
    assert(pctRoaming >= 0 && pctRoaming <= 1,
      s"pct_roaming should be in [0,1], got $pctRoaming")
  }
}
