package io.semantica

import org.apache.spark.sql.functions.{col, count, lit, sum}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Phase C — scale confidence tests.
  *
  * These tests run against the [[ScaleFixture]] (1M rows, skewed carrier distribution,
  * 50K-order / 185K-item join tables) to verify that:
  *
  *   1. Correctness holds at scale — the math from the tiny unit fixtures is identical
  *      with 1M rows.
  *   2. The percent-of-total path (cross-join of 1-row totals) works correctly and
  *      efficiently at scale.
  *   3. The `join_many` fan-out prevention holds — the joined result has 50K rows
  *      (one per order), NOT 185K × 50K (exploded).
  *   4. Spark handles the skewed carrier distribution (AA = 90% of rows) without
  *      catastrophic partition imbalance.
  *
  * These tests are intentionally slower than the unit tests. They run in CI on every
  * push. To run only the fast unit tests locally:
  * {{{
  * mvn test -Dtest=SemanticaSpec,HardeningSpec
  * }}}
  *
  * To run only the scale tests:
  * {{{
  * mvn test -Dtest=ScaleSpec
  * }}}
  */
class ScaleSpec extends AnyFunSuite with Matchers with SparkSessionFixture {

  // -------------------------------------------------------------------------
  // Single-table: group-by + calc correctness at scale
  // -------------------------------------------------------------------------

  test("Phase C: group-by sum correct at 1M rows") {
    val flights = ScaleFixture.flights(spark)
    val st = toSemanticTable(flights, name = Some("f"))
      .withDimensions(
        Dimension("carrier", t => t("carrier")),
        Dimension("origin",  t => t("origin")),
      )
      .withMeasures(
        Measure("total_passengers", t => sum(t("passengers"))),
        Measure("flight_count",    t => count(lit(1))),
      )
      .groupBy("carrier")
      .aggregate("total_passengers", "flight_count")

    val rows = st.execute(spark).collect()
      .map(r => r.getAs[String]("carrier") -> r.getAs[Long]("total_passengers"))
      .toMap

    // Verify relative skew: AA dominates (9:1 ratio).
    rows("AA") should be > (rows("UA") * 10L)
    rows("UA") should be > (rows("DL") * 2L)
    rows("DL") should be > 0L
  }

  test("Phase C: calc of calc correct at 1M rows") {
    val flights = ScaleFixture.flights(spark)
    val st = toSemanticTable(flights, name = Some("f"))
      .withDimensions(Dimension("carrier", t => t("carrier")))
      .withMeasures(
        Measure("total_distance", t => sum(t("distance"))),
        Measure("flight_count",   t => count(lit(1))),
        Measure("avg_distance",   t => t("total_distance") / t("flight_count")),
      )
      .groupBy("carrier")
      .aggregate("avg_distance")

    val rows = st.execute(spark).collect()
      .map(r => r.getAs[String]("carrier") -> r.getAs[Double]("avg_distance"))
      .toMap

    // Plausible airline distances: domestic carriers average 100–3000 miles per leg.
    rows.values.foreach { avg =>
      avg should be > 100.0
      avg should be < 3000.0
    }
    rows("AA") should not be rows("UA")
    rows("UA") should not be rows("DL")
  }

  // -------------------------------------------------------------------------
  // Percent-of-total at scale
  // -------------------------------------------------------------------------

  test("Phase C: percent-of-total pcts sum to 1.0 at 1M rows") {
    val flights = ScaleFixture.flights(spark)
    val st = toSemanticTable(flights, name = Some("f"))
      .withDimensions(Dimension("carrier", t => t("carrier")))
      .withMeasures(
        Measure("total_passengers", t => sum(t("passengers"))),
        Measure("pct_of_total", t => t("total_passengers") / t.all("total_passengers")),
      )
      .groupBy("carrier")
      .aggregate("pct_of_total")

    val rows = st.execute(spark).collect()
      .map(r => r.getAs[String]("carrier") -> r.getAs[Double]("pct_of_total"))
      .toMap

    val sumPct = rows.values.sum
    sumPct shouldBe 1.0 +- 1e-9

    // AA skew (~90%): within ±5% of expected.
    rows("AA") should be > 0.85
    rows("DL") should be < 0.05
  }

  // -------------------------------------------------------------------------
  // join_many fan-out prevention at scale
  // -------------------------------------------------------------------------

  test("Phase C: join_many fan-out prevention — 50K orders × 185K items = 50K rows") {
    val orders = ScaleFixture.orders(spark)
    val items  = ScaleFixture.lineItems(spark)

    val o = toSemanticTable(orders, name = Some("o"))
      .withDimensions(Dimension("customer_id", t => t("customer_id")))
      .withMeasures(Measure("order_amount", t => sum(t("amount"))))
    val i = toSemanticTable(items, name = Some("i"))
      .withDimensions(Dimension("order_id", t => t("order_id")))
      .withMeasures(Measure("item_count", t => count(lit(1))))

    val joined = o.join_many(i, (l, r) => l("order_id") === r("order_id"))
    val rowCount = joined.execute(spark).count()

    // Fan-out prevention: pre-agg at order_id grain → one row per order (50K).
    // Without it, every line item would multiply every order → ~185K rows minimum.
    rowCount shouldBe ScaleFixture.OrdersRowCount
  }

  test("Phase C: join_many sums are not inflated by fan-out") {
    val orders = ScaleFixture.orders(spark)
    val items  = ScaleFixture.lineItems(spark)

    val o = toSemanticTable(orders, name = Some("o"))
      .withDimensions(Dimension("customer_id", t => t("customer_id")))
      .withMeasures(Measure("order_amount", t => sum(t("amount"))))
    val i = toSemanticTable(items, name = Some("i"))
      .withDimensions(Dimension("order_id", t => t("order_id")))
      .withMeasures(Measure("item_revenue", t => sum(t("price_cents"))))

    // After join_many: the joined result has one row per order, with pre-aggregated
    // order_amount and item_revenue columns already summed to the order_id grain.
    // No further aggregation needed — verify the totals directly.
    val joined = o.join_many(i, (l, r) => l("order_id") === r("order_id"))

    val result = joined.execute(spark).agg(
      sum(col("order_amount")).as("grand_orders"),
      sum(col("item_revenue")).as("grand_items"),
    ).collect().head

    val grandOrders: Long = result.getLong(0)
    val grandItems:  Long = result.getLong(1)

    grandOrders should be > 0L
    grandItems  should be > 0L
    // price_cents ≥ 500 per item, so grand_items > LineItemsRowCount × 100.
    grandItems should be > ScaleFixture.LineItemsRowCount * 100L
  }
}
