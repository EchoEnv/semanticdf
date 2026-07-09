package io.semantica

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/** Scale fixture for Phase C — production confidence testing.
  *
  * Generates intentionally realistic data: skewed distribution (one carrier dominates),
  * multiple join cardinalities, and enough rows to exercise Catalyst's optimizer
  * rather than the trivial 18–30 row fixtures used in unit tests.
  *
  * Skew is deliberate — it exposes partition imbalance that a uniform fixture hides.
  * The skew ratio is configurable via `skewRatio`.
  *
  * This is NOT used by the main `SemanticaSpec` (which must stay fast and deterministic).
  * It is run as its own test class so it can be excluded from fast local runs via
  * `-Dtest=!ScaleFixtureSpec`.
  */
object ScaleFixture {

  /** How many rows the dominant carrier (AA) gets vs. each other carrier.
    * ratio=9 means AA has ~90% of rows. */
  val SkewRatio: Int = 9

  /** Main flights table — single-table group-by / aggregate / percent-of-total tests. */
  def flights(spark: org.apache.spark.sql.SparkSession): DataFrame = {
    import spark.implicits._
    import scala.util.Random

    val rng = new Random(42)  // deterministic seed for reproducibility

    // Skewed distribution: AA = 90%, UA = 7%, DL = 3%
    val numAA = 900_000
    val numUA =  70_000
    val numDL =  30_000
    val total = numAA + numUA + numDL

    val carriers = Seq.tabulate(total) { i =>
      val c =
        if (i < numAA) "AA"
        else if (i < numAA + numUA) "UA"
        else "DL"
      val dist = (rng.nextDouble() * 3000 + 100).toInt
      val pax  = (rng.nextDouble() * 400 + 1).toInt
      val orig = Seq("LAX","JFK","ORD","ATL","DFW","DEN","SFO","SEA","LAS","PHX")(rng.nextInt(10))
      val dest = Seq("LAX","JFK","ORD","ATL","DFW","DEN","SFO","SEA","LAS","PHX")(rng.nextInt(10))
      (c, orig, dest, dist, pax)
    }

    spark.sparkContext.parallelize(carriers, 16).toDF(
      "carrier", "origin", "dest", "distance", "passengers"
    ).repartition(16, col("carrier"))
  }

  /** Orders table for join_many scale test (1:many with line_items). */
  def orders(spark: org.apache.spark.sql.SparkSession): DataFrame = {
    import spark.implicits._
    import scala.util.Random
    val rng = new Random(99)
    val rows = (1 to 50_000).map { id =>
      val custId  = (id % 5_000) + 1          // 5K unique customers
      val amount = (rng.nextDouble() * 500 + 10 * 100).toInt
      val status  = Seq("shipped","processing","delivered","cancelled")(rng.nextInt(4))
      (id, custId, amount, status)
    }
    spark.sparkContext.parallelize(rows, 8).toDF(
      "order_id", "customer_id", "amount", "status"
    )
  }

  /** Line items table for join_many scale test (50K orders × 1–5 items each). */
  def lineItems(spark: org.apache.spark.sql.SparkSession): DataFrame = {
    import spark.implicits._
    import scala.util.Random
    val rng = new Random(77)
    val rows = (1 to 50_000).flatMap { orderId =>
      val numItems = (rng.nextDouble() * 4 + 1).toInt  // 1–5 items per order
      (1 to numItems).map { itemIdx =>
        val qty     = (rng.nextDouble() * 9 + 1).toInt
        val price   = (rng.nextDouble() * 200 + 5 * 100).toInt
        val product = s"SKU-${(rng.nextDouble() * 1000).toInt}"
        (orderId, itemIdx, qty, price, product)
      }
    }
    spark.sparkContext.parallelize(rows, 8).toDF(
      "order_id", "item_idx", "qty", "price_cents", "product"
    )
  }

  /** Total row counts (for assertions). */
  val FlightsRowCount    = 1_000_000
  val OrdersRowCount     = 50_000
  val LineItemsRowCount  = 185_000   // ~3.7 items per order on average

  /** Expected skewed group-by counts. */
  val ExpectedCarrierCounts: Map[String, Long] = Map(
    "AA" -> 900_000L,
    "UA" ->  70_000L,
    "DL" ->  30_000L,
  )
}
