package io.semanticdf.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{count, lit, sum}
import io.semanticdf._

/** Example 03 — join_many: one-to-many with fan-out prevention
  *
  * Star-schema: orders (1) → line_items (many).
  *
  * Without fan-out prevention: every line item MULTIPLIES every order row.
  * With fan-out prevention: each table is pre-aggregated at join-key grain BEFORE joining.
  *
  * Run with: mvn scala:run -DmainClass=io.semanticdf.examples.OrdersJoinMany
  */
object OrdersJoinMany {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    try {
      val orders = spark.createDataFrame(Seq(
        (1, 101, 5000, "shipped"),
        (2, 102, 3000, "shipped"),
        (3, 101, 7500, "delivered"),
        (4, 103, 2000, "processing"),
        (5, 102, 1000, "shipped"),
      )).toDF("order_id", "customer_id", "amount", "status")

      val lineItems = spark.createDataFrame(Seq(
        (1, 1, 2, 2500, "Widget A"),
        (2, 1, 1, 3000, "Widget B"),
        (3, 2, 3, 1000, "Gadget"),
        (4, 3, 1, 7500, "Premium Widget"),
        (5, 4, 2, 1000, "Basic Part"),
        (6, 5, 1, 1000, "Small Part"),
      )).toDF("item_id", "order_id", "qty", "price_cents", "product")

      // Build each side's semantic model
      val ordersModel = toSemanticTable(orders, name = Some("orders"))
        .withDimensions(
          Dimension("customer_id", t => t("customer_id")),
          Dimension("status",     t => t("status")),
        )
        .withMeasures(Measure("order_amount", t => sum(t("amount"))))

      val itemsModel = toSemanticTable(lineItems, name = Some("items"))
        .withDimensions(Dimension("order_id", t => t("order_id")))
        .withMeasures(Measure("item_count", t => count(lit(1))))

      // join_many: fan-out safe (both sides pre-aggregated at join-key grain)
      val joined = ordersModel.join_many(itemsModel, (l, r) => l("order_id") === r("order_id"))

      // Query by customer
      val byCustomer = joined
        .withDimensions(Dimension("customer_id", t => t("customer_id")))
        .withMeasures(
          Measure("total_order_amount", t => sum(t("order_amount"))),
          Measure("total_items",       t => sum(t("item_count"))),
        )
        .groupBy("customer_id")
        .aggregate("total_order_amount", "total_items")

      SemanticLogger.info("=== Revenue by customer (join_many) ===")
      byCustomer.execute(spark).show(truncate = false)

      SemanticLogger.info("=== SemanticDF plan ===")
      println(byCustomer.explain())

      SemanticLogger.info("=== What happened ===")
      SemanticLogger.info("orders pre-agg at customer_id: 101→12500, 102→4000, 103→2000")
      SemanticLogger.info("items pre-agg at order_id:     each order → item_count")
      SemanticLogger.info("join on order_id: one row per ORDER (50K), not cartesian explosion")
      SemanticLogger.info("groupBy customer_id: correctly aggregates to customer level")
      SemanticLogger.info("")
      SemanticLogger.info("WITHOUT fan-out prevention:")
      SemanticLogger.info("  Each line item would multiply every order row → wrong sums")

    } finally {
      spark.stop()
    }
  }
}
