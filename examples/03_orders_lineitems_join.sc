// Example 03 — join_many: one-to-many (fan-out prevention)
// ---------------------------------------------------------------------------
// Star-schema: orders (1) → line_items (many)
// Without fan-out prevention: every line item MULTIPLIES every order row
// With fan-out prevention: each table is pre-aggregated at join-key grain BEFORE joining
//
// This example uses the Boring Semantic Layer safe-aggregation pattern:
//   - orders: pre-agg at customer_id grain
//   - line_items: pre-agg at order_id grain
//   - join on order_id → one row per order (correct)
//
// Key: when grouping by a DIMENSION from the "many" side (order_id from line_items),
// you MUST pre-aggregate at that grain before joining, or you'll inflate your measures.
// ---------------------------------------------------------------------------

import io.semantica._
import org.apache.spark.sql.functions.{count, lit, sum}

// Source data
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

// ---------------------------------------------------------------------------
// Step 1 — Build each side's semantic model
// ---------------------------------------------------------------------------

val ordersModel = toSemanticTable(orders, name = Some("orders"))
  .withDimensions(
    Dimension("customer_id", t => t("customer_id")),
    Dimension("status",     t => t("status")),
  )
  .withMeasures(
    Measure("order_amount", t => sum(t("amount"))),
  )

val itemsModel = toSemanticTable(lineItems, name = Some("items"))
  .withDimensions(Dimension("order_id", t => t("order_id")))
  .withMeasures(Measure("item_count", t => count(lit(1))))

// ---------------------------------------------------------------------------
// Step 2 — join_many (fan-out safe)
// ---------------------------------------------------------------------------

val joined = ordersModel.join_many(itemsModel, (l, r) => l("order_id") === r("order_id"))

// ---------------------------------------------------------------------------
// Step 3 — Query by customer (join-key grain, no fan-out risk)
// ---------------------------------------------------------------------------

val byCustomer = joined
  .withDimensions(Dimension("customer_id", t => t("customer_id")))
  .withMeasures(
    Measure("total_order_amount", t => sum(t("order_amount"))),
    Measure("total_items",       t => sum(t("item_count"))),
  )
  .groupBy("customer_id")
  .aggregate("total_order_amount", "total_items")

println("=== Revenue by customer ===")
byCustomer.execute(spark).show(truncate = false)

// ---------------------------------------------------------------------------
// What happened:
//   orders pre-agg at customer_id: 101→12500, 102→4000, 103→2000
//   items pre-agg at order_id:     1→2items, 2→3items, 3→1item, 4→2items, 5→1item
//   join on order_id:
//     order 1 (cust 101) + items [A,B]  → 1 row, amount=5000, count=2  ✓
//     order 3 (cust 101) + item [D]       → 1 row, amount=7500, count=1  ✓
//     order 2 (cust 102) + item [G]       → 1 row, amount=3000, count=3  ✓
//   After join: 3 rows (one per ORDER), not 9 rows (cartesian)
//
//   groupBy customer_id:
//     cust 101: 5000+7500=20000, items=2+1=3
//     cust 102: 4000, items=3
//     cust 103: 2000, items=2
//
// Without fan-out prevention:
//   cust 101: would see 5000×2 + 7500×1 = 17500 (WRONG)
//   Because each line item multiplies every order row
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Key concepts:
//   - join_many: one-to-many / star-schema (fact → dimension)
//   - join_one: one-to-one / parent-child (no fan-out risk)
//   - join_cross: Cartesian product (use with extreme caution)
//   - Fan-out prevention: both sides pre-aggregated at join-key grain before joining
// ---------------------------------------------------------------------------
