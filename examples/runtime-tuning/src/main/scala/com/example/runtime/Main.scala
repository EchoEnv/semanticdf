package com.example.runtime

import io.semanticdf._
import io.semanticdf.audit.AuditSink
import io.semanticdf.cache.ResultCache

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel

import java.nio.file.{Files, Path, Paths}

/** Runnable example for the runtime-tuning walk-through.
  *
  * Builds two models — customers and orders — and queries them
  * three different ways to simulate a customer analytics dashboard.
  * Each query demonstrates the six runtime knobs:
  *
  *   1. withMaxRows(10_000)        — driver-memory safety cap
  *   2. withResultCache(cache)     — shape-keyed row cache
  *   3. withAuditSink(sink)        — log every query
  *   4. withBroadcastJoinThreshold — broadcast small dimensions
  *   5. withMaterialize(level)     — persist the compiled DataFrame
  *   6. withSalt(5)               — AQE skew handling
  *
  * Run with:
  *   mvn scala:run -DmainClass=com.example.runtime.Main
  *
  * See `docs/tutorial-runtime-tuning.md` for the full walk-through.
  */
object Main {

  /** Customer row used to build the customers DataFrame. Case class
    * so Spark's encoder can derive a schema automatically. */
  private case class CustomerRow(id: Int, name: String, region: String, lifetime_value: Double)

  /** Order row used to build the orders DataFrame. */
  private case class OrderRow(id: Int, customer_id: Int, amount: Double, category: String, ordered_at: String)

  /** Build a small customers dataset. Realistic distribution:
    * 1K customers across 3 regions. ~167 per region on average. */
  private def buildCustomers(spark: SparkSession, n: Int = 1000) = {
    import spark.implicits._
    val rand = new scala.util.Random(42)
    val regions = Array("East", "West", "Central")
    val rows = (1 to n).map { i =>
      CustomerRow(
        id            = i,
        name          = s"customer_$i",
        region        = regions.apply(i % regions.length),
        lifetime_value = 100.0 + (i * 137 % 5000),
      )
    }
    rows.toDF
  }

  /** Build an orders dataset. Realistic distribution:
    * 50K orders; one customer (id=1) has 25% of all orders (a heavy
    * customer — this triggers the skew-handling hint). */
  private def buildOrders(spark: SparkSession, n: Int = 50000) = {
    import spark.implicits._
    val rand = new scala.util.Random(42)
    val categories = Array("electronics", "books", "clothing", "food")
    val rows = (1 to n).map { i =>
      // 25% of orders go to customer 1 (heavy customer); the rest
      // are distributed across the other 999 customers.
      val customerId = if (rand.nextDouble() < 0.25) 1 else (rand.nextInt(999) + 2)
      val amount = 5.0 + (rand.nextDouble() * 500.0)
      val category = categories.apply(i % categories.length)
      OrderRow(i, customerId, amount, category, "2025-01-01")
    }
    rows.toDF
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("runtime-tuning-example")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try {
      println("=== Building sample data ===")
      val customersDf = buildCustomers(spark)
      val ordersDf = buildOrders(spark)
      println(s"Wrote ${customersDf.count()} customers")
      println(s"Wrote ${ordersDf.count()} orders")

      println()
      println("=== Building customer analytics models ===")
      val cache = ResultCache.inMemory(maxEntries = 256)
      val sink  = AuditSink.inMemory(maxEvents = 1024)

      // The customers dimension is small (1K rows). Set a 10 MB
      // broadcast threshold — orders > 10 MB would skip broadcast.
      val customers = toSemanticTable(customersDf, name = Some("customers"))
        .withDimensions(
          Dimension("id",     _ => customersDf("id")),
          Dimension("region", _ => customersDf("region")),
        )
        .withMeasures(
          Measure("ltv",   _ => sum(customersDf("lifetime_value"))),
          Measure("count", _ => count(lit(1))),
        )
        .withMaxRows(10_000)
        .withResultCache(cache)
        .withAuditSink(sink)
        .withMaterialize(StorageLevel.MEMORY_AND_DISK)
        .withSalt(5)

      // The orders fact table is large (50K rows). Set the broadcast
      // threshold to 1 MB — orders is way over this, so broadcast
      // doesn't fire. The salt hint triggers AQE skew handling.
      val orders = toSemanticTable(ordersDf, name = Some("orders"))
        .withDimensions(
          Dimension("customer_id", _ => ordersDf("customer_id")),
          Dimension("category",    _ => ordersDf("category")),
        )
        .withMeasures(
          Measure("amount", _ => sum(ordersDf("amount"))),
          Measure("count",  _ => count(lit(1))),
        )
        .withMaxRows(10_000)
        .withResultCache(cache)
        .withAuditSink(sink)
        .withMaterialize(StorageLevel.MEMORY_AND_DISK)
        .withSalt(5)
        .withBroadcastJoinThreshold(1L * 1024 * 1024)   // 1 MB

      println("  customers model: 2 dimensions, 2 measures")
      println("  orders model: 2 dimensions, 2 measures")

      println()
      println("=== Widget 1: top customers by LTV, per region ===")
      val widget1T0 = System.nanoTime()
      customers
        .query(measures = Seq("ltv", "count"), dimensions = Seq("region"))
        .execute(spark)
        .orderBy(col("ltv").desc)
        .show()
      val widget1T1 = System.nanoTime()
      println(s"  elapsed: ${(widget1T1 - widget1T0) / 1e6}ms")

      println()
      println("=== Widget 2: orders per region ===")
      val widget2T0 = System.nanoTime()
      orders
        .query(measures = Seq("amount", "count"), dimensions = Seq("category"))
        .execute(spark)
        .orderBy(col("amount").desc)
        .show()
      val widget2T1 = System.nanoTime()
      println(s"  elapsed: ${(widget2T1 - widget2T0) / 1e6}ms")

      println()
      println("=== Widget 3: LTV + orders per customer (join) ===")
      val widget3T0 = System.nanoTime()
      orders.join_one(customers, (l, r) => l("customer_id") === r("id"))
        .query(measures = Seq("ltv", "amount"))
        .execute(spark)
        .orderBy(col("ltv").desc)
        .limit(5)
        .show()
      val widget3T1 = System.nanoTime()
      println(s"  elapsed: ${(widget3T1 - widget3T0) / 1e6}ms")

      println()
      println("=== Audit events emitted ===")
      val events = sink.snapshot()
      println(s"  ${events.length} events captured:")
      events.foreach { e =>
        println(s"    [${e.status}] model=${e.model} rows=${e.rowCount} " +
          s"elapsed=${e.elapsedMs}ms dedupHash=${e.dedupHash.take(12)}...")
      }

      println()
      println("=== Verifying cache hits (run widgets 1+2 again) ===")
      val widget1AgainT0 = System.nanoTime()
      customers
        .query(measures = Seq("ltv", "count"), dimensions = Seq("region"))
        .execute(spark)
        .collect()
      val widget1AgainT1 = System.nanoTime()
      println(s"  Widget 1 second call: ${(widget1AgainT1 - widget1AgainT0) / 1e6}ms (cache hit)")

      val widget2AgainT0 = System.nanoTime()
      orders
        .query(measures = Seq("amount", "count"), dimensions = Seq("category"))
        .execute(spark)
        .collect()
      val widget2AgainT1 = System.nanoTime()
      println(s"  Widget 2 second call: ${(widget2AgainT1 - widget2AgainT0) / 1e6}ms (cache hit)")

      println()
      println("=== Salt hint verification ===")
      val adaptiveEnabled = spark.conf.get("spark.sql.adaptive.enabled", "false")
      val skewJoinEnabled = spark.conf.get("spark.sql.adaptive.skewJoin.enabled", "false")
      val factor          = spark.conf.get("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
      println(s"  adaptive.enabled=${adaptiveEnabled} (set by withSalt)")
      println(s"  skewJoin.enabled=${skewJoinEnabled}")
      println(s"  skewJoin.skewedPartitionFactor=${factor}")

      println()
      println("Done. Try modifying the knobs in this file and re-running.")
    } finally {
      spark.stop()
    }
  }
}
