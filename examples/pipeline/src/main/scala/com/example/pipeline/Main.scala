package com.example.pipeline

import io.semantica._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

/** ETL pipeline + semantica verification — the "data engineering" companion to the
  * starter template.
  *
  * This walks through the real BI lifecycle:
  *
  *   1. INGEST   — read raw CSV (with messy data)
  *   2. CLEAN    — drop nulls, dedupe, cast types, normalize strings
  *   3. ENRICH   — derive business columns (e.g. total_amount)
  *   4. VALIDATE — enforce business rules (drop invalid rows)
  *   5. WRITE    — save as parquet, partitioned by year
  *   6. CATALOG  — register as temp views for downstream queries
  *   7. SEMANTIC — build YAML models on the parquet, run queries
  *
  * Run with:
  *   1. mvn install the parent semantica project (so the local jar is available)
  *   2. From this directory: mvn scala:run -DmainClass=com.example.pipeline.Main
  *
  * Output (parquet tables and temp views) goes to ./output/
  */
object Main {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("semantica-pipeline")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    try {
      println("=" * 70)
      println("STEP 1: INGEST raw CSV")
      println("=" * 70)

      // Read raw data with EVERYTHING as strings first. We cast types after cleaning.
      // In real pipelines you'd often use inferSchema=false + permissive mode.
      val rawOrders = spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")  // bad rows become nulls, not failures
        .csv("raw/orders_raw.csv")
      val rawCustomers = spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .csv("raw/customers_raw.csv")

      println(s"  raw orders:    ${rawOrders.count()} rows")
      println(s"  raw customers: ${rawCustomers.count()} rows")

      println("\n" + "=" * 70)
      println("STEP 2: CLEAN (drop nulls, dedupe, cast, normalize)")
      println("=" * 70)

      val cleanOrders = cleanOrdersDf(rawOrders)
      val cleanCustomers = cleanCustomersDf(rawCustomers)

      println(s"  clean orders:    ${cleanOrders.count()} rows (was ${rawOrders.count()})")
      println(s"  clean customers: ${cleanCustomers.count()} rows (was ${rawCustomers.count()})")

      println("\n" + "=" * 70)
      println("STEP 3: ENRICH (derive business columns)")
      println("=" * 70)

      val enrichedOrders = enrichOrdersDf(cleanOrders)
      enrichedOrders.select("order_id", "qty", "price_usd", "total_amount", "order_year")
        .show(10, false)

      println("\n" + "=" * 70)
      println("STEP 4: VALIDATE (enforce business rules)")
      println("=" * 70)

      val validOrders = validateOrdersDf(enrichedOrders)
      println(s"  valid orders after validation: ${validOrders.count()} rows")

      println("\n" + "=" * 70)
      println("STEP 5: WRITE parquet (partitioned by year)")
      println("=" * 70)

      val ordersPath = "output/orders"
      val customersPath = "output/customers"

      // Parquet is the "trusted" / silver layer. Partitioning by year lets
      // downstream queries prune partitions they don't need.
      validOrders
        .write
        .mode(SaveMode.Overwrite)
        .partitionBy("order_year")
        .parquet(ordersPath)
      cleanCustomers
        .write
        .mode(SaveMode.Overwrite)
        .parquet(customersPath)

      println(s"  wrote parquet: $ordersPath")
      println(s"  wrote parquet: $customersPath")

      println("\n" + "=" * 70)
      println("STEP 6: CATALOG (register as temp views for downstream use)")
      println("=" * 70)

      // Re-read the parquet (proves the round-trip) and register as temp views.
      // In production you'd use a Hive metastore or Unity Catalog here.
      val savedOrders = spark.read.parquet(ordersPath)
      val savedCustomers = spark.read.parquet(customersPath)
      savedOrders.createOrReplaceTempView("orders")
      savedCustomers.createOrReplaceTempView("customers")

      println(s"  temp views: orders (${savedOrders.count()} rows), customers (${savedCustomers.count()} rows)")
      println(s"  sample query: ${spark.sql("SELECT country, COUNT(*) FROM customers GROUP BY country").count()} countries")

      println("\n" + "=" * 70)
      println("STEP 7: SEMANTIC MODELS on the parquet (the gold layer)")
      println("=" * 70)

      // The semantic models point to the parquet-backed temp views.
      // This is the bridge: parquet on disk → temp view → SemanticTable.
      val tables = Map(
        "orders_csv"    -> savedOrders,
        "customers_csv" -> savedCustomers,
      )

      val models = YamlLoader.loadDir("models/", tables)
      val orders = models("orders")

      println(s"  loaded ${models.size} semantic models: ${models.keys.mkString(", ")}")

      // Query 1: Revenue per customer
      println("\n--- Q1: Revenue per customer (joined orders + customers) ---")
      orders
        .groupBy("name", "city", "country")
        .aggregate("total_revenue", "order_count", "total_units")
        .toDataFrame(spark)
        .orderBy(col("total_revenue").desc)
        .show(10, false)

      // Query 2: Revenue per country
      println("\n--- Q2: Revenue per country ---")
      orders
        .groupBy("country")
        .aggregate("total_revenue", "order_count")
        .toDataFrame(spark)
        .orderBy(col("total_revenue").desc)
        .show(false)

      // Query 3: Monthly trend
      println("\n--- Q3: Monthly revenue trend ---")
      orders
        .atTimeGrain("order_date", "month")
        .groupBy("order_date")
        .aggregate("total_revenue", "order_count")
        .toDataFrame(spark)
        .orderBy("order_date")
        .show(false)

      // Schema introspection — every field from every model as a DataFrame
      println("\n--- Model schema (every field, full metadata) ---")
      orders.schema(spark).show(50, false)

      println("\n" + "=" * 70)
      println("Pipeline complete.")
      println("  Parquet output:  output/orders/, output/customers/")
      println("  Semantic models: models/orders.yml, models/customers.yml")
      println("  Edit raw/*.csv and re-run to test the pipeline with new data.")
      println("=" * 70)
    } finally spark.stop()
  }

  // ---------------------------------------------------------------------------
  // Pipeline steps
  // ---------------------------------------------------------------------------

  /** Clean orders: drop null critical fields, dedupe, cast types, normalize strings. */
  private def cleanOrdersDf(raw: DataFrame): DataFrame = {
    import raw.sparkSession.implicits._

    raw
      // 1. Drop rows with null qty or price (can't compute revenue without them)
      .filter(col("qty").isNotNull && col("price_usd").isNotNull)
      // 2. Dedupe by order_id — keep the first occurrence
      .dropDuplicates("order_id")
      // 3. Cast types: qty → int, price → double, order_date → date
      .withColumn("qty", col("qty").cast(DataTypes.IntegerType))
      .withColumn("price_usd", col("price_usd").cast(DataTypes.DoubleType))
      .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
      // 4. Normalize strings: trim whitespace, lowercase status
      .withColumn("product", trim(col("product")))
      .withColumn("status", lower(trim(col("status"))))
      // 5. Drop rows where qty cast failed (e.g. non-numeric strings)
      .filter(col("qty").isNotNull && col("price_usd").isNotNull)
  }

  /** Clean customers: drop nulls, dedupe by customer_id (keep first), normalize. */
  private def cleanCustomersDf(raw: DataFrame): DataFrame = {
    raw
      // 1. Drop rows with null name (can't have an unnamed customer)
      .filter(col("name").isNotNull && col("customer_id").isNotNull)
      // 2. Dedupe by customer_id (keep first occurrence)
      .dropDuplicates("customer_id")
      // 3. Normalize strings: trim whitespace, title-case city, uppercase country
      .withColumn("name", trim(col("name")))
      .withColumn("city", initcap(trim(col("city"))))
      .withColumn("country", upper(trim(col("country"))))
  }

  /** Enrich: derive total_amount and order_year from existing columns. */
  private def enrichOrdersDf(orders: DataFrame): DataFrame = {
    orders
      .withColumn("total_amount", col("qty") * col("price_usd"))
      .withColumn("order_year", year(col("order_date")))
  }

  /** Validate: drop invalid business rows.
    *
    * Rules:
    *   - qty == 0 is invalid (would give zero revenue and skew counts)
    *   - status must be one of {shipped, pending, cancelled, returned}
    *   - total_amount must be > 0
    */
  private def validateOrdersDf(orders: DataFrame): DataFrame = {
    orders
      .filter(col("qty") =!= 0)
      .filter(col("status").isin("shipped", "pending", "cancelled", "returned"))
      .filter(col("total_amount") > 0)
  }
}
