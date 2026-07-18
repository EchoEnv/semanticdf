package com.example.pipeline

import io.semanticdf._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

/** ETL pipeline + semanticdf verification — the "data engineering" companion to the
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
  * ==Typed field references (v0.1.x typed API)==
  *
  * See the [[Refs]] object — phantom-typed witnesses for every dimension
  * and measure used in the semantic queries. Downstream calls use the
  * typed refs (groupByDimensions, aggregateMeasures, SortKey). A typo
  * in a ref name is a compile error. See `examples/starter/Main.scala`
  * and `docs/phase-E-plan.md` for the full story.
  *
  * Note: the pipeline steps 1-6 use raw Spark DataFrame APIs, not
  * semanticdf — they're for transforming raw CSVs into parquet, not
  * for running semantic queries. The typed refs are only relevant for
  * the semantic queries in STEP 7.
  *
  * Run with:
  *   1. mvn install the parent semanticdf project (so the local jar is available)
  *   2. From this directory: mvn scala:run -DmainClass=com.example.pipeline.Main
  *
  * Output (parquet tables and temp views) goes to ./output/
  */
/** Narrative logger for this template.
  *
  * Uses java.util.logging.Logger (JDK built-in). The public API
  * (`info` / `warn` / `error` / `debug`) is logger-agnostic — swap the
  * underlying implementation for SLF4J / log4j2 in production by
  * changing only the body of these four methods. Callsites stay stable.
  *
  * For spark-heavy projects that want logging routed through Spark's
  * log4j infrastructure, `io.semanticdf.SemanticLogger` is available —
  * but using it from a consumer template couples the template to a
  * library internal; this template-local logger is the recommended
  * pattern.
  */
object Logger {
  import java.util.logging.{Level, Logger => JulLogger}
  private val jul: JulLogger = JulLogger.getLogger("com.example.pipeline")
  jul.setLevel(Level.INFO)

  def info(msg: => String): Unit  = jul.info(msg)
  def warn(msg: => String): Unit  = jul.warning(msg)
  def error(msg: => String): Unit = jul.severe(msg)
  def debug(msg: => String): Unit = jul.fine(msg)
}

object Main {

  // -----------------------------------------------------------------------
  // Phantom-typed field references. Only the fields used in STEP 7
  // (semantic queries) need typed refs. The pipeline steps use raw
  // Spark DataFrame APIs on raw column names.
  // -----------------------------------------------------------------------
  object Refs {
    // Dimensions (orders model)
    sealed trait OrderId
    sealed trait OrderDate
    // Dimensions (joined from customers)
    sealed trait Name
    sealed trait City
    sealed trait Country
    // Measures (orders model)
    sealed trait TotalRevenue
    sealed trait OrderCount
    sealed trait TotalUnits

    implicit val orderId:      SemanticDimension[OrderId]      = SemanticDimension.of[OrderId]("order_id")
    implicit val orderDate:    SemanticDimension[OrderDate]    = SemanticDimension.of[OrderDate]("order_date")
    implicit val name:         SemanticDimension[Name]         = SemanticDimension.of[Name]("name")
    implicit val city:         SemanticDimension[City]         = SemanticDimension.of[City]("city")
    implicit val country:      SemanticDimension[Country]      = SemanticDimension.of[Country]("country")
    implicit val totalRevenue:  SemanticMeasure[TotalRevenue]   = SemanticMeasure.of[TotalRevenue]("total_revenue")
    implicit val orderCount:   SemanticMeasure[OrderCount]    = SemanticMeasure.of[OrderCount]("order_count")
    implicit val totalUnits:   SemanticMeasure[TotalUnits]    = SemanticMeasure.of[TotalUnits]("total_units")
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("semanticdf-pipeline")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    try {
      // Bring the typed refs into scope
      import Refs._

      Logger.info("=" * 70)
      Logger.info("STEP 1: INGEST raw CSV")
      Logger.info("=" * 70)

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

      Logger.info(s"  raw orders:    ${rawOrders.count()} rows")
      Logger.info(s"  raw customers: ${rawCustomers.count()} rows")

      Logger.info("=" * 70)
      Logger.info("STEP 2: CLEAN (drop nulls, dedupe, cast, normalize)")
      Logger.info("=" * 70)

      val cleanOrders = cleanOrdersDf(rawOrders)
      val cleanCustomers = cleanCustomersDf(rawCustomers)

      Logger.info(s"  clean orders:    ${cleanOrders.count()} rows (was ${rawOrders.count()})")
      Logger.info(s"  clean customers: ${cleanCustomers.count()} rows (was ${rawCustomers.count()})")

      Logger.info("=" * 70)
      Logger.info("STEP 3: ENRICH (derive business columns)")
      Logger.info("=" * 70)

      val enrichedOrders = enrichOrdersDf(cleanOrders)
      enrichedOrders.select("order_id", "qty", "price_usd", "total_amount", "order_year")
        .show(10, false)

      Logger.info("=" * 70)
      Logger.info("STEP 4: VALIDATE (enforce business rules)")
      Logger.info("=" * 70)

      val validOrders = validateOrdersDf(enrichedOrders)
      Logger.info(s"  valid orders after validation: ${validOrders.count()} rows")

      Logger.info("=" * 70)
      Logger.info("STEP 5: WRITE parquet (partitioned by year)")
      Logger.info("=" * 70)

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

      Logger.info(s"  wrote parquet: $ordersPath")
      Logger.info(s"  wrote parquet: $customersPath")

      Logger.info("=" * 70)
      Logger.info("STEP 6: CATALOG (register as temp views for downstream use)")
      Logger.info("=" * 70)

      // Re-read the parquet (proves the round-trip) and register as temp views.
      // In production you'd use a Hive metastore or Unity Catalog here.
      val savedOrders = spark.read.parquet(ordersPath)
      val savedCustomers = spark.read.parquet(customersPath)
      savedOrders.createOrReplaceTempView("orders")
      savedCustomers.createOrReplaceTempView("customers")

      Logger.info(s"  temp views: orders (${savedOrders.count()} rows), customers (${savedCustomers.count()} rows)")
      Logger.info(s"  sample query: ${spark.sql("SELECT country, COUNT(*) FROM customers GROUP BY country").count()} countries")

      Logger.info("=" * 70)
      Logger.info("STEP 7: SEMANTIC MODELS on the parquet (the gold layer)")
      Logger.info("=" * 70)

      // The semantic models point to the parquet-backed temp views.
      // This is the bridge: parquet on disk → temp view → SemanticTable.
      val tables = Map(
        "orders_csv"    -> savedOrders,
        "customers_csv" -> savedCustomers,
      )

      val models = YamlLoader.loadDir("models/", tables)
      val orders = models("orders")

      Logger.info(s"  loaded ${models.size} semantic models: ${models.keys.mkString(", ")}")

      // Query 1: Revenue per customer (joined orders + customers).
      // groupByDimensions takes dimension refs only — the compiler rejects
      // measure refs here. aggregateMeasures takes measure refs only.
      Logger.info("--- Q1: Revenue per customer (joined orders + customers) ---")
      orders
        .groupByDimensions(name, city, country)
        .aggregateMeasures(totalRevenue, orderCount, totalUnits)
        .orderBy(SortKey.desc(totalRevenue))
        .execute(spark)
        .show(10, false)

      // Query 2: Revenue per country
      Logger.info("--- Q2: Revenue per country ---")
      orders
        .groupByDimensions(country)
        .aggregateMeasures(totalRevenue, orderCount)
        .orderBy(SortKey.desc(totalRevenue))
        .execute(spark)
        .show(false)

      // Query 3: Monthly trend
      Logger.info("--- Q3: Monthly revenue trend ---")
      orders
        .atTimeGrain(orderDate.name, "month")
        .groupByDimensions(orderDate)
        .aggregateMeasures(totalRevenue, orderCount)
        .orderBy(SortKey.asc(orderDate))
        .execute(spark)
        .show(false)

      // Schema introspection — every field from every model as a DataFrame.
      // schema(spark) is a SemanticTable method that returns the catalog
      // schema (one row per field, with type/description/metadata).
      Logger.info("--- Model schema (every field, full metadata) ---")
      val schema = orders.schema(spark)
      schema.show(50, false)

      Logger.info("=" * 70)
      Logger.info("STEP 8: PERSIST schema as catalog table (gold layer metadata)")
      Logger.info("=" * 70)

      // The YAML model is the gold layer's *definition*. Persisting the schema
      // as a catalog table makes it *queryable* — anyone in the org can ask
      // "what models exist, what fields do they have, who owns them?" without
      // parsing YAML files.
      //
      // ⚠ DEMO-ONLY CODE. For production, replace with:
      //   - Delta Lake: .format("delta").mode("append").partitionBy("snapshot_date")
      //   - Git SHA tracking: .withColumn("git_sha", lit(getCurrentSha()))
      //   - Catalog registration: saveAsTable("main._semanticdf.catalog")
      // See README → "Production-grade catalog patterns" for the full pattern.
      val catalogPath = "output/_semanticdf_catalog"
      schema
        .withColumn("loaded_at", current_timestamp())
        .withColumn("source_path", lit("models/orders.yml"))
        .write
        .mode(SaveMode.Overwrite)
        .parquet(catalogPath)
      Logger.info(s"  wrote catalog: $catalogPath")
      Logger.info("  query example:")
      Logger.info("    spark.read.parquet(\"output/_semanticdf_catalog\")")
      Logger.info("      .filter(col(\"metadata_owner\") === \"finance-team\")")
      Logger.info("      .select(\"model_name\", \"field_name\", \"metadata_values\")")
      Logger.info("      .show(false)")

      // Verify by re-reading the catalog
      val catalog = spark.read.parquet(catalogPath)
      Logger.info(s"  re-read catalog: ${catalog.count()} rows")
      catalog.filter(col("metadata_values").contains("finance-team"))
        .select("model_name", "field_name", "metadata_values")
        .show(false)

      Logger.info("=" * 70)
      Logger.info("Pipeline complete.")
      Logger.info("  Parquet output:   output/orders/, output/customers/")
      Logger.info("  Semantic models:  models/orders.yml, models/customers.yml")
      Logger.info("  Schema catalog:   output/_semanticdf_catalog/")
      Logger.info("  Edit raw/*.csv and re-run to test the pipeline with new data.")
      Logger.info("=" * 70)
    } finally spark.stop()
  }

  // ---------------------------------------------------------------------------
  // Pipeline steps (STAGES 1-4 — pure Spark, not semanticdf)
  //
  // These transforms use raw column names, not typed refs — they're
  // ETL helpers, not semantic queries. The typed refs in [[Refs]] are
  // for the semantic queries in STEP 7.
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
