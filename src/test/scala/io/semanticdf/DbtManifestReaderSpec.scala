package io.semanticdf

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import java.nio.file.{Files, Paths}

/** Tests for the dbt `manifest.json` reader.
  *
  * The reader is split into two phases:
  *   1. `read(path)` parses the manifest into a `DbtProject` (pure, no Spark).
  *   2. `toSemanticTables(project, spark, resolve)` binds each model to a
  *      real `DataFrame` and produces a `Map[String, SemanticTable]`.
  *
  * We exercise both phases against a hand-crafted fixture (`minimal-
  * manifest.json`) that mirrors a small dbt project with two models
  * (orders, customers), one source, one seed, and one test. */
class DbtManifestReaderSpec extends AnyFunSuite {

  // -----------------------------------------------------------------
  // Phase 1: parse-only
  // -----------------------------------------------------------------

  test("read: parses the minimal manifest fixture") {
    val project = DbtManifestReader.read(
      Paths.get("src/test/resources/dbt-fixtures/minimal-manifest.json"))

    assert(project.manifestVersion ==
      Some("https://schemas.getdbt.com/dbt/manifest/v12.json"))
    assert(project.models.keySet == Set("orders", "customers"))
    assert(project.sources.keySet == Set("charges"))
  }

  test("read: orders model has 4 dimensions and 2 measures") {
    val project = DbtManifestReader.read(
      Paths.get("src/test/resources/dbt-fixtures/minimal-manifest.json"))
    val orders = project.models("orders")

    assert(orders.name == "orders")
    assert(orders.database == Some("analytics"))
    assert(orders.schema == Some("main"))
    assert(orders.sourceTable == "analytics.main.orders")
    assert(orders.description == Some("Customer order fact table"))
    assert(orders.tags == Seq("fact", "core"))

    // 4 dimensions (order_id, customer_id, order_date, amount) +
    // 2 measures (total_revenue, order_count).
    assert(orders.dimensions.map(_.name).toSet ==
      Set("order_id", "customer_id", "order_date", "amount"))
    assert(orders.measures.map(_.name).toSet ==
      Set("total_revenue", "order_count"))

    val total = orders.measures.find(_.name == "total_revenue").get
    assert(total.expr == "sum(amount)")
    assert(total.description == Some("Total revenue across all orders"))
  }

  test("read: customers model has 2 dimensions, no measures") {
    val project = DbtManifestReader.read(
      Paths.get("src/test/resources/dbt-fixtures/minimal-manifest.json"))
    val customers = project.models("customers")
    assert(customers.dimensions.map(_.name).toSet == Set("customer_id", "name"))
    assert(customers.measures.isEmpty)
  }

  test("read: seeds, tests, and non-model nodes are filtered out") {
    val project = DbtManifestReader.read(
      Paths.get("src/test/resources/dbt-fixtures/minimal-manifest.json"))
    assert(!project.models.contains("raw_orders"),
      "seed nodes must not appear in the models map")
    assert(!project.models.contains("not_null_orders_order_id"),
      "test nodes must not appear in the models map")
  }

  test("read: source entries are preserved") {
    val project = DbtManifestReader.read(
      Paths.get("src/test/resources/dbt-fixtures/minimal-manifest.json"))
    val src = project.sources("charges")
    assert(src.name == "charges")
    assert(src.identifier == Some("charges"))
    assert(src.database == Some("raw"))
    assert(src.schema == Some("stripe"))
    assert(src.description == Some("Stripe charge records"))
  }

  test("read: missing file throws IllegalArgumentException with the path") {
    val ex = intercept[IllegalArgumentException] {
      DbtManifestReader.read(Paths.get("does/not/exist.json"))
    }
    assert(ex.getMessage.contains("does/not/exist.json"))
  }

  test("read: from an in-memory Map (for tests / embedded callers)") {
    val manifest = Map[String, Any](
      "metadata" -> Map[String, Any]("dbt_schema_version" -> "https://x"),
      "nodes" -> Map[String, Any](
        "model.p.t" -> Map[String, Any](
          "resource_type" -> "model",
          "name"          -> "t",
          "database"      -> "db",
          "schema"        -> "sc",
          "alias"         -> "t",
          "columns"       -> Map[String, Any](
            "id" -> Map[String, Any]("name" -> "id", "data_type" -> "int"),
          ),
        ),
      ),
      "sources" -> Map[String, Any](),
    )
    val project = DbtManifestReader.read(manifest)
    assert(project.models.keySet == Set("t"))
    assert(project.models("t").dimensions.head.name == "id")
  }

  // -----------------------------------------------------------------
  // Phase 2: bind to Spark
  // -----------------------------------------------------------------

  test("toSemanticTables: builds a working SemanticTable for the orders model") {
    val spark = newSparkSession()
    try {
      val project = DbtManifestReader.read(
        Paths.get("src/test/resources/dbt-fixtures/minimal-manifest.json"))

      val tables = DbtManifestReader.toSemanticTables(project, spark, _ =>
        // Source table doesn't matter for this check — the resolver is
        // the caller's contract. Return a minimal DataFrame.
        emptyOrdersDf(spark))

      assert(tables.keySet == Set("orders", "customers"))

      val orders = tables("orders")
      assert(orders.name == Some("orders"))
      assert(orders.dimensions.keySet == Set("order_id", "customer_id", "order_date", "amount"))
      assert(orders.measures.keySet == Set("total_revenue", "order_count"))
      assert(orders.measures("total_revenue").description ==
        Some("Total revenue across all orders"))
    } finally spark.stop()
  }

  test("toSemanticTables: query end-to-end against the manifest's model") {
    val spark = newSparkSession()
    try {
      val project = DbtManifestReader.read(
        Paths.get("src/test/resources/dbt-fixtures/minimal-manifest.json"))

      val tables = DbtManifestReader.toSemanticTables(project, spark, _ => ordersDf(spark))
      val orders = tables("orders")

      // Query: SELECT order_id, total_revenue GROUP BY order_id.
      // With 2 rows (1+2) the measure evaluates to 3.
      val result = orders.query(
        measures   = Seq("total_revenue"),
        dimensions = Seq("order_id"),
        orderBy    = Seq(SortKey.Asc("order_id")),
        limit      = Some(10),
      ).toDataFrame(spark).collect()

      assert(result.length == 2)
      val byId = result.map(r => (r.getInt(0), r.getDouble(1))).toMap
      assert(byId(1) == 10.0)
      assert(byId(2) == 20.0)
    } finally spark.stop()
  }

  test("toSemanticTables: unresolvable source table raises a typed error") {
    val spark = newSparkSession()
    try {
      val project = DbtManifestReader.read(
        Paths.get("src/test/resources/dbt-fixtures/minimal-manifest.json"))

      val ex = intercept[IllegalArgumentException] {
        DbtManifestReader.toSemanticTables(project, spark, _ =>
          throw new RuntimeException("not found"))
      }
      assert(ex.getMessage.contains("Could not resolve"))
      assert(ex.getMessage.contains("orders"))
      assert(ex.getMessage.contains("customers"))
    } finally spark.stop()
  }

  // -----------------------------------------------------------------
  // Edge cases
  // -----------------------------------------------------------------

  test("formatSourceTable: alias only when no database/schema") {
    val project = DbtManifestReader.read(Map[String, Any](
      "metadata" -> Map[String, Any](),
      "nodes" -> Map[String, Any](
        "model.x.t" -> Map[String, Any](
          "resource_type" -> "model",
          "name"          -> "t",
          "alias"         -> "t",
          "columns"       -> Map[String, Any](),
        ),
      ),
      "sources" -> Map[String, Any](),
    ))
    assert(project.models("t").sourceTable == "t")
  }

  test("formatSourceTable: schema-qualified when only schema is set") {
    val project = DbtManifestReader.read(Map[String, Any](
      "metadata" -> Map[String, Any](),
      "nodes" -> Map[String, Any](
        "model.x.t" -> Map[String, Any](
          "resource_type" -> "model",
          "name"          -> "t",
          "schema"        -> "main",
          "alias"         -> "t",
          "columns"       -> Map[String, Any](),
        ),
      ),
      "sources" -> Map[String, Any](),
    ))
    assert(project.models("t").sourceTable == "main.t")
  }

  test("dimension-without-meta stays a dimension (not silently promoted)") {
    val project = DbtManifestReader.read(Map[String, Any](
      "metadata" -> Map[String, Any](),
      "nodes" -> Map[String, Any](
        "model.x.t" -> Map[String, Any](
          "resource_type" -> "model",
          "name"          -> "t",
          "alias"         -> "t",
          "columns"       -> Map[String, Any](
            "id" -> Map[String, Any]("name" -> "id"),
            // meta exists but has no kind/expr — should stay a dimension.
            "amt" -> Map[String, Any]("name" -> "amt", "meta" -> Map[String, Any]("owner" -> "data")),
            // meta with kind=measure but no expr — also a dimension (no expr to evaluate).
            "weird" -> Map[String, Any]("name" -> "weird", "meta" -> Map[String, Any]("kind" -> "measure")),
          ),
        ),
      ),
      "sources" -> Map[String, Any](),
    ))
    val t = project.models("t")
    assert(t.dimensions.map(_.name).toSet == Set("id", "amt", "weird"))
    assert(t.measures.isEmpty)
  }

  // -----------------------------------------------------------------
  // Fixtures
  // -----------------------------------------------------------------

  private def newSparkSession(): SparkSession = {
    val s = SparkSession.builder()
      .master("local[1]")
      .appName("dbt-reader-test")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    s.sparkContext.setLogLevel("WARN")
    s
  }

  private def emptyOrdersDf(spark: SparkSession) = {
    val schema = StructType(Seq(
      StructField("order_id",    IntegerType),
      StructField("customer_id", IntegerType),
      StructField("order_date",  StringType),
      StructField("amount",      org.apache.spark.sql.types.DoubleType),
    ))
    spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
  }

  private def ordersDf(spark: SparkSession) = {
    val rows = spark.sparkContext.parallelize(Seq(
      Row(1, 100, "2026-01-01", 10.0),
      Row(2, 100, "2026-01-02", 20.0),
    ))
    val schema = StructType(Seq(
      StructField("order_id",    IntegerType),
      StructField("customer_id", IntegerType),
      StructField("order_date",  StringType),
      StructField("amount",      org.apache.spark.sql.types.DoubleType),
    ))
    spark.createDataFrame(rows, schema)
  }
}
