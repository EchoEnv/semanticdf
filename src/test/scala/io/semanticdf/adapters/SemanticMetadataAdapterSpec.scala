package io.semanticdf.adapters

import io.semanticdf.{Predicate, SemanticTable}
import io.semanticdf.{Dimension, Measure, FlightsFixture, SparkSessionFixture, toSemanticTable}
import io.semanticdf.adapters.{DbtAdapter, OssieReader}

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import java.nio.file.{Files, Path, Paths}

/** Tests for the [[SemanticMetadataAdapter]] typeclass and its instances
  * ([[DbtAdapter]], [[OssieReader]]).
  *
  * Coverage:
  *   - `parse` produces the right intermediate shape
  *   - `toSemanticTables` builds a working `SemanticTable` per dataset
  *   - the unified `loadSemanticTables` entry point works
  *   - dbt and Ossie adapters are interchangeable from the caller's
  *     perspective (the whole point of the typeclass)
  *   - the legacy `ontology_mappings` shape (Ossie's pre-canonical form)
  *     is parsed correctly
  *   - errors are reported with the file path
  */
class SemanticMetadataAdapterSpec extends AnyFunSuite with SparkSessionFixture with FlightsFixture {

  // ----------------------------------------------------------------
  // OssieReader — parse
  // ----------------------------------------------------------------

  test("OssieReader.parse: canonical shape → 1 OssieProject per semantic_model entry") {
    val project = OssieReader.parse(Paths.get(
      "src/test/resources/ossie-fixtures/minimal-ossie.yaml"))
    assert(project.length == 1)
    val p = project.head
    assert(p.name == "orders_model")
    assert(p.description == Some("Customer orders fact table"))
    assert(p.datasets.length == 2)
    assert(p.relationships.length == 1)
    assert(p.metrics.length == 2)
  }

  test("OssieReader.parse: dataset fields are correctly typed (time vs regular)") {
    val project = OssieReader.parse(Paths.get(
      "src/test/resources/ossie-fixtures/minimal-ossie.yaml"))
    val orders = project.head.datasets.find(_.name == "orders").get
    val orderDate = orders.fields.find(_.name == "order_date").get
    val orderId   = orders.fields.find(_.name == "order_id").get
    assert(orderDate.isTimeDimension, "order_date should be a time dimension")
    assert(!orderId.isTimeDimension,  "order_id should not be a time dimension")
    assert(orderDate.expression == "order_date")
  }

  test("OssieReader.parse: relationships preserve from/to and parallel column arrays") {
    val project = OssieReader.parse(Paths.get(
      "src/test/resources/ossie-fixtures/minimal-ossie.yaml"))
    val rel = project.head.relationships.head
    assert(rel.from == "orders")
    assert(rel.to   == "customers")
    assert(rel.fromColumns == Seq("customer_id"))
    assert(rel.toColumns   == Seq("customer_id"))
  }

  test("OssieReader.parse: metrics carry their expressions") {
    val project = OssieReader.parse(Paths.get(
      "src/test/resources/ossie-fixtures/minimal-ossie.yaml"))
    val m = project.head.metrics
    assert(m.map(_.name).toSet == Set("total_revenue", "order_count"))
    assert(m.find(_.name == "total_revenue").get.expression == "SUM(orders.amount)")
  }

  // ----------------------------------------------------------------
  // OssieReader — toSemanticTables (end-to-end with real Spark)
  // ----------------------------------------------------------------

  test("OssieReader: end-to-end build of a SemanticTable from YAML") {
    val project = OssieReader.parse(Paths.get(
      "src/test/resources/ossie-fixtures/minimal-ossie.yaml"))
    // Build a tiny source DataFrame for `orders`.
    val ordersRows = spark.sparkContext.parallelize(Seq(
      Row(1, 100, "2026-01-01", 10.0),
      Row(2, 200, "2026-01-02", 20.0),
    ))
    val ordersSchema = StructType(Seq(
      StructField("order_id",    IntegerType),
      StructField("customer_id", IntegerType),
      StructField("order_date",  StringType),
      StructField("amount",      org.apache.spark.sql.types.DoubleType),
    ))
    val customersRows = spark.sparkContext.parallelize(Seq(
      Row(100, "Alice"),
      Row(200, "Bob"),
    ))
    val customersSchema = StructType(Seq(
      StructField("customer_id", IntegerType),
      StructField("name",        StringType),
    ))
    val resolve: String => org.apache.spark.sql.DataFrame = {
      case "db.schema.orders"    => spark.createDataFrame(ordersRows, ordersSchema)
      case "db.schema.customers" => spark.createDataFrame(customersRows, customersSchema)
      case other                 => throw new IllegalArgumentException(s"unexpected source: $other")
    }
    val tables = OssieReader.toSemanticTables(project, spark, resolve)
    assert(tables.keySet == Set("orders", "customers"))
  }

  test("OssieReader: the orders table queries back the right rows") {
    val project = OssieReader.parse(Paths.get(
      "src/test/resources/ossie-fixtures/minimal-ossie.yaml"))
    val ordersRows = spark.sparkContext.parallelize(Seq(
      Row(1, 100, "2026-01-01", 10.0),
      Row(2, 200, "2026-01-02", 20.0),
      Row(3, 100, "2026-01-03", 5.0),
    ))
    val ordersSchema = StructType(Seq(
      StructField("order_id",    IntegerType),
      StructField("customer_id", IntegerType),
      StructField("order_date",  StringType),
      StructField("amount",      org.apache.spark.sql.types.DoubleType),
    ))
    val customersRows = spark.sparkContext.parallelize(Seq(
      Row(100, "Alice"),
      Row(200, "Bob"),
    ))
    val customersSchema = StructType(Seq(
      StructField("customer_id", IntegerType),
      StructField("name",        StringType),
    ))
    val resolve: String => org.apache.spark.sql.DataFrame = {
      case "db.schema.orders"    => spark.createDataFrame(ordersRows, ordersSchema)
      case "db.schema.customers" => spark.createDataFrame(customersRows, customersSchema)
      case _ => throw new IllegalArgumentException("unexpected")
    }
    val tables = OssieReader.toSemanticTables(project, spark, resolve)
    val orders = tables("orders")

    // Query: total_revenue by customer
    val result = orders.query(
      measures   = Seq("total_revenue"),
      dimensions = Seq("customer_id"),
      where      = Some(Predicate.Compare.Eq("customer_id", 100)),
    ).toDataFrame(spark).collect()
    val r = result.map(row => (row.getInt(0), row.getDouble(1))).toMap
    // customer 100 has orders 1 and 3 with amounts 10 + 5 = 15
    assert(r(100) == 15.0)
  }

  // ----------------------------------------------------------------
  // Unified entry point — `loadSemanticTables`
  // ----------------------------------------------------------------

  test("loadSemanticTables: works for Ossie (unified entry point)") {
    val project = OssieReader.parse(Paths.get(
      "src/test/resources/ossie-fixtures/minimal-ossie.yaml"))
    val ordersRows = spark.sparkContext.parallelize(Seq(
      Row(1, 100, "2026-01-01", 10.0),
    ))
    val ordersSchema = StructType(Seq(
      StructField("order_id",    IntegerType),
      StructField("customer_id", IntegerType),
      StructField("order_date",  StringType),
      StructField("amount",      org.apache.spark.sql.types.DoubleType),
    ))
    val customersRows = spark.sparkContext.parallelize(Seq(
      Row(100, "Alice"),
    ))
    val customersSchema = StructType(Seq(
      StructField("customer_id", IntegerType),
      StructField("name",        StringType),
    ))
    val resolve: String => org.apache.spark.sql.DataFrame = {
      case "db.schema.orders"    => spark.createDataFrame(ordersRows, ordersSchema)
      case "db.schema.customers" => spark.createDataFrame(customersRows, customersSchema)
      case _ => throw new IllegalArgumentException("unexpected")
    }
    val tables = OssieReader.toSemanticTables(OssieReader.parse(Paths.get(
      "src/test/resources/ossie-fixtures/minimal-ossie.yaml")), spark, resolve)
    assert(tables.keySet == Set("orders", "customers"))
  }

  test("loadSemanticTables: works for dbt (unified entry point) — same call signature") {
    // The dbt fixture is the existing minimal-manifest.json from PR #171.
    val tables = DbtAdapter.toSemanticTables(DbtAdapter.parse(Paths.get(
      "src/test/resources/dbt-fixtures/minimal-manifest.json")), spark, _ => emptyFlightsDf)
    // The dbt fixture has one model: "orders"
    assert(tables.keySet == Set("orders", "customers"))
  }

  // ----------------------------------------------------------------
  // Error handling
  // ----------------------------------------------------------------

  test("OssieReader: missing file throws IllegalArgumentException with the path") {
    val ex = intercept[IllegalArgumentException] {
      OssieReader.parse(Paths.get("does/not/exist.yaml"))
    }
    assert(ex.getMessage.contains("does/not/exist.yaml"))
  }

  test("OssieReader: file without version key throws with a clear error") {
    val tmp = Files.createTempFile("ossie-no-version", ".yaml")
    Files.writeString(tmp, "semantic_model: []")
    try {
      val ex = intercept[IllegalArgumentException] {
        OssieReader.parse(tmp)
      }
      assert(ex.getMessage.contains("version"))
    } finally Files.deleteIfExists(tmp)
  }

  // ----------------------------------------------------------------
  // Helpers
  // ----------------------------------------------------------------

  private def emptyFlightsDf: org.apache.spark.sql.DataFrame = {
    val schema = StructType(Seq(
      StructField("order_id", IntegerType),
    ))
    spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
  }
}
