package io.semanticdf

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.must.Matchers

/** Regression tests for io.semanticdf.tools.Introspector. */
class IntrospectorSpec extends AnyFunSpec with SparkSessionFixture with Matchers {

  import scala.language.implicitConversions

  describe("Introspector") {

    it("infers string columns as dimensions") {
      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(
          Row("Boeing", "ATL"),
          Row("Airbus", "LAX"),
        )), StructType(Seq(
          StructField("manufacturer", StringType),
          StructField("origin", StringType),
      )))

      val yaml = new io.semanticdf.tools.Introspector().toYaml(df, "test_model")
      yaml must include("manufacturer:")
      yaml must include("origin:")
      yaml must include("  expr: manufacturer")
      yaml must include("  expr: origin")
      yaml must not include "sum("
    }

    it("infers numeric columns as measures") {
      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(
          Row(150.5, 42L),
          Row(200.0, 18L),
        )), StructType(Seq(
          StructField("revenue", DoubleType),
          StructField("pax_count", LongType),
      )))

      val yaml = new io.semanticdf.tools.Introspector().toYaml(df, "test_model")
      yaml must include("revenue:")
      yaml must include("pax_count:")
      yaml must include("sum(revenue)")
      yaml must include("sum(pax_count)")
    }

    it("marks entity columns (id / _id / _key) as entity dims with metadata") {
      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(
          Row("order_123", "cust_456"),
          Row("order_789", "cust_012"),
        )), StructType(Seq(
          StructField("order_id", StringType),
          StructField("customer_id", StringType),
      )))

      val yaml = new io.semanticdf.tools.Introspector().toYaml(df, "test_model")
      yaml must include("tags: identifier")
      // Both should be inferred as dimensions (string type), and join placeholders too
      yaml must include("order_id:")
      yaml must include("customer_id:")
      yaml must include("join")
    }

    it("marks email/phone/ssn columns as pii metadata") {
      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(
          Row("user_1", "alice@example.com", "555-1234"),
        )), StructType(Seq(
          StructField("user_id", StringType),
          StructField("email", StringType),
          StructField("phone", StringType),
      )))

      val yaml = new io.semanticdf.tools.Introspector().toYaml(df, "test_model")
      yaml must include("pii: true")
      // email and phone are pii strings → dims with pii metadata
      yaml must include("email:")
      yaml must include("phone:")
    }

    it("infers timestamp columns as time dimensions") {
      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(
          Row(java.sql.Timestamp.valueOf("2024-01-01 10:00:00")),
        )), StructType(Seq(
          StructField("created_at", TimestampType),
      )))

      val yaml = new io.semanticdf.tools.Introspector().toYaml(df, "test_model")
      yaml must include("created_at:")
      yaml must include("is_time_dimension: true")
    }

    it("limits measures to maxMeasures") {
      val fields = (1 to 20).map(i => StructField(s"m_$i", DoubleType))
      val df = spark.createDataFrame(
        spark.sparkContext.emptyRDD[Row],
        StructType(fields),
      )

      val yaml = new io.semanticdf.tools.Introspector(
        io.semanticdf.tools.Introspector.Config(maxMeasures = 5)
      ).toYaml(df, "test_model")

      // Count measure entries by their expr: lines (reliable, no whitespace ambiguity)
      val measureExprLines = yaml.linesIterator.count(l => l.trim.startsWith("expr:"))
      measureExprLines mustEqual 5
    }

    it("handles a mixed-schema table (flights-like)") {
      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(
          Row("Boeing", "ATL", 150, 3.14, 2024, java.sql.Timestamp.valueOf("2024-03-01 08:00:00")),
        )), StructType(Seq(
          StructField("carrier", StringType),
          StructField("origin", StringType),
          StructField("passengers", IntegerType),
          StructField("distance", DoubleType),
          StructField("year", IntegerType),
          StructField("flight_date", TimestampType),
      )))

      val yaml = new io.semanticdf.tools.Introspector().toYaml(df, "flights")
      yaml must include("carrier:")
      yaml must include("origin:")
      yaml must include("passengers:")
      yaml must include("distance:")
      yaml must include("year:")
      yaml must include("flight_date:")
      yaml must include("is_time_dimension: true")
      yaml must include("sum(passengers)")
      yaml must include("avg(distance)")
    }

    it("fromFile reads a CSV and produces YAML") {
      val csvPath = getClass.getResource("/flights.csv").getPath
      val yaml = new io.semanticdf.tools.Introspector().fromFile(
        spark, csvPath, format = "csv", modelName = "flights_from_csv"
      )
      yaml must include("flights_from_csv:")
      yaml must include("dimensions:")
      yaml must include("measures:")
      yaml must include("carrier")
      // Numeric columns should become measures
      yaml must include("passengers") // integer → sum
      // Verify the output is useful (not just placeholders)
      yaml must include("sum(passengers)")
      yaml must include("sum(distance)")
      // String dims (carrier, origin, dest) should be listed
      yaml must include("expr: carrier")
    }

    // Demo test — shows what the introspector produces for the flights CSV.
    // Run with: mvn test -Dtest="IntrospectorSpec::flights demo"
    it("flights demo: show real YAML output") {
      val csvPath = getClass.getResource("/flights.csv").getPath
      val yaml = new io.semanticdf.tools.Introspector(
        io.semanticdf.tools.Introspector.Config(maxMeasures = 8)
      ).fromFile(spark, csvPath, "csv", "flights")
      println("\n========== INTROSPECTOR OUTPUT (flights.csv) ==========")
      println(yaml)
      println("=====================================================\n")
      yaml must include("flights:")
    }

    it("throws IllegalArgumentException for empty model name") {
      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1.0))),
        StructType(Seq(StructField("value", DoubleType))),
      )
      val caught = the[IllegalArgumentException] thrownBy {
        new io.semanticdf.tools.Introspector().toYaml(df, "")
      }
      caught.getMessage must include("model name")
    }

    // (N) # WARN: header lines for fields the Introspector couldn't classify.
    // These are the raw signals the MCP `introspect` tool surfaces via
    // `response.warnings` — the contract requires them so an agent knows
    // which fields were silently dropped.

    it("emits # WARN: for unclassified fields when they don't make the measure cut") {
      // MapType has no dimension or measure classification — it falls
      // through to the `_ =>` branch in classifyField and gets
      // (false, None, None), i.e. suggestedMeasureKind = None. With
      // maxMeasures=1, the unclassified field is dropped (numeric
      // measures always sort higher) and should be warned.
      val schema = StructType(Seq(
        StructField("category", StringType),       // dimension
        StructField("kv_pairs", MapType(StringType, IntegerType)), // unclassified
        StructField("revenue", DoubleType),       // numeric measure (priority 2)
      ))
      val df = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
      val yaml = new io.semanticdf.tools.Introspector(
        io.semanticdf.tools.Introspector.Config(maxMeasures = 1)
      ).toYaml(df, "test_model")
      yaml must include("# WARN:")
      yaml must include("'kv_pairs'")
      yaml must include("no dimension or measure classification")
      // The classified fields are NOT warned.
      yaml must not include "'category'"
      yaml must not include "'revenue'"
    }

    it("emits # WARN: when a measure candidate is dropped by maxMeasures") {
      // With maxMeasures=1 and three numeric candidates, two should be dropped.
      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(
          Row(1.0, 2.0, 3.0),
        )),
        StructType(Seq(
          StructField("a", DoubleType),
          StructField("b", DoubleType),
          StructField("c", DoubleType),
        )),
      )
      val yaml = new io.semanticdf.tools.Introspector(
        io.semanticdf.tools.Introspector.Config(maxMeasures = 1)
      ).toYaml(df, "test_model")
      yaml must include("max measures limit reached")
      val warnCount = "# WARN:".r.findAllIn(yaml).length
      warnCount mustBe 2
    }

    it("emits no # WARN: lines when every field is classified") {
      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(
          Row("AA", 100.0),
        )),
        StructType(Seq(
          StructField("carrier", StringType),
          StructField("revenue", DoubleType),
        )),
      )
      val yaml = new io.semanticdf.tools.Introspector().toYaml(df, "test_model")
      yaml must not include "# WARN:"
    }
  }
}
