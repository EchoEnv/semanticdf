package io.semanticdf.tools

import io.semanticdf.YamlLoader

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** End-to-end test for the SQL CLI: build a SemanticTable from a real
  * YAML, run a SQL string through SqlCli, execute the result against
  * a real Spark session. */
class SqlCliEndToEndSpec extends AnyFunSuite with Matchers {

  private def setupSpark(): SparkSession = {
    val s = SparkSession.builder()
      .master("local[1]").appName("sql-cli-e2e").getOrCreate()
    s.sparkContext.setLogLevel("WARN")
    val rows = s.sparkContext.parallelize(Seq(
      Row("AA", 100L), Row("AA", 200L),
      Row("UA", 50L),  Row("UA", 75L),
    ))
    val schema = StructType(Seq(
      StructField("carrier", StringType),
      StructField("passengers", LongType),
    ))
    s.createDataFrame(rows, schema).createOrReplaceTempView("flights_csv")
    s
  }

  private def loadModel(spark: SparkSession) =
    YamlLoader.loadDir("src/test/resources/sql-cli-fixtures", spark)("flights")

  test("runs a SQL query end-to-end") {
    val spark = setupSpark()
    try {
      val parsed = SqlCli.parse(
        "SELECT carrier, total_passengers FROM flights " +
        "GROUP BY carrier ORDER BY total_passengers DESC LIMIT 10")
      val rows = SqlCli(parsed, loadModel(spark))
        .toDataFrame(spark).collect()
        .map(r => (r.getString(0), r.getLong(1))).toSeq
      assert(rows == Seq(("AA", 300L), ("UA", 125L)))
    } finally spark.stop()
  }

  test("WHERE clause filters before aggregation") {
    val spark = setupSpark()
    try {
      val parsed = SqlCli.parse(
        "SELECT carrier, total_passengers FROM flights " +
        "WHERE carrier = 'AA' GROUP BY carrier")
      val rows = SqlCli(parsed, loadModel(spark))
        .toDataFrame(spark).collect()
        .map(r => (r.getString(0), r.getLong(1)))
      assert(rows.toSeq == Seq(("AA", 300L)))
    } finally spark.stop()
  }

  test("SELECT * returns all dims then all measures") {
    val spark = setupSpark()
    try {
      val parsed = SqlCli.parse("SELECT * FROM flights GROUP BY carrier")
      val result = SqlCli(parsed, loadModel(spark)).toDataFrame(spark)
      assert(result.columns.toSet == Set("carrier", "total_passengers"))
    } finally spark.stop()
  }

  test("unknown field name surfaces a clear error") {
    val spark = setupSpark()
    try {
      val parsed = SqlCli.parse(
        "SELECT carrier, nope FROM flights GROUP BY carrier")
      val ex = intercept[IllegalArgumentException] {
        SqlCli.resolve(parsed, loadModel(spark))
      }
      assert(ex.getMessage.contains("nope"))
      assert(ex.getMessage.contains("Dims:"))
      assert(ex.getMessage.contains("Measures:"))
    } finally spark.stop()
  }
}
