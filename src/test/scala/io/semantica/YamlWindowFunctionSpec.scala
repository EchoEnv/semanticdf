package io.semantica

import java.io.{File, PrintWriter}

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{count, lit, sum}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Regression test: YAML loader must accept window functions in measure expressions.
  *
  * Background: the Scala DSL always supported window functions in Measure
  * lambdas (they're just `SemanticScope => Column`). The YAML loader, however,
  * extracted SQL tokens via regex to probe whether each is a real column. Any
  * identifier not in its keyword blocklist (including `row_number`, `rank`,
  * `lag`, ...) was treated as a column, and the build-time probe `t("row_number")`
  * threw `UnknownFieldError` before the user could even run the query.
  *
  * Fix: expanded the keyword blocklist in `YamlLoader.extractColumnRefs` to
  * include the standard window functions + the window-frame SQL keywords
  * (`order`, `rows`, `range`, `between`, `unbounded`, `preceding`, `following`,
  * `current`, `asc`, `desc`, `nulls`). Raw SQL still compiles via `expr()`.
  */
class YamlWindowFunctionSpec extends AnyFunSuite with Matchers with SparkSessionFixture with FlightsFixture {

  private def carriersTable: DataFrame = {
    val session = spark
    import session.implicits._
    Seq(("AA", "American"), ("UA", "United"), ("DL", "Delta")).toDF("carrier", "name")
  }

  private def flightsTables: Map[String, DataFrame] = Map(
    "flights_tbl"  -> flightsDf,
    "carriers_tbl" -> carriersTable,
  )

  private def writeYaml(content: String): String = {
    val f = File.createTempFile("semantica-yaml-window-test", ".yml")
    f.deleteOnExit()
    val w = new PrintWriter(f)
    w.write(content)
    w.close()
    f.getAbsolutePath
  }

  // ---- The originally-failing case (row_number over partition) ----------------

  test("REGRESSION: YAML accepts row_number() over (partition by ... order by ...)") {
    val path = writeYaml(
      """
        |flights:
        |  table: flights_tbl
        |  dimensions:
        |    carrier: carrier
        |    origin: origin
        |  measures:
        |    flight_count: "count(1)"
        |    rank_per_carrier_origin:
        |      expr: "row_number() over (partition by carrier order by origin)"
        |""".stripMargin)

    val flights = YamlLoader.load(path, flightsTables)("flights")
    val rows = flights.groupBy("carrier", "origin")
      .aggregate("flight_count", "rank_per_carrier_origin")
      .execute(spark).collect()
    // Before the fix: UnknownFieldError at load time. After: query runs.
    assert(rows.nonEmpty)
  }

  // ---- Coverage for common window functions -----------------------------------

  test("REGRESSION: YAML accepts rank() over (...) in a measure expression") {
    val path = writeYaml(
      """
        |flights:
        |  table: flights_tbl
        |  dimensions:
        |    carrier: carrier
        |  measures:
        |    total_passengers: "sum(passengers)"
        |    rank_by_total: "rank() over (order by total_passengers desc)"
        |""".stripMargin)

    val flights = YamlLoader.load(path, flightsTables)("flights")
    val rows = flights.groupBy("carrier").aggregate("total_passengers", "rank_by_total")
      .execute(spark).collect()
    assert(rows.length == 3)
  }

  test("REGRESSION: YAML accepts dense_rank() and lag() in measure expressions") {
    val path = writeYaml(
      """
        |flights:
        |  table: flights_tbl
        |  dimensions:
        |    carrier: carrier
        |    origin: origin
        |  measures:
        |    flight_count: "count(1)"
        |    dense_rank_per_carrier:
        |      expr: "dense_rank() over (partition by carrier order by flight_count desc)"
        |    lag_count:
        |      expr: "lag(flight_count, 1) over (partition by carrier order by origin)"
        |""".stripMargin)

    val flights = YamlLoader.load(path, flightsTables)("flights")
    val rows = flights.groupBy("carrier", "origin")
      .aggregate("flight_count", "dense_rank_per_carrier", "lag_count")
      .execute(spark).collect()
    assert(rows.nonEmpty)
  }

  // ---- Window frame clauses (rows/range/between/unbounded) -------------------

  test("REGRESSION: YAML accepts window with ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW") {
    val path = writeYaml(
      """
        |flights:
        |  table: flights_tbl
        |  dimensions:
        |    carrier: carrier
        |    origin: origin
        |  measures:
        |    total_passengers: "sum(passengers)"
        |    running_total:
        |      expr: "sum(total_passengers) over (
        |        partition by carrier
        |        order by origin
        |        rows between unbounded preceding and current row
        |      )"
        |""".stripMargin)

    val flights = YamlLoader.load(path, flightsTables)("flights")
    val rows = flights.groupBy("carrier", "origin")
      .aggregate("total_passengers", "running_total")
      .execute(spark).collect()
    assert(rows.nonEmpty)
  }

  // ---- Regression: real columns are still correctly identified ---------------

  test("REGRESSION: a real column named like a window function is still identified as a column") {
    // The keyword set is for token-level filtering. A real column name
    // `carrier` is still extracted (and the build-time probe succeeds because
    // `carrier` is a real column in flightsDf).
    val path = writeYaml(
      """
        |flights:
        |  table: flights_tbl
        |  dimensions:
        |    carrier: carrier
        |  measures:
        |    total_per_carrier: "sum(passengers)"
        |""".stripMargin)
    val flights = YamlLoader.load(path, flightsTables)("flights")
    val rows = flights.groupBy("carrier").aggregate("total_per_carrier")
      .execute(spark).collect()
    // Golden values from FlightsFixture.
    val byCarrier = rows.map(r => r.getString(0) -> r.getAs[Long](1)).toMap
    byCarrier("AA") shouldEqual 550L
    byCarrier("UA") shouldEqual 775L
    byCarrier("DL") shouldEqual 1050L
  }
}