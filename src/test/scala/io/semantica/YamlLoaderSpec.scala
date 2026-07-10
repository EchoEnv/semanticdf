package io.semantica

import java.io.{File, PrintWriter}

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

/** Tests for the YAML model loader.
  *
  * These verify that YAML-defined models produce IDENTICAL results to the Scala DSL
  * for every feature: dimensions, base measures, calc measures, percent-of-total,
  * and joins. The YAML loader builds the same Dimension/Measure/SemanticTable objects,
  * so correctness parity is the key invariant.
  *
  * Golden values match FlightsFixture: {AA→550, UA→775, DL→1050} total passengers,
  * 10 flights per carrier, grand total = 2375.
  */
class YamlLoaderSpec extends AnyFunSuite with SparkSessionFixture with FlightsFixture {

  private def carriersTable: DataFrame = {
    val session = spark
    import session.implicits._
    // Keyed by 'carrier' (same name as flights) so the equi-join uses symmetric keys.
    Seq(("AA", "American"), ("UA", "United"), ("DL", "Delta")).toDF("carrier", "name")
  }

  private def flightsTables: Map[String, DataFrame] = Map(
    "flights_tbl"  -> flightsDf,
    "carriers_tbl" -> carriersTable,
  )

  private def writeYaml(content: String): String = {
    val f = File.createTempFile("semantica-test", ".yml")
    f.deleteOnExit()
    val w = new PrintWriter(f)
    w.write(content)
    w.close()
    f.getAbsolutePath
  }

  // -------------------------------------------------------------------------
  // Basic loading
  // -------------------------------------------------------------------------

  test("load YAML — catalog accessors match Scala DSL") {
    val path = writeYaml(
      """
        |flights:
        |  table: flights_tbl
        |  dimensions:
        |    carrier: carrier
        |    origin: origin
        |  measures:
        |    total_passengers: "sum(passengers)"
        |    flight_count: "count(1)"
        |  calculated_measures:
        |    avg_passengers: "total_passengers / flight_count"
        |""".stripMargin)

    val flights = YamlLoader.load(path, flightsTables)("flights")

    flights.dimensions.keys should contain theSameElementsAs Set("carrier", "origin")
    flights.measures.keys should contain theSameElementsAs
      Set("total_passengers", "flight_count", "avg_passengers")
  }

  test("load YAML — group-by produces same golden values as Scala DSL") {
    val path = writeYaml(
      """
        |flights:
        |  table: flights_tbl
        |  dimensions:
        |    carrier: carrier
        |  measures:
        |    total_passengers: "sum(passengers)"
        |    flight_count: "count(1)"
        |""".stripMargin)

    val flights = YamlLoader.load(path, flightsTables)("flights")
    val rows = flights.groupBy("carrier").aggregate("total_passengers")
      .execute(spark).collect()
      .map(r => r.getString(0) -> r.getAs[Any](1).toString.toLong).toMap

    // FlightsFixture golden: {AA→550, UA→775, DL→1050}
    rows("AA") shouldEqual 550L
    rows("UA") shouldEqual 775L
    rows("DL") shouldEqual 1050L
  }

  // -------------------------------------------------------------------------
  // Calc measures
  // -------------------------------------------------------------------------

  test("load YAML — calc measure (ratio) matches Scala DSL") {
    val path = writeYaml(
      """
        |flights:
        |  table: flights_tbl
        |  dimensions:
        |    carrier: carrier
        |  measures:
        |    total_passengers: "sum(passengers)"
        |    flight_count: "count(1)"
        |  calculated_measures:
        |    avg_passengers: "total_passengers / flight_count"
        |""".stripMargin)

    val flights = YamlLoader.load(path, flightsTables)("flights")
    val rows = flights.groupBy("carrier").aggregate("avg_passengers")
      .execute(spark).collect()
      .map(r => r.getString(0) -> r.getAs[Double]("avg_passengers")).toMap

    // AA: 550/10=55, UA: 775/10=77.5, DL: 1050/10=105
    rows("AA") shouldEqual 55.0 +- 0.001
    rows("UA") shouldEqual 77.5 +- 0.001
    rows("DL") shouldEqual 105.0 +- 0.001
  }

  test("load YAML — calc-of-calc (transitive) works") {
    val path = writeYaml(
      """
        |flights:
        |  table: flights_tbl
        |  dimensions:
        |    carrier: carrier
        |  measures:
        |    total_distance: "sum(distance)"
        |    total_passengers: "sum(passengers)"
        |    flight_count: "count(1)"
        |  calculated_measures:
        |    avg_distance: "total_distance / flight_count"
        |    doubled_avg: "avg_distance * 2"
        |""".stripMargin)

    val flights = YamlLoader.load(path, flightsTables)("flights")
    val rows = flights.groupBy("carrier").aggregate("avg_distance", "doubled_avg")
      .execute(spark).collect()
      .map(r => r.getString(0) -> (r.getAs[Double]("avg_distance"), r.getAs[Double]("doubled_avg")))

    rows.foreach { case (_, (avg, doubled)) =>
      doubled shouldEqual avg * 2.0 +- 0.001
    }
  }

  // -------------------------------------------------------------------------
  // Percent-of-total
  // -------------------------------------------------------------------------

  test("load YAML — percent-of-total all() recomputes at zero grain") {
    val path = writeYaml(
      """
        |flights:
        |  table: flights_tbl
        |  dimensions:
        |    carrier: carrier
        |  measures:
        |    total_passengers: "sum(passengers)"
        |  calculated_measures:
        |    pct_of_total: "total_passengers / all(total_passengers)"
        |""".stripMargin)

    val flights = YamlLoader.load(path, flightsTables)("flights")
    val rows = flights.groupBy("carrier").aggregate("total_passengers", "pct_of_total")
      .execute(spark).collect()
      .map(r => r.getString(0) -> r.getAs[Any](2).toString.toDouble).toMap

    // Grand total = 2375. all(total_passengers) = 2375, NOT 550+775+1050 summed per-group.
    val total = 2375.0
    rows("AA") shouldEqual 550.0 / total +- 0.001
    rows("UA") shouldEqual 775.0 / total +- 0.001
    rows("DL") shouldEqual 1050.0 / total +- 0.001
  }

  test("load YAML — all() on a calc measure recomputes the formula at zero grain") {
    // The classic BI trap: all(avg) should recompute avg at zero grain, not sum per-group avgs.
    val path = writeYaml(
      """
        |flights:
        |  table: flights_tbl
        |  dimensions:
        |    carrier: carrier
        |  measures:
        |    total_passengers: "sum(passengers)"
        |    flight_count: "count(1)"
        |  calculated_measures:
        |    avg_passengers: "total_passengers / flight_count"
        |    # all() of a calc recomputes: (grand total_pax) / (grand flight_count)
        |    avg_share: "avg_passengers / all(avg_passengers)"
        |""".stripMargin)

    val flights = YamlLoader.load(path, flightsTables)("flights")
    val rows = flights.groupBy("carrier").aggregate("avg_passengers", "avg_share")
      .execute(spark).collect()
      .map(r => r.getString(0) -> (r.getAs[Double]("avg_passengers"), r.getAs[Double]("avg_share"))).toMap

    // Grand avg = 2375 / 30 = 79.1666...
    val grandAvg = 2375.0 / 30.0
    rows("AA")._2 shouldEqual (55.0 / grandAvg) +- 0.01
    rows("UA")._2 shouldEqual (77.5 / grandAvg) +- 0.01
    rows("DL")._2 shouldEqual (105.0 / grandAvg) +- 0.01
  }

  // -------------------------------------------------------------------------
  // Calc classification correctness (the load-bearing test)
  // -------------------------------------------------------------------------

  test("calc with all() is correctly classified (not misclassified as base)") {
    // This is the test that proves CalcExpr goes through the scope.
    // If all() were in an expr() string, classification would NOT detect it,
    // and the totals table would never be built → runtime error.
    val path = writeYaml(
      """
        |flights:
        |  table: flights_tbl
        |  dimensions:
        |    carrier: carrier
        |  measures:
        |    total_passengers: "sum(passengers)"
        |  calculated_measures:
        |    pct: "total_passengers / all(total_passengers)"
        |""".stripMargin)

    val flights = YamlLoader.load(path, flightsTables)("flights")
    // If classification failed, this would throw (no totals table built).
    val rows = flights.groupBy("carrier").aggregate("pct").execute(spark).collect()
    rows should have size 3
  }

  // -------------------------------------------------------------------------
  // Joins
  // -------------------------------------------------------------------------

  test("load YAML — join_one adds joined dimensions") {
    val path = writeYaml(
      """
        |flights:
        |  table: flights_tbl
        |  dimensions:
        |    carrier: carrier
        |  measures:
        |    total_passengers: "sum(passengers)"
        |  joins:
        |    carriers:
        |      model: carriers
        |      type: one
        |      left_on: carrier
        |      right_on: carrier
        |carriers:
        |  table: carriers_tbl
        |  dimensions:
        |    carrier: carrier
        |    name: name
        |  measures:
        |    carrier_count: "count(1)"
        |""".stripMargin)

    val flights = YamlLoader.load(path, flightsTables)("flights")

    // Joined model should have dimensions from both sides (right dims prefixed).
    flights.dimensions.keys should contain("carrier")
    flights.dimensions.keys should contain("carriers.name")

    val rows = flights.groupBy("carriers.name").aggregate("total_passengers")
      .execute(spark).collect()

    // Sort in Scala — orderBy("carriers.name") mis-parses the dot as catalog.table.col.
    rows.map(_.getString(0)).sorted shouldEqual Seq("American", "Delta", "United")
  }

  test("load YAML — join_many prevents fan-out") {
    // Custom fixture: orders has 'amount' (unique), items has 'qty' (unique).
    // The measure sum(amount) must NOT resolve on the items side (no 'amount' col),
    // so pre-agg only computes it on orders → no duplication after join.
    val session = spark
    import session.implicits._
    val tablesWithOrders = flightsTables ++ Map(
      "orders_tbl" -> Seq(
        ("o1", "cust_A", 100), ("o2", "cust_A", 200), ("o3", "cust_B", 150)
      ).toDF("order_id", "customer_id", "amount"),
      "items_tbl"  -> Seq(
        ("o1", 10), ("o1", 20), ("o2", 30), ("o3", 40), ("o3", 50), ("o3", 60)
      ).toDF("order_id", "qty"),
    )

    val path = writeYaml(
      """
        |orders:
        |  table: orders_tbl
        |  dimensions:
        |    order_id: order_id
        |    customer_id: customer_id
        |  measures:
        |    total_amount: "sum(amount)"
        |  joins:
        |    items:
        |      model: items
        |      type: many
        |      left_on: order_id
        |      right_on: order_id
        |items:
        |  table: items_tbl
        |  dimensions:
        |    order_id: order_id
        |  measures:
        |    item_qty: "sum(qty)"
        |""".stripMargin)

    val models = YamlLoader.load(path, tablesWithOrders)
    val orders = models("orders")

    // total_amount must NOT be inflated by the 1:many item rows.
    // sum(amount) = 100+200+150 = 450 (NOT multiplied by item-row fan-out).
    val total = orders.groupBy().aggregate("total_amount").execute(spark).collect()
    total.head.getAs[Long]("total_amount") shouldEqual 450L

    // Per-customer breakdown (proves join + fan-out prevention together):
    // cust_A = 100+200 = 300, cust_B = 150.
    val byCust = orders.groupBy("customer_id").aggregate("total_amount")
      .execute(spark).collect()
      .map(r => r.getString(0) -> r.getAs[Long]("total_amount")).toMap
    byCust("cust_A") shouldEqual 300L
    byCust("cust_B") shouldEqual 150L
  }

  // -------------------------------------------------------------------------
  // Error handling
  // -------------------------------------------------------------------------

  test("missing 'table' field gives a clear error") {
    val path = writeYaml(
      """
        |flights:
        |  dimensions:
        |    carrier: carrier
        |""".stripMargin)

    val ex = intercept[IllegalArgumentException] {
      YamlLoader.load(path, flightsTables)
    }
    ex.getMessage should include("table")
  }

  test("join to undefined model gives a clear error") {
    val path = writeYaml(
      """
        |flights:
        |  table: flights_tbl
        |  joins:
        |    ghost:
        |      model: nonexistent
        |      type: one
        |      left_on: carrier
        |      right_on: code
        |""".stripMargin)

    val ex = intercept[IllegalArgumentException] {
      YamlLoader.load(path, flightsTables)
    }
    ex.getMessage should include("nonexistent")
  }

  // -------------------------------------------------------------------------
  // CalcExpr parser unit tests (the engine under the YAML loader)
  // -------------------------------------------------------------------------

  test("CalcExpr: precedence — a + b * c parses without error") {
    val df = spark.emptyDataFrame.withColumn("a", lit(1)).withColumn("b", lit(2)).withColumn("c", lit(3))
    val scope = new MeasureScope(df, Set("a", "b", "c"))
    CalcExpr(scope, "a + b * c")  // should not throw
    CalcExpr(scope, "(a + b) * c")  // parens override
  }

  test("CalcExpr: identical expression returns the same cached AST node") {
    // The AST cache must be keyed by expression string so the parser runs once
    // and every subsequent invocation is a pure tree-walk eval. This test pins
    // that contract: if caching regresses (e.g. someone removes parseCached),
    // the node identity check fails.
    val a = CalcExpr.parseCached("total_a / total_b")
    val b = CalcExpr.parseCached("total_a / total_b")
    assert(a eq b, "identical expressions must return the same cached AST node")
    // A different expression returns a different node.
    val c = CalcExpr.parseCached("total_a * 2")
    assert(a ne c)
  }

  test("CalcExpr: trailing input is rejected") {
    val df = spark.emptyDataFrame.withColumn("a", lit(1))
    val scope = new MeasureScope(df, Set("a"))
    intercept[IllegalArgumentException] {
      CalcExpr(scope, "a )")
    }
  }

  test("CalcExpr: all() detected by ClassificationScope") {
    // The load-bearing correctness property: CalcExpr calls scope.all(name),
    // which the ClassificationScope records. This is what makes YAML calcs
    // participate in the framework's classification (base vs calc, totals).
    val df = spark.emptyDataFrame.withColumn("total", lit(1))
    val probe = new ClassificationScope(df, Set("total"))
    CalcExpr(probe, "total / all(total)")
    probe.referencedMeasures should contain("total")
    probe.referencedTotals should contain("total")
  }

  test("CalcExpr: calc references detected by ClassificationScope") {
    val df = spark.emptyDataFrame.withColumn("base_col", lit(1))
    val probe = new ClassificationScope(df, Set("total_a", "total_b"))
    CalcExpr(probe, "total_a / total_b")
    probe.referencedMeasures should contain("total_a")
    probe.referencedMeasures should contain("total_b")
  }

  // -------------------------------------------------------------------------
  // Spark metastore loading (spark.table)
  // -------------------------------------------------------------------------

  test("load from Spark metastore — spark.table resolution") {
    flightsDf.createOrReplaceTempView("flights_tbl")

    val path = writeYaml(
      """
        |flights:
        |  table: flights_tbl
        |  dimensions:
        |    carrier: carrier
        |  measures:
        |    total_passengers: "sum(passengers)"
        |""".stripMargin)

    val flights = YamlLoader.load(path, spark)("flights")
    val rows = flights.groupBy("carrier").aggregate("total_passengers")
      .execute(spark).collect().map(_.getString(0)).toSet

    rows shouldEqual Set("AA", "UA", "DL")
  }

  // -------------------------------------------------------------------------
  // Time dimension
  // -------------------------------------------------------------------------

  test("load YAML — time dimension enables atTimeGrain") {
    val path = writeYaml(
      """
        |flights:
        |  table: flights_tbl
        |  dimensions:
        |    carrier: carrier
        |    dep_time:
        |      expr: dep_time
        |      is_time_dimension: true
        |      smallest_time_grain: day
        |  measures:
        |    flight_count: "count(1)"
        |""".stripMargin)

    val flights = YamlLoader.load(path, flightsTables)("flights")
    val depTime = flights.findDimension("dep_time")
    depTime shouldBe defined
    depTime.get.isTimeDimension shouldEqual true
    depTime.get.smallestTimeGrain shouldEqual Some("day")
  }

  // -------------------------------------------------------------------------
  // Metadata fields (description + arbitrary key-value pairs)
  // -------------------------------------------------------------------------

  test("DSL: Dimension carries description and metadata") {
    import MeasureExtra._
    val d = Dimension(
      "carrier",
      t => t("carrier"),
      description = Some("Airline code"),
      metadata = Map("owner" -> "platform", "tier" -> "core", "pii" -> "false"),
    )
    d.description shouldEqual Some("Airline code")
    d.metadata("owner") shouldEqual "platform"
    d.metadata("tier") shouldEqual "core"
    d.metadata("pii") shouldEqual "false"
  }

  test("DSL: Measure carries description and metadata") {
    import MeasureExtra._
    val m = Measure(
      "total_rev",
      t => org.apache.spark.sql.functions.sum(t("amount")),
      description = Some("Total revenue in USD"),
      metadata = Map("owner" -> "finance", "unit" -> "USD", "aggregation" -> "sum"),
    )
    m.description shouldEqual Some("Total revenue in USD")
    m.metadata("owner") shouldEqual "finance"
    m.metadata("unit") shouldEqual "USD"
    m.metadata("aggregation") shouldEqual "sum"
  }

  test("DSL: MeasureExtra helper methods work") {
    import MeasureExtra._
    val base = Measure("rev", t => org.apache.spark.sql.functions.sum(t("amount")))
    val tagged = owner(unit(tag(base, "tier" -> "primary"), "USD"), "finance")
    tagged.metadata shouldEqual Map("tier" -> "primary", "unit" -> "USD", "owner" -> "finance")
    describe(tagged, "Total revenue").description shouldEqual Some("Total revenue")
  }

  test("YAML: dimensions and measures carry metadata through load") {
    val path = writeYaml(
      """
        |flights:
        |  table: flights_tbl
        |  dimensions:
        |    carrier:
        |      expr: carrier
        |      description: "Airline code"
        |      metadata:
        |        owner: platform-team
        |        pii: "false"
        |  measures:
        |    total_passengers:
        |      expr: "sum(passengers)"
        |      description: "Total passengers"
        |      metadata:
        |        owner: analytics-team
        |        unit: count
        |        aggregation: sum
        |""".stripMargin)

    val flights = YamlLoader.load(path, flightsTables)("flights")
    val carrier = flights.dimensions("carrier")
    carrier.description shouldEqual Some("Airline code")
    carrier.metadata("owner") shouldEqual "platform-team"
    carrier.metadata("pii") shouldEqual "false"

    val totalPax = flights.measures("total_passengers")
    totalPax.description shouldEqual Some("Total passengers")
    totalPax.metadata("owner") shouldEqual "analytics-team"
    totalPax.metadata("unit") shouldEqual "count"
    totalPax.metadata("aggregation") shouldEqual "sum"
  }
}
