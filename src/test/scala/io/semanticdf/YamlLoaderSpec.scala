package io.semanticdf

import java.io.{File, PrintWriter}

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import io.semanticdf.StreamingSupport._

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

  /** Tables fixture with a `ts` timestamp column for time-dimension tests.
    * Uses the [[flightsWithTimeDf]] fixture (3 months × 6 carriers, 18 rows). */
  private def flightsTimeTables: Map[String, DataFrame] = Map(
    "flights_ts_tbl" -> flightsWithTimeDf,
  )

  private def writeYaml(content: String): String = {
    val f = File.createTempFile("semanticdf-test", ".yml")
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
        |  table: flights_ts_tbl
        |  dimensions:
        |    carrier: carrier
        |    ts:
        |      expr: ts
        |      is_time_dimension: true
        |      smallest_time_grain: day
        |  measures:
        |    flight_count: "count(1)"
        |""".stripMargin)

    val flights = YamlLoader.load(path, flightsTimeTables)("flights")
    val depTime = flights.findDimension("ts")
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

  test("YAML: calc measures also carry metadata (not silently dropped)") {
    // Regression: buildCalcMeasure used to discard metadata, breaking the
    // schema introspection for calculated_measures.
    val path = writeYaml(
      """
        |flights:
        |  table: flights_tbl
        |  dimensions:
        |    carrier: carrier
        |  measures:
        |    total: "sum(passengers)"
        |  calculated_measures:
        |    pct:
        |      expr: "total / all(total)"
        |      description: "Percent of total"
        |      metadata:
        |        owner: analytics-team
        |        unit: ratio
        |""".stripMargin)

    val flights = YamlLoader.load(path, flightsTables)("flights")
    val pct = flights.measures("pct")
    pct.description shouldEqual Some("Percent of total")
    pct.metadata("owner") shouldEqual "analytics-team"
    pct.metadata("unit") shouldEqual "ratio"
  }

  test("YAML: metadata values may be lists (auto-joined to comma-string)") {
    // Lists in metadata are coerced to comma-separated strings so they fit in
    // Map[String, String]. This avoids ClassCastException at load time.
    val path = writeYaml(
      """
        |flights:
        |  table: flights_tbl
        |  dimensions:
        |    carrier:
        |      expr: carrier
        |      metadata:
        |        tags: [airline, identifier]
        |""".stripMargin)
    val flights = YamlLoader.load(path, flightsTables)("flights")
    flights.dimensions("carrier").metadata("tags") shouldEqual "airline,identifier"
  }

  // -------------------------------------------------------------------------
  // Model introspection — schema(spark)  (the analogue of df.schema)
  // -------------------------------------------------------------------------

  test("schema(spark): returns a DataFrame with one row per field") {
    val path = writeYaml(
      """
        |flights:
        |  table: flights_tbl
        |  description: "Flight facts"
        |  dimensions:
        |    carrier:
        |      expr: carrier
        |      description: "Airline code"
        |      metadata:
        |        owner: platform-team
        |  measures:
        |    total_passengers:
        |      expr: "sum(passengers)"
        |      description: "Total passengers"
        |      metadata:
        |        owner: analytics-team
        |        unit: count
        |""".stripMargin)

    val flights = YamlLoader.load(path, flightsTables)("flights")
    val schema = flights.schema(spark)

    schema.columns should contain theSameElementsAs Seq(
      "model_name", "model_description", "field_name", "field_type",
      "description", "metadata_keys", "metadata_values",
      "is_entity", "is_time_dimension", "smallest_grain",
      "join_source", "join_cardinality",
    )

    val rows = schema.collect()
    rows.length shouldEqual 2  // 1 dimension + 1 measure

    val dimRow = rows.find(_.getAs[String]("field_type") == "dimension").get
    dimRow.getAs[String]("field_name") shouldEqual "carrier"
    dimRow.getAs[String]("model_name") shouldEqual "flights"
    dimRow.getAs[String]("model_description") shouldEqual "Flight facts"
    dimRow.getAs[String]("description") shouldEqual "Airline code"
    dimRow.getAs[String]("metadata_keys") shouldEqual "owner"
    dimRow.getAs[String]("metadata_values") shouldEqual "platform-team"
    dimRow.getAs[Boolean]("is_entity") shouldEqual false

    val measRow = rows.find(_.getAs[String]("field_type") == "measure").get
    measRow.getAs[String]("field_name") shouldEqual "total_passengers"
    measRow.getAs[String]("description") shouldEqual "Total passengers"
    measRow.getAs[Boolean]("is_entity") shouldEqual false  // measures are never entity dims
    measRow.getAs[Boolean]("is_time_dimension") shouldEqual false
  }

  test("schema(spark): join_source is set for fields from a joined table") {
    // Both models must be in the same YAML file so load() sees both at once.
    // carriers_tbl uses 'carrier' as the key column (same as flights_tbl) for symmetric join.
    val path = writeYaml(
      """
        |flights:
        |  table: flights_tbl
        |  description: "Flight facts"
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
        |""".stripMargin)

    val models = YamlLoader.load(path, flightsTables)
    val flights: io.semanticdf.SemanticTable = models("flights")
    val schema: org.apache.spark.sql.DataFrame = flights.schema(spark)

    // The "name" field (from carriers) should have join_source set.
    val nameRows = schema.filter(col("field_name") === "name").collect()
    assert(nameRows.length >= 1, "Expected at least 1 'name' row from carriers join")
    val nameRow = nameRows.head
    val js: String = nameRow.getAs[String]("join_source")
    val jc: String = nameRow.getAs[String]("join_cardinality")
    assert(js == "carriers", "join_source for 'name' should be 'carriers': " + js)
    assert(jc == "One", "join_cardinality for 'name' should be 'One': " + jc)

    // Primary model fields (carrier) have no join_source.
    val carrierRows = schema.filter(col("field_name") === "carrier").collect()
    assert(carrierRows.length >= 1, "Expected at least 1 'carrier' row")
    val carrierJs: String = carrierRows.head.getAs[String]("join_source")
    assert(carrierJs == null, "join_source for 'carrier' should be null: " + carrierJs)
  }

  // -------------------------------------------------------------------------
  // loadDir — directory-based multi-file loading
  // -------------------------------------------------------------------------

  test("loadDir: loads all *.yml files in a directory into one merged map") {
    // Create a temp directory with two YAML files.
    val dir = java.nio.file.Files.createTempDirectory("semanticdf-loadDir").toFile
    dir.deleteOnExit()

    def writeFile(name: String, content: String): Unit = {
      val f = new java.io.File(dir, name)
      f.deleteOnExit()
      new java.io.PrintWriter(f) { write(content); close() }
    }

    writeFile("flights.yml",
      """
        |flights:
        |  table: flights_tbl
        |  dimensions:
        |    carrier: carrier
        |  measures:
        |    total_passengers: "sum(passengers)"
        |""".stripMargin)

    writeFile("carriers.yml",
      """
        |carriers:
        |  table: carriers_tbl
        |  dimensions:
        |    code: carrier
        |    name: name
        |""".stripMargin)

    val models = YamlLoader.loadDir(dir.getAbsolutePath, flightsTables)

    assert(models.size == 2, s"Expected 2 models, got ${models.size}: ${models.keySet}")
    assert(models.contains("flights"), "Should contain 'flights'")
    assert(models.contains("carriers"), "Should contain 'carriers'")

    // Verify models are functional.
    val rows = models("flights").groupBy("carrier").aggregate("total_passengers")
      .toDataFrame(spark).collect()
    assert(rows.nonEmpty, "Should produce rows from flights model")
  }

  test("loadDir: throws on duplicate model name across files") {
    val dir = java.nio.file.Files.createTempDirectory("semanticdf-loadDir-dup").toFile
    dir.deleteOnExit()

    def writeFile(name: String, content: String): Unit = {
      val f = new java.io.File(dir, name)
      f.deleteOnExit()
      new java.io.PrintWriter(f) { write(content); close() }
    }

    writeFile("flights_a.yml",
      """
        |flights:
        |  table: flights_tbl
        |  dimensions:
        |    carrier: carrier
        |""".stripMargin)

    writeFile("flights_b.yml",
      """
        |flights:
        |  table: flights_tbl
        |  dimensions:
        |    origin: origin
        |""".stripMargin)

    val ex = intercept[IllegalArgumentException] {
      YamlLoader.loadDir(dir.getAbsolutePath, flightsTables)
    }
    assert(ex.getMessage.contains("Duplicate model names"),
      s"Expected duplicate-model error, got: ${ex.getMessage}")
    assert(ex.getMessage.contains("flights"),
      s"Error should mention 'flights': ${ex.getMessage}")
  }

  test("loadDir: throws when directory does not exist") {
    val ex = intercept[IllegalArgumentException] {
      YamlLoader.loadDir("/nonexistent/directory/path", flightsTables)
    }
    assert(ex.getMessage.contains("not found") || ex.getMessage.contains("Nonexistent"))
  }

  test("loadDir: throws when directory has no .yml files") {
    val dir = java.nio.file.Files.createTempDirectory("semanticdf-loadDir-empty").toFile
    dir.deleteOnExit()
    new java.io.PrintWriter(new java.io.File(dir, "readme.txt")) {
      write("just a text file"); close()
    }
    val ex = intercept[IllegalArgumentException] {
      YamlLoader.loadDir(dir.getAbsolutePath, flightsTables)
    }
    assert(ex.getMessage.contains("no .yml files") || ex.getMessage.contains("no .yml"))
  }

  test("loadDir: cross-file joins work (model in file A references model in file B)") {
    // This is the real-world pattern: each YAML file defines one logical model,
    // and joins across models span files. Without the two-pass build, the join
    // would fail because the lookup model isn't yet defined when flights.yml is
    // parsed.
    val dir = java.nio.file.Files.createTempDirectory("semanticdf-loadDir-crossjoin").toFile
    dir.deleteOnExit()

    def writeFile(name: String, content: String): Unit = {
      val f = new java.io.File(dir, name)
      f.deleteOnExit()
      new java.io.PrintWriter(f) { write(content); close() }
    }

    writeFile("flights.yml",
      """
        |flights:
        |  table: flights_tbl
        |  dimensions:
        |    carrier: carrier
        |  measures:
        |    total: "sum(passengers)"
        |  joins:
        |    carriers:
        |      model: carriers
        |      type: one
        |      left_on: carrier
        |      right_on: carrier
        |""".stripMargin)

    writeFile("carriers.yml",
      """
        |carriers:
        |  table: carriers_tbl
        |  dimensions:
        |    carrier: carrier
        |    name: name
        |""".stripMargin)

    val models = YamlLoader.loadDir(dir.getAbsolutePath, flightsTables)
    assert(models.size == 2, s"Expected 2 models, got ${models.size}")
    assert(models.contains("flights"))
    assert(models.contains("carriers"))

    // The cross-file join should compile and run.
    val rows = models("flights")
      .groupBy("carrier", "name")
      .aggregate("total")
      .toDataFrame(spark)
      .collect()
    assert(rows.nonEmpty, "Cross-file join should produce rows")
  }

  // -------------------------------------------------------------------------
  // Per-model version: field declared at the top of the model block
  // -------------------------------------------------------------------------

  test("YAML 'version:' is exposed via SemanticTable.version") {
    val path = writeYaml(
      """
        |flights:
        |  version: 2
        |  table: flights_tbl
        |  dimensions:
        |    carrier: carrier
        |  measures:
        |    flight_count: "count(1)"
        |""".stripMargin)
    val flights = YamlLoader.load(path, flightsTables)("flights")
    assert(flights.version == 2, s"Expected version=2, got ${flights.version}")
  }

  test("YAML without 'version:' defaults to version 0") {
    val path = writeYaml(
      """
        |flights:
        |  table: flights_tbl
        |  dimensions:
        |    carrier: carrier
        |  measures:
        |    flight_count: "count(1)"
        |""".stripMargin)
    val flights = YamlLoader.load(path, flightsTables)("flights")
    assert(flights.version == 0, s"Expected version=0 (pre-versioning default), got ${flights.version}")
  }

  test("YAML 'version: 0' is accepted (explicit pre-versioning marker)") {
    val path = writeYaml(
      """
        |flights:
        |  version: 0
        |  table: flights_tbl
        |  dimensions:
        |    carrier: carrier
        |  measures:
        |    flight_count: "count(1)"
        |""".stripMargin)
    val flights = YamlLoader.load(path, flightsTables)("flights")
    assert(flights.version == 0, s"Expected version=0, got ${flights.version}")
  }

  test("YAML 'version: -1' is rejected (must be non-negative)") {
    val path = writeYaml(
      """
        |flights:
        |  version: -1
        |  table: flights_tbl
        |  dimensions:
        |    carrier: carrier
        |""".stripMargin)
    val ex = intercept[IllegalArgumentException] {
      YamlLoader.load(path, flightsTables)
    }
    assert(ex.getMessage.toLowerCase.contains("non-negative"),
      s"Expected non-negative error, got: ${ex.getMessage}")
  }

  test("YAML 'version: \"two\"' is rejected (must be an integer)") {
    val path = writeYaml(
      """
        |flights:
        |  version: "two"
        |  table: flights_tbl
        |  dimensions:
        |    carrier: carrier
        |""".stripMargin)
    val ex = intercept[IllegalArgumentException] {
      YamlLoader.load(path, flightsTables)
    }
    assert(ex.getMessage.toLowerCase.contains("integer"),
      s"Expected integer error, got: ${ex.getMessage}")
  }

  test("Scala DSL: .version(n) attaches version to a freshly built SemanticTable") {
    val flights = YamlLoader.load(
      writeYaml(
        """
          |flights:
          |  table: flights_tbl
          |  dimensions:
          |    carrier: carrier
          |  measures:
          |    flight_count: "count(1)"
          |""".stripMargin),
      flightsTables)("flights")
    assert(flights.version == 0, "Sanity: unmodified loader -> v0")
    val bumped = flights.version(3)
    assert(bumped.version == 3, s"Expected version=3 after .version(3), got ${bumped.version}")
    // Original is untouched (immutable)
    assert(flights.version == 0, "Original SemanticTable.version must remain 0 after .version()")
  }

  // -------------------------------------------------------------------------
  // Pre-join filters: YAML `filters:` block
  // -------------------------------------------------------------------------

  test("YAML 'filters:' loads and exposes them via SemanticTable.filters") {
    val path = writeYaml(
      """
        |flights:
        |  table: flights_tbl
        |  filters:
        |    require_origin_and_carrier:
        |      expr: "origin IS NOT NULL AND carrier IS NOT NULL"
        |      description: "Drop rows with null origin or carrier."
        |      metadata:
        |        owner: data-platform-team
        |        tags: [data-quality]
        |  dimensions:
        |    carrier: carrier
        |  measures:
        |    flight_count: "count(1)"
        |""".stripMargin)
    val flights = YamlLoader.load(path, flightsTables)("flights")
    assert(flights.filters.length == 1, s"Expected 1 filter, got ${flights.filters.length}")
    val f = flights.filters.head
    assert(f.name == "require_origin_and_carrier")
    assert(f.expr == "origin IS NOT NULL AND carrier IS NOT NULL")
    assert(f.description.exists(_.contains("Drop rows")),
      s"Expected description to mention 'Drop rows', got: ${f.description}")
    assert(f.metadata("owner") == "data-platform-team")
    assert(f.metadata("tags") == "[data-quality]")
    // Sanity: filters appear in declaration order; only one here.
    assert(flights.filters.map(_.name) == Seq("require_origin_and_carrier"))
  }

  test("YAML model without 'filters:' has empty filters list") {
    val path = writeYaml(
      """
        |flights:
        |  table: flights_tbl
        |  dimensions:
        |    carrier: carrier
        |  measures:
        |    flight_count: "count(1)"
        |""".stripMargin)
    val flights = YamlLoader.load(path, flightsTables)("flights")
    assert(flights.filters.isEmpty,
      s"Expected empty filters, got ${flights.filters}")
  }

  test("YAML filter is enforced at execute time — null rows are dropped") {
    val path = writeYaml(
      """
        |flights:
        |  table: flights_tbl
        |  filters:
        |    require_origin:
        |      expr: "origin IS NOT NULL"
        |  dimensions:
        |    carrier: carrier
        |    origin: origin
        |  measures:
        |    flight_count: "count(1)"
        |""".stripMargin)
    val flights = YamlLoader.load(path, flightsTables)("flights")
    // flights_fixture has rows with origin=null and origin=non-null — verify the filter
    // actually drops the nulls at execute time.
    val rows = flights.toDataFrame(spark).collect()
    val nulls = rows.filter(_.isNullAt(rows(0).fieldIndex("origin")))
    assert(nulls.isEmpty,
      s"Filter 'require_origin' must drop null-origin rows; got ${nulls.length} survivors")
  }

  test("YAML filter that references a column NOT in the source table is rejected at load time") {
    val path = writeYaml(
      """
        |flights:
        |  table: flights_tbl
        |  filters:
        |    bad_filter:
        |      expr: "non_existent_column IS NOT NULL"
        |  dimensions:
        |    carrier: carrier
        |  measures:
        |    flight_count: "count(1)"
        |""".stripMargin)
    val ex = intercept[IllegalArgumentException] {
      YamlLoader.load(path, flightsTables)
    }
    assert(ex.getMessage.contains("non_existent_column"),
      s"Expected missing-column error, got: ${ex.getMessage}")
  }

  test("YAML filter is rejected when expr is missing") {
    val path = writeYaml(
      """
        |flights:
        |  table: flights_tbl
        |  filters:
        |    noop:
        |      description: "forgot the expr"
        |  dimensions:
        |    carrier: carrier
        |  measures:
        |    flight_count: "count(1)"
        |""".stripMargin)
    val ex = intercept[IllegalArgumentException] {
      YamlLoader.load(path, flightsTables)
    }
    assert(ex.getMessage.contains("expr"),
      s"Expected missing-expr error, got: ${ex.getMessage}")
  }

  test("YAML filter with malformed Spark SQL is rejected with parser error") {
    val path = writeYaml(
      """
        |flights:
        |  table: flights_tbl
        |  filters:
        |    bad_sql:
        |      expr: "this is not even close to SQL"
        |  dimensions:
        |    carrier: carrier
        |  measures:
        |    flight_count: "count(1)"
        |""".stripMargin)
    val ex = intercept[IllegalArgumentException] {
      YamlLoader.load(path, flightsTables)
    }
    assert(ex.getMessage.toLowerCase.contains("parse") ||
           ex.getMessage.toLowerCase.contains("sql"),
      s"Expected parse error, got: ${ex.getMessage}")
  }

  test("Scala DSL: filters are preserved through fluent operations") {
    val path = writeYaml(
      """
        |flights:
        |  table: flights_tbl
        |  filters:
        |    require_origin:
        |      expr: "origin IS NOT NULL"
        |  dimensions:
        |    carrier: carrier
        |  measures:
        |    flight_count: "count(1)"
        |""".stripMargin)
    val flights = YamlLoader.load(path, flightsTables)("flights")
    assert(flights.filters.length == 1)
    // withMeasures should preserve filters since they live in the op tree.
    val extraMeasure = io.semanticdf.Measure(
      "extra", t => org.apache.spark.sql.functions.count(org.apache.spark.sql.functions.lit(1)))
    val expanded = flights.withMeasures(extraMeasure)
    assert(expanded.filters.length == 1,
      s"Filters should survive .withMeasures, got ${expanded.filters}")
  }

  // -------------------------------------------------------------------------
  // exprString preservation (PR: feat/describe-model-expr-string)
  //
  // Regression: DescribeModel previously serialised Dimension/Measure `expr`
  // via the lambda's `toString`, producing opaque Lambda$... addresses. The
  // library fix: YamlLoader populates `exprString` from the YAML `expr:`
  // value for each dimension, base measure, and calc measure. DescribeModel
  // then prefers this string over the lambda fallback (back-compat).
  // -------------------------------------------------------------------------

  test("YamlLoader: dimension `expr:` value is preserved as exprString") {
    val path = writeYaml(
      """
        |flights:
        |  table: flights_tbl
        |  dimensions:
        |    carrier: carrier
        |    distance_km: "distance * 1.6"
        |""".stripMargin)
    val flights = YamlLoader.load(path, flightsTables)("flights")
    val dims = flights.dimensions

    // Simple identifier \u2014 scope lookup
    dims("carrier").exprString shouldBe Some("carrier")
    // Complex expression \u2014 functions.expr
    dims("distance_km").exprString shouldBe Some("distance * 1.6")
  }

  test("YamlLoader: base measure `expr:` value is preserved as exprString") {
    val path = writeYaml(
      """
        |flights:
        |  table: flights_tbl
        |  dimensions:
        |    carrier: carrier
        |  measures:
        |    flight_count: "count(1)"
        |    total_distance: "sum(distance)"
        |""".stripMargin)
    val flights = YamlLoader.load(path, flightsTables)("flights")
    flights.measures("flight_count").exprString shouldBe Some("count(1)")
    flights.measures("total_distance").exprString shouldBe Some("sum(distance)")
  }

  test("YamlLoader: calc measure `expr:` value is preserved as exprString") {
    val path = writeYaml(
      """
        |flights:
        |  table: flights_tbl
        |  dimensions:
        |    carrier: carrier
        |  measures:
        |    flight_count: "count(1)"
        |    total_distance: "sum(distance)"
        |  calculated_measures:
        |    avg_distance: "total_distance / flight_count"
        |""".stripMargin)
    val flights = YamlLoader.load(path, flightsTables)("flights")
    flights.measures("avg_distance").exprString shouldBe Some("total_distance / flight_count")
  }

  test("YamlLoader: entity and time dimensions also carry exprString") {
    val path = writeYaml(
      """
        |flights:
        |  table: flights_tbl
        |  dimensions:
        |    carrier:
        |      expr: carrier
        |      is_entity: true
        |    origin:
        |      expr: origin
        |      is_time_dimension: true
        |      smallest_time_grain: day
        |""".stripMargin)
    val flights = YamlLoader.load(path, flightsTables)("flights")
    flights.dimensions("carrier").exprString       shouldBe Some("carrier")
    flights.dimensions("carrier").isEntity         shouldBe true
    flights.dimensions("origin").exprString        shouldBe Some("origin")
    flights.dimensions("origin").isTimeDimension   shouldBe true
  }

  test("YamlLoader routes to toStreamingSemanticTable when the source DataFrame is streaming") {
    // YAML streaming auto-routing: passing a streaming DataFrame (the result
    // of `spark.readStream.*`) via the `tables` Map produces a streaming
    // model. The factory used internally must be `toStreamingSemanticTable`,
    // not `toSemanticTable`, otherwise `toStreamingQuery` rejects the model
    // with "could not find SemanticStreamingTableOp at the root".
    val stream = spark.readStream.format("rate").load()

    val f = File.createTempFile("yaml-streaming", ".yml")
    f.deleteOnExit()
    val w = new PrintWriter(f)
    try {
      w.write(
        """rate_events:
          |  table: rate_tbl
          |  dimensions:
          |    ts: timestamp
          |  measures:
          |    event_count: "count(1)"
          |""".stripMargin)
    } finally { w.close() }

    val models = YamlLoader.load(f.getAbsolutePath, Map("rate_tbl" -> stream))
    val m = models("rate_events")

    // The model has the streaming root op (so toStreamingQuery accepts it).
    val root = m.root
    assert(root.isInstanceOf[SemanticStreamingTableOp],
      s"expected SemanticStreamingTableOp root, got ${root.getClass.getSimpleName}")

    // Validator accepts the streaming model with a window + watermark spec.
    val options = StreamingQueryOptions(
      window     = Some(WindowSpec("timestamp", "1 second")),
      watermark  = Some(WatermarkSpec("timestamp", "10 minutes")),
      outputMode = "append",
    )
    StreamingValidator.validate(m, options)
  }

}
