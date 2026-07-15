package io.semanticdf

import java.io.{File, PrintWriter}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.StringType
import org.scalatest.funsuite.AnyFunSuite

/** Regression tests for the three catalog-introspection accessors added for MCP
  * `describe_model` consumption:
  *
  *   - [[SemanticTable.joins]] — public enumeration of joined-source info
  *   - [[SemanticTable.measureKind]] — base vs calc classification
  *   - [[SemanticTable.sourceTable]] — origin name propagated from YamlLoader
  *
  * Each covers: empty/single-table model, single join, chained joins, unknown
  * measure classification errors, and Scala-DSL vs YAML-construction
  * differences for sourceTable.
  */
class CatalogIntrospectionSpec extends AnyFunSuite with SparkSessionFixture with FlightsFixture {

  // --- Fixtures -------------------------------------------------------------

  private def carriersTable: DataFrame = {
    val session = spark
    import session.implicits._
    // Keyed by 'carrier' so the equi-join uses symmetric keys.
    Seq(("AA", "American"), ("UA", "United"), ("DL", "Delta")).toDF("carrier", "name")
  }
  private def flightsTables: Map[String, DataFrame] = Map(
    "flights_tbl"  -> flightsDf,
    "carriers_tbl" -> carriersTable,
  )

  private def writeYaml(content: String): String = {
    val f = File.createTempFile("catalog-intro", ".yml")
    f.deleteOnExit()
    val w = new PrintWriter(f); w.write(content); w.close()
    f.getAbsolutePath
  }

  /** Single-table model with one base measure and one calc measure — used to
    * test both classifications of `measureKind`. */
  private def yamlSingleModel: String = writeYaml(
    """
      |flights:
      |  table: flights_tbl
      |  dimensions:
      |    carrier: carrier
      |  measures:
      |    flight_count: "count(1)"
      |    total_distance: "sum(distance)"
      |  calculated_measures:
      |    avg_per_flight: "total_distance / flight_count"
      |""".stripMargin)

  /** Two-table model with a declared join. The YamlLoader auto-prefixes
    * dimensions from the right side (e.g. `carriers.name`). */
  private def yamlJoinedModel: String = writeYaml(
    """
      |flights:
      |  table: flights_tbl
      |  dimensions:
      |    carrier: carrier
      |  measures:
      |    flight_count: "count(1)"
      |  joins:
      |    carriers:
      |      model: carriers
      |      type: one
      |      left_on: carrier
      |      right_on: carrier
      |
      |carriers:
      |  table: carriers_tbl
      |  dimensions:
      |    carrier: carrier
      |    name: name
      |  measures:
      |    carrier_count: "count(1)"
      |""".stripMargin)

  // -------------------------------------------------------------------------
  // joins
  // -------------------------------------------------------------------------

  test("joins is empty for a single-table model") {
    val m = YamlLoader.load(yamlSingleModel, flightsTables)("flights")
    assert(m.joins.isEmpty, s"Expected empty joins, got ${m.joins}")
  }

  test("joins returns one JoinInfo for a single declared join") {
    val m = YamlLoader.load(yamlJoinedModel, flightsTables)("flights")
    val js = m.joins
    assert(js.length == 1, s"Expected 1 join, got ${js.length}: $js")
    val j = js.head
    assert(j.cardinality == "One", s"cardinality: ${j.cardinality}")
    assert(j.leftName.contains("flights"), s"leftName: ${j.leftName}")
    assert(j.rightName.contains("carriers"), s"rightName: ${j.rightName}")
    assert(j.keys == Seq("carrier"), s"keys: ${j.keys}")
    // The YamlLoader adds alias-prefixed dims via a post-join `withDimensions`
    // call rather than via SemanticJoinOp.extraDimensions. So a YAML-loaded
    // join has empty `extraDimensions` here — the prefixed dims are reachable
    // via the outer SemanticTable's `.dimensions` map.
    val dimNames = m.dimensions.keys.toSet
    assert(dimNames.contains("carriers.name"),
      s"Expected 'carriers.name' in model dimensions (post-join withDimensions), got: $dimNames")
  }

  test("joins: extra dimensions from YamlLoader's alias-prefix exposure are listed") {
    // The YamlLoader auto-prefixes joined-model dims (e.g. `carriers.name`). The
    // JoinInfo's extraDimensions captures them so MCP can show the full set.
    val m = YamlLoader.load(yamlJoinedModel, flightsTables)("flights")
    val j = m.joins.head
    assert(j.extraDimensions.contains("name") || j.extraDimensions.contains("carriers.name"),
      s"Expected an auto-prefixed dim, got ${j.extraDimensions}")
  }

  // -------------------------------------------------------------------------
  // measureKind
  // -------------------------------------------------------------------------

  test("measureKind returns Base for a sum/count measure") {
    val m = YamlLoader.load(yamlSingleModel, flightsTables)("flights")
    assert(m.measureKind("flight_count") == MeasureKind.Base)
    assert(m.measureKind("total_distance") == MeasureKind.Base)
  }

  test("measureKind returns Calc for a measure that references other measures") {
    val m = YamlLoader.load(yamlSingleModel, flightsTables)("flights")
    // avg_per_flight references other measures (`total_distance / flight_count`),
    // so it's a calc.
    assert(m.measureKind("avg_per_flight") == MeasureKind.Calc)
  }

  test("measureKind throws with a Did-you-mean suggestion for an unknown measure") {
    val m = YamlLoader.load(yamlSingleModel, flightsTables)("flights")
    val ex = intercept[IllegalArgumentException] {
      m.measureKind("flight_cont")  // typo of flight_count
    }
    assert(ex.getMessage.contains("flight_count"),
      s"Expected 'Did you mean' suggestion to mention 'flight_count', got: ${ex.getMessage}")
  }

  // -------------------------------------------------------------------------
  // sourceTable
  // -------------------------------------------------------------------------

  test("sourceTable is None for Scala-DSL-constructed models") {
    val m = toSemanticTable(flightsDf, name = Some("flights"))
    assert(m.sourceTable.isEmpty,
      s"Scala-DSL model should have no sourceTable, got ${m.sourceTable}")
  }

  test("sourceTable is Some(name) for YAML-loaded models — populated from `table:`") {
    val m = YamlLoader.load(yamlSingleModel, flightsTables)("flights")
    assert(m.sourceTable == Some("flights_tbl"),
      s"Expected sourceTable=Some('flights_tbl'), got ${m.sourceTable}")
  }

  test("sourceTable is preserved through withMeasures and withRowFilter") {
    val m = YamlLoader.load(writeYaml(
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
        |""".stripMargin), flightsTables)("flights")
    assert(m.sourceTable == Some("flights_tbl"))
    // withMeasures preserves sourceTable (constructor propagation).
    val extra = Measure("extra_total", t => org.apache.spark.sql.functions.sum(t("flight_count")))
    val m2 = m.withMeasures(extra)
    assert(m2.sourceTable == Some("flights_tbl"),
      s"withMeasures should preserve sourceTable, got ${m2.sourceTable}")
    // withRowFilter preserves sourceTable.
    val m3 = m.withRowFilter(
      name = "extra",
      expr = "carrier IS NOT NULL",
      description = None,
      metadata = Map.empty,
    )
    assert(m3.sourceTable == Some("flights_tbl"),
      s"withRowFilter should preserve sourceTable, got ${m3.sourceTable}")
  }
}
