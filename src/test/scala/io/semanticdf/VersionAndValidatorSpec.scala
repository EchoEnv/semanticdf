package io.semanticdf

import java.io.{File, PrintWriter}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite

/** Regression tests for three small-but-persistent correctness gaps:
  *
  *  1. `version` was silently dropped by `where` / `having` /
  *     `withTransforms`-on-join / `SemanticGroupBy.aggregate`. Now all four
  *     preserve it (matching `withDimensions` / `withMeasures` / `orderBy`
  *     / `limit` / `withHint`).
  *
  *  2. `requireRoot` threw `MatchError` on `SemanticFilterOp` /
  *     `SemanticOrderByOp` / `SemanticLimitOp` / `SemanticHintOp` roots
  *     (pre-existing — flagged by clean-compile as non-exhaustive). Now
  *     raises a clear error.
  *
  *  3. `SparkFilterValidator` lowercased parsed refs but not `sourceColumns`,
  *     so a filter `Origin IS NOT NULL` against a column named `Origin`
  *     was falsely rejected. Now both sides are lowercased.
  */
class VersionAndValidatorSpec extends AnyFunSuite with SparkSessionFixture with FlightsFixture {

  // Common fixtures ------------------------------------------------------------

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
    val f = File.createTempFile("version-validator", ".yml")
    f.deleteOnExit()
    val w = new PrintWriter(f); w.write(content); w.close()
    f.getAbsolutePath
  }

  /** Build a basic unfiltered flights model with declared version 2. */
  private def versionedModel = YamlLoader.load(writeYaml(
    """
      |flights:
      |  table: flights_tbl
      |  version: 2
      |  dimensions:
      |    carrier: carrier
      |    origin: origin
      |  measures:
      |    flight_count: "count(1)"
      |""".stripMargin), flightsTables)("flights")

  // --- 1. version preservation ----------------------------------------------

  test("version is preserved through where()") {
    val m = versionedModel
    assert(m.version == 2)
    val m2 = m.where(io.semanticdf.Predicate.Compare("eq", "carrier", "AA"))
    assert(m2.version == 2, s"where() dropped version, got ${m2.version}")
  }

  test("version is preserved through having()") {
    val m = versionedModel
    val m2 = m.having(io.semanticdf.Predicate.Compare("gt", "flight_count", 5))
    assert(m2.version == 2, s"having() dropped version, got ${m2.version}")
  }

  test("version is preserved through groupBy().aggregate()") {
    val m = versionedModel
    val aggregated = m.groupBy("carrier").aggregate("flight_count")
    assert(aggregated.version == 2,
      s"groupBy().aggregate() dropped version, got ${aggregated.version}")
  }

  test("version is preserved through withTransforms() on a join") {
    val flights = versionedModel
    val carriers = YamlLoader.load(writeYaml(
      """
        |carriers:
        |  table: carriers_tbl
        |  dimensions:
        |    carrier: carrier
        |    name: name
        |""".stripMargin), flightsTables)("carriers")
    val joined = flights.join_one(carriers, (l, r) => l("carrier") === r("carrier"))
    // Joins create a new table (no version by design — caller chooses). Set it
    // explicitly to 2 here so we can test that withTransforms preserves it.
    val joinedV = joined.version(2)
    val expanded = joinedV.withTransforms(io.semanticdf.Transform(
      "name_upper", _ => org.apache.spark.sql.functions.upper(org.apache.spark.sql.functions.col("name"))))
    assert(expanded.version == 2,
      s"withTransforms() on join dropped version, got ${expanded.version}")
  }

  // --- 2. requireRoot wrapper handling --------------------------------------

  test("requireRoot throws a clear error for SemanticFilterOp root") {
    val m = versionedModel.where(io.semanticdf.Predicate.Compare("eq", "carrier", "AA"))
    // Inner join would call requireRoot on a FilterOp-wrapped model; we
    // exercise it via the public DSL: join_one on a filtered left side.
    val ex = intercept[IllegalArgumentException] {
      m.join_one(versionedModel, (l, r) => l("carrier") === r("carrier"))
    }
    assert(ex.getMessage.contains("query wrapper"),
      s"Expected message about query wrapper, got: ${ex.getMessage}")
  }

  test("requireRoot throws a clear error for SemanticLimitOp root") {
    val m = versionedModel.limit(5)
    val ex = intercept[IllegalArgumentException] {
      m.join_one(versionedModel, (l, r) => l("carrier") === r("carrier"))
    }
    assert(ex.getMessage.contains("query wrapper"),
      s"Expected message about query wrapper, got: ${ex.getMessage}")
  }

  test("requireRoot throws a clear error for SemanticHintOp root") {
    val m = versionedModel.withHint("broadcast")
    val ex = intercept[IllegalArgumentException] {
      m.join_one(versionedModel, (l, r) => l("carrier") === r("carrier"))
    }
    assert(ex.getMessage.contains("query wrapper"),
      s"Expected message about query wrapper, got: ${ex.getMessage}")
  }

  // --- 3. SparkFilterValidator case-insensitive column matching -------------

  test("SparkFilterValidator accepts a filter that references MixedCase columns") {
    // Build a fixture with mixed-case columns. Without the fix, a filter
    // `Origin IS NOT NULL` (matching the column `Origin`) would be falsely
    // rejected because `sourceColumns` keeps original case but parsed refs
    // are lowercased.
    val schema = StructType(Seq(
      StructField("Origin",  StringType, nullable = true),
      StructField("Carrier", StringType, nullable = true),
    ))
    val rows: java.util.List[Row] = new java.util.ArrayList[Row]()
    rows.add(Row("JFK", "AA"))
    val df = spark.createDataFrame(rows, schema)
    val tables = Map[String, DataFrame]("mixed_tbl" -> df)

    val path = writeYaml(
      """
        |mixed:
        |  table: mixed_tbl
        |  filters:
        |    require_origin_mixed:
        |      expr: "Origin IS NOT NULL"
        |  dimensions:
        |    origin: "Origin"
        |""".stripMargin)
    // Should NOT throw IllegalArgumentException.
    val models = YamlLoader.load(path, tables)
    assert(models.contains("mixed"))
    assert(models("mixed").filters.length == 1)
    assert(models("mixed").filters.head.name == "require_origin_mixed")
  }
}
