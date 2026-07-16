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

  // --- 3b. SparkFilterValidator sees transform outputs (v0.1.1) -----
  //
  // Before v0.1.1, SparkFilterValidator only knew about the source DataFrame's
  // columns. A filter that referenced a transform output (e.g. a derived
  // timestamp column produced by a `transforms:` block) would be falsely
  // rejected. The fix: the YamlLoader passes the cumulative column set
  // (source + transforms) to the validator, so transform outputs are visible.

  test("SparkFilterValidator accepts a filter that references a transform output") {
    // Source has `ts`; transform adds `flight_year`; filter references `flight_year`.
    val schema = org.apache.spark.sql.types.StructType(Seq(
      org.apache.spark.sql.types.StructField("ts", org.apache.spark.sql.types.StringType, nullable = true),
    ))
    val rows: java.util.List[org.apache.spark.sql.Row] = new java.util.ArrayList[org.apache.spark.sql.Row]()
    rows.add(org.apache.spark.sql.Row("2024-06-15"))
    val df = spark.createDataFrame(rows, schema)
    val tables = Map[String, DataFrame]("events_tbl" -> df)

    val path = writeYaml(
      """
        |events:
        |  table: events_tbl
        |  transforms:
        |    flight_year:
        |      expr: "year(ts)"
        |      description: "Year extracted from the timestamp string"
        |  filters:
        |    require_recent_year:
        |      expr: "flight_year >= 2024"
        |      description: "Only recent events"
        |""".stripMargin)
    // Should NOT throw — the filter's `flight_year` is a transform output, not a
    // source column, but transforms run before filters, so it's visible.
    val models = YamlLoader.load(path, tables)
    assert(models.contains("events"))
    assert(models("events").filters.length == 1)
    assert(models("events").filters.head.name == "require_recent_year")
  }

  // ---------------------------------------------------------------------------
  // 4. ExpressionValidator — dims/transforms/measures fail fast on typos
  // ---------------------------------------------------------------------------
  //
  // Before this validator, a typo in a dimension/transform/measure expression
  // (e.g. `case when carrrier in (...) ...` against a source column `carrier`)
  // loaded silently and surfaced only when the expression was first evaluated,
  // as a cryptic Spark `UNRESOLVED_COLUMN` error. The validator parses every
  // expression via CatalystSqlParser and checks that all column references
  // exist in the visible column set at that point.

  test("ExpressionValidator: dimension expression with a typo is rejected at load time") {
    val path = writeYaml(
      """
        |flights:
        |  table: flights_tbl
        |  dimensions:
        |    carrier_class:
        |      expr: "case when carrrier in ('AA') then 'legacy' else 'low_cost' end"
        |  measures:
        |    flight_count: "count(1)"
        |""".stripMargin)
    val ex = intercept[IllegalArgumentException] {
      YamlLoader.load(path, flightsTables)
    }
    assert(ex.getMessage.contains("carrrier"),
      s"Expected missing-column error mentioning 'carrrier', got: ${ex.getMessage}")
    assert(ex.getMessage.contains("dimension"),
      s"Expected error to mention 'dimension', got: ${ex.getMessage}")
  }

  test("ExpressionValidator: transform expression with a typo is rejected at load time") {
    val path = writeYaml(
      """
        |flights:
        |  table: flights_tbl
        |  transforms:
        |    flight_date_str:
        |      expr: "cast(departure_date as string)"  # departure_date doesn't exist
        |  dimensions:
        |    carrier: carrier
        |  measures:
        |    flight_count: "count(1)"
        |""".stripMargin)
    val ex = intercept[IllegalArgumentException] {
      YamlLoader.load(path, flightsTables)
    }
    assert(ex.getMessage.contains("departure_date"),
      s"Expected missing-column error mentioning 'departure_date', got: ${ex.getMessage}")
    assert(ex.getMessage.contains("transform"),
      s"Expected error to mention 'transform', got: ${ex.getMessage}")
  }

  test("ExpressionValidator: measure expression with a typo is rejected at load time") {
    val path = writeYaml(
      """
        |flights:
        |  table: flights_tbl
        |  dimensions:
        |    carrier: carrier
        |  measures:
        |    total_distance:
        |      expr: "sum(distance_miles)"  # distance_miles doesn't exist (real col: distance)
        |""".stripMargin)
    val ex = intercept[IllegalArgumentException] {
      YamlLoader.load(path, flightsTables)
    }
    assert(ex.getMessage.contains("distance_miles"),
      s"Expected missing-column error mentioning 'distance_miles', got: ${ex.getMessage}")
    assert(ex.getMessage.contains("measure"),
      s"Expected error to mention 'measure', got: ${ex.getMessage}")
  }

  test("ExpressionValidator: valid dim/transform/measure expressions load cleanly") {
    // Chain: transform produces a new column; subsequent measure references it.
    val path = writeYaml(
      """
        |flights:
        |  table: flights_tbl
        |  transforms:
        |    pax_per_mile:
        |      expr: "passengers / distance"
        |  dimensions:
        |    carrier: carrier
        |    origin: origin
        |    carrier_class:
        |      expr: "case when distance > 200 then 'long_haul' else 'short_haul' end"
        |  measures:
        |    flight_count: "count(1)"
        |    total_ppm:
        |      expr: "sum(pax_per_mile)"  # references the transform output
        |""".stripMargin)
    // Should NOT throw.
    val models = YamlLoader.load(path, flightsTables)
    assert(models.contains("flights"))
    assert(models("flights").measures.keySet == Set("flight_count", "total_ppm"))
  }

  test("ExpressionValidator: a measure can reference a previously-defined measure") {
    // Window measures commonly ORDER BY another measure's output. E.g.
    // `rank() over (order by total_passengers desc)` — `total_passengers`
    // is a measure, not a source column, but it's visible in the projected
    // DataFrame after the prior measure ran.
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
    // Should NOT throw.
    val models = YamlLoader.load(path, flightsTables)
    assert(models("flights").measures.keySet == Set("total_passengers", "rank_by_total"))
  }

  test("ExpressionValidator: a typo in a measure-to-measure reference is still caught") {
    val path = writeYaml(
      """
        |flights:
        |  table: flights_tbl
        |  dimensions:
        |    carrier: carrier
        |  measures:
        |    total_passengers: "sum(passengers)"
        |    rank_by_total: "rank() over (order by total_pax desc)"  # total_pax doesn't exist
        |""".stripMargin)
    val ex = intercept[IllegalArgumentException] {
      YamlLoader.load(path, flightsTables)
    }
    assert(ex.getMessage.contains("total_pax"),
      s"Expected missing-identifier error mentioning 'total_pax', got: ${ex.getMessage}")
  }

  test("ExpressionValidator: case-insensitive column matching (parity with Spark)") {
    // Source column `passengers`, expression `PASSENGERS` — should pass.
    val path = writeYaml(
      """
        |flights:
        |  table: flights_tbl
        |  dimensions:
        |    carrier: carrier
        |  measures:
        |    total_pax: "sum(PASSENGERS)"  # upper-case matches `passengers`
        |""".stripMargin)
    // Should NOT throw — Spark resolves column names case-insensitively.
    val models = YamlLoader.load(path, flightsTables)
    assert(models.contains("flights"))
  }

  // ---------------------------------------------------------------------------
  // 5. Calc-measure validation — calculated_measures: typos caught at load time
  // ---------------------------------------------------------------------------
  //
  // The CalcExpr DSL (arithmetic over already-aggregated measures, with
  // `all(name)` for percent-of-total) is parsed up-front so a typo in a
  // referenced measure name fails fast instead of surfacing as a cryptic
  // UnknownFieldError at query time.

  test("CalcValidator: calc measure with a typo in a referenced measure name is rejected at load time") {
    val path = writeYaml(
      """
        |flights:
        |  table: flights_tbl
        |  dimensions:
        |    carrier: carrier
        |  measures:
        |    flight_count: "count(1)"
        |    total_passengers: "sum(passengers)"
        |  calculated_measures:
        |    avg_per_passenger:
        |      expr: "total_pax / flight_count"  # total_pax doesn't exist
        |""".stripMargin)
    val ex = intercept[IllegalArgumentException] {
      YamlLoader.load(path, flightsTables)
    }
    assert(ex.getMessage.contains("total_pax"),
      s"Expected missing-measure error mentioning 'total_pax', got: ${ex.getMessage}")
    assert(ex.getMessage.contains("calculated_measures"),
      s"Expected error to mention 'calculated_measures', got: ${ex.getMessage}")
  }

  test("CalcValidator: calc measure with a typo in an `all()` arg is rejected at load time") {
    val path = writeYaml(
      """
        |flights:
        |  table: flights_tbl
        |  dimensions:
        |    carrier: carrier
        |  measures:
        |    total_passengers: "sum(passengers)"
        |  calculated_measures:
        |    pct_of_total:
        |      expr: "total_passengers / all(total_pax)"  # total_pax doesn't exist
        |""".stripMargin)
    val ex = intercept[IllegalArgumentException] {
      YamlLoader.load(path, flightsTables)
    }
    assert(ex.getMessage.contains("total_pax"),
      s"Expected missing-measure error mentioning 'total_pax', got: ${ex.getMessage}")
  }

  test("CalcValidator: a calc can reference a previously-declared calc measure") {
    val path = writeYaml(
      """
        |flights:
        |  table: flights_tbl
        |  dimensions:
        |    carrier: carrier
        |  measures:
        |    flight_count: "count(1)"
        |    total_passengers: "sum(passengers)"
        |  calculated_measures:
        |    avg_per_flight:
        |      expr: "total_passengers / flight_count"
        |    pct_of_total_avg:
        |      expr: "avg_per_flight / all(avg_per_flight)"  # refs earlier calc
        |""".stripMargin)
    // Should NOT throw.
    val models = YamlLoader.load(path, flightsTables)
    assert(models("flights").measures.keySet == Set(
      "flight_count", "total_passengers", "avg_per_flight", "pct_of_total_avg"))
  }

  test("CalcValidator: valid calc expressions load cleanly") {
    val path = writeYaml(
      """
        |flights:
        |  table: flights_tbl
        |  dimensions:
        |    carrier: carrier
        |  measures:
        |    flight_count: "count(1)"
        |    total_passengers: "sum(passengers)"
        |    total_distance: "sum(distance)"
        |  calculated_measures:
        |    avg_per_flight:
        |      expr: "total_passengers / flight_count"
        |    pct_of_total:
        |      expr: "total_passengers / all(total_passengers)"
        |""".stripMargin)
    // Should NOT throw.
    val models = YamlLoader.load(path, flightsTables)
    assert(models("flights").measures.keySet == Set(
      "flight_count", "total_passengers", "total_distance", "avg_per_flight", "pct_of_total"))
  }

  // -------------------------------------------------------------------------
  // 6. YAML declaration-order preservation (regression: HashMap dropped order)
  // -------------------------------------------------------------------------
  //
  // YamlLoader.toScalaVal/toScalaMap convert SnakeYAML's order-preserving
  // LinkedHashMap to a Scala immutable.Map. For >4 entries, .toMap produces
  // a HashMap that does NOT preserve insertion order. The transforms /
  // measures / calculated_measures fold-lefts all depend on declaration
  // order (a later entry may reference an earlier entry's output). This
  // broke nondeterministically whenever the HashMap iteration order put a
  // dependent before its dependency. The fix: preserve order via ListMap.

  test("YAML declaration order: 5-entry transform chain loads regardless of key hashes") {
    // Each transform references the PREVIOUS transform's output column.
    // This is a strict dependency chain — the only valid iteration order is
    // t1 → t2 → t3 → t4 → t5. With the old HashMap conversion (5+ entries),
    // at least one transform was validated before its dependency and the
    // load threw. ListMap preserves declaration order, so all 5 resolve.
    val path = writeYaml(
      """
        |flights:
        |  table: flights_tbl
        |  dimensions:
        |    carrier: carrier
        |  transforms:
        |    t1: "distance + 1"
        |    t2: "t1 + 1"
        |    t3: "t2 + 1"
        |    t4: "t3 + 1"
        |    t5: "t4 + 1"
        |  measures:
        |    grand_total: "sum(t5)"
        |""".stripMargin)
    // Should NOT throw.
    val models = YamlLoader.load(path, flightsTables)
    assert(models("flights").measures.keySet == Set("grand_total"))
  }

  test("YAML declaration order: 5-entry measure chain loads regardless of key hashes") {
    // Same pattern with measures: m2 references m1, m3 references m2, etc.
    // (e.g. a window measure that ORDER BYs a prior aggregate). With 5+
    // entries the old HashMap conversion reordered nondeterministically.
    val path = writeYaml(
      """
        |flights:
        |  table: flights_tbl
        |  dimensions:
        |    carrier: carrier
        |  measures:
        |    m1: "sum(distance)"
        |    m2: "rank() over (order by m1 desc)"
        |    m3: "rank() over (order by m2 desc)"
        |    m4: "rank() over (order by m3 desc)"
        |    m5: "rank() over (order by m4 desc)"
        |""".stripMargin)
    // Should NOT throw.
    val models = YamlLoader.load(path, flightsTables)
    assert(models("flights").measures.keySet == Set("m1", "m2", "m3", "m4", "m5"))
  }
}
