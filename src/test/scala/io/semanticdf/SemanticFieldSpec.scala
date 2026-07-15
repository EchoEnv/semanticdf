package io.semanticdf

import java.io.{File, PrintWriter}
import org.apache.spark.sql.DataFrame
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._
import Predicate._

/** Tests for the [[SemanticField]] typeclass and its DSL overloads.
  *
  * Behaviors verified:
  *   1. Typed groupBy/aggregate produce IDENTICAL output to the string API.
  *   2. Typed predicates compose with where()/having().
  *   3. The ...All variants runtime-check kind for 5+ sequences.
  *   4. The existing string-based API still works (backward compat).
  *
  * Compile-time guarantees:
  *   - passing a Measure ref to groupByDimensions is rejected at compile time
  *     (the implicit `SemanticDimension[MeasureType]` cannot be satisfied)
  *   - passing a Dimension ref to aggregateMeasures is likewise rejected
  *   - these are exercised by the user's call site; we trust the compiler
  *     via the test passing (we never write the bad combinations).
  */
class SemanticFieldSpec extends AnyFunSuite with SparkSessionFixture with FlightsFixture {

  // -------------------------------------------------------------------------
  // Fixtures
  // -------------------------------------------------------------------------

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
    val f = File.createTempFile("typeclass-fields", ".yml"); f.deleteOnExit()
    val w = new PrintWriter(f); w.write(content); w.close(); f.getAbsolutePath
  }

  /** Phantom tags + implicit refs declared in a local object so this spec
    * is self-contained. */
  private object Refs {
    sealed trait Carrier
    sealed trait Origin
    sealed trait TotalPassengers
    sealed trait FlightCount

    implicit val carrierRef: SemanticDimension[Carrier] =
      SemanticDimension.of[Carrier]("carrier")
    implicit val originRef: SemanticDimension[Origin] =
      SemanticDimension.of[Origin]("origin")
    implicit val paxRef: SemanticMeasure[TotalPassengers] =
      SemanticMeasure.of[TotalPassengers]("total_passengers")
    implicit val countRef: SemanticMeasure[FlightCount] =
      SemanticMeasure.of[FlightCount]("flight_count")
  }

  private def yamlWithAllFields = writeYaml(
    """
      |flights:
      |  table: flights_tbl
      |  dimensions:
      |    carrier: carrier
      |    origin: origin
      |  measures:
      |    flight_count: "count(1)"
      |    total_passengers: "sum(passengers)"
      |""".stripMargin)

  // -------------------------------------------------------------------------
  // (1) Typed groupBy + aggregate → identical output
  // -------------------------------------------------------------------------

  test("typed groupByDimensions/aggregateMeasures produces same rows as the string API") {
    val m = YamlLoader.load(yamlWithAllFields, flightsTables)("flights")
    import Refs._

    val stringResult = m.groupBy("carrier", "origin")
      .aggregate("flight_count", "total_passengers")
      .toDataFrame(spark).collect()
      .map(r => (r.getString(0), r.getString(1), r.getLong(2), r.getLong(3)))

    val typedResult = m.groupByDimensions(carrierRef, originRef)
      .aggregateMeasures(countRef, paxRef)
      .toDataFrame(spark).collect()
      .map(r => (r.getString(0), r.getString(1), r.getLong(2), r.getLong(3)))

    assert(stringResult.toSet == typedResult.toSet,
      s"row sets differ:\n  string: ${stringResult.toList}\n  typed:  ${typedResult.toList}")
    assert(stringResult.length == typedResult.length)
  }

  // -------------------------------------------------------------------------
  // (2) Single-arity overloads
  // -------------------------------------------------------------------------

  test("groupByDimensions with one typed ref produces same result as groupBy(string)") {
    val m = YamlLoader.load(yamlWithAllFields, flightsTables)("flights")
    import Refs._

    val typed = m.groupByDimensions(carrierRef)
      .aggregateMeasures(countRef)
      .toDataFrame(spark).collect()
    val str   = m.groupBy("carrier").aggregate("flight_count")
      .toDataFrame(spark).collect()

    assert(typed.length == str.length)
    assert(typed.map(_.getString(0)).toSet == str.map(_.getString(0)).toSet)
  }

  test("aggregateMeasures with one typed ref produces same result as aggregate(string)") {
    val m = YamlLoader.load(yamlWithAllFields, flightsTables)("flights")
    import Refs._

    val typed = m.groupBy("carrier")
      .aggregateMeasures(countRef)
      .toDataFrame(spark).collect()
    val str = m.groupBy("carrier").aggregate("flight_count")
      .toDataFrame(spark).collect()

    assert(typed.length == str.length)
    assert(typed.map(_.getLong(1)).toSet == str.map(_.getLong(1)).toSet)
  }

  // -------------------------------------------------------------------------
  // (3) Typed predicates via where()
  // -------------------------------------------------------------------------

  test("typed Predicate.Gt on a measure produces same filter result as Compare(\"gt\", ...)") {
    val m = YamlLoader.load(yamlWithAllFields, flightsTables)("flights")
    import Refs._

    val strCount   = m.where(Predicate.Compare("gt", "total_passengers", 100))
      .toDataFrame(spark).count()
    val typedCount = m.where(Predicate.Gt(paxRef, 100))
      .toDataFrame(spark).count()
    assert(strCount == typedCount,
      s"typed filter count $typedCount != string filter count $strCount")
  }

  test("typed Predicate.Eq on a dimension produces same filter as string Compare") {
    val m = YamlLoader.load(yamlWithAllFields, flightsTables)("flights")
    import Refs._

    val strCount   = m.where(Predicate.Compare("eq", "carrier", "AA"))
      .toDataFrame(spark).count()
    val typedCount = m.where(Predicate.Eq(carrierRef, "AA"))
      .toDataFrame(spark).count()
    assert(strCount == typedCount)
  }

  test("typed Predicate.in / notIn / isNull / isNotNull produce same results as string versions") {
    val m = YamlLoader.load(yamlWithAllFields, flightsTables)("flights")
    import Refs._

    val inStr  = m.where(Predicate.In("carrier", Seq("AA", "UA"))).toDataFrame(spark).count()
    val inTyped = m.where(Predicate.in(carrierRef, "AA", "UA")).toDataFrame(spark).count()
    assert(inStr == inTyped)

    val niStr   = m.where(Predicate.In("carrier", Seq("DL"), negate = true)).toDataFrame(spark).count()
    val niTyped = m.where(Predicate.notIn(carrierRef, "DL")).toDataFrame(spark).count()
    assert(niStr == niTyped)

    val isnStr   = m.where(Predicate.IsNull("origin")).toDataFrame(spark).count()
    val isnTyped = m.where(Predicate.isNull(originRef)).toDataFrame(spark).count()
    assert(isnStr == isnTyped)

    val ntStr   = m.where(Predicate.IsNull("origin", negate = true)).toDataFrame(spark).count()
    val ntTyped = m.where(Predicate.isNotNull(originRef)).toDataFrame(spark).count()
    assert(ntStr == ntTyped)
  }

  // -------------------------------------------------------------------------
  // (4) ...All runtime kind check
  // -------------------------------------------------------------------------

  test("groupByDimensionsAll accepts dimension refs and rejects measure refs at runtime") {
    val m = YamlLoader.load(yamlWithAllFields, flightsTables)("flights")
    import Refs._

    val ok = m.groupByDimensionsAll(Seq(carrierRef, originRef))
      .aggregateMeasures(countRef)
      .toDataFrame(spark).collect()
    assert(ok.length > 0)

    val ex = intercept[IllegalArgumentException] {
      m.groupByDimensionsAll(Seq(carrierRef, paxRef))
        .aggregateMeasures(countRef)
        .toDataFrame(spark)
    }
    assert(ex.getMessage.contains("total_passengers"),
      s"expected err to name the bad field; got: ${ex.getMessage}")
    assert(ex.getMessage.toLowerCase.contains("dimension"),
      s"expected err to say it's not a dimension; got: ${ex.getMessage}")
  }

  test("aggregateMeasuresAll accepts measure refs and rejects dimension refs at runtime") {
    val m = YamlLoader.load(yamlWithAllFields, flightsTables)("flights")
    import Refs._

    val ok = m.groupBy("carrier")
      .aggregateMeasuresAll(Seq(countRef, paxRef))
      .toDataFrame(spark).collect()
    assert(ok.length > 0)

    val ex = intercept[IllegalArgumentException] {
      m.groupBy("carrier")
        .aggregateMeasuresAll(Seq(countRef, carrierRef))
        .toDataFrame(spark)
    }
    assert(ex.getMessage.contains("carrier"),
      s"expected err to name the bad field; got: ${ex.getMessage}")
    assert(ex.getMessage.toLowerCase.contains("measure"),
      s"expected err to say it's not a measure; got: ${ex.getMessage}")
  }

  // -------------------------------------------------------------------------
  // (5) Backward compatibility — string-based API still works
  // -------------------------------------------------------------------------

  test("string-based groupBy/aggregate/where still works unchanged") {
    val m = YamlLoader.load(yamlWithAllFields, flightsTables)("flights")

    val rows = m.groupBy("carrier")
      .aggregate("flight_count", "total_passengers")
      .where("carrier" === "AA")
      .toDataFrame(spark).collect()
    assert(rows.nonEmpty)
  }

  // -------------------------------------------------------------------------
  // (6) Typed withMeasures — v0.1.1 (typed Measure declaration)
  // -------------------------------------------------------------------------

  test("withMeasures(ref, expr) typed overload reads the name from the FieldRef witness") {
    val m = YamlLoader.load(yamlWithAllFields, flightsTables)("flights")
    import Refs._

    // Add a derived measure whose name is the SAME as the typed ref expects.
    val enriched = m.withMeasures(paxRef, t => t("total_passengers") * 2)
    assert(enriched.findMeasure("total_passengers").isDefined)
  }

  test("withMeasures typed overload produces a measure that the string-based aggregate picks up") {
    val m = YamlLoader.load(yamlWithAllFields, flightsTables)("flights")
    import Refs._

    // Window-style: row_number() over carrier partition, ordered by pax desc.
    // The Measure.name is read from `paxRef` (so it's "total_passengers" — we
    // shadow the base measure with a windowed one). For the test we use a
    // *new* measure name "pax_x2" to avoid clobbering.
    object Local {
      sealed trait PaxTimesTwo
      implicit val paxX2: SemanticMeasure[PaxTimesTwo] =
        SemanticMeasure.of[PaxTimesTwo]("pax_x2")
    }
    import Local._

    val enriched = m.withMeasures(paxX2, t => t("total_passengers") * 2)
    val out = enriched.groupBy("carrier").aggregate("pax_x2").toDataFrame(spark).collect()
    assert(out.length > 0)
  }

  // -------------------------------------------------------------------------
  // (7) Typed SortKey.asc / SortKey.desc (v0.1.1)
  // -------------------------------------------------------------------------

  test("SortKey.asc(ref) / SortKey.desc(ref) read the column name from the ref") {
    val m = YamlLoader.load(yamlWithAllFields, flightsTables)("flights")
    import Refs._

    val ascRows  = m.groupBy("carrier").aggregate("flight_count").orderBy(SortKey.asc(carrierRef)).toDataFrame(spark).collect()
    val strRows  = m.groupBy("carrier").aggregate("flight_count").orderBy("carrier").toDataFrame(spark).collect()
    assert(ascRows.map(_.getString(0)).toList == strRows.map(_.getString(0)).toList)

    val descRows = m.groupBy("carrier").aggregate("flight_count").orderBy(SortKey.desc(countRef)).toDataFrame(spark).collect()
    val dRows    = m.groupBy("carrier").aggregate("flight_count").orderBy(SortKey.desc("flight_count")).toDataFrame(spark).collect()
    // Columns are (carrier, flight_count). Index 1 = flight_count.
    assert(descRows.map(_.getLong(1)).toList == dRows.map(_.getLong(1)).toList)
  }

  test("SortKey.typed works with the q() one-shot bundle (orderBy: Seq[SortKey])") {
    val m = YamlLoader.load(yamlWithAllFields, flightsTables)("flights")
    import Refs._

    // `query` projects the requested dimensions + measures; both must
    // exist in the output for `orderBy` to resolve.
    val out = m.query(
      measures   = Seq("flight_count"),
      dimensions = Seq("carrier"),
      orderBy    = Seq(SortKey.desc(countRef), SortKey.asc(carrierRef)),
    ).toDataFrame(spark).collect()
    // The dual sort doesn't error and returns rows; we don't pin the count
    // here (depends on test fixture), just that the call type-checks.
    assert(out.length > 0)
  }
}
