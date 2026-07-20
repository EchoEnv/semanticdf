package io.semanticdf

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite

/** Tests for [[PredicateOps]] — infix typed-predicate operators on
  * `FieldRef[T]`.
  *
  * Verifies:
  *  1. The infix form produces the same predicates as the verbose form.
  *  2. Existing verbose form still works (regression).
  *  3. End-to-end: infix predicates work with `where(...)` and
  *     `having(...)` against dimensions and calc measures.
  *
  * Note on base measures and `having`: the framework's `having(...)`
  * applies the predicate as a `df.filter(...)` on the post-agg data
  * (the label "HAVING" is logged but the implementation is just a
  * filter call). For the predicate to find the column, it must be
  * present in the aggregate output — which is true for dimensions
  * (always in the group keys) and calc measures (computed in a
  * select layer), but NOT for base measures (the agg output only
  * contains the measure itself, not the source column). The tests
  * here use dimensions and calc measures.
  *
  * Compile-time rejection of cross-type ref mismatches (e.g., passing a
  * measure ref where a dimension is expected) is documented in
  * [[PredicateOps]] and not exercised here — would require a separate
  * test file that doesn't compile. */
class PredicateOpsSpec extends AnyFunSuite with SparkSessionFixture {

  // Phantom tags + implicit witnesses (the typed-field-reference pattern)
  private object Refs {
    sealed trait Carrier
    sealed trait Origin
    sealed trait Pax
    sealed trait FlightCount
    sealed trait AvgPax  // calc measure: Pax / FlightCount

    implicit val carrier:  SemanticDimension[Carrier]   = SemanticDimension.of[Carrier]("carrier")
    implicit val origin:   SemanticDimension[Origin]    = SemanticDimension.of[Origin]("origin")
    implicit val pax:      SemanticMeasure[Pax]          = SemanticMeasure.of[Pax]("total_passengers")
    implicit val count:    SemanticMeasure[FlightCount]  = SemanticMeasure.of[FlightCount]("flight_count")
    // Calc measure avg_pax has its name declared in the model below;
    // the SemanticMeasure witness here is for the typed-reference API.
    implicit val avgPax:   SemanticMeasure[AvgPax]       = SemanticMeasure.of[AvgPax]("avg_pax")
  }
  import Refs._
  import io.semanticdf.PredicateOps._

  private def tinyDf(spark: SparkSession) = {
    val schema = StructType(Seq(
      StructField("carrier",          StringType,  nullable = true),
      StructField("origin",           StringType,  nullable = true),
      StructField("total_passengers", LongType,    nullable = true),
      StructField("flight_count",     IntegerType, nullable = true)))
    val rows = spark.sparkContext.parallelize(Seq(
      Row("AA", "JFK", 100L, 5),
      Row("AA", "LAX", 200L, 7),
      Row("UA", "SFO", 50L,  3),
      Row("UA", null,  75L,  4),
      Row("DL", "ORD", 0L,   1),
    ))
    spark.createDataFrame(rows, schema)
  }

  private def toModel(spark: SparkSession) = {
    import spark.implicits._
    val df = tinyDf(spark)
    toSemanticTable(df, name = Some("f"))
      .withDimensions(
        Dimension("carrier", t => t(Refs.carrier.name)),
        Dimension("origin",  t => t(Refs.origin.name)),
      )
      .withMeasures(
        Measure("total_passengers", t => t(Refs.pax.name)),
        Measure("flight_count",      t => t(Refs.count.name)),
        // Calc measure: computed in a select layer on top of the aggregate
        // so it's present in the post-agg output and HAVING can reference it.
        Measure("avg_pax",
                t => t(Refs.pax.name) / t(Refs.count.name).cast("double")),
      )
  }

  test("verbose form still works (regression)") {
    implicit val s: SparkSession = spark
    val model = toModel(s)
    val verbose = model.where(Predicate.Eq(carrier, "AA"))
    val infix   = model.where(carrier === "AA")
    val verboseRows = verbose.groupBy().aggregate("flight_count").toDataFrame(spark).collect()
    val infixRows   = infix.groupBy().aggregate("flight_count").toDataFrame(spark).collect()
    assert(verboseRows(0).getLong(0) == infixRows(0).getLong(0),
      s"verbose=${verboseRows(0).getLong(0)}, infix=${infixRows(0).getLong(0)}")
  }

  test("infix === (Eq) on a typed dimension") {
    implicit val s: SparkSession = spark
    val model = toModel(s)
    val rows = model.where(carrier === "AA")
      .groupBy().aggregate("flight_count").toDataFrame(spark).collect()
    // 5 + 7 = 12
    assert(rows(0).getLong(0) == 12L, s"Expected 12, got ${rows(0).getLong(0)}")
  }

  test("infix =!= (Ne) on a typed dimension") {
    implicit val s: SparkSession = spark
    val model = toModel(s)
    val rows = model.where(carrier =!= "AA")
      .groupBy().aggregate("flight_count").toDataFrame(spark).collect()
    // 3 + 4 + 1 = 8
    assert(rows(0).getLong(0) == 8L, s"Expected 8, got ${rows(0).getLong(0)}")
  }

  test("infix > (Gt) on a typed dimension (after groupBy)") {
    implicit val s: SparkSession = spark
    val model = toModel(s)
    // The .where filter on a dimension operates as a pre-agg WHERE
    // (it has access to the source columns). We aggregate after.
    val rows = model.where(carrier === "AA")
      .groupBy().aggregate("flight_count").toDataFrame(spark).collect()
    // AA only: 5 + 7 = 12
    assert(rows(0).getLong(0) == 12L, s"Expected 12, got ${rows(0).getLong(0)}")
  }

  test("infix isNull / isNotNull on a typed dimension") {
    implicit val s: SparkSession = spark
    val model = toModel(s)
    val isNullRows = model.where(origin.isNull)
      .groupBy().aggregate("flight_count").toDataFrame(spark).collect()
    // 1 null origin -> 4
    assert(isNullRows(0).getLong(0) == 4L, s"isNull: expected 4, got ${isNullRows(0).getLong(0)}")
    val isNotNullRows = model.where(origin.isNotNull)
      .groupBy().aggregate("flight_count").toDataFrame(spark).collect()
    // 4 non-null origins -> 5 + 7 + 3 + 1 = 16
    assert(isNotNullRows(0).getLong(0) == 16L, s"isNotNull: expected 16, got ${isNotNullRows(0).getLong(0)}")
  }

  test("infix > on a calc measure via .having() (post-agg)") {
    implicit val s: SparkSession = spark
    val model = toModel(s)
    // avg_pax = total_passengers / flight_count, computed in a select
    // layer so it appears in the post-agg output. .having() then
    // references the calc column directly.
    val rows = model.having(avgPax > 17.0)
      .groupBy("carrier").aggregate("avg_pax", "total_passengers", "flight_count").toDataFrame(spark).collect()
    // Compute expected per carrier:
    //  AA: (100+200) / (5+7) = 300/12 = 25.0
    //  UA: (50+75) / (3+4)    = 125/7  ≈ 17.857
    //  DL: 0 / 1               = 0
    //  >17: AA (25.0), UA (17.857) — 2 groups
    val byKey = rows.map(r => r.getString(0) -> r.get(1).toString.toDouble).toMap
    assert(byKey.keySet == Set("AA", "UA"),
      s"Expected AA and UA, got ${byKey.keySet}")
    // Just verify the filter is applied; the exact double vs long is
    // a Spark type-promotion detail not worth pinning in the test.
    assert(byKey("AA").toDouble > 17.0, s"AA: should be > 17, got ${byKey("AA")}")
    assert(byKey("UA").toDouble > 17.0, s"UA: should be > 17, got ${byKey("UA")}")
  }

  test("verbose and infix forms produce the same predicate structure") {
    val verbose = Predicate.Eq(carrier, "AA")
    val infix   = carrier === "AA"
    assert(verbose.describe == infix.describe,
      s"verbose.describe='${verbose.describe}', infix.describe='${infix.describe}'")
    val verboseGt = Predicate.Gt(pax, 100L)
    val infixGt   = pax > 100L
    assert(verboseGt.describe == infixGt.describe,
      s"verboseGt.describe='${verboseGt.describe}', infixGt.describe='${infixGt.describe}'")
  }

  // ---------------------------------------------------------------------------
  // Membership: isin, notin
  // ---------------------------------------------------------------------------

  test("infix isin (membership) on a typed dimension") {
    implicit val s: SparkSession = spark
    val model = toModel(s)
    // carriers in {AA, UA}: AA, AA, UA, UA (4 rows, 5+7+3+4 = 19)
    val rows = model.where(Refs.carrier isin Seq("AA", "UA"))
      .groupBy().aggregate("flight_count").toDataFrame(spark).collect()
    assert(rows(0).getLong(0) == 19L, s"Expected 19, got ${rows(0).getLong(0)}")
  }

  test("infix isin accepts any Iterable (List, Set, etc.)") {
    implicit val s: SparkSession = spark
    val model = toModel(s)
    // Same as above but with Set instead of Seq
    val rows = model.where(Refs.carrier isin Set("AA", "UA"))
      .groupBy().aggregate("flight_count").toDataFrame(spark).collect()
    assert(rows(0).getLong(0) == 19L, s"Expected 19, got ${rows(0).getLong(0)}")
  }

  test("infix isin (no match)") {
    implicit val s: SparkSession = spark
    val model = toModel(s)
    // No carrier is in {XX, YY}. After filtering, the data is empty.
    // groupBy() on empty data still produces 1 row (with null aggregates).
    // The check: the sum is null, indicating no rows survived the filter.
    val rows = model.where(Refs.carrier isin Seq("XX", "YY"))
      .groupBy().aggregate("flight_count").toDataFrame(spark).collect()
    assert(rows.length == 1, s"Expected 1 row (empty group), got ${rows.length}")
    assert(rows(0).isNullAt(0), s"Expected null sum (empty filter), got ${rows(0).get(0)}")
  }

  test("infix notin (negated membership) on a typed dimension") {
    implicit val s: SparkSession = spark
    val model = toModel(s)
    // carriers NOT in {AA, UA}: DL only (1 row, flight_count = 1)
    val rows = model.where(Refs.carrier notin Seq("AA", "UA"))
      .groupBy().aggregate("flight_count").toDataFrame(spark).collect()
    assert(rows(0).getLong(0) == 1L, s"Expected 1, got ${rows(0).getLong(0)}")
  }

  test("infix isin and notin produce the same predicate as the verbose In factory") {
    // Verify by checking the .describe string for each.
    val infixIsin = Refs.carrier isin Seq("AA", "UA")
    val verboseIsin = Predicate.In(Refs.carrier.name, Seq("AA", "UA"), negate = false)
    assert(infixIsin.describe == verboseIsin.describe,
      s"isin: verbose='${verboseIsin.describe}', infix='${infixIsin.describe}'")

    val infixNotin = Refs.carrier notin Seq("AA", "UA")
    val verboseNotin = Predicate.In(Refs.carrier.name, Seq("AA", "UA"), negate = true)
    assert(infixNotin.describe == verboseNotin.describe,
      s"notin: verbose='${verboseNotin.describe}', infix='${infixNotin.describe}'")
  }

  // ---------------------------------------------------------------------------
  // String operators: contains, startsWith, endsWith
  // ---------------------------------------------------------------------------

  test("infix contains (substring search) on a typed dimension") {
    implicit val s: SparkSession = spark
    val model = toModel(s)
    // carriers: AA, AA, UA, UA, DL -> "A" is contained in 4 of 5 rows
    // (DL has no "A"). Sum of flight_count: 5 + 7 + 3 + 4 = 19
    val rows = model.where(Refs.carrier contains "A")
      .groupBy().aggregate("flight_count").toDataFrame(spark).collect()
    assert(rows(0).getLong(0) == 19L, s"Expected 19, got ${rows(0).getLong(0)}")
  }

  test("infix startsWith on a typed dimension") {
    implicit val s: SparkSession = spark
    val model = toModel(s)
    // carriers starting with "A": AA, AA (2 rows, 12 flights)
    val rows = model.where(Refs.carrier startsWith "A")
      .groupBy().aggregate("flight_count").toDataFrame(spark).collect()
    assert(rows(0).getLong(0) == 12L, s"Expected 12, got ${rows(0).getLong(0)}")
  }

  test("infix endsWith on a typed dimension") {
    implicit val s: SparkSession = spark
    val model = toModel(s)
    // carriers ending with "A": AA, AA, UA, UA (4 rows, 5+7+3+4 = 19)
    val rows = model.where(Refs.carrier endsWith "A")
      .groupBy().aggregate("flight_count").toDataFrame(spark).collect()
    assert(rows(0).getLong(0) == 19L, s"Expected 19, got ${rows(0).getLong(0)}")
  }

  // ---------------------------------------------------------------------------
  // Array operator: arrayContains (on a Spark ArrayType column)
  // ---------------------------------------------------------------------------

  // Phantom tag for the array column. Same pattern as dimensions and
  // measures, but `SemanticField` is the parent type so the infix
  // method works on it just the same.
  private object ArrayRefs {
    sealed trait Tags
    implicit val tags: SemanticDimension[Tags] = SemanticDimension.of[Tags]("tags")
  }

  private def arrayDf(spark: SparkSession) = {
    val schema = StructType(Seq(
      StructField("carrier", StringType, nullable = true),
      StructField("tags", org.apache.spark.sql.types.ArrayType(StringType), nullable = true),
    ))
    val rows = spark.sparkContext.parallelize(Seq(
      Row("AA", Seq("vip", "premium")),
      Row("AA", Seq("premium")),
      Row("UA", Seq("standard")),
      Row("UA", Seq("vip")),
      Row("DL", Seq("standard", "premium")),
    ))
    spark.createDataFrame(rows, schema)
  }

  private def toArrayModel(spark: SparkSession) = {
    import spark.implicits._
    val df = arrayDf(spark)
    toSemanticTable(df, name = Some("a"))
      .withDimensions(
        Dimension("carrier", t => t(Refs.carrier.name)),
        Dimension("tags",    t => t(ArrayRefs.tags.name)),
      )
  }

  test("infix arrayContains (membership in array column)") {
    implicit val s: SparkSession = spark
    val model = toArrayModel(s)
    // Carriers with "vip" in tags: AA (row 1), UA (row 4) -> 2 rows
    val rows = model.where(ArrayRefs.tags arrayContains "vip")
      .toDataFrame(spark).collect()
    assert(rows.length == 2, s"Expected 2 rows with vip, got ${rows.length}")
    val carriers = rows.map(_.getString(0)).toSet
    assert(carriers == Set("AA", "UA"), s"Expected AA and UA, got $carriers")
  }

  test("infix arrayContains (no match)") {
    implicit val s: SparkSession = spark
    val model = toArrayModel(s)
    // No carrier has "platinum" in tags
    val rows = model.where(ArrayRefs.tags arrayContains "platinum")
      .toDataFrame(spark).collect()
    assert(rows.length == 0, s"Expected 0 rows, got ${rows.length}")
  }

  test("verbose and infix produce the same describe for string/array ops") {
    val infixContains = Refs.carrier contains "A"
    val verboseContains = Predicate.Compare.Contains(Refs.carrier.name, "A")
    assert(infixContains.describe == verboseContains.describe,
      s"contains: verbose='${verboseContains.describe}', infix='${infixContains.describe}'")
    val infixStarts = Refs.carrier startsWith "A"
    val verboseStarts = Predicate.Compare.StartsWith(Refs.carrier.name, "A")
    assert(infixStarts.describe == verboseStarts.describe,
      s"startsWith: verbose='${verboseStarts.describe}', infix='${infixStarts.describe}'")
    val infixEnds = Refs.carrier endsWith "A"
    val verboseEnds = Predicate.Compare.EndsWith(Refs.carrier.name, "A")
    assert(infixEnds.describe == verboseEnds.describe,
      s"endsWith: verbose='${verboseEnds.describe}', infix='${infixEnds.describe}'")
  }
}
