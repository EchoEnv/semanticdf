package io.semanticdf

import java.io.{File, PrintWriter}
import org.apache.spark.sql.DataFrame
import org.scalatest.funsuite.AnyFunSuite
import io.semanticdf.Predicate.Compare
import Predicate._

/** Regression tests for the sealed comparison-predicate hierarchy.
  *
  * `Predicate.Compare` is now a sealed trait (was a single case class) with six
  * sealed case classes inside its companion object: `Eq`, `Ne`, `Lt`, `Le`,
  * `Gt`, `Ge`. The companion's `apply(op, field, value)` factory is preserved
  * for backward compatibility and dispatches on the operator string.
  *
  * Behaviors verified:
  *   1. The new sealed cases produce byte-identical output to the prior
  *      `Compare("op", field, value)` stringly-typed form.
  *   2. The factory `Compare.apply(op, field, value)` keeps working
  *      (back-compat) and returns the matching sealed case.
  *   3. Unknown operator strings throw the same message as before.
  *   4. End-to-end row output is identical between legacy and sealed forms.
  *   5. WHERE/HAVING routing handles sealed cases polymorphically through
  *      the trait.
  */
class PredicateSpec extends AnyFunSuite with SparkSessionFixture with FlightsFixture {

  // NOTE: avoid using `sealed` as a variable name — it's a Scala 2.13 soft
  // keyword and confuses the parser when combined with `==` (interpreted as
  // a pattern-context).

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
    val f = File.createTempFile("predicate-spec", ".yml"); f.deleteOnExit()
    val w = new PrintWriter(f); w.write(content); w.close(); f.getAbsolutePath
  }

  private def yamlSingle = writeYaml(
    """
      |flights:
      |  table: flights_tbl
      |  dimensions:
      |    carrier: carrier
      |  measures:
      |    flight_count: "count(1)"
      |""".stripMargin)

  // ==================================================
  // (1) Sealed-case compile() output == legacy Compare.compile() output
  // ==================================================

  test("each Compare sealed case produces the same compile() output as the legacy Compare form") {
    val scope = new BaseScope(flightsDf)
    assert(Predicate.Compare("eq", "carrier", "AA").compile(scope).expr ==
             Compare.Eq("carrier", "AA").compile(scope).expr)
    assert(Predicate.Compare("ne", "carrier", "AA").compile(scope).expr ==
             Compare.Ne("carrier", "AA").compile(scope).expr)
    assert(Predicate.Compare("lt", "distance", 1000).compile(scope).expr ==
             Compare.Lt("distance", 1000).compile(scope).expr)
    assert(Predicate.Compare("le", "distance", 1000).compile(scope).expr ==
             Compare.Le("distance", 1000).compile(scope).expr)
    assert(Predicate.Compare("gt", "distance", 1000).compile(scope).expr ==
             Compare.Gt("distance", 1000).compile(scope).expr)
    assert(Predicate.Compare("ge", "distance", 1000).compile(scope).expr ==
             Compare.Ge("distance", 1000).compile(scope).expr)
  }

  // ==================================================
  // (2) describe() == legacy form
  // ==================================================

  test("each Compare sealed case produces the same describe() as the legacy form") {
    assert(Compare.Eq("carrier", "AA").describe     == "carrier = AA")
    assert(Compare.Ne("carrier", "AA").describe     == "carrier != AA")
    assert(Compare.Lt("distance", 1000).describe  == "distance < 1000")
    assert(Compare.Le("distance", 1000).describe  == "distance <= 1000")
    assert(Compare.Gt("distance", 1000).describe  == "distance > 1000")
    assert(Compare.Ge("distance", 1000).describe  == "distance >= 1000")
    assert(Predicate.Compare("eq", "carrier", "AA").describe == "carrier = AA")
  }

  // ==================================================
  // (3) Backward-compat factory
  // ==================================================

  test("Compare.apply returns the matching sealed case") {
    assert(Predicate.Compare("eq", "c", 1).isInstanceOf[Compare.Eq])
    assert(Predicate.Compare("ne", "c", 1).isInstanceOf[Compare.Ne])
    assert(Predicate.Compare("gt", "c", 1).isInstanceOf[Compare.Gt])
    assert(Predicate.Compare("lt", "c", 1).isInstanceOf[Compare.Lt])
    assert(Predicate.Compare("le", "c", 1).isInstanceOf[Compare.Le])
    assert(Predicate.Compare("ge", "c", 1).isInstanceOf[Compare.Ge])
  }

  test("Compare.apply on unknown op throws with the same message as before") {
    val ex = intercept[IllegalArgumentException] {
      Predicate.Compare("grater than", "carrier", "AA")
    }
    assert(ex.getMessage.contains("Unknown compare op"))
    assert(ex.getMessage.contains("grater than"))
  }

  // ==================================================
  // (4) End-to-end: row output is byte-identical between legacy and sealed forms
  // ==================================================

  test("where with Compare.Gt and legacy Compare produce identical rows") {
    val m = YamlLoader.load(yamlSingle, flightsTables)("flights")
    val sealedRows = m.where(Compare.Gt("distance", 1000)).toDataFrame(spark).collect()
    val legacyRows = m.where(Predicate.Compare("gt", "distance", 1000)).toDataFrame(spark).collect()
    assert(sealedRows.toSeq == legacyRows.toSeq)
  }

  test("where with fluent DSL produces identical rows to Compare.Eq") {
    val m = YamlLoader.load(yamlSingle, flightsTables)("flights")
    val fluentRows = m.where("carrier" === "AA").toDataFrame(spark).collect()
    val sealedRows = m.where(Compare.Eq("carrier", "AA")).toDataFrame(spark).collect()
    assert(fluentRows.toSeq == sealedRows.toSeq)
  }

  // ==================================================
  // (5) Routing still works through the trait
  // ==================================================

  test("referencesMeasure handles sealed cases polymorphically") {
    assert(Predicate.referencesMeasure(Compare.Gt("flight_count", 5), Set("flight_count")))
    assert(!Predicate.referencesMeasure(Compare.Eq("carrier", "AA"), Set("flight_count")))
  }

  test("splitFilter routes sealed Compare based on known-measure set") {
    val p = Compare.Gt("flight_count", 5)
    val (pre, post)  = Predicate.splitFilter(p, Set("flight_count"))
    assert(pre.isEmpty && post.length == 1)

    val q = Compare.Eq("carrier", "AA")
    val (pre2, post2) = Predicate.splitFilter(q, Set("flight_count"))
    assert(pre2.length == 1 && post2.isEmpty)
  }
}
