package io.semanticdf

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** v0.1.14: asymmetric-key join support across all four surfaces.
  *
  * Surfaces that MUST work end-to-end for asymmetric joins
  * (`flights.carrier` joined to `carriers.code`):
  *
  *   1. DSL        — `SemanticTable.join_one(rT, (l, r) => l("carrier") === r("code"))`
  *   2. YamlLoader — `joins: { carriers: { left_on: carrier, right_on: code } }`
  *   3. Manifest   — `toJoinedJson` emits asymmetric `leftKeys[]`/`rightKeys[]`/
  *                   `predicate_ast`; `fromJoinedJson` round-trips bit-perfect
  *   4. toDataFrame — runtime execution returns the joined DataFrame
  *
  * Pinning these tests is the v0.1.14 acceptance criterion. */
class AsymmetricKeysSpec extends AnyFunSuite with Matchers {

  private val mapper = new ObjectMapper()

  // ── shared fixtures ──────────────────────────────────────────────

  private def setupSpark(): (SparkSession, org.apache.spark.sql.DataFrame, org.apache.spark.sql.DataFrame) = {
    val spark = SparkSession.builder().master("local[1]").appName("asym").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    val flights = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1, "AA"), Row(2, "UA"), Row(3, "B6")
      )),
      StructType(Seq(
        StructField("id", IntegerType),
        StructField("carrier", StringType),
      ))
    )
    val carriers = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row("AA", "American Airlines"),
        Row("UA", "United Airlines"),
        Row("B6", "JetBlue Airways"),
      )),
      StructType(Seq(
        StructField("code", StringType),
        StructField("name", StringType),
      ))
    )
    (spark, flights, carriers)
  }

  // ── 1. DSL ───────────────────────────────────────────────────────

  test("v0.1.14: DSL asymmetric join: l(\"carrier\") === r(\"code\")") {
    val (spark, flightsDf, carriersDf) = setupSpark()
    try {
      val lT = toSemanticTable(flightsDf, Some("flights"))
        .withDimensions(
          Dimension("id", t => t("id")),
          Dimension("carrier", t => t("carrier")),
        )
      val rT = toSemanticTable(carriersDf, Some("carriers"))
        .withDimensions(
          Dimension("code", t => t("code")),
          Dimension("name", t => t("name")),
        )

      val joined = lT.join_one(rT, (l, r) => l("carrier") === r("code"))

      // AST captures asymmetric names
      val j = joined.root.asInstanceOf[SemanticJoinOp]
      assert(j.leftKeys  == Seq("carrier"))
      assert(j.rightKeys == Seq("code"))
      assert(j.predicateAst.contains(PredicateAst.Predicate(
        PredicateAst.Op.Eq,
        PredicateAst.Operand.ColumnRef("left", "carrier"),
        PredicateAst.Operand.ColumnRef("right", "code"),
      )))

      // Runtime execution returns 3 joined rows
      val result = joined.toDataFrame(spark)
      assert(result.count() == 3, s"expected 3 joined rows, got ${result.count()}")
    } finally spark.stop()
  }

  // ── 2. YamlLoader ────────────────────────────────────────────────

  test("v0.1.14: YamlLoader asymmetric join: left_on=carrier / right_on=code") {
    val (spark, flightsDf, carriersDf) = setupSpark()
    try {
      flightsDf.createOrReplaceTempView("flights_csv")
      carriersDf.createOrReplaceTempView("carriers_csv")

      val tmp = java.nio.file.Files.createTempDirectory("asym-yaml-").toFile
      def writeYaml(name: String, body: String): Unit = {
        val f = new java.io.File(tmp, name)
        val pw = new java.io.PrintWriter(f, "UTF-8")
        try pw.write(body) finally pw.close()
      }
      writeYaml("carriers.yml",
        """carriers:
          |  status: published
          |  version: 1
          |  table: carriers_csv
          |  dimensions:
          |    code: code
          |    name: name
          |""".stripMargin)
      writeYaml("flights.yml",
        """flights:
          |  status: published
          |  version: 1
          |  table: flights_csv
          |  dimensions:
          |    id: id
          |    carrier: carrier
          |  joins:
          |    carriers:
          |      type: one
          |      model: carriers
          |      left_on: carrier
          |      right_on: code
          |""".stripMargin)

      val all = YamlLoader.loadDir(tmp.getAbsolutePath, spark)
      val flights = all("flights")

      // Runtime execution returns 3 joined rows (the same as the DSL case)
      val result = flights.toDataFrame(spark)
      assert(result.count() == 3, s"expected 3 joined rows, got ${result.count()}")
    } finally spark.stop()
  }

  // ── 3. Manifest wire format ─────────────────────────────────────

  test("v0.1.14: Manifest emits + round-trips asymmetric keys") {
    val (spark, flightsDf, carriersDf) = setupSpark()
    try {
      val lT = toSemanticTable(flightsDf, Some("flights"))
        .withDimensions(
          Dimension("id", t => t("id")),
          Dimension("carrier", t => t("carrier")),
        )
      val rT = toSemanticTable(carriersDf, Some("carriers"))
        .withDimensions(
          Dimension("code", t => t("code")),
          Dimension("name", t => t("name")),
        )
      val joined = lT.join_one(rT, (l, r) => l("carrier") === r("code"))

      // Emit
      val identity = SemanticManifest.Identity(
        id = "io.example.asym.flights_carriers", namespace = "demo",
      )
      val json = SemanticManifest.toJoinedJson(joined, identity, prettyPrint = true)

      // Wire carries BOTH sides' names independently
      val tree = mapper.readTree(json)
      val joinObj = tree.path("model").path("join")
      assert(joinObj.path("leftKeys").toString  == "[\"carrier\"]")
      assert(joinObj.path("rightKeys").toString == "[\"code\"]")
      assert(!joinObj.path("predicate_ast").isMissingNode,
        "writer should emit predicate_ast for an asymmetric equi join")

      // Round-trip back through fromJoinedJson preserves the asymmetric shape
      val restored = SemanticManifest.fromJoinedJson(json, flightsDf, carriersDf)
      val restoredJ = restored.root.asInstanceOf[SemanticJoinOp]
      assert(restoredJ.leftKeys  == Seq("carrier"))
      assert(restoredJ.rightKeys == Seq("code"))
      assert(restoredJ.predicateAst.isDefined)
      val ast = restoredJ.predicateAst.get
      assert(ast.op == PredicateAst.Op.Eq)
      assert(ast.left  == PredicateAst.Operand.ColumnRef("left", "carrier"))
      assert(ast.right == PredicateAst.Operand.ColumnRef("right", "code"))
    } finally spark.stop()
  }

  // ── 4. Symmetric join still works (regression guard) ────────────

  test("v0.1.14: symmetric join still works (no regression)") {
    val (spark, _, _) = setupSpark()
    try {
      val flightsDf = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1, "AA"), Row(2, "UA"))),
        StructType(Seq(StructField("id", IntegerType), StructField("carrier", StringType)))
      )
      val carriersDf = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row("AA", "American"), Row("UA", "United"))),
        StructType(Seq(StructField("carrier", StringType), StructField("name", StringType)))
      )
      val lT = toSemanticTable(flightsDf, Some("flights"))
        .withDimensions(Dimension("carrier", t => t("carrier")))
      val rT = toSemanticTable(carriersDf, Some("carriers"))
        .withDimensions(Dimension("carrier", t => t("carrier")))
      val joined = lT.join_on(rT, "carrier" -> "carrier")
      val result = joined.toDataFrame(spark)
      assert(result.count() == 2, s"expected 2 rows, got ${result.count()}")
    } finally spark.stop()
  }

  // ── 5. Many cardinality (pre-aggregation path) with asymmetric keys ─

  test("v0.1.14: join_many with asymmetric keys pre-aggregates correctly") {
    val (spark, flightsDf, carriersDf) = setupSpark()
    try {
      val lT = toSemanticTable(flightsDf, Some("flights"))
        .withDimensions(
          Dimension("id", t => t("id")),
          Dimension("carrier", t => t("carrier")),
        )
        .withMeasures(Measure("id_count", _ => count(lit(1))))
      val rT = toSemanticTable(carriersDf, Some("carriers"))
        .withDimensions(
          Dimension("code", t => t("code")),
          Dimension("name", t => t("name")),
        )
      val joined = lT.join_many(rT, (l, r) => l("carrier") === r("code"))
      val result = joined.toDataFrame(spark)
      assert(result.count() == 3, s"expected 3 rows, got ${result.count()}")
    } finally spark.stop()
  }

  // ── 6. Collision detection still works for asymmetric keys ───────

  test("v0.1.14: collision detection exempts both sides' key names") {
    val spark = SparkSession.builder().master("local[1]").appName("asym-collide").getOrCreate()
    try {
      val flightsDf = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1, "AA"))),
        StructType(Seq(StructField("id", IntegerType), StructField("carrier", StringType)))
      )
      val carriersDf = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row("AA", "American"))),
        StructType(Seq(StructField("code", StringType), StructField("name", StringType)))
      )
      val lT = toSemanticTable(flightsDf, Some("flights"))
        .withDimensions(Dimension("name", _ => col("carrier")))
      val rT = toSemanticTable(carriersDf, Some("carriers"))
        .withDimensions(Dimension("name", t => t("name")))
      val ex = intercept[IllegalArgumentException] {
        lT.join_on(rT, "carrier" -> "code").toDataFrame(spark)
      }
      assert(ex.getMessage.contains("Dimension name collision"),
        s"expected collision error, got: ${ex.getMessage}")
      assert(ex.getMessage.contains("'name'"),
        s"collision should mention 'name', got: ${ex.getMessage}")
    } finally spark.stop()
  }
}
