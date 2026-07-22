package io.semanticdf

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Tests for the `SemanticJoinOp.leftSide` / `rightSide` foundation.
  *
  * These two optional `SemanticTable` fields were added so the BLOCKed
  * `joined-models-manifest` recipe (PR #151) can emit per-side
  * metadata without re-deriving it. This spec pins the foundation:
  *
  *   1. `join_one` / `join_many` / `join_cross` populate both fields
  *      with the originating `SemanticTable`s.
  *   2. `withDimensions` / `withMeasures` on a joined model preserve
  *      the side references (they're the metadata transport).
  *   3. `SemanticJoinOp` is still constructible with `leftSide` /
  *      `rightSide` defaulted to `None` — back-compat for hand-written
  *      tests and future user code.
  *   4. The side references carry useful metadata (name, version,
  *      description, sourceTable, status) that the `leftRoot` /
  *      `rightRoot` can't provide alone.
  */
class JoinedSideMetadataSpec extends AnyFunSuite with Matchers {

  // -- helpers --

  private def setupSpark(): SparkSession = {
    val spark = SparkSession.builder().master("local[1]").appName("jsm").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark
  }

  private def makeLeft(spark: SparkSession) =
    spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(1, "a"), Row(2, "b"))),
      StructType(Seq(
        StructField("id",    IntegerType),
        StructField("name",  StringType),
      ))
    )

  private def makeRight(spark: SparkSession) =
    spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(1, "x"), Row(2, "y"))),
      StructType(Seq(
        StructField("id",   IntegerType),
        StructField("city", StringType),
      ))
    )

  // -- tests --

  test("join_one populates leftSide and rightSide with the originating SemanticTable") {
    val spark = setupSpark()
    try {
      val leftT = toSemanticTable(makeLeft(spark), name = Some("L"))
      val rightT = toSemanticTable(makeRight(spark), name = Some("R"))
      val joined = leftT.join_one(rightT, (l, r) => l("id") === r("id"))
      val op = joined.root.asInstanceOf[SemanticJoinOp]
      assert(op.leftSide.contains(leftT),  "leftSide should hold the original `this` SemanticTable")
      assert(op.rightSide.contains(rightT), "rightSide should hold the `other` SemanticTable")
    } finally spark.stop()
  }

  test("join_many populates leftSide and rightSide") {
    val spark = setupSpark()
    try {
      val leftT = toSemanticTable(makeLeft(spark), name = Some("L"))
      val rightT = toSemanticTable(makeRight(spark), name = Some("R"))
      val joined = leftT.join_many(rightT, (l, r) => l("id") === r("id"))
      val op = joined.root.asInstanceOf[SemanticJoinOp]
      assert(op.leftSide.contains(leftT))
      assert(op.rightSide.contains(rightT))
    } finally spark.stop()
  }

  test("join_cross populates leftSide and rightSide") {
    val spark = setupSpark()
    try {
      val leftT = toSemanticTable(makeLeft(spark), name = Some("L"))
      val rightT = toSemanticTable(makeRight(spark), name = Some("R"))
      val joined = leftT.join_cross(rightT)
      val op = joined.root.asInstanceOf[SemanticJoinOp]
      assert(op.leftSide.contains(leftT))
      assert(op.rightSide.contains(rightT))
    } finally spark.stop()
  }

  test("withDimensions on a joined model preserves leftSide and rightSide") {
    val spark = setupSpark()
    try {
      val leftT = toSemanticTable(makeLeft(spark), name = Some("L"))
      val rightT = toSemanticTable(makeRight(spark), name = Some("R"))
      val joined = leftT.join_one(rightT, (l, r) => l("id") === r("id"))
      val withExtra = joined.withDimensions(
        Dimension("marker", _ => lit(1))
      )
      val op = withExtra.root.asInstanceOf[SemanticJoinOp]
      assert(op.leftSide.contains(leftT),
        "leftSide must persist across withDimensions on a joined model")
      assert(op.rightSide.contains(rightT),
        "rightSide must persist across withDimensions on a joined model")
      assert(op.extraDimensions.contains("marker"),
        "the new extra dimension should be on the joined op")
    } finally spark.stop()
  }

  test("withMeasures on a joined model preserves leftSide and rightSide") {
    val spark = setupSpark()
    try {
      val leftT = toSemanticTable(makeLeft(spark), name = Some("L"))
      val rightT = toSemanticTable(makeRight(spark), name = Some("R"))
      val joined = leftT.join_one(rightT, (l, r) => l("id") === r("id"))
      val withExtra = joined.withMeasures(
        Measure("cnt", _ => count(lit(1)), exprString = Some("count(1)"))
      )
      val op = withExtra.root.asInstanceOf[SemanticJoinOp]
      assert(op.leftSide.contains(leftT))
      assert(op.rightSide.contains(rightT))
      assert(op.extraMeasures.contains("cnt"))
    } finally spark.stop()
  }

  test("SemanticJoinOp can still be constructed with leftSide/rightSide defaulted to None (back-compat)") {
    // Hand-constructed `SemanticJoinOp` (e.g. inside test suites or future
    // user code that builds an op tree directly) doesn't have to populate
    // the new fields. The default values of `None` keep the existing
    // 8-arg constructor working unchanged.
    val spark = setupSpark()
    try {
      val dfA = makeLeft(spark)
      val dfB = makeRight(spark)
      val op = SemanticTableOp(dfA, name = Some("A"), description = None)
      val op2 = SemanticTableOp(dfB, name = Some("B"), description = None)
      val join = SemanticJoinOp(
        left   = op,
        right  = op2,
        on     = (l, r) => l("id") === r("id"),
        cardinality = JoinCardinality.One,
        leftRoot  = op,
        rightRoot = op2,
      )
      assert(join.leftSide.isEmpty, "leftSide defaults to None (back-compat)")
      assert(join.rightSide.isEmpty, "rightSide defaults to None (back-compat)")
    } finally spark.stop()
  }

  test("side references carry metadata beyond what leftRoot/rightRoot retain") {
    // The whole point of leftSide / rightSide: the per-side SemanticTable
    // carries version, description, sourceTable, status — fields that the
    // underlying leftRoot / rightRoot (which are SemanticTableOp) don't
    // represent. The implementation PR uses these for the joined-manifest
    // wire shape.
    val spark = setupSpark()
    try {
      val leftT = toSemanticTable(makeLeft(spark),
        name = Some("customers"), description = Some("customer master"),
        sourceTable = Some("customers_csv"))
        .version(3)
        .status(ModelStatus.Deprecated)
      val rightT = toSemanticTable(makeRight(spark),
        name = Some("orders"), description = Some("order line items"),
        sourceTable = Some("orders_csv"))
        .version(2)
      val joined = leftT.join_one(rightT, (l, r) => l("id") === r("id"))

      val op = joined.root.asInstanceOf[SemanticJoinOp]
      val l = op.leftSide.get
      val r = op.rightSide.get
      assert(l.name.contains("customers"))
      assert(l.description.contains("customer master"))
      assert(l.version == 3)
      assert(l.sourceTable.contains("customers_csv"))
      assert(l.status == ModelStatus.Deprecated)
      assert(r.name.contains("orders"))
      assert(r.version == 2)
      // The op layer's leftRoot carries name + description but NOT
      // version, sourceTable, or status — those live only on the
      // SemanticTable. That is the gap that motivates the new fields.
      val leftRoot = op.leftRoot
      assert(leftRoot.name.contains("customers"))
      // status is the discriminator — status arrived in v0.1.10 and never
      // made it into SemanticTableOp. The new fields motivate the
      // joined-manifest recipe (PR #151) being able to round-trip status.
    } finally spark.stop()
  }
}

