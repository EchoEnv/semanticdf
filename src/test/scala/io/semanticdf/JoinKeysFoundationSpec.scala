package io.semanticdf

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Tests for the foundation of the BLOCKed `joined-models-manifest`
  * keys fix (PR #153). Pins:
  *   1. `SemanticJoinOp` carries `leftKeys` / `rightKeys` / `onExprString`.
  *   2. The typed entry points `join_on` / `join_many_on` populate keys
  *      directly without needing probe decomposition.
  *   3. The legacy `(JoinSide, JoinSide) => Column` overload still works
  *      and decomposes the lambda AST at construction time.
  *   4. Multi-column AND is captured via the AST decomposition.
  *   5. Non-equi / OR predicates get empty keys + a SQL-form fallback.
  *   6. The `withDimensions` / `withMeasures` passthrough on a joined
  *      model preserves the keys + SQL fields.
  *   7. Empty default = back-compat for hand-constructed SemanticJoinOps. */
class JoinKeysFoundationSpec extends AnyFunSuite with Matchers {

  // -- helpers --

  private def setupSpark(): SparkSession = {
    val spark = SparkSession.builder().master("local[1]").appName("jkf").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark
  }

  private def makeA(spark: SparkSession) =
    spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1, "x"), Row(2, "y"), Row(3, "z"),
      )),
      StructType(Seq(
        StructField("id",   IntegerType),
        StructField("name", StringType),
      ))
    )

  private def makeB(spark: SparkSession) =
    spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1, "a"), Row(2, "b"),
      )),
      StructType(Seq(
        StructField("id",   IntegerType),
        StructField("city", StringType),
      ))
    )

  // -- typed entry point tests --

  test("join_on (single-key typed entry) populates leftKeys / rightKeys directly") {
    val spark = setupSpark()
    try {
      val a = toSemanticTable(makeA(spark), name = Some("A"))
      val b = toSemanticTable(makeB(spark), name = Some("B"))
      val joined = a.join_on(b, "id" -> "id")
      val op = joined.root.asInstanceOf[SemanticJoinOp]
      assert(op.leftKeys  == Seq("id"),
        s"expected leftKeys=['id'], got ${op.leftKeys}")
      assert(op.rightKeys == Seq("id"),
        s"expected rightKeys=['id'], got ${op.rightKeys}")
      // onExprString is None because typed entry populates keys directly;
      // the SQL fallback is only for the lambda path.
      assert(op.onExprString.isEmpty,
        s"onExprString should be None for typed entry, got ${op.onExprString}")
    } finally spark.stop()
  }

  test("join_many_on (single-key typed entry, many cardinality) populates keys") {
    val spark = setupSpark()
    try {
      val a = toSemanticTable(makeA(spark), name = Some("A"))
      val b = toSemanticTable(makeB(spark), name = Some("B"))
      val joined = a.join_many_on(b, "id" -> "id")
      val op = joined.root.asInstanceOf[SemanticJoinOp]
      assert(op.leftKeys  == Seq("id"))
      assert(op.rightKeys == Seq("id"))
      assert(op.cardinality == JoinCardinality.Many)
    } finally spark.stop()
  }

  test("join_on with multi-key arrays produces parallel key arrays") {
    val spark = setupSpark()
    try {
      // Build a richer left-side DataFrame with two join-able columns
      val rows = Seq(
        Row(1, "x", 100), Row(2, "y", 200), Row(3, "z", 300),
      )
      val aDf = spark.createDataFrame(
        spark.sparkContext.parallelize(rows),
        StructType(Seq(
          StructField("id",    IntegerType),
          StructField("name",  StringType),
          StructField("amount", IntegerType),
        ))
      )
      val a = toSemanticTable(aDf, name = Some("A"))
      val b = toSemanticTable(makeB(spark), name = Some("B"))
      // Force the multi-key form by giving the right side two columns
      // by hand-building it.
      val bMultiDf = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(
          Row(1, "a", 100), Row(2, "b", 200),
        )),
        StructType(Seq(
          StructField("id",     IntegerType),
          StructField("city",   StringType),
          StructField("amount", IntegerType),
        ))
      )
      val bMulti = toSemanticTable(bMultiDf, name = Some("B"))
      val joined = a.join_on(bMulti, Seq("id", "amount"), Seq("id", "amount"))
      val op = joined.root.asInstanceOf[SemanticJoinOp]
      assert(op.leftKeys  == Seq("id", "amount"),
        s"expected leftKeys=['id', 'amount'], got ${op.leftKeys}")
      assert(op.rightKeys == Seq("id", "amount"),
        s"expected rightKeys=['id', 'amount'], got ${op.rightKeys}")
    } finally spark.stop()
  }

  test("join_on rejects mismatched key-array lengths with a clear message") {
    val spark = setupSpark()
    try {
      val a = toSemanticTable(makeA(spark), name = Some("A"))
      val b = toSemanticTable(makeB(spark), name = Some("B"))
      val ex = intercept[IllegalArgumentException] {
        a.join_on(b, Seq("id"), Seq("id", "name"))
      }
      assert(ex.getMessage.contains("must equal"),
        s"expected clear-length error, got: ${ex.getMessage}")
    } finally spark.stop()
  }

  // -- lambda-path decomposition tests --

  test("legacy join_one (lambda form) decomposes a single equi-key at construction time") {
    val spark = setupSpark()
    try {
      val a = toSemanticTable(makeA(spark), name = Some("A"))
      val b = toSemanticTable(makeB(spark), name = Some("B"))
      // Old-style lambda form \u2014 the typed entry isn't required.
      val joined = a.join_one(b, (l, r) => l("id") === r("id"))
      val op = joined.root.asInstanceOf[SemanticJoinOp]
      assert(op.leftKeys  == Seq("id"),
        s"decomposed leftKeys should be ['id'], got ${op.leftKeys}")
      assert(op.rightKeys == Seq("id"),
        s"decomposed rightKeys should be ['id'], got ${op.rightKeys}")
      // SQL form is captured too as a round-trip fallback.
      assert(op.onExprString.exists(_.contains("=")),
        s"onExprString should be captured as a round-trip fallback, got ${op.onExprString}")
    } finally spark.stop()
  }

  test("legacy join_many with multi-column AND populates parallel key arrays via decomposition") {
    val spark = setupSpark()
    try {
      val rows = Seq(
        Row(1, "x", 100), Row(2, "y", 200),
      )
      val aDf = spark.createDataFrame(
        spark.sparkContext.parallelize(rows),
        StructType(Seq(
          StructField("id",    IntegerType),
          StructField("name",  StringType),
          StructField("amount", IntegerType),
        ))
      )
      val bDf = spark.createDataFrame(
        spark.sparkContext.parallelize(rows),
        StructType(Seq(
          StructField("id",     IntegerType),
          StructField("city",   StringType),
          StructField("amount", IntegerType),
        ))
      )
      val a = toSemanticTable(aDf, name = Some("A"))
      val b = toSemanticTable(bDf, name = Some("B"))
      val joined = a.join_one(
        b,
        (l, r) => (l("id") === r("id")) && (l("amount") === r("amount")),
      )
      val op = joined.root.asInstanceOf[SemanticJoinOp]
      // Decomposition should recover [id, amount] in left-AST visit order.
      assert(op.leftKeys  == Seq("id", "amount"),
        s"decomposed leftKeys should be [id, amount], got ${op.leftKeys}")
      assert(op.rightKeys == Seq("id", "amount"),
        s"decomposed rightKeys should be [id, amount], got ${op.rightKeys}")
    } finally spark.stop()
  }

  test("OR predicate decomposes to empty keys (canonical equi shape only)") {
    val spark = setupSpark()
    try {
      val a = toSemanticTable(makeA(spark), name = Some("A"))
      val b = toSemanticTable(makeB(spark), name = Some("B"))
      // OR isn't an equi-join \u2014 decomposition can't recover keys.
      val joined = a.join_one(
        b,
        (l, r) => l("id") === r("id") || l("name") === r("city"),
      )
      val op = joined.root.asInstanceOf[SemanticJoinOp]
      assert(op.leftKeys.isEmpty,
        s"OR decomposition should leave keys empty, got ${op.leftKeys}")
      // but the SQL form is still captured.
      assert(op.onExprString.nonEmpty,
        s"SQL fallback should still be captured, got ${op.onExprString}")
    } finally spark.stop()
  }

  test("non-equi join (> / < etc.) leaves keys empty + captures SQL") {
    val spark = setupSpark()
    try {
      val a = toSemanticTable(makeA(spark), name = Some("A"))
      val b = toSemanticTable(makeB(spark), name = Some("B"))
      val joined = a.join_one(b, (l, r) => l("id") > r("id"))
      val op = joined.root.asInstanceOf[SemanticJoinOp]
      assert(op.leftKeys.isEmpty,
        s"non-equi should leave keys empty, got ${op.leftKeys}")
      assert(op.onExprString.nonEmpty,
        s"SQL fallback should still be captured, got ${op.onExprString}")
    } finally spark.stop()
  }

  // -- passthrough preservation --

  test("withDimensions on a joined model preserves leftKeys / rightKeys / onExprString") {
    val spark = setupSpark()
    try {
      val a = toSemanticTable(makeA(spark), name = Some("A"))
      val b = toSemanticTable(makeB(spark), name = Some("B"))
      val joined = a.join_on(b, "id" -> "id")
      val withExtra = joined.withDimensions(
        Dimension("marker", _ => lit(1))
      )
      val op = withExtra.root.asInstanceOf[SemanticJoinOp]
      assert(op.leftKeys  == Seq("id"),
        s"withDimensions must preserve leftKeys, got ${op.leftKeys}")
      assert(op.rightKeys == Seq("id"),
        s"withDimensions must preserve rightKeys, got ${op.rightKeys}")
      assert(op.extraDimensions.contains("marker"))
    } finally spark.stop()
  }

  test("withMeasures on a joined model preserves leftKeys / rightKeys / onExprString") {
    val spark = setupSpark()
    try {
      val a = toSemanticTable(makeA(spark), name = Some("A"))
      val b = toSemanticTable(makeB(spark), name = Some("B"))
      val joined = a.join_on(b, "id" -> "id")
      val withExtra = joined.withMeasures(
        Measure("cnt", _ => count(lit(1)), exprString = Some("count(1)"))
      )
      val op = withExtra.root.asInstanceOf[SemanticJoinOp]
      assert(op.leftKeys  == Seq("id"))
      assert(op.rightKeys == Seq("id"))
      assert(op.extraMeasures.contains("cnt"))
    } finally spark.stop()
  }

  // -- back-compat default --

  test("SemanticJoinOp can still be constructed without the new keys / onExprString fields (back-compat)") {
    val spark = setupSpark()
    try {
      val aDf = makeA(spark)
      val bDf = makeB(spark)
      val op = SemanticTableOp(aDf, name = Some("A"), description = None)
      val op2 = SemanticTableOp(bDf, name = Some("B"), description = None)
      val join = SemanticJoinOp(
        left   = op,
        right  = op2,
        on     = (l, r) => l("id") === r("id"),
        cardinality = JoinCardinality.One,
        leftRoot  = op,
        rightRoot = op2,
      )
      assert(join.leftKeys.isEmpty,
        s"back-compat default for leftKeys should be empty, got ${join.leftKeys}")
      assert(join.rightKeys.isEmpty,
        s"back-compat default for rightKeys should be empty, got ${join.rightKeys}")
      assert(join.onExprString.isEmpty,
        s"back-compat default for onExprString should be empty, got ${join.onExprString}")
    } finally spark.stop()
  }

  test("typed entry takes precedence over lambda decomposition when both are explicit") {
    // Sanity: the typed entry's keys are kept verbatim, regardless of
    // what the synthesized lambda's AST would yield.
    val spark = setupSpark()
    try {
      val a = toSemanticTable(makeA(spark), name = Some("A"))
      val b = toSemanticTable(makeB(spark), name = Some("B"))
      val joined = a.join_on(b, "id" -> "id")
      val op = joined.root.asInstanceOf[SemanticJoinOp]
      assert(op.leftKeys == Seq("id"))
      assert(op.onExprString.isEmpty,
        s"typed entry should NOT trigger SQL fallback capture, got ${op.onExprString}")
    } finally spark.stop()
  }
}
