package io.semanticdf

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite

/** Regression test for H-D1 (PR #332).
  *
  * H-D1: every `join_one` / `join_many` / `join_cross` silently reset
  * `version` / `sourceTable` / `status` on the joined result. Two distinct
  * failure modes per call site:
  *
  *   (a) Lambda-path early-return hardcoded the three fields to defaults
  *       (`version = 0`, `sourceTable = None`, `status = ModelStatus.Published`).
  *   (b) Typed-key fall-through didn't name the fields, so they fell to
  *       the same defaults via the `SemanticTable` constructor defaults.
  *
  * The fix propagates `this.{version, sourceTable, status}` at all 5 join
  * sites (join_oneWithKeys lambda early-return + typed-key fall-through;
  * join_manyWithKeys lambda early-return + typed-key fall-through;
  * join_cross).
  *
  * Note: `postAggPredicates` propagates the same way, but the only public
  * setter (`.where(...)`) wraps the table in a query wrapper that
  * [[docs/known-limitations.md]] prohibits joining. Propagation is
  * type-systemically identical (a list of predicates carried in `this` and
  * re-passed via `new SemanticTable(... postAggPredicates = this.postAggPredicates, ...)`),
  * so this spec asserts the three fields that don't hit the query-wrapper
  * limitation. The MD/RD reviewer combined asserts the four-arg contract
  * in `SemanticTableMutation.scala`.
  */
class JoinMetadataPreservationSpec extends AnyFunSuite {

  // -- helpers --

  private def setupSpark(): SparkSession = {
    val spark = SparkSession.builder().master("local[1]").appName("h-d1").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark
  }

  private def makeLeft(spark: SparkSession) =
    spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(1, "a"), Row(2, "b"))),
      StructType(Seq(
        StructField("id",   IntegerType),
        StructField("name", StringType),
      ))
    )

  private def makeRight(spark: SparkSession) =
    spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(1, "x"), Row(2, "y"))),
      StructType(Seq(
        StructField("id",  IntegerType),
        StructField("city", StringType),
      ))
    )

  // -- tests: join_one --

  test("join_on preserves version / sourceTable / status (typed-key fall-through)") {
    // Path (b): typed-key form (`join_on`) never enters the lambda
    // early-return — it falls through to the bottom `new SemanticTable(...)`
    // which originally didn't name the four metadata fields.
    val spark = setupSpark()
    try {
      val leftT = toSemanticTable(makeLeft(spark),
        name = Some("customers"), sourceTable = Some("customers_csv"))
        .version(7)
        .status(ModelStatus.Deprecated)
      val rightT = toSemanticTable(makeRight(spark),
        name = Some("orders"), sourceTable = Some("orders_csv"))
        .version(2)
      val joined = leftT.join_on(rightT, ("id", "id"))

      assert(joined.version == 7, s"version should propagate, got ${joined.version}")
      assert(joined.sourceTable.contains("customers_csv"),
        s"sourceTable should propagate, got ${joined.sourceTable}")
      assert(joined.status == ModelStatus.Deprecated,
        s"status should propagate, got ${joined.status}")
    } finally spark.stop()
  }

  test("join_one preserves metadata (lambda early-return path)") {
    // Path (a): lambda form triggers early-return with hardcoded values.
    // The lambda deliberately returns a non-trivial expression so the
    // AST-extraction probe populates `leftKeys` / `rightKeys` and the
    // early-return branch fires.
    val spark = setupSpark()
    try {
      val leftT = toSemanticTable(makeLeft(spark),
        name = Some("customers"), sourceTable = Some("customers_csv"))
        .version(11)
        .status(ModelStatus.Draft)
      val rightT = toSemanticTable(makeRight(spark),
        name = Some("orders"), sourceTable = Some("orders_csv"))
      val joined = leftT.join_one(rightT, (l, r) => l("id") === r("id"))

      assert(joined.version == 11, s"version should propagate through lambda early-return, got ${joined.version}")
      assert(joined.sourceTable.contains("customers_csv"),
        s"sourceTable should propagate, got ${joined.sourceTable}")
      assert(joined.status == ModelStatus.Draft,
        s"status should propagate, got ${joined.status}")
    } finally spark.stop()
  }

  // -- tests: join_many --

  test("join_many_on preserves metadata (typed-key fall-through)") {
    val spark = setupSpark()
    try {
      val leftT = toSemanticTable(makeLeft(spark), sourceTable = Some("L_csv"))
        .version(13)
        .status(ModelStatus.Deprecated)
      val rightT = toSemanticTable(makeRight(spark), sourceTable = Some("R_csv"))
      val joined = leftT.join_many_on(rightT, ("id", "id"))

      assert(joined.version == 13, s"got ${joined.version}")
      assert(joined.sourceTable.contains("L_csv"), s"got ${joined.sourceTable}")
      assert(joined.status == ModelStatus.Deprecated, s"got ${joined.status}")
    } finally spark.stop()
  }

  test("join_many preserves metadata (lambda early-return path)") {
    val spark = setupSpark()
    try {
      val leftT = toSemanticTable(makeLeft(spark), sourceTable = Some("L_csv"))
        .version(17)
      val rightT = toSemanticTable(makeRight(spark))
      val joined = leftT.join_many(rightT, (l, r) => l("id") === r("id"))

      assert(joined.version == 17, s"got ${joined.version}")
      assert(joined.sourceTable.contains("L_csv"), s"got ${joined.sourceTable}")
    } finally spark.stop()
  }

  // -- tests: join_cross --

  test("join_cross preserves metadata") {
    val spark = setupSpark()
    try {
      val leftT = toSemanticTable(makeLeft(spark), sourceTable = Some("L_csv"))
        .version(19)
        .status(ModelStatus.Draft)
      val joined = leftT.join_cross(toSemanticTable(makeRight(spark)))

      assert(joined.version == 19, s"got ${joined.version}")
      assert(joined.sourceTable.contains("L_csv"), s"got ${joined.sourceTable}")
      assert(joined.status == ModelStatus.Draft, s"got ${joined.status}")
    } finally spark.stop()
  }

  // -- tests: falsification --

  test("FALSIFICATION: revert the fix and these assertions fire") {
    // Sanity check that the test pins the bug. If you revert the fix in
    // SemanticTableMutation.scala, joined.version should be 0 (not 7),
    // joined.status should be Published (not Deprecated), and
    // joined.sourceTable should be None. The assertion messages above
    // will fire and this spec will go red.
    val spark = setupSpark()
    try {
      val leftT = toSemanticTable(makeLeft(spark))
        .version(7)
        .status(ModelStatus.Deprecated)
      val rightT = toSemanticTable(makeRight(spark))
      val joined = leftT.join_one(rightT, (l, r) => l("id") === r("id"))
      // Re-pinned assertions with explicit default-vs-propagated expected
      // values. Defaults are `0 / None / Published`; propagation carries
      // the LHS user's values.
      assert(joined.version == 7, s"version should be 7 (propagated), got ${joined.version}")
      assert(joined.sourceTable.isEmpty, "no sourceTable was set on LHS, expected None (propagated)")
      assert(joined.status == ModelStatus.Deprecated,
        s"status should be Deprecated (propagated), got ${joined.status}")
    } finally spark.stop()
  }
}
