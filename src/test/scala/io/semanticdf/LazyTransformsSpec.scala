package io.semanticdf

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import scala.jdk.CollectionConverters._

/** Regression tests for the v0.1.2 lazy-transforms refactor.
  *
  * The bug: before the refactor, `withTransforms` on a join model called
  * `j.compile(SparkSession.active)` to get the joined DataFrame eagerly,
  * then applied `withColumn` against it. Two problems:
  *
  *   1. `SparkSession.active` is a side effect — it auto-creates a default
  *      session if none is set. A consumer building a SemanticTable in a
  *      context without a session would silently get a Spark session.
  *   2. The join was forced to build its logical plan at op-tree
  *      construction time, breaking the lazy compile contract.
  *
  * The fix: `withTransforms` now wraps the source in a `SemanticTransformsOp`.
  * The transforms are applied lazily at `toDataFrame(spark)` time, consistent
  * with every other op in the tree.
  *
  * ==Test design==
  *
  * The cleanest verification of the fix is a tree-shape test: after
  * `withTransforms` on a joined model, the op tree's root is
  * `SemanticTransformsOp` (not a `SemanticTableOp` with eagerly-compiled
  * data), and the source of that `SemanticTransformsOp` is the original
  * `SemanticJoinOp` (still un-compiled). Before the fix, the root would
  * have been a `SemanticTableOp` (the eagerly-compiled join+transform
  * result), and there would be no `SemanticTransformsOp` in the tree.
  *
  * This test does NOT use `SparkSessionFixture` so it can verify the
  * op-tree shape without depending on the test runner's session lifecycle.
  * We create+stop a session inside the test; the `SemanticTable` holds a
  * DataFrame that references a stopped session, but that's fine — the test
  * never calls `toDataFrame`, so the DataFrame is never compiled. */
class LazyTransformsSpec extends AnyFunSuite with BeforeAndAfterEach {

  // Each test starts and ends with a clean Spark session state. This isolates
  // the test from any session created by other test suites (ScalaTest runs
  // all tests in the same JVM; a session created in one suite persists
  // across suites).
  override def beforeEach(): Unit = {
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
  }
  override def afterEach(): Unit = {
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
  }

  /** Build a tiny in-memory DataFrame backed by `spark`. The DataFrame holds
    * a reference to the session it was created in; if the session is stopped,
    * the DataFrame is dead. The op tree doesn't care — it just holds the
    * DataFrame reference and doesn't try to compile it.
    */
  private def buildTinyDf(spark: SparkSession, name: String, kvs: (String, Int)*): DataFrame = {
    import spark.implicits._
    val rows = kvs.map { case (k, v) => (k, v) }
    val schema = StructType(Seq(
      StructField("k", StringType, nullable = false),
      StructField("v", IntegerType, nullable = false),
    ))
    spark.createDataFrame(rows.map { case (k, v) => Row(k, v) }.asJava, schema)
  }

  /** Walk the op tree and return the first `SemanticTableOp` (deepest leaf). */
  private def firstLeaf(op: SemanticOp): SemanticTableOp = op match {
    case t: SemanticTableOp          => t
    case SemanticTransformsOp(s, _) => firstLeaf(s)
    case SemanticFilterOp(s, _)      => firstLeaf(s)
    case SemanticOrderByOp(s, _)     => firstLeaf(s)
    case SemanticLimitOp(s, _)       => firstLeaf(s)
    case SemanticHintOp(s, _, _)     => firstLeaf(s)
    case j: SemanticJoinOp          => firstLeaf(j.left)  // just the left side
    case _                          =>
      throw new IllegalStateException(s"Unexpected op: ${op.getClass.getSimpleName}")
  }

  /** The op just below a `SemanticTransformsOp` (or `None` if the root isn't
    * a `SemanticTransformsOp`). Used to assert the op-tree shape. */
  private def sourceUnderTransforms(op: SemanticOp): Option[SemanticOp] = op match {
    case t: SemanticTransformsOp => Some(t.source)
    case _ => None
  }

  test("withTransforms on a single-table model wraps in SemanticTransformsOp (lazy)") {
    val spark = SparkSession.builder().master("local[1]").appName("test").getOrCreate()
    try {
      val st = toSemanticTable(buildTinyDf(spark, "a", "a" -> 1, "b" -> 2), name = Some("t"))
      val withT = st.withTransforms(Transform("vv", t => t("v") * 2))

      // The op tree shape changed:
      //   BEFORE: SemanticTable(SemanticTableOp(transformedDf))
      //   AFTER:  SemanticTable(SemanticTransformsOp(SemanticTableOp(origDf), [Transform]))
      withT.root match {
        case SemanticTransformsOp(src, transforms) =>
          assert(src.isInstanceOf[SemanticTableOp],
            s"expected SemanticTableOp under transforms, got ${src.getClass.getSimpleName}")
          assert(transforms.length == 1)
          assert(transforms.head.name == "vv")
        case other =>
          fail(s"expected SemanticTransformsOp as root, got ${other.getClass.getSimpleName}")
      }
    } finally {
      spark.stop()
    }
  }

  test("withTransforms on a join model wraps the SemanticJoinOp in SemanticTransformsOp (no eager compile)") {
    val spark = SparkSession.builder().master("local[1]").appName("test").getOrCreate()
    try {
      val left  = toSemanticTable(buildTinyDf(spark, "l", "a" -> 1, "b" -> 2), name = Some("left"))
      val right = toSemanticTable(buildTinyDf(spark, "r", "a" -> 10, "b" -> 20), name = Some("right"))
      val joined = left.join_one(right, (l, r) => l("k") === r("k"))
        .withTransforms(Transform("weighted", t => t("v") * 10))

      // The op tree shape changed:
      //   BEFORE: SemanticTable(SemanticTableOp(eagerly-compiled joinedDf + transform))
      //   AFTER:  SemanticTable(SemanticTransformsOp(SemanticJoinOp(left, right), [Transform]))
      joined.root match {
        case SemanticTransformsOp(src, transforms) =>
          // CRITICAL: the source is a SemanticJoinOp, NOT a SemanticTableOp.
          // If the source were a SemanticTableOp, the fix is broken — the
          // join would have been compiled eagerly.
          assert(src.isInstanceOf[SemanticJoinOp],
            s"expected SemanticJoinOp under transforms (lazy), got ${src.getClass.getSimpleName}. " +
            s"This means withTransforms is still eager-compiling the join.")
          assert(transforms.length == 1)
          assert(transforms.head.name == "weighted")
        case other =>
          fail(s"expected SemanticTransformsOp as root, got ${other.getClass.getSimpleName}")
      }
    } finally {
      spark.stop()
    }
  }

  test("op tree catalog accessors work after withTransforms on a join (transparency)") {
    val spark = SparkSession.builder().master("local[1]").appName("test").getOrCreate()
    try {
      val left  = toSemanticTable(buildTinyDf(spark, "l", "a" -> 1, "b" -> 2), name = Some("left"))
        .withDimensions(Dimension("k", t => t("k")))
      val right = toSemanticTable(buildTinyDf(spark, "r", "a" -> 10, "b" -> 20), name = Some("right"))
        .withDimensions(Dimension("k", t => t("k")))
      val joined = left.join_one(right, (l, r) => l("k") === r("k"))
        .withDimensions(Dimension("weighted", t => t("v") * 10))
        .withTransforms(Transform("doubled", t => t("v") * 2))
        .withMeasures(Measure("summed", t => sum(t("v"))))

      // Catalog accessors must recurse through the new SemanticTransformsOp.
      // Before the refactor these would still work (because withTransforms
      // returned a SemanticTableOp), but the test pins the contract: they
      // must keep working with the new op in the tree.
      assert(joined.dimensions.contains("k"),     "should see 'k' dimension from the left side")
      assert(joined.dimensions.contains("weighted"), "should see the join's extra dimension")
      assert(joined.measures.contains("summed"),   "should see the measure added after transforms")

      // The transform output (`doubled`) is NOT a catalog dimension or measure;
      // it's just a DataFrame column. We don't assert it here.
    } finally {
      spark.stop()
    }
  }

  test("chained withTransforms composes into a single SemanticTransformsOp layer") {
    val spark = SparkSession.builder().master("local[1]").appName("test").getOrCreate()
    try {
      val st = toSemanticTable(buildTinyDf(spark, "a", "a" -> 1, "b" -> 2), name = Some("t"))
      val withT = st
        .withTransforms(Transform("a", t => t("v") + 1))
        .withTransforms(Transform("b", t => t("a") * 2))

      // Chained transforms compose into a single layer (preserves
      // declaration order). The structure should be:
      //   SemanticTransformsOp(SemanticTableOp(orig), [Transform("a"), Transform("b")])
      withT.root match {
        case SemanticTransformsOp(src, transforms) =>
          assert(src.isInstanceOf[SemanticTableOp])
          assert(transforms.length == 2, s"expected 2 transforms, got ${transforms.length}")
          assert(transforms.head.name == "a")
          assert(transforms(1).name == "b")
        case other =>
          fail(s"expected SemanticTransformsOp as root, got ${other.getClass.getSimpleName}")
      }
    } finally {
      spark.stop()
    }
  }
}
