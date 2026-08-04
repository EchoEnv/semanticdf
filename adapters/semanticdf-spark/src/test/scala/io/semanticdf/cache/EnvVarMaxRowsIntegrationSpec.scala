package io.semanticdf.cache

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.semanticdf.{Dimension, Measure, SparkSessionFixture, toSemanticTable}

/** Integration test for the `SEMANTICDF_MAX_ROWS` env-var override
  * on `CacheBridge.executeQuery`.
  *
  * Drives a real `executeQuery` call with `envMaxRowsOverride`
  * set to verify the cap is actually applied end-to-end.
  * `EnvVarMaxRowsSpec` covers the pure parser; this test
  * covers the wiring through the 5-arg `executeQuery`.
  */
class EnvVarMaxRowsIntegrationSpec extends AnyFunSuite with Matchers with SparkSessionFixture {
  test("5-arg executeQuery honors envMaxRowsOverride (the H1 fix)") {
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize((1 to 1000).map(i => Row(i))),
      StructType(Seq(StructField("x", IntegerType)))
    )
    val model = toSemanticTable(df, name = Some("t"))
      .withDimensions(Dimension("x", t => t("x")))
      .withMeasures(Measure("c", _ => lit(1), exprString = Some("1")))

    val saved = CacheBridge.envMaxRowsOverride
    try {
      // Set the test-only override to a small value. The 5-arg
      // executeQuery should cap the result at 7 rows.
      CacheBridge.envMaxRowsOverride = Some(7)
      val result = CacheBridge.executeQuery(
        model, spark,
        java.util.Arrays.asList("c"),
        java.util.Arrays.asList("x"),
        ""
      )
      assert(result.rows.length == 7,
        s"expected 7 rows (the override cap), got ${result.rows.length}")
    } finally {
      CacheBridge.envMaxRowsOverride = saved
    }
  }

  test("0 = no-cap sentinel propagates through executeQuery (the H1 fix for env=0)") {
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize((1 to 50).map(i => Row(i))),
      StructType(Seq(StructField("x", IntegerType)))
    )
    val model = toSemanticTable(df, name = Some("t"))
      .withDimensions(Dimension("x", t => t("x")))
      .withMeasures(Measure("c", _ => lit(1), exprString = Some("1")))

    val saved = CacheBridge.envMaxRowsOverride
    try {
      // 0 = no cap. All 50 rows should be returned.
      CacheBridge.envMaxRowsOverride = Some(0)
      val result = CacheBridge.executeQuery(
        model, spark,
        java.util.Arrays.asList("c"),
        java.util.Arrays.asList("x"),
        ""
      )
      assert(result.rows.length == 50,
        s"expected 50 rows (0=no-cap), got ${result.rows.length}")
    } finally {
      CacheBridge.envMaxRowsOverride = saved
    }
  }
}
