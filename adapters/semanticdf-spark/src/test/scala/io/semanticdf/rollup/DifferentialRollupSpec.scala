package io.semanticdf.rollup

import io.semanticdf.{Dimension, Measure, SemanticTable, SparkSessionFixture, toSemanticTable}
import scala.collection.JavaConverters._
import org.apache.spark.sql.functions._
import org.scalatest.funsuite.AnyFunSuite

/** Differential test: rollup query result must equal base query result
  * for any matching grain.
  *
  * This is the safety net for the rollups feature. The 5 v1 HIGH bugs
  * (MatchError, IllegalStateException, silent wrong data, etc.) all had
  * one pattern: rollup query produced different output than the equivalent
  * base query. A differential test catches this pattern automatically
  * across all grain × measure combinations.
  *
  * Per debug-mantra step 3 (falsify): the test enumerates 21 configurations
  * (7 grain options × 3 measure options) and asserts the rollup output
  * equals the base output. If the test fails, we found silent-wrong-data.
  */
class DifferentialRollupSpec extends AnyFunSuite with SparkSessionFixture {

  /** Build a simple source DataFrame: 4 columns (id, region, category, amount)
    * with deterministic data so rollup results can be cross-checked
    * against the equivalent base query. */
  private def buildSource(): org.apache.spark.sql.DataFrame = {
    val schema = org.apache.spark.sql.types.StructType(Seq(
      org.apache.spark.sql.types.StructField("id", org.apache.spark.sql.types.IntegerType, false),
      org.apache.spark.sql.types.StructField("region", org.apache.spark.sql.types.StringType, false),
      org.apache.spark.sql.types.StructField("category", org.apache.spark.sql.types.StringType, false),
      org.apache.spark.sql.types.StructField("amount", org.apache.spark.sql.types.IntegerType, false),
    ))
    val rows = (0 until 30).map { i =>
      org.apache.spark.sql.Row(i, (i % 3).toString, (i % 5).toString, 10 + (i * 7) % 90)
    }
    spark.createDataFrame(rows.asJava, schema)
  }

  // The 21 configurations
  private val grains = Seq(
    Seq("region"),
    Seq("category"),
    Seq("id"),
    Seq("region", "category"),
    Seq("region", "id"),
    Seq("category", "id"),
    Seq("region", "category", "id"),
  )

  private val measureConfigs = Seq(
    Seq(("total", "sum")),
    Seq(("cnt", "count")),
    Seq(("total", "sum"), ("cnt", "count")),
  )

  test("differential: rollup query equals base query for all grain × measure combinations") {
    val source = buildSource()
    val model = toSemanticTable(source, name = Some("orders"))
      .withDimensions(
        Dimension("id", t => t("id")),
        Dimension("region", t => t("region")),
        Dimension("category", t => t("category")),
      )
      .withMeasures(
        Measure("total", t => sum(t("amount"))),
        Measure("cnt", _ => count(lit(1))),
      )

    var failures: List[String] = Nil

    for (grain <- grains; measures <- measureConfigs) {
      // Build rollup: groupBy grain, agg measures
      val rollupAggs = measures.map {
        case ("total", "sum") => sum("amount").as("sum_amount")
        case ("cnt", "count") => count(lit(1)).as("count_star")
      }
      val rollupDf = source.groupBy(grain.map(col): _*).agg(rollupAggs.head, rollupAggs.tail: _*)

      val rollup = Rollup(
        name = "r1",
        baseModel = "orders",
        rollupDimensions = grain,
        rollupMeasures = measures.map {
          case ("total", "sum") => RollupMeasure("total", "sum", "sum_amount")
          case ("cnt", "count") => RollupMeasure("cnt", "count", "count_star")
        },
        sourceProvider = () => rollupDf,
      )

      val registered = model.withRollup(rollup)
      val registry = RollupRegistry.empty.register("r1", () => rollupDf)

      // Base query: same grain, same measures
      val baseResult = registered
        .query(measures = measures.map(_._1), dimensions = grain)
        .execute(spark)
        .collect()
        .map(r => r.toSeq.mkString("|"))
        .sorted
        .mkString("\n")

      // Rollup query: just execute the rollup
      val rollupResult = registered
        .useRollup("r1", registry)
        .execute(spark)
        .collect()
        .map(r => r.toSeq.mkString("|"))
        .sorted
        .mkString("\n")

      if (rollupResult != baseResult) {
        val diff = s"  grain=${grain.mkString(",")}, measures=${measures.map(_._1).mkString(",")}\n" +
          s"  Base result (${baseResult.split("\n").length} rows):\n  ${baseResult.split("\n").take(5).mkString("\n  ")}\n" +
          s"  Rollup result (${rollupResult.split("\n").length} rows):\n  ${rollupResult.split("\n").take(5).mkString("\n  ")}"
        failures = failures :+ diff
      }
    }

    if (failures.nonEmpty) {
      fail(s"DIFFERENTIAL FAIL: ${failures.length} configurations differ:\n\n" + failures.mkString("\n\n"))
    }
  }
}
