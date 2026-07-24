package io.semanticdf

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, lit}

/** Tiny test fixture used by PredicateAstSpec. Returns two simple
  * in-memory SemanticTables joined on `id`, with a `date` column for
  * non-equi tests. */
object SemanticTableFixture {

  /** Shared SparkSession accessor for tests. */
  def spark: SparkSession = SparkSession.getActiveSession.getOrElse(
    SparkSession.builder().appName("PredicateAstFixture").master("local[1]").getOrCreate()
  )

  /** Build a simple SemanticTable with columns id, date, name. The
    * `seed` is appended to id so left and right tables have distinct
    * rows. */
  def simple(seed: Int, side: String): SemanticTable = {
    val s = spark
    val df = {
      import s.implicits._
      Seq(
        (seed * 100 + 1, "2024-01-01", s"$side-1"),
        (seed * 100 + 2, "2024-01-02", s"$side-2"),
      ).toDF("id", "date", "name")
    }
    toSemanticTable(df, name = Some(side))
      .withDimensions(
        Dimension("id",    _ => col("id")),
        Dimension("date",  _ => col("date")),
        Dimension("name",  _ => col("name")),
      )
      .withMeasures(
        Measure("row_count", _ => count(lit(1))),
      )
  }

  /** Build a tiny single-row DataFrame. */
  def tinyDf(seed: String, n: Int) = {
    val s = spark
    import s.implicits._
    Seq((n, "2024-01-01", seed)).toDF("id", "date", "name")
  }
}
