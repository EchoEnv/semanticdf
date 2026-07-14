package io.semanticdf.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{count, lit, sum}
import io.semanticdf._
/** Benchmark harness — measures latency for core operations.
  *
  * Run with: mvn scala:run -DmainClass=io.semanticdf.examples.Benchmark
  *
  * Each example is run 3 times; the first pass is a JVM warmup (discarded).
  * Results show min / median / max across the remaining 2 runs.
  *
  * This establishes a baseline. Run it against your production data size before
  * any performance work so that regressions are detectable.
  */
object Benchmark {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.sql.ansi.enabled", "false")
      .getOrCreate()
    try {
      println("\n" + "=" * 60)
      println("semanticdf benchmark — establishing baseline")
      println("=" * 60)

      val flights =
        if (args.nonEmpty && args.head != "--small") {
          // Read from a CSV path supplied as the first argument (e.g. src/test/resources/flights_large.csv)
          println(s"Reading from $args head: ${args.head}")
          spark.read.option("header", "true").option("inferSchema", "true").csv(args.head)
        } else {
          println("Using 5-row in-memory fixture (pass a CSV path arg for larger data)")
          spark.createDataFrame(Seq(
            ("AA", "LAX", "JFK", 100, 5),
            ("AA", "JFK", "LAX", 120, 4),
            ("UA", "LAX", "ORD",  80, 3),
            ("UA", "ORD", "LAX",  90, 2),
            ("DL", "ATL", "JFK", 150, 6),
          )).toDF("carrier", "origin", "dest", "distance", "passengers")
        }
      println(s"Row count: ${flights.count()}")

      val model = toSemanticTable(flights, name = Some("flights"))
        .withDimensions(
          Dimension("carrier", t => t("carrier")),
          Dimension("origin",  t => t("origin")),
        )
        .withMeasures(
          Measure("total_passengers", t => sum(t("passengers"))),
          Measure("flight_count",    t => count(lit(1))),
          Measure("total_distance",  t => sum(t("distance"))),
          Measure("avg_passengers",  t => t("total_passengers") / t("flight_count")),
        )

      // --- Warmup pass (discarded) ---
      benchmark("warmup", 1) {
        model.groupBy("carrier").aggregate("avg_passengers").execute(spark).collect()
      }

      // --- Benchmark pass (3 runs each) ---
      benchmark("groupBy + base measures (AA/UA/DL)", 3) {
        model.groupBy("carrier").aggregate("total_passengers", "flight_count").execute(spark).collect()
      }

      benchmark("groupBy + calc measures", 3) {
        model.groupBy("carrier").aggregate("avg_passengers").execute(spark).collect()
      }

      benchmark("groupBy + 3 calcs (transitive chain)", 3) {
        model.groupBy("carrier").aggregate("total_passengers", "avg_passengers").execute(spark).collect()
      }

      benchmark("zero-grain aggregate (totals)", 3) {
        model.groupBy().aggregate("total_passengers", "flight_count").execute(spark).collect()
      }

      // --- With filter ---
      val filtered = model.where(Predicate.Compare("eq", "carrier", "AA"))

      benchmark("filter + groupBy (WHERE routing)", 3) {
        filtered.groupBy("carrier").aggregate("total_passengers").execute(spark).collect()
      }

      // --- explain (no execution) ---
      benchmark("explain() — no Spark compile", 3) {
        model.groupBy("carrier").aggregate("avg_passengers").explain()
      }

      // --- compiledSchema (compile, no rows) ---
      benchmark("compiledSchema() — compile only, no row execution", 3) {
        model.groupBy("carrier").aggregate("avg_passengers").compiledSchema(spark)
      }

      println("\n" + "=" * 60)
      println("Benchmark complete.")
      println("Establish a baseline BEFORE making performance changes.")
      println("Run on your target data size (not this 5-row fixture).")
      println("=" * 60)

    } finally {
      spark.stop()
    }
  }

  /** Run `block` `n` times, print min/median/max wall-clock time in ms. */
  private def benchmark(label: String, runs: Int)(block: => Unit): Unit = {
    val timings = (1 to runs).map { _ =>
      val t0 = System.nanoTime()
      block
      val ms = (System.nanoTime() - t0) / 1_000_000.0
      ms
    }
    val sorted = timings.sorted
    val min    = sorted.head
    val median = sorted(runs / 2)
    val max    = sorted.last
    println(f"[bench] $label%-45s  min=${min%7.1f}ms  med=${median%7.1f}ms  max=${max%7.1f}ms")
  }
}
