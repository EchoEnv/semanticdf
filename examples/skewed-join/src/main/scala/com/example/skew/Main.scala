package com.example.skew

import io.semanticdf._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/** Runnable example: skewed joins with `withSalt(n)`.
  *
  * Builds a dataset where one join key (user_id=1) has 90% of the
  * data — the classic "celebrity user" problem. Demonstrates the
  * `withSalt(n)` skew-handling hint, which translates to Spark AQE
  * skew handling.
  *
  * Run with:
  *   mvn scala:run -DmainClass=com.example.skew.Main
  *
  * See `docs/tutorial-runtime-tuning.md` § 6 for the full design
  * rationale (why a custom salt column would produce wrong results,
  * how Spark AQE handles skew at the shuffle stage, etc.).
  */
object Main {

  /** Event row used to build the events DataFrame. Case class so
    * Spark's encoder can derive a schema automatically (avoids the
    * `ClassTag[String]` issue with anonymous tuples). Must be public
    * (or package-private, NOT `private`) so Spark's code generator
    * can call its constructor from compiled Catalyst expressions.
    */
  case class EventRow(event_id: Int, user_id: Int, event_type: String, value: Double)

  /** User row used to build the users DataFrame. */
  case class UserRow(user_id: Int, name: String, region: String)

  /** Build 1M events. user_id=1 has 900K events; the other 10K users
    * share the remaining 100K. This 90/10 split is the classic
    * "heavy hitter" skew pattern in star-schema joins. */
  private def buildEvents(spark: SparkSession, n: Int = 1_000_000) = {
    import spark.implicits._
    val rand = new scala.util.Random(42)
    val eventTypes = Array("click", "view", "purchase")
    val rows = (1 to n).map { i =>
      // 90% of events go to user_id=1 (heavy hitter). The rest go
      // to one of 9,999 other users.
      val userId = if (rand.nextDouble() < 0.90) 1 else (rand.nextInt(9999) + 2)
      val eventType = eventTypes.apply(rand.nextInt(3))
      EventRow(event_id = i, user_id = userId, event_type = eventType,
               value = 1.0 + (rand.nextDouble() * 10.0))
    }
    rows.toDF
  }

  /** Build 10K users. Small dimension table. */
  private def buildUsers(spark: SparkSession, n: Int = 10_000) = {
    import spark.implicits._
    val regions = Array("North", "South", "East", "West")
    val rows = (1 to n).map { i =>
      UserRow(
        user_id = i,
        name    = s"user_$i",
        region  = regions.apply(i % regions.length),
      )
    }
    rows.toDF
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[4]")
      .appName("skewed-join-example")
      .config("spark.sql.shuffle.partitions", "8")
      .config("spark.sql.adaptive.enabled", "false")  // start with AQE off so we can show withSalt turning it on
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try {
      println("=== Building skewed dataset ===")
      val eventsDf = buildEvents(spark)
      val usersDf = buildUsers(spark)
      val totalEvents = eventsDf.count()
      val heavyUserEvents = eventsDf.where(col("user_id") === 1).count()
      println(s"Total events: $totalEvents")
      println(s"Top user (id=1) has $heavyUserEvents events " +
        s"(${(heavyUserEvents * 100.0 / totalEvents).toInt}%)")

      println()
      println("=== Building models ===")
      val events = toSemanticTable(eventsDf, name = Some("events"))
        .withDimensions(
          Dimension("user_id",    _ => eventsDf("user_id")),
          Dimension("event_type", _ => eventsDf("event_type")),
        )
        .withMeasures(
          Measure("count", _ => count(lit(1))),
          Measure("sum_v", _ => sum(eventsDf("value"))),
        )

      val users = toSemanticTable(usersDf, name = Some("users"))
        .withDimensions(
          Dimension("user_id", _ => usersDf("user_id")),
          Dimension("region",  _ => usersDf("region")),
        )
        .withMeasures(
          Measure("count", _ => count(lit(1))),
        )

      println("  events model: 2 dimensions, 2 measures")
      println("  users model: 2 dimensions, 1 measures")

      println()
      println("=== Without withSalt: skew stragglers possible ===")
      println("The events fact table has one user_id with " +
        s"$heavyUserEvents events (90%). Without skew handling,")
      println("Spark's default behavior partitions by hash(user_id) " +
        "mod N — one partition gets")
      println("~90% of the data, others get ~10% each. The task " +
        "processing that partition takes")
      println("9x longer than others — a classic straggler pattern.")

      println()
      println("=== Verifying the join produces correct results (no withSalt) ===")
      val noSaltT0 = System.nanoTime()
      // The query aggregates over the whole dataset (no dimensions
      // specified), so it returns exactly 1 row with the total event count.
      val noSaltResult = events
        .join_one(users, (l, r) => l("user_id") === r("user_id"))
        .query(measures = Seq("count"))
        .execute(spark)
        .collect()
      val noSaltT1 = System.nanoTime()
      println(s"  Result: ${noSaltResult.length} row (total event count, aggregated across the join)")
      println(s"  Total events in result: ${noSaltResult.head.getLong(0)}")
      println(s"  Elapsed: ${(noSaltT1 - noSaltT0) / 1e6}ms (no AQE skew handling)")

      println()
      println("=== With withSalt(5): AQE handles skew ===")
      val eventsWithSalt = events.withSalt(5)
      val usersWithSalt = users.withSalt(5)
      val saltT0 = System.nanoTime()
      val saltResult = eventsWithSalt
        .join_one(usersWithSalt, (l, r) => l("user_id") === r("user_id"))
        .query(measures = Seq("count"))
        .execute(spark)
        .collect()
      val saltT1 = System.nanoTime()
      println(s"  Result: ${saltResult.length} row (same correctness)")
      println(s"  Total events in result: ${saltResult.head.getLong(0)}")
      println(s"  Elapsed: ${(saltT1 - saltT0) / 1e6}ms (with AQE skew handling)")
      println()

      // Verify the AQE config was set
      val adaptiveEnabled = spark.conf.get("spark.sql.adaptive.enabled", "false")
      val skewJoinEnabled = spark.conf.get("spark.sql.adaptive.skewJoin.enabled", "false")
      val factor          = spark.conf.get("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
      println(s"  adaptive.enabled=${adaptiveEnabled}              (set by withSalt)")
      println(s"  skewJoin.enabled=${skewJoinEnabled}               (set by withSalt)")
      println(s"  skewJoin.skewedPartitionFactor=${factor}    (set by withSalt)")
      println(s"  Expected: the 900K-row partition gets split into ~5 sub-partitions")
      println(s"  (~180K each), eliminating the straggler task.")

      println()
      println("=== Verifying the join produces the SAME result with/without withSalt ===")
      val noSaltCount = noSaltResult.head.getLong(0)
      val saltCount   = saltResult.head.getLong(0)
      val sameResult = noSaltCount == saltCount
      println(s"  Same total event count: $sameResult")
      println(s"  noSalt: $noSaltCount, salt: $saltCount")
      if (!sameResult) {
        println(s"  ERROR: results differ! Salt handling may have changed the join semantics.")
      }

      println()
      println("Done. Try modifying withSalt(5) to withSalt(2) or withSalt(20) and re-run.")
    } finally {
      spark.stop()
    }
  }
}
