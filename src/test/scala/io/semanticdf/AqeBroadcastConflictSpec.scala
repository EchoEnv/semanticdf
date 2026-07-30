package io.semanticdf

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{count, lit}

import org.apache.log4j.{AppenderSkeleton, Level, Logger}
import org.apache.log4j.spi.LoggingEvent
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable

/** Tests for the AQE-vs-broadcast-threshold conflict warning.
  *
  * `withBroadcastJoinThreshold(n)` emits a plan-time `broadcast(right)`
  * hint when the right side is < `n` bytes. AQE may re-plan that
  * decision at runtime based on actual shuffle stats. The library
  * logs a WARN when AQE is on and the user's threshold risks being
  * overridden.
  *
  * The warning fires once per `compileEquiJoin` call (each join
  * independently). Tests use a custom `AppenderSkeleton` to capture
  * log events on the `io.semanticdf.SemanticLogger` logger.
  */
class AqeBroadcastConflictSpec
    extends AnyFunSuite
    with Matchers
    with SparkSessionFixture {

  /** Custom log4j appender that captures events for inspection. */
  private class CapturingAppender extends AppenderSkeleton {
    val events: mutable.Buffer[LoggingEvent] = mutable.Buffer.empty
    override def append(event: LoggingEvent): Unit = {
      events.synchronized { events += event }
    }
    override def close(): Unit = ()
    override def requiresLayout(): Boolean = false
    def clear(): Unit = events.synchronized { events.clear() }
  }

  /** Capture WARN-level events emitted during `body`. */
  private def withCapturedWarns(body: => Unit): Seq[String] = {
    val logger = Logger.getLogger("io.semanticdf.SemanticLogger")
    val appender = new CapturingAppender
    val originalLevel = logger.getLevel
    appender.setThreshold(Level.WARN)
    logger.setLevel(Level.WARN)
    logger.addAppender(appender)
    try {
      body
      appender.events.map(_.getRenderedMessage).toSeq
    } finally {
      logger.removeAppender(appender)
      logger.setLevel(originalLevel)
    }
  }

  /** Standard tiny join shape: two `spark.range(10)` DataFrames with
    * a `k` dimension. The right side has stats available
    * (`sizeInBytes` < 1 KiB), so the broadcast hint fires. */
  private def buildJoin(
      spark: SparkSession,
      threshold: Long,
  ): SemanticTable = {
    val fact = toSemanticTable(spark.range(10).toDF("k"), name = Some("fact"))
      .withDimensions(Dimension("k", t => t("k")))
      .withMeasures(Measure("n", t => count(lit(1))))
    val dim = toSemanticTable(spark.range(10).toDF("k"), name = Some("dim"))
      .withDimensions(Dimension("k", t => t("k")))

    val joined = fact.withBroadcastJoinThreshold(threshold)
      .join_one(dim, (l, r) => l("k") === r("k"))

    // Force execution so compileEquiJoin runs
    val _ = joined.execute(spark).collect()
    joined
  }

  // ----------------------------------------------------------------
  // No conflict: AQE disabled
  // ----------------------------------------------------------------

  test("no warning when AQE is disabled (user threshold is the only arbiter)") {
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    spark.conf.unset("spark.sql.adaptive.autoBroadcastJoinThreshold")

    val warns = withCapturedWarns {
      buildJoin(spark, threshold = 1024L * 1024L)
    }
    // No AQE conflict warning — debug join log may still fire but at
    // DEBUG level (filtered out by the WARN threshold).
    assert(warns.filter(_.contains("AQE")).isEmpty,
      s"expected no AQE conflict warning with AQE off; got: $warns")
  }

  // ----------------------------------------------------------------
  // No conflict: user threshold <= AQE threshold
  // ----------------------------------------------------------------

  test("no warning when user threshold is BELOW AQE threshold (user broadcast stricter)") {
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", (10L * 1024 * 1024).toString)  // 10 MiB

    val warns = withCapturedWarns {
      buildJoin(spark, threshold = 1024L)  // 1 KiB << 10 MiB
    }
    assert(warns.filter(_.contains("AQE")).isEmpty,
      s"expected no AQE warning when user threshold is lower; got: $warns")
  }

  test("no warning when user threshold EQUALS AQE threshold") {
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    val tenMb = 10L * 1024 * 1024
    spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", tenMb.toString)

    val warns = withCapturedWarns {
      buildJoin(spark, threshold = tenMb)
    }
    assert(warns.filter(_.contains("AQE")).isEmpty,
      s"expected no AQE warning at equal thresholds; got: $warns")
  }

  // ----------------------------------------------------------------
  // Warning: user threshold > AQE threshold
  // ----------------------------------------------------------------

  test("warns when user threshold EXCEEDS AQE threshold") {
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    val oneMb = 1024L * 1024
    spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", oneMb.toString)  // 1 MiB

    val warns = withCapturedWarns {
      buildJoin(spark, threshold = 50L * 1024 * 1024)  // 50 MiB
    }

    val aqeWarns = warns.filter(_.contains("AQE"))
    aqeWarns should have size 1
    aqeWarns.head should include("withBroadcastJoinThreshold=52428800B")
    aqeWarns.head should include("adaptive.autoBroadcastJoinThreshold=1048576B")
    aqeWarns.head should include("AQE may re-plan")
  }

  // ----------------------------------------------------------------
  // Warning: AQE has broadcast disabled (-1)
  // ----------------------------------------------------------------

  test("warns when AQE has broadcast disabled (user threshold may be ignored at runtime)") {
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", "-1")

    val warns = withCapturedWarns {
      buildJoin(spark, threshold = 1024L * 1024L)
    }

    val aqeWarns = warns.filter(_.contains("AQE"))
    aqeWarns should have size 1
    aqeWarns.head should include("AQE has broadcast disabled")
    aqeWarns.head should include("adaptive.autoBroadcastJoinThreshold=-1")
  }

  // ----------------------------------------------------------------
  // Default fallback: AQE key unset → use non-adaptive default
  // ----------------------------------------------------------------

  test("falls back to non-adaptive autoBroadcastJoinThreshold when AQE key unset") {
    // AQE on, but AQE-specific key NOT set — should fall back to
    // spark.sql.autoBroadcastJoinThreshold (default 10 MiB).
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.unset("spark.sql.adaptive.autoBroadcastJoinThreshold")
    // The non-adaptive key is also at default (10 MiB), so a user
    // threshold of 50 MiB should warn (50 > 10).
    val warns = withCapturedWarns {
      buildJoin(spark, threshold = 50L * 1024 * 1024)
    }
    val aqeWarns = warns.filter(_.contains("AQE"))
    aqeWarns should have size 1
    aqeWarns.head should include("adaptive.autoBroadcastJoinThreshold=10485760B")
  }

  // ----------------------------------------------------------------
  // (No test for "AQE key unparseable" — unreachable via the public
  // API: `RuntimeConfig.set` validates bytesConf values at set time,
  // so `getOption` can only return a valid bytesConf string. The
  // `Try(...).toOption` defensive code in `logAqeBroadcastConflict`
  // remains as a robustness net for direct SQLConf mutation.)
  // ----------------------------------------------------------------

  // ----------------------------------------------------------------
  // Test fixture safety: reset conf after each test
  // ----------------------------------------------------------------

  // The SparkSessionFixture gives us a session per test. We unset
  // the AQE keys at the start of each test (above) so order doesn't
  // matter. Final cleanup is implicit in the per-test fixture.
}
