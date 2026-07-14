package io.semanticdf

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

/** Lightweight structured logging for semanticdf.
  *
  * Uses Spark's logging infrastructure so that:
  * - In tests: all output goes to the console (ScalaTest captures it).
  * - In a real Spark app: log level and appenders are controlled by the
  *   user's Spark configuration (`log4j.properties` / `log4j2.properties`).
  *
  * Each op class gets its own logger named `io.semanticdf.<OpClass>` so users can
  * enable/disable semanticdf logging independently of Spark's own noise.
  *
  * Log messages are intentionally terse and structured so they can be piped to
  * a log aggregator. Every message carries a `what` field and relevant identifiers.
  *
  * Log levels used:
  *   DEBUG — detailed classification breakdown, routing decisions, op-tree shape
  *   INFO  — milestones (compile start/end, summary of decisions)
  *   WARN  — silent fallbacks or unexpected-but-recovered situations
  *   ERROR — unexpected exceptions (not used for user-facing IllegalArgumentException;
  *           those are documented in the API and raised directly)
  */
private[semanticdf] object SemanticLogger extends Logging {

  /** Emit a log message at the given level via Spark's logging infrastructure.
    *
    * Earlier versions included a fallback branch that used `println` when no
    * `SparkSession` was bound. That branch was unreachable in practice — every
    * test fixture and entry point constructs a session before calling into
    * semanticdf, so `SparkSession.getDefaultSession.isDefined` is always true.
    * If a future test environment runs without a session, restore the fallback
    * here, but it'd be cosmetic: Spark's `Logging` trait handles level routing
    * for us. */
  private def logAtLevel(level: String, msg: => String): Unit = level match {
    case "DEBUG" => if (log.isDebugEnabled) logDebug(msg)
    case "INFO"  => if (log.isInfoEnabled)  logInfo(msg)
    case "WARN"  => if (log.isWarnEnabled)  logWarning(msg)
    case "ERROR" => if (log.isErrorEnabled) logError(msg)
  }

  def debug(msg: => String): Unit  = logAtLevel("DEBUG", msg)
  def info (msg: => String): Unit  = logAtLevel("INFO",  msg)
  def warn (msg: => String): Unit  = logAtLevel("WARN",  msg)
  def error(msg: => String): Unit  = logAtLevel("ERROR", msg)

  // -------------------------------------------------------------------------
  // Structured log helpers — these are the user-facing hooks into explain()
  // -------------------------------------------------------------------------

  /** Emitted once at the top of aggregate compilation. */
  def logAggregateStart(
      keys: Seq[String],
      measures: Seq[String],
      requested: Seq[String],
  ): Unit = info {
    val k = if (keys.isEmpty) "(none — aggregate over all rows)" else keys.mkString(", ")
    s"aggregate(keys=[$k], measures=[${requested.mkString(", ")}])"
  }

  /** Emitted per measure after classification. */
  def logMeasureClassified(name: String, kind: String, deps: Set[String], totals: Set[String]): Unit = debug {
    val d = if (deps.isEmpty) "" else s" [deps: ${deps.mkString(", ")}]"
    val t = if (totals.isEmpty) "" else s" [totals: ${totals.mkString(", ")}]"
    s"  $name: $kind$d$t"
  }

  /** Emitted when calc measures are ordered into topological layers. */
  def logCalcLayers(layers: Seq[Seq[String]]): Unit = debug {
    layers.zipWithIndex.map { case (names, i) =>
      s"  layer ${i + 1}: [${names.mkString(", ")}]"
    }.mkString("calc layers:\n", "\n", "")
  }

  /** Emitted when a percent-of-total cross-join is built. */
  def logTotalsCrossJoin(totals: Set[String]): Unit = info {
    s"building grand totals for [${totals.mkString(", ")}] (percent-of-total)"
  }

  /** Emitted when a filter is applied. */
  def logFilterApplied(
      predicate: String,
      wrapping: String, // "source" = WHERE (pre-agg), "aggregate" = HAVING (post-agg)
  ): Unit = info {
    val clause = if (wrapping == "source") "WHERE" else "HAVING"
    s"filter($predicate) → $clause"
  }

  /** Emitted when a join is compiled. */
  def logJoinCompiled(
      cardinality: String,
      grainCols: Seq[String],
      preAggregated: Boolean,
  ): Unit = info {
    val pre = if (preAggregated) " (pre-aggregated at grain to prevent fan-out)" else ""
    s"join($cardinality) on [$grainCols]$pre"
  }

  /** Emitted when a terminal produces the final DataFrame. */
  def logTerminalOutput(columns: Seq[String], rowCountHint: String): Unit = debug {
    s"produced ${columns.size} columns, $rowCountHint"
  }
}
