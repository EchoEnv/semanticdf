package io.semanticdf

import org.apache.logging.log4j.{Level, LogManager}
import org.apache.logging.log4j.core.{Logger => CoreLogger, LogEvent}
import org.apache.logging.log4j.core.appender.AbstractAppender
import org.apache.logging.log4j.core.config.Property
import org.apache.logging.log4j.core.layout.PatternLayout
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable

/** Pin the log level of `SemanticLogger.logBroadcastHint` at INFO.
  *
  * The implementation originally placed the broadcast-hint log at DEBUG. The
  * A review (convergent LOW finding) flagged this as too quiet
  * for a user-driven tuning event — the user explicitly opted
  * into the threshold via `withBroadcastJoinThreshold(n)`, so when
  * the hint fires they want to see it. Promoted to INFO.
  *
  * The tests attach a custom `AbstractAppender` to the underlying
  * `io.semanticdf.SemanticLogger` log4j2 core logger and assert the
  * captured event's level (or absence). Two complementary tests:
  *
  *  1. **Positive** (INFO level): the event fires at INFO; the
  *     captured event's level is `Level.INFO`. Pre-fix, the event
  *     would be filtered out at INFO threshold (because the level
  *     was DEBUG) and the captured list would be empty.
  *  2. **Negative** (WARN level): the event does NOT fire at WARN;
  *     the captured list is empty. Catches any future
  *     over-emission (e.g., a code change that demotes the
  *     broadcast hint to DEBUG for the wrong reason).
  */
class LogBroadcastHintLevelSpec extends AnyFunSuite with Matchers {
  test("logBroadcastHint emits at INFO level (recent audit cycle promotion)") {
    val coreLogger = LogManager.getLogger("io.semanticdf.SemanticLogger")
      .asInstanceOf[CoreLogger]

    val captured = mutable.Buffer.empty[LogEvent]
    val appender = new AbstractAppender("test-appender-info", null, PatternLayout.createDefaultLayout(), false, Property.EMPTY_ARRAY) {
      override def append(event: LogEvent): Unit = captured.synchronized { captured += event.toImmutable }
    }
    appender.start()
    coreLogger.addAppender(appender)
    val originalLevel = coreLogger.getLevel
    coreLogger.setLevel(Level.INFO)
    try {
      SemanticLogger.logBroadcastHint(
        threshold   = 1024L * 1024L,
        actualSize  = 512L,
        cardinality = "one",
      )
      if (captured.isEmpty) {
        fail(s"logBroadcastHint emitted no event at INFO threshold. " +
          s"logger level=${coreLogger.getLevel}. " +
          s"The recent audit cycle promotion to INFO did not take effect.")
      }
      captured.head.getLevel shouldBe Level.INFO
    } finally {
      coreLogger.setLevel(originalLevel)
      coreLogger.removeAppender(appender)
      appender.stop()
    }
  }

  test("logBroadcastHint is suppressed at WARN (negative case)") {
    val coreLogger = LogManager.getLogger("io.semanticdf.SemanticLogger")
      .asInstanceOf[CoreLogger]

    val captured = mutable.Buffer.empty[LogEvent]
    val appender = new AbstractAppender("test-appender-warn", null, PatternLayout.createDefaultLayout(), false, Property.EMPTY_ARRAY) {
      override def append(event: LogEvent): Unit = captured.synchronized { captured += event.toImmutable }
    }
    appender.start()
    coreLogger.addAppender(appender)
    val originalLevel = coreLogger.getLevel
    coreLogger.setLevel(Level.WARN)
    try {
      SemanticLogger.logBroadcastHint(
        threshold   = 1024L * 1024L,
        actualSize  = 512L,
        cardinality = "one",
      )
      assert(captured.isEmpty,
        s"logBroadcastHint emitted ${captured.size} event(s) at WARN threshold — " +
        s"the promotion to INFO is over-emitting. " +
        s"Expected: no event at WARN. Got: ${captured.map(_.getLevel).mkString(", ")}")
    } finally {
      coreLogger.setLevel(originalLevel)
      coreLogger.removeAppender(appender)
      appender.stop()
    }
  }
}
