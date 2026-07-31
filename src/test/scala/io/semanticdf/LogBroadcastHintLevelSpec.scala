package io.semanticdf

import org.apache.logging.log4j.{Level, LogManager}
import org.apache.logging.log4j.core.{Logger => CoreLogger, LogEvent, Filter}
import org.apache.logging.log4j.core.appender.AbstractAppender
import org.apache.logging.log4j.core.config.Property
import org.apache.logging.log4j.core.layout.PatternLayout
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable

class LogBroadcastHintLevelSpec extends AnyFunSuite with Matchers {
  test("logBroadcastHint emits at INFO level (post-#299 review promotion)") {
    val coreLogger = LogManager.getLogger("io.semanticdf.SemanticLogger")
      .asInstanceOf[CoreLogger]

    val captured = mutable.Buffer.empty[LogEvent]
    val appender = new AbstractAppender("test-appender", null, PatternLayout.createDefaultLayout(), false, Property.EMPTY_ARRAY) {
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
          s"The post-#299 review promotion to INFO did not take effect.")
      }
      captured.head.getLevel shouldBe Level.INFO
    } finally {
      coreLogger.setLevel(originalLevel)
      coreLogger.removeAppender(appender)
      appender.stop()
    }
  }
}
