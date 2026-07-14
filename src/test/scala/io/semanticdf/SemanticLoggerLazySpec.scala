package io.semanticdf

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Regression test for the SemanticLogger lazy-string fix.
  *
  * Before the fix, `SemanticLogger.debug(s"expensive ${computation}")` eagerly
  * evaluated the string interpolation even when the log level was INFO (where
  * the message would be discarded). This test proves the bug by constructing
  * a sentinel-throwing message and verifying it's NOT evaluated at INFO level.
  */
class SemanticLoggerLazySpec extends AnyFunSuite with Matchers {

  test("REGRESSION: log messages are NOT evaluated when the level is disabled") {
    // The bug: `SemanticLogger.logX(msg: => String)` is by-name, but
    // `logAtLevel(level: String, msg: String)` takes `msg: String` eagerly.
    // When debug(msg) calls logAtLevel("DEBUG", msg), the by-name thunk
    // is forced to a String before logAtLevel's body runs, regardless of
    // whether the log level is enabled. Fix: logAtLevel must take by-name
    // AND check the level before passing msg to Spark's logDebug.
    //
    // To prove the bug, we wrap the message in a zero-arg thunk that
    // toggles a side-effect flag. By-name semantics require the thunk
    // to be invoked EXACTLY ONCE per call, so the flag flip is observable.
    val logger = org.apache.log4j.Logger.getLogger("io.semanticdf.SemanticLogger")

    @volatile var evaluated = false
    def expensiveMessage(): String = {
      evaluated = true
      "this message was constructed"
    }

    // Ensure DEBUG is disabled (Spark default is INFO — DEBUG is off).
    val originalLevel = logger.getLevel
    logger.setLevel(org.apache.log4j.Level.INFO)
    try {
      evaluated = false
      // Pass the ZERO-ARG THUNK as the argument. Scala wraps `expensiveMessage()`
      // in a by-name thunk so it's only invoked when forced.
      SemanticLogger.debug(expensiveMessage())
      assert(!evaluated,
        "log message was evaluated even though DEBUG is disabled — the " +
        "by-name thunk is being forced prematurely. Fix: make logAtLevel " +
        "take by-name and check the level before calling logDebug(msg).")
    } finally {
      logger.setLevel(originalLevel)
    }
  }

  test("REGRESSION: log messages ARE evaluated when the level is enabled") {
    @volatile var evaluated = false
    def expensiveMessage(): String = {
      evaluated = true
      "this message was constructed"
    }
    val logger = org.apache.log4j.Logger.getLogger("io.semanticdf.SemanticLogger")
    val originalLevel = logger.getLevel
    logger.setLevel(org.apache.log4j.Level.TRACE)
    try {
      evaluated = false
      SemanticLogger.debug(expensiveMessage())
      assert(evaluated, "log message was not evaluated at TRACE level — should be")
    } finally {
      logger.setLevel(originalLevel)
    }
  }
}
