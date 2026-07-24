package com.example.joinedmanifeste2e

/** Template-local logger for the joined-manifest-e2e example.
  *
  * Uses `java.util.logging.Logger` (JDK built-in). The public API
  * (`info` / `warn` / `error` / `debug`) is logger-agnostic — swap
  * the underlying implementation for SLF4J / log4j2 in production
  * by changing only the body of these four methods. Call sites stay
  * stable. This pattern matches every other example template (see
  * e.g. `examples/hospital/src/main/scala/com/example/hospital/Main.scala`).
  *
  * Note: JUL's default handler does not always route to stdout under
  * `mvn scala:run`. We attach an explicit ConsoleHandler so output is
  * visible in the build log.
  */
private[joinedmanifeste2e] object Logger {
  import java.util.logging.{ConsoleHandler, Level, Logger => JulLogger, SimpleFormatter}

  private val underlying: JulLogger = {
    val log = JulLogger.getLogger("com.example.joinedmanifeste2e")
    log.setUseParentHandlers(false)
    log.setLevel(Level.ALL)
    val ch = new ConsoleHandler()
    ch.setLevel(Level.ALL)
    ch.setFormatter(new SimpleFormatter())
    log.addHandler(ch)
    log
  }

  def info(msg: String): Unit  = underlying.info(msg)
  def warn(msg: String): Unit  = underlying.warning(msg)
  def error(msg: String): Unit = underlying.severe(msg)
  def debug(msg: String): Unit = underlying.fine(msg)
}
