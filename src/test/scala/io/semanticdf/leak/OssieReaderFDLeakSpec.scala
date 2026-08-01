package io.semanticdf.leak

import io.semanticdf.SparkSessionFixture
import io.semanticdf.adapters.OssieReader
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import java.io.File
import java.nio.file.{Files, Path, Paths}
import java.lang.management.ManagementFactory

/** File-descriptor leak test for [[OssieReader.parse]].
  *
  * The data-engineering review of v0.2.0 flagged
  * a real leak: `OssieReader.parse` opens a `BufferedReader` via
  * `Files.newBufferedReader(source)` and passes it to SnakeYAML's
  * `Yaml.load(Reader)`, but **never closes the reader**. SnakeYAML's
  * `Yaml.load(Reader)` does NOT close the passed Reader — this is
  * documented SnakeYAML behavior. The library's existing leak test
  * (`OssieReaderLeakSpec`) only checks heap delta, not FD count, so
  * it missed the FD leak entirely.
  *
  * The fix is a try/finally close in `OssieReader.parse`. This test
  * is the regression guard.
  *
  * How we measure: on Linux, open FDs are listed under
  * `/proc/self/fd/`. We count entries before and after many parse
  * calls. On non-Linux platforms (macOS, Windows), the test is
  * skipped (the JVM and the FD-leak surface are both Linux-specific
  * for this check).
  */
class OssieReaderFDLeakSpec extends AnyFunSuite with SparkSessionFixture {

  // Cross-platform detection: /proc/self/fd only exists on Linux.
  private val isLinux: Boolean =
    new File("/proc/self/fd").isDirectory

  test("OssieReader.parse: doesn't leak file descriptors") {
    if (!isLinux) {
      info("Skipping FD-leak test: /proc/self/fd not available on this OS")
      cancel("FD-leak test is Linux-only")
    }
    val fdDir = new File("/proc/self/fd")
    require(fdDir.isDirectory, "FD-leak test requires /proc/self/fd")

    val path = Paths.get("src/test/resources/ossie-fixtures/medium-ossie.yaml")
    require(Files.exists(path), s"missing test fixture: $path")

    def openFdCount(): Int = fdDir.listFiles().length

    val before = openFdCount()

    // Parse N times. With the leak, FD count grows by ~N (one
    // BufferedReader per call, never closed).
    val n = 100
    for (_ <- 0 until n) {
      val _ = OssieReader.parse(path)
    }

    val after = openFdCount()
    val delta = after - before

    // Allow some JVM noise (e.g. lazy class loading triggered
    // many times). 10 is a generous bound. The fix is "no growth
    // proportional to n"; pre-fix, delta was ~n.
    assert(delta < 10,
      s"FD count grew by $delta after $n parses (expected ~0; " +
        s"this indicates an unclosed Reader leak in OssieReader.parse)")
  }
}
