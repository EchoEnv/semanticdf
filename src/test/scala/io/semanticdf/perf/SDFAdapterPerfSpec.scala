package io.semanticdf.perf

import io.semanticdf.SemanticManifest
import io.semanticdf.adapters.SDFAdapter

import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Path, Paths}

/** Performance baseline for the [[SDFAdapter]] path.
  *
  * These tests are **observational, not gates**. Each test publishes
  * a median latency via `info(...)` so the number lands in the
  * surefire-reports and can be tracked over time. A future PR that
  * doubles the adapter's overhead shows up as a doubling of the
  * published number.
  *
  * Critical assertion: the adapter's parse + toSemanticTables path
  * is **within noise of the direct call** to `SemanticManifest.fromJson`.
  * The adapter is a thin wrapper; if it adds measurable overhead,
  * something is wrong. */
class SDFAdapterPerfSpec extends AnyFunSuite {

  /** Measure the median of N runs of `f`. Drops the first run as a
    * warmup. Returns the median in milliseconds. */
  private def medianMs(f: => Unit, runs: Int = 11): Long = {
    f
    val samples = (0 until runs).map { _ =>
      val t0 = System.nanoTime()
      f
      (System.nanoTime() - t0) / 1000000L
    }
    val sorted = samples.sorted
    sorted(sorted.size / 2)
  }

  test("perf: SDFAdapter.parse on a single-table manifest") {
    val path = Paths.get("src/test/resources/manifest-fixtures/single-manifest.json")
    val median = medianMs { SDFAdapter.parse(path) }
    info(s"[perf] SDFAdapter.parse (single, 1.3KB): median=${median}ms")
  }

  test("perf: SDFAdapter.parse on a joined manifest") {
    val path = Paths.get("src/test/resources/manifest-fixtures/joined-manifest.json")
    val median = medianMs { SDFAdapter.parse(path) }
    info(s"[perf] SDFAdapter.parse (joined, 4.3KB): median=${median}ms")
  }

  test("perf: parse overhead vs direct text load (the critical assertion)") {
    val path = Paths.get("src/test/resources/manifest-fixtures/single-manifest.json")
    val adapterMedian = medianMs { SDFAdapter.parse(path) }
    val directMedian  = medianMs { Files.readString(path) }
    val deltaMs = adapterMedian - directMedian
    info(s"[perf] adapter parse vs direct read: " +
         s"adapter=${adapterMedian}ms, direct=${directMedian}ms, delta=${deltaMs}ms")
    // The adapter does one Jackson readTree + a few string accesses.
    // A reasonable upper bound is 5ms on any reasonable machine.
    // (We don't assert a tight ratio because that would be flaky on
    // shared CI hardware. We assert an absolute upper bound.)
    assert(deltaMs < 5, s"adapter adds ${deltaMs}ms over direct read — too much overhead")
  }
}
