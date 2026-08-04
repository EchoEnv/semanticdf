package io.semanticdf.perf

import io.semanticdf.adapters.OssieReader

import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Path, Paths}

/** Performance baseline for the [[OssieReader]] parse path.
  *
  * These tests are **observational, not gates**: a slow day in CI
  * doesn't fail the build. Each test publishes its median latency
  * via `info(...)` so the number lands in the surefire-reports and
  * can be tracked over time. A future PR that doubles the parse
  * time shows up as a doubling of the published number — visible
  * in trends, not blocking individual PRs.
  *
  * The first run on a fresh machine establishes the baseline. See
  * `docs/design/perf-baseline.md` for the v0.1.17 numbers.
  *
  * What we measure:
  *   1. Small Ossie file (~2KB, 2 datasets, 7 fields, 2 metrics)
  *   2. Medium Ossie file (~19KB, 7 datasets, ~100 fields, 7 metrics)
  *      — the TPC-DS example from the Ossie repo
  *   3. Large synthetic Ossie file (~1MB, 50 datasets, 1000 fields,
  *      200 metrics) — generated at test time
  *
  * To run locally: `mvn -o test -Dsuites='io.semanticdf.perf.OssieReaderPerfSpec'`
  * Numbers land in `target/surefire-reports/semanticdf-test-suite.txt`
  * with the `[perf]` prefix. */
class OssieReaderPerfSpec extends AnyFunSuite {

  /** Measure the median of N runs of `f`. Drops the first run as a
    * warmup. Returns the median in milliseconds. */
  private def medianMs(f: => Unit, runs: Int = 11): Long = {
    f  // warmup
    val samples = (0 until runs).map { _ =>
      val t0 = System.nanoTime()
      f
      (System.nanoTime() - t0) / 1000000L
    }
    val sorted = samples.sorted
    sorted(sorted.size / 2)
  }

  test("perf: parse small Ossie file (2 datasets, 7 fields)") {
    val path = Paths.get("src/test/resources/ossie-fixtures/minimal-ossie.yaml")
    val median = medianMs { OssieReader.parse(path) }
    info(s"[perf] OssieReader.parse (small, 2KB): median=${median}ms")
  }

  test("perf: parse medium Ossie file (7 datasets, ~100 fields, 7 metrics)") {
    val path = Paths.get("src/test/resources/ossie-fixtures/medium-ossie.yaml")
    val median = medianMs { OssieReader.parse(path) }
    info(s"[perf] OssieReader.parse (medium, 19KB TPC-DS): median=${median}ms")
  }

  test("perf: parse large synthetic Ossie file (50 datasets, 1000 fields, 200 metrics)") {
    val path = generateLargeOssieFile()
    val median = medianMs { OssieReader.parse(path) }
    info(s"[perf] OssieReader.parse (large, ~1MB synthetic): median=${median}ms")
  }

  test("perf: parse + stripTablePrefix on a file with many metrics") {
    // The stripTablePrefix regex runs once per metric. This test
    // tracks the cost on a file with 200 metrics to make any future
    // regression in the regex path visible.
    val path = generateLargeOssieFile()
    val median = medianMs { OssieReader.parse(path) }
    info(s"[perf] OssieReader.parse (large, regex pass over 200 metrics): median=${median}ms")
  }

  /** Generate a synthetic ~1MB Ossie file with the specified shape.
    * The file is written to a temp path and cleaned up by the test
    * runner. The generator is deterministic — same shape on every
    * run, so the published numbers are comparable across runs. */
  private def generateLargeOssieFile(): Path = {
    val datasets = 50
    val fieldsPerDataset = 20
    val metrics = 200
    val sb = new StringBuilder
    sb.append("version: \"0.2.0.dev0\"\n")
    sb.append("semantic_model:\n")
    sb.append("  - name: large_synthetic\n")
    sb.append("    description: Synthetic file for perf testing\n")
    sb.append("    datasets:\n")
    for (d <- 0 until datasets) {
      sb.append(s"      - name: ds_$d\n")
      sb.append(s"        source: schema.ds_$d\n")
      sb.append("        fields:\n")
      for (f <- 0 until fieldsPerDataset) {
        val isTime = f == 0
        sb.append(s"          - name: col_$f\n")
        sb.append("            expression:\n")
        sb.append("              dialects:\n")
        sb.append(s"                - dialect: ANSI_SQL\n                  expression: col_$f\n")
        if (isTime) sb.append("            dimension:\n              is_time: true\n")
      }
    }
    sb.append("    metrics:\n")
    for (m <- 0 until metrics) {
      // Alternate between plain and table-prefixed expressions so the
      // stripTablePrefix regex actually runs.
      val ds = m % datasets
      val expr = if (m % 2 == 0) s"SUM(ds_$ds.col_1)" else s"COUNT(1)"
      sb.append(s"      - name: metric_$m\n")
      sb.append("        expression:\n")
      sb.append("          dialects:\n")
      sb.append(s"            - dialect: ANSI_SQL\n              expression: $expr\n")
    }
    val tmp = Files.createTempFile("ossie-large-", ".yaml")
    Files.writeString(tmp, sb.toString)
    tmp
  }
}
