package io.semanticdf.portableloader

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** v0.3.2 (Step 3 PR #B.1): tests for [[ManifestFormatDetector]].
  *
  * Per the v0.3.2 design doc (PR #437): the dual reader (Step 3
  * PR #B) auto-detects the format. These tests pin the detection
  * heuristic.
  *
  * Per scala-spark-batch-bugs §1: assert the actual detected format
  * (not just "detection ran"). */
class ManifestFormatDetectorSpec extends AnyFunSuite with Matchers {

  test("detect: portable YAML with source.type: ByName → Portable") {
    val yaml =
      """name: flights
        |source:
        |  type: ByName
        |  table: orders
        |""".stripMargin
    ManifestFormatDetector.detect(yaml) shouldBe ManifestFormat.Portable
  }

  test("detect: portable YAML with source.type: ByPath → Portable") {
    val yaml =
      """name: events
        |source:
        |  type: ByPath
        |  path: /tmp/data.parquet
        |  format: parquet
        |""".stripMargin
    ManifestFormatDetector.detect(yaml) shouldBe ManifestFormat.Portable
  }

  test("detect: portable YAML with source.type: ByProvider → Portable") {
    val yaml =
      """name: orders
        |source:
        |  type: ByProvider
        |  provider: iceberg
        |  identifier: orders
        |""".stripMargin
    ManifestFormatDetector.detect(yaml) shouldBe ManifestFormat.Portable
  }

  test("detect: legacy YAML with top-level wrapper → Legacy") {
    val yaml =
      """flights:
        |    status: published
        |    table: flights_csv
        |    description: "Flight facts"
        |""".stripMargin
    ManifestFormatDetector.detect(yaml) shouldBe ManifestFormat.Legacy
  }

  test("detect: legacy YAML with table: field (no source.type) → Legacy") {
    val yaml =
      """name: my_model
        |table: my_table
        |""".stripMargin
    ManifestFormatDetector.detect(yaml) shouldBe ManifestFormat.Legacy
  }

  test("detect: malformed YAML → Legacy (fallback; actual parse error surfaces from reader)") {
    // Per the detector design: at worst, the detector picks the wrong
    // format; the actual reader surfaces the parse error. So malformed
    // YAML falls back to Legacy (legacy YamlLoader will fail loud).
    val yaml = "this is :: not [valid yaml"
    ManifestFormatDetector.detect(yaml) shouldBe ManifestFormat.Legacy
  }

  test("detect: empty string → Legacy (fallback)") {
    ManifestFormatDetector.detect("") shouldBe ManifestFormat.Legacy
  }

  test("detect: source.type with UNKNOWN value → Legacy (NOT portable)") {
    // If source.type is set but to an unrecognized value, the YAML is
    // neither portable (unknown discriminator) nor a clean legacy format.
    // Fall back to Legacy — the legacy reader will surface the shape error.
    val yaml =
      """name: flights
        |source:
        |  type: BogusSource
        |  foo: bar
        |""".stripMargin
    ManifestFormatDetector.detect(yaml) shouldBe ManifestFormat.Legacy
  }

  test("ManifestFormat.asString: round-trip via wire-stable lowercase strings") {
    ManifestFormat.Legacy.asString   shouldBe "legacy"
    ManifestFormat.Portable.asString shouldBe "portable"
  }
}
