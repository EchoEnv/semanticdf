package io.semanticdf.mcp.handlers

import io.semanticdf.mcp.SparkFixture
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

/** End-to-end coverage of the `introspect` tool — exercises the full
  * composition from a real `Introspector.fromFile(...)` Spark call
  * through `parseInventory` / `parseWarnings` and into the
  * `Envelope[Data]` response shape that MCP agents (and the REST
  * transport) consume.
  *
  * Complements `IntrospectSpec` (which tests the pure-function YAML
  * parsers in isolation). This file owns the composition:
  *
  *   read .csv via Spark →
  *     `Introspector.fromFile(...)` →  YAML with `# WARN:` lines
  *     `parseInventory(yaml)`        →  dimensions / measures / skipped
  *     `parseWarnings(yaml)`         →  warnings array
  *
  * The previous library `IntrospectorSpec` tests cover "library emits
  * `# WARN:` lines"; this file covers "MCP `handle()` wires them through
  * to the response in a shape callers can use."
  *
  * Regression target: keep the inventory count and the warnings array
  * agreeing (`inventory.skipped == warnings.length`) so an MCP agent
  * never sees contradictory data. The current implementation routes
  * `inventory.skipped` through `parseWarnings(yaml).length` for that
  * reason. */
class IntrospectEndToEndSpec extends AnyFunSuite with org.scalatest.BeforeAndAfterAll with SparkFixture {

  // Locate the CSV fixture in the test classpath. Using a fixed path lets
  // the test be re-run without copying artefacts to /tmp.
  private val csvPath: String =
    getClass.getResource("/introspect-busy.csv").getPath

  // -------------------------------------------------------------------------
  // (1) End-to-end handle() returns warnings populated + inventory in sync
  // -------------------------------------------------------------------------
  //
  // The fixture CSV has 12 columns: id (string, entity), category (string),
  // and 10 DoubleType measure candidates (m1..m10). With maxMeasures=2,
  // eight measure candidates get dropped — each should surface a `# WARN:`
  // line with reason "max measures limit reached".

  test("handle() returns warnings for every dropped measure candidate (overflow maxMeasures)") {
    val handler = new Introspect(spark)
    val env = handler.handle(
      registry      = null,   // Introspect does NOT use registry (read-only)
      table         = Some("busy"),
      format        = Some("csv"),
      path          = Some(csvPath),
      modelName     = Some("busy"),
      readOptions   = Map("header" -> "true", "inferSchema" -> "true"),
    )

    env.status shouldBe "ok"

    // DEFAULT Config.maxMeasures=8 in `Introspector.fromFile`. With 11
    // numeric candidates (id + m1..m10), 8 are picked (`id` on priority
    // "sum", then 7 doubles on "avg"). The remaining 3 (`m8..m10`) get
    // surface as # WARN: lines.
    val w = env.data.warnings
    w should have size 3
    w.foreach { line =>
      line should include("max measures limit reached")
    }
    w.exists(_.contains("'m8'")) shouldBe true
    w.exists(_.contains("'m9'")) shouldBe true
    w.exists(_.contains("'m10'")) shouldBe true
  }

  test("handle() inventory skips under-count when a column appears in multiple sections (regression target)") {
    // Regression target: an MCP agent that reads both
    // `field_inventory.skipped` and `warnings` must get the same answer.
    //
    // The fixture CSV has `id` (numeric, matches the entity pattern) which
    // appears in BOTH the `measures:` section AND the `joins:` section.
    // The old `parseInventory` formula `total - declared` counted `id` twice
    // (once for its section entry in measures, once for its join
    // placeholder), inflating `declared` past reality and clamping
    // `skipped` to a smaller number. Single-source-of-truth fix:
    // `parseInventory.skipped == parseWarnings(yaml).length`.
    val handler = new Introspect(spark)
    val env = handler.handle(
      registry      = null,
      table         = Some("busy"),
      format        = Some("csv"),
      path          = Some(csvPath),
      modelName     = Some("busy"),
      readOptions   = Map("header" -> "true", "inferSchema" -> "true"),
    )

    val inv = env.data.field_inventory
    // 12-col CSV → 8 measures + 1 dim + 1 join placeholder (the entity `id`).
    inv.dimensions shouldBe 1     // category (id is classified as measure)
    inv.measures   shouldBe 8     // id + m1..m7 (the top 8 by priority)
    // Single source of truth: warnings.length, not "total - declared".
    inv.skipped    shouldBe env.data.warnings.size
    inv.skipped    shouldBe 3      // m8, m9, m10
  }

  test("handle() surface BOTH warning reasons end-to-end") {
    // The fixture CSV only hits "max measures limit reached" — to test the
    // other reason ("no dimension or measure classification") we synthesise
    // a hand-rolled YAML with both lines and run `parseWarnings` through
    // the same MCP helper, demonstrating the helper carries both shapes.
    //
    // (Library-level coverage for the second reason lives in
    // IntrospectorSpec's `emits # WARN: for unclassified fields` test —
    // that path isn't easily reachable via CSV because MapType/array
    // columns don't survive a Spark CSV round-trip.)
    val yamlWithBothWarnings =
      """# Auto-generated by semanticdf introspector.
        |# 10 rows, 5 columns.
        |# WARN: field 'raw_payload' (string) was skipped — no dimension or measure classification
        |# WARN: field 'unmapped_field' (double) was skipped — max measures limit reached
        |
        |synthetic_model:
        |  table: foo
        |""".stripMargin

    val warnings = Introspect.parseWarnings(yamlWithBothWarnings)
    warnings should have size 2
    warnings should contain ("field 'raw_payload' (string) was skipped — no dimension or measure classification")
    warnings should contain ("field 'unmapped_field' (double) was skipped — max measures limit reached")
  }

  // -------------------------------------------------------------------------
  // (2) Inventory single-source-of-truth: skipped == parseWarnings.length
  // -------------------------------------------------------------------------

  test("parseInventory.skipped tracks parseWarnings(yaml).length (entity-double-count safety)") {
    // YAML that an Introspector would emit when a SchemaField is BOTH an
    // entity dim (appears in dimensions: AND in joins:) AND there are real
    // dropped fields. The old `total - declared` formula in parseInventory
    // over-counts `declared` in this case (the entity is counted twice),
    // making `skipped` underreport against the warnings list.
    val yaml =
      """# Auto-generated by semanticdf introspector.
        |# 1000 rows, 5 columns.
        |# WARN: field 'raw_payload' (string) was skipped — no dimension or measure classification
        |
        |orders:
        |  table: <source name>
        |  description: "Auto-generated"
        |
        |  dimensions:
        |    customer_id:
        |      expr: customer_id
        |      is_entity: true
        |    category:
        |      expr: category
        |
        |  measures:
        |    revenue:
        |      expr: "sum(revenue)"
        |
        |  joins:
        |    customer_id_model:
        |      model: <target-model-name>
        |      type: many
        |      left_on: customer_id
        |      right_on: <target-key>
        |""".stripMargin

    val inv = Introspect.parseInventory(yaml)
    val ws  = Introspect.parseWarnings(yaml)

    // The single source of truth: inventory.skipped == warnings.length.
    // 1 warn line above → inv.skipped must be 1.
    ws should have size 1
    inv.dimensions shouldBe 2      // customer_id, category
    inv.measures   shouldBe 1      // revenue
    inv.skipped    shouldBe ws.length
  }
}
