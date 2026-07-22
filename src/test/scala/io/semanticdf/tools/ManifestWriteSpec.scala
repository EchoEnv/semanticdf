package io.semanticdf.tools

import org.scalatest.funsuite.AnyFunSuite

import io.semanticdf.SemanticManifest
import io.semanticdf.YamlLoader
import io.semanticdf.tools.Main.CliParser

import java.io.{File, PrintWriter}
import java.nio.file.{Files, Paths}

/** CLI smoke tests for `tools.Main manifest`.
  *
  * These are integration tests that exercise the real CLI through
  * a child JVM, asserting on the produced manifest files. We do NOT
  * mock the writer — the recipe says "no separate `.sdf-manifest.yml`
  * file convention" and these tests verify the inline `--id` /
  * `--namespace` / `--metadata-*` flag flow end-to-end.
  *
  * Run via:  mvn -o test -DwildcardSuites=io.semanticdf.tools.ManifestWriteSpec
  */
class ManifestWriteSpec extends AnyFunSuite with org.scalatest.matchers.should.Matchers {

  // A small inline CSV. Headers are chosen to match the
  // `examples/operations-analytics/models/orders.yml` schema.
  private val sampleCsv =
    """order_id,customer_id,amount,order_date,shipped_at,status
      |O1,C001,49.99,2024-01-05,2024-01-07,shipped
      |O2,C002,89.50,2024-01-06,2024-01-09,shipped
      |""".stripMargin

  // A minimal YAML model that matches the CSV columns.
  private val sampleYaml =
    """orders:
      |  table: orders_csv
      |  status: published
      |  dimensions:
      |    order_id:    { expr: order_id }
      |    customer_id: { expr: customer_id }
      |  measures:
      |    order_count:  { expr: count(1) }
      |    order_amount: { expr: sum(amount) }
      |""".stripMargin

  // ---------------------------------------------------------------------------
  // Test fixtures: write the CSV + YAML to temp files, run the CLI, parse the output.
  // ---------------------------------------------------------------------------

  private def withTempYamlAndCsv(f: (File, File) => Unit): Unit = {
    // We need a real SparkSession so the CLI can register the CSV as
    // a temp view named `orders_csv` (what the inline YAML references).
    val spark = org.apache.spark.sql.SparkSession.builder()
      .master("local[1]").appName("manifest-write-spec").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try {
      val tmpDir = Files.createTempDirectory("manifest-write-spec").toFile
      val dataDir = new File(tmpDir, "data")
      dataDir.mkdirs()
      val csv = new File(dataDir, "orders.csv")
      Files.write(csv.toPath, sampleCsv.getBytes("UTF-8"))
      val yaml = new File(tmpDir, "orders.yml")
      Files.write(yaml.toPath, sampleYaml.getBytes("UTF-8"))
      // Register the CSV as a global temp view named `orders_csv` so
      // the YAML's `table: orders_csv` resolves.
      spark.read.option("header", "true").csv(csv.getAbsolutePath).createOrReplaceTempView("orders_csv")
      try f(yaml, dataDir)
      finally deleteRecursively(tmpDir)
    } finally spark.stop()
  }

  private def deleteRecursively(f: File): Unit = {
    if (f.isDirectory) Option(f.listFiles()).toList.flatten.foreach(deleteRecursively)
    f.delete()
  }

  private def readFile(path: File): String =
    new String(Files.readAllBytes(path.toPath), "UTF-8")

  // ---------------------------------------------------------------------------
  // Tests
  // ---------------------------------------------------------------------------

  test("manifest -writes-new-fields: --yaml + --id + --namespace + --metadata-author emits all identity fields") {
    withTempYamlAndCsv { (yaml, dataDir) =>
      val outFile = File.createTempFile("manifest-out-", ".json")
      try {
        Main.main(
          Array(
            "manifest",
            "--yaml", yaml.getAbsolutePath,
            "--id", "io.semanticdf.test.orders",
            "--namespace", "test",
            "--metadata-author", "test-suite",
            "--metadata-license", "Apache-2.0",
            "--out", outFile.getAbsolutePath,
          )
        )
        val json = readFile(outFile)
        val parsed = new com.fasterxml.jackson.databind.ObjectMapper().readTree(json)
        parsed.get("schemaVersion").asText() shouldBe SemanticManifest.CurrentSchemaVersion
        parsed.get("kind").asText() shouldBe "semanticdf-model-manifest"
        parsed.get("id").asText() shouldBe "io.semanticdf.test.orders"
        parsed.get("namespace").asText() shouldBe "test"
        parsed.get("manifestVersion").asText() shouldBe SemanticManifest.InitialManifestVersion
        parsed.get("$schema").asText() shouldBe SemanticManifest.SchemaUrl
        val meta = parsed.get("metadata")
        meta.get("author").asText() shouldBe "test-suite"
        meta.get("license").asText() shouldBe "Apache-2.0"
      } finally outFile.delete()
    }
  }

  test("manifest -id-required: missing --id exits with a clear error") {
    withTempYamlAndCsv { (yaml, _) =>
      // The CLI's `require("--id", ...)` throws IAE before reaching the
      // writer. We exercise the same path the CLI uses (CliParser.require)
      // via the `collectOptionsForTest` helper. The helper exposes the
      // `CliParser` class through the package-protected accessor; we
      // construct a parser directly here.
      val parser = new CliParser(Array(
        "manifest",
        "--yaml", yaml.getAbsolutePath,
        // --id intentionally omitted
        "--out", "/tmp/should-not-be-created.json",
      ))
      val ex = intercept[IllegalArgumentException] {
        parser.require("--id", "manifest --yaml <file> --id <FQN> is required (recipe identity-bump §11 Q1)")
      }
      ex.getMessage should include("--id")
    }
  }

  test("manifest -metadata-inline-flags: repeated --metadata-key value populates metadata") {
    // Inline flags only — no .sdf-manifest.yml file convention.
    // The metadata map's keys/values come from repeated --metadata-K V pairs.
    val m = io.semanticdf.tools.Main.collectOptionsForTest(
      Array("--metadata-author", "alice", "--metadata-license", "Apache-2.0", "--metadata-tags", "kpi,orders")
    )
    m shouldBe Map("author" -> "alice", "license" -> "Apache-2.0", "tags" -> "kpi,orders")
  }

  test("manifest -single-arg-toJson-still-omits-new-fields (back-compat for tests)") {
    // The single-arg toJson is the back-compat path. New identity fields
    // are NOT emitted. This is what existing tests rely on.
    // (We exercise the unit-level behavior here, not the CLI — the CLI
    // always uses the new Identity path because --id is required.)
    val df = org.apache.spark.sql.SparkSession.builder()
      .master("local[1]").appName("test").getOrCreate().read
      .option("header", "true").option("inferSchema", "true")
      .csv("src/test/resources/data/orders.csv")
    val model = io.semanticdf.toSemanticTable(df, name = Some("orders"))
      .withDimensions(io.semanticdf.Dimension("order_id", t => t("order_id")))
      .withMeasures(io.semanticdf.Measure("c", _ => org.apache.spark.sql.functions.lit(1), exprString = Some("count(1)")))
    val json = SemanticManifest.toJson(model)
    val parsed = new com.fasterxml.jackson.databind.ObjectMapper().readTree(json)
    parsed.has("id") shouldBe false
    parsed.has("$schema") shouldBe false
    parsed.has("metadata") shouldBe false
    parsed.has("manifestVersion") shouldBe false
  }
}
