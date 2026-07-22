package io.semanticdf

import java.io.File
import io.semanticdf.SemanticJoinOp
import com.fasterxml.jackson.databind.ObjectMapper
import scala.io.Source

/** Audit: does the new `Identity`-path `SemanticManifest.toJson` work
  * for every example model — without touching the YAML files?
  *
  * For each example, we (1) parse the YAML header to learn the expected
  * `table:` view name, (2) register the matching CSV from `data/` as that
  * view, (3) load the YAML into a `SemanticTable`, and (4) emit + reparse
  * the manifest via the new Identity path. If YAML edits were required,
  * this would fail at step 3 or 4. */
class ManifestIdentityAuditSpec extends org.scalatest.funsuite.AnyFunSuite
    with org.scalatest.matchers.should.Matchers {

  private val mapper = new ObjectMapper()

  // (example dir, model yml filename, FQN id, namespace)
  private val cases = Seq(
    ("examples/starter",                 "flights.yml",  "io.semanticdf.examples.starter.flights",        "default"),
    ("examples/starter",                 "carriers.yml", "io.semanticdf.examples.starter.carriers",       "default"),
    ("examples/customer-analytics",      "customers.yml","io.semanticdf.examples.customeranalytics.customers", "default"),
    ("examples/customer-analytics",      "orders.yml",   "io.semanticdf.examples.customeranalytics.orders",   "default"),
    ("examples/hospital",                "patients.yml", "io.semanticdf.examples.hospital.patients",          "default"),
    ("examples/hospital",                "encounters.yml","io.semanticdf.examples.hospital.encounters",        "default"),
    ("examples/operations-analytics",    "orders.yml",   "io.semanticdf.examples.operationsanalytics.orders", "default"),
    ("examples/telco-analytics",         "usage.yml",    "io.semanticdf.examples.telcoanalytics.usage",      "default"),
    ("examples/telco-analytics",         "plans.yml",    "io.semanticdf.examples.telcoanalytics.plans",       "default"),
    ("examples/telco-analytics",         "promotions.yml","io.semanticdf.examples.telcoanalytics.promotions",  "default"),
    ("examples/window-analytics",        "flights.yml",  "io.semanticdf.examples.windowanalytics.flights",    "default"),
  )

  /** Read the YAML's first non-comment, non-blank `table:` line.
    * Comments start with `#`; YAML files in this repo use inline
    * indented `table:` blocks per model. */
  private def readTableFromYml(yml: File): String = {
    val src = Source.fromFile(yml)
    try src.getLines()
        .map(_.trim)
        .find(line => !line.startsWith("#") && line.startsWith("table:"))
        .map(_.stripPrefix("table:").trim)
        .getOrElse(throw new IllegalStateException(s"no table: in ${yml}"))
    finally src.close()
  }

  /** Register all CSVs in `data/` as `_csv` view names. The example
    * YAMLs reference views like `flights_csv`, `encounters_clean_csv`,
    * `patients_raw_csv`. We pick the matching one for the model's
    * `table:` field. */
  private def registerDataViews(spark: org.apache.spark.sql.SparkSession,
                                exDir: String,
                                expectedView: String): Unit = {
    val dataDir = new File(s"$exDir/data")
    require(dataDir.isDirectory, s"missing $exDir/data")
    val csvs = Option(dataDir.listFiles()).getOrElse(Array.empty).filter(_.getName.endsWith(".csv"))
    csvs.foreach { csv =>
      // Map "patients_raw.csv" -> "patients_raw_csv" view (matches YAML).
      val base = csv.getName.stripSuffix(".csv")
      val view = base + "_csv"
      spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(csv.getAbsolutePath)
        .createOrReplaceTempView(view)
    }
    // Sanity check.
    val df = spark.sql(s"SHOW TABLES").collect()
    val names = df.map(_.getString(1)).toSet
    require(names.contains(expectedView),
      s"view `$expectedView` was not registered. Got: $names")
  }

  for ((exDir, yml, id, namespace) <- cases) {
    val label = s"$exDir/$yml"
    test(s"manifest identity-bump works on existing $label without YAML edits") {
      val yamlFile = new File(exDir, s"models/$yml")
      require(yamlFile.isFile, s"missing $yamlFile")
      val tableName = readTableFromYml(yamlFile)

      val spark = org.apache.spark.sql.SparkSession.builder()
        .master("local[1]").appName("identity-audit").getOrCreate()
      spark.sparkContext.setLogLevel("WARN")
      try {
        registerDataViews(spark, exDir, tableName)

        // Load the FULL `models/` directory — some YAMLs cross-reference
        // other models in the same dir (e.g. flights.yml joins carriers).
        // The model we audit is the one whose filename (sans .yml) matches
        // our `yml` parameter; other models are simply loaded into scope
        // so the joins resolve.
        val models = YamlLoader.loadDir(new File(exDir, "models").getAbsolutePath, spark)
        val expectedName = yml.stripSuffix(".yml")
        val model  = models.getOrElse(
          expectedName,
          throw new IllegalStateException(
            s"`$expectedName` not produced by $yamlFile. Got: ${models.keys.mkString(", ")}"
          )
        )
        val identity = SemanticManifest.Identity(
          id              = id,
          manifestVersion = SemanticManifest.InitialManifestVersion,
          namespace       = namespace,
          metadata        = Map(
            "source"   -> "identity-audit",
            "license"  -> "Apache-2.0",
          ),
        )

        // Joined-rooted tables are anti-scope (recipe §10) — verify that
        // `toJson` throws an `IllegalStateException` with a clear message,
        // rather than emitting an incorrect manifest.
        if (model.root match {
          case s: SemanticJoinOp => true; case _ => false
        }) {
          val ex = intercept[IllegalStateException] {
            SemanticManifest.toJson(model, identity, prettyPrint = true)
          }
          assert(ex.getMessage.contains("joined"))
          cancel(s"$label is a joined model — anti-scope (recipe §10), verified separately")
        }


        // Emit.
        val json = SemanticManifest.toJson(model, identity, prettyPrint = true)
        val tree = mapper.readTree(json)

        // Verify identity fields are emitted.
        withClue(s"id field missing or wrong in $label: ") {
          assert(tree.get("id").asText() == id)
        }
        assert(tree.get("namespace").asText() == namespace)
        assert(tree.get("manifestVersion").asText() == SemanticManifest.InitialManifestVersion)
        assert(tree.get("$schema").asText() == SemanticManifest.SchemaUrl)
        val meta = tree.get("metadata")
        assert(meta != null && meta.isObject, "metadata must be an object")
        assert(meta.get("source").asText() == "identity-audit")
        assert(meta.get("license").asText() == "Apache-2.0")

        // Round-trip via parseMeta.
        val pm = SemanticManifest.parseMeta(json)
        assert(pm.id.contains(id),                 s"parseMeta lost id in $label: $pm")
        assert(pm.namespace.contains(namespace),   s"parseMeta lost namespace in $label: $pm")
        assert(pm.manifestVersion.contains(SemanticManifest.InitialManifestVersion))
        assert(pm.metadata.get("source")  == Some("identity-audit"))
        assert(pm.metadata.get("license") == Some("Apache-2.0"))
      } finally spark.stop()
    }
  }
}
