package com.example.joinedmanifeste2e

import io.semanticdf._
import io.semanticdf.SemanticManifest
import io.semanticdf.SemanticManifest.Identity

import org.apache.spark.sql.SparkSession

/** Phase 1 — Build: emit a joined-manifest artifact to disk.
  *
  * Represents the CI / deploy step in the artifact workflow:
  *
  *   source YAMLs + source DataFrames  →  toJoinedJson  →  JSON on disk
  *
  * The artifact is portable. It carries everything a downstream
  * app needs to reconstruct the joined model: per-side metadata,
  * join keys, predicate shape, dims + measures. No source YAML
  * needed at load time.
  *
  * Run:
  *   mvn -o scala:run -DmainClass=com.example.joinedmanifeste2e.Build
  *
  * Output:
  *   target/clinical_encounters.joined-manifest.json
  */
object Build {

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession.builder()
      .master("local[2]")
      .appName("joined-manifest-e2e-build")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    Logger.info("=== Build: emit joined manifest artifact ===")

    // Register source CSVs as temp views (matches what a real app does
    // after reading from disk / catalog).
    val dataDir = "data"
    spark.read.option("header", "true").option("inferSchema", "true")
      .csv(s"$dataDir/patients_clean.csv").createOrReplaceTempView("patients_clean_csv")
    spark.read.option("header", "true").option("inferSchema", "true")
      .csv(s"$dataDir/encounters_clean.csv").createOrReplaceTempView("encounters_clean_csv")
    spark.read.option("header", "true").option("inferSchema", "true")
      .csv(s"$dataDir/diagnoses.csv").createOrReplaceTempView("diagnoses_csv")

    // Load all YAMLs in the models dir (YAML-side metadata + joins).
    val all = YamlLoader.loadDir("models", spark)
    Logger.info(s"loaded models: ${all.keys.toSeq.sorted.mkString(", ")}")

    val clinical = all("clinical_encounters")
    Logger.info(s"clinical_encounters.isJoined = ${clinical.isJoined}")
    Logger.info(s"clinical_encounters.joins.size = ${clinical.joins.size}")

    // Identify the artifact.
    val identity = Identity(
      id              = "io.example.joined-manifest-e2e.clinical_encounters",
      manifestVersion = SemanticManifest.InitialManifestVersion,
      namespace       = "demo",
      metadata        = Map(
        "owner" -> "data-platform",
        "demo"  -> "joined-manifest-e2e",
      ),
    )

    // Emit the joined manifest.
    val json = SemanticManifest.toJoinedJson(clinical, identity, prettyPrint = true)
    val outFile = new java.io.File("target/clinical_encounters.joined-manifest.json")
    outFile.getParentFile.mkdirs()
    val pw = new java.io.PrintWriter(outFile, "UTF-8")
    try pw.write(json) finally pw.close()
    Logger.info(s"wrote joined manifest: ${outFile.getAbsolutePath} (${json.length} bytes)")

    // Source-free inspect — proves the artifact is self-describing.
    val meta = SemanticManifest.parseJoinedMeta(json)
    Logger.info("joined header (no Spark needed):")
    Logger.info(s"  kind              : ${meta.kind}")
    Logger.info(s"  cardinality       : ${meta.cardinality}")
    Logger.info(s"  leftDimensions    : ${meta.leftDimensions}")
    Logger.info(s"  rightDimensions   : ${meta.rightDimensions}")
    Logger.info(s"  mergedDimensions  : ${meta.mergedDimensions}")
    Logger.info(s"  extraDimensions   : ${meta.extraDimensions}")
    Logger.info(s"  identity.id       : ${meta.id.getOrElse("(none)")}")
    Logger.info(s"  leftKeys          : ${meta.leftKeys}")
    Logger.info(s"  rightKeys         : ${meta.rightKeys}")
    Logger.info(s"  predicateAst      : ${meta.predicateAst}")

    Logger.info("=== build complete — artifact ready for the Query phase ===")
    spark.stop()
  }
}
