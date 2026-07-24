package com.example.joinedmanifeste2e

import io.semanticdf._

import org.apache.spark.sql.SparkSession

/** Phase 2 — TypedQuery: same queries as Query.scala, with phantom-typed
  * field references.
  *
  * Side-by-side comparison with [[Query]]:
  *
  *   - Query.scala       uses string names: `.query(dimensions = Seq("department"), ...)`
  *   - TypedQuery.scala  uses typed refs:  `.query(groupByDimensions(department), ...)`
  *
  * Both run against the same JSON artifact (loaded from disk, same
  * `SemanticManifest.fromJoinedJson` call). The only difference is the
  * caller-side API style.
  *
  * The typed ref pattern:
  *   1. Declare one phantom-typed carrier per field name (e.g. `Department`).
  *   2. Register an implicit `SemanticDimension[T]` / `SemanticMeasure[T]`
  *      val in the `Refs` object, naming the underlying YAML field.
  *   3. Import `Refs._` and pass the ref directly to the typed DSL.
  *   4. A typo in a ref name is a compile error, not a runtime error.
  *
  * Run:
  *   mvn -o scala:run -DmainClass=com.example.joinedmanifeste2e.TypedQuery
  *
  * Requires: target/clinical_encounters.joined-manifest.json (run Build first).
  */
object TypedQuery {

  // ── Phantom-typed field references ──────────────────────────────
  // Names declared ONCE here; downstream calls use typed refs. A typo
  // in a ref name is a compile error (the carrier trait won't exist).
  object Refs {
    sealed trait EncounterId
    sealed trait Department
    sealed trait DischargeStatus
    sealed trait PrimaryDiagnosis
    sealed trait EncounterCount
    sealed trait TotalLos
    sealed trait ExpiredCount
    sealed trait AvgLos

    implicit val encounterId:      SemanticDimension[EncounterId]      = SemanticDimension.of[EncounterId]("encounter_id")
    implicit val department:        SemanticDimension[Department]        = SemanticDimension.of[Department]("department")
    implicit val dischargeStatus:   SemanticDimension[DischargeStatus]   = SemanticDimension.of[DischargeStatus]("discharge_status")
    implicit val primaryDiagnosis:  SemanticDimension[PrimaryDiagnosis]  = SemanticDimension.of[PrimaryDiagnosis]("primary_diagnosis")
    implicit val encounterCount:    SemanticMeasure[EncounterCount]    = SemanticMeasure.of[EncounterCount]("encounter_count")
    implicit val totalLos:          SemanticMeasure[TotalLos]          = SemanticMeasure.of[TotalLos]("total_los")
    implicit val expiredCount:      SemanticMeasure[ExpiredCount]      = SemanticMeasure.of[ExpiredCount]("expired_count")
    implicit val avgLos:            SemanticMeasure[AvgLos]            = SemanticMeasure.of[AvgLos]("avg_los")
  }

  def main(args: Array[String]): Unit = {
    import Refs._

    implicit val spark: SparkSession = SparkSession.builder()
      .master("local[2]")
      .appName("joined-manifest-e2e-typed-query")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    Logger.info("=== TypedQuery: load artifact + run typed-DSL queries ===")

    val artifact = new java.io.File("target/clinical_encounters.joined-manifest.json")
    if (!artifact.exists()) {
      Logger.error(s"artifact not found at ${artifact.getAbsolutePath}")
      Logger.error("Run `mvn scala:run -DmainClass=com.example.joinedmanifeste2e.Build` first.")
      sys.exit(1)
    }
    val json = scala.io.Source.fromFile(artifact, "UTF-8").mkString
    Logger.info(s"loaded artifact: ${artifact.getAbsolutePath} (${json.length} bytes)")

    val encountersDf = spark.read.option("header", "true").csv("data/encounters_clean.csv")
    val diagnosesDf  = spark.read.option("header", "true").csv("data/diagnoses.csv")

    val restored = SemanticManifest.fromJoinedJson(
      json,
      encountersDf.as("encounters"),
      diagnosesDf.as("diagnoses"),
    )
    Logger.info(s"restored model joined=${restored.isJoined}, joins=${restored.joins.size}")

    // ── Analytics with typed DSL ────────────────────────────────────
    // Compare to Query.scala: same queries, but each field name appears
    // ONCE (in the Refs object above). A typo in `department` here
    // would fail to compile, not at runtime.

    Logger.info("── T1: encounters per department (typed ref) ──")
    restored
      .groupByDimensions(department)
      .aggregateMeasures(encounterCount, totalLos, avgLos)
      .orderBy(SortKey.desc(encounterCount))
      .execute
      .show(false)

    Logger.info("── T2: encounters per discharge_status (typed ref) ──")
    restored
      .groupByDimensions(dischargeStatus)
      .aggregateMeasures(encounterCount, expiredCount, avgLos)
      .orderBy(SortKey.desc(encounterCount))
      .execute
      .show(false)

    Logger.info("── T3: top departments by total length-of-stay (typed ref + limit) ──")
    restored
      .groupByDimensions(department)
      .aggregateMeasures(encounterCount, totalLos, avgLos)
      .orderBy(SortKey.desc(totalLos))
      .limit(5)
      .execute
      .show(false)

    Logger.info("=== typed query complete — same artifact, different caller style ===")
    spark.stop()
  }
}
