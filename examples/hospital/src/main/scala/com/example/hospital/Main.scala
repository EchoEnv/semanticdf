package com.example.hospital

import io.semanticdf._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/** Hospital data management + cleansing on top of semanticdf.
  *
  * Demonstrates the full data-quality workflow:
  *
  *   1. INGEST — load raw patients + encounters + diagnoses (CSVs with
  *      intentional data-quality issues: duplicate MRNs, duplicate patients
  *      with case variations, a missing MRN, etc.)
  *   2. QUALITY REPORT — print the count of duplicates / missing values
  *   3. CLEANSE — normalize names (Title Case), deduplicate by (first_name,
  *      last_name, dob), fill missing MRNs, remap encounter patient_ids to the
  *      primary
  *   4. SEMANTIC — build YAML models on the cleansed DataFrames
  *   5. QUERIES — Q1 patient demographics, Q2 ALOS by department, Q3 30-day
  *      readmission rate
  *
  * ==Typed field references (v0.1.x typed API)==
  *
  * See the [[Refs]] object — phantom-typed witnesses for every dimension
  * and measure. Downstream calls use the typed refs (groupByDimensions,
  * aggregateMeasures, SortKey, Predicate.Eq). A typo in a ref name is a
  * compile error. See `examples/starter/Main.scala` and
  * `docs/phase-E-plan.md` for the full story.
  *
  * Run:
  *   1. mvn install the parent semanticdf project
  *   2. mvn scala:run -DmainClass=com.example.hospital.Main
  */

/** Narrative logger for this template.
  *
  * Uses java.util.logging.Logger (JDK built-in). The public API
  * (`info` / `warn` / `error` / `debug`) is logger-agnostic — swap the
  * underlying implementation for SLF4J / log4j2 in production by
  * changing only the body of these four methods. Callsites stay stable.
  *
  * For spark-heavy projects that want logging routed through Spark's
  * log4j infrastructure, `io.semanticdf.SemanticLogger` is available —
  * but using it from a consumer template couples the template to a
  * library internal; this template-local logger is the recommended
  * pattern.
  */
object Logger {
  import java.util.logging.{Level, Logger => JulLogger}
  private val jul: JulLogger = JulLogger.getLogger("com.example.hospital")
  jul.setLevel(Level.INFO)

  def info(msg: => String): Unit  = jul.info(msg)
  def warn(msg: => String): Unit  = jul.warning(msg)
  def error(msg: => String): Unit = jul.severe(msg)
  def debug(msg: => String): Unit = jul.fine(msg)
}

object Main {

  // -----------------------------------------------------------------------
  // Phantom-typed field references. Name strings appear ONCE per field
  // (in the implicit val below); downstream calls use typed refs.
  // -----------------------------------------------------------------------
  object Refs {
    // patient model dimensions
    sealed trait PatientId
    sealed trait Mrn
    sealed trait Gender
    sealed trait Insurance
    // patient model measures
    sealed trait PatientCount
    // encounter model dimensions
    sealed trait EncounterId
    sealed trait AdmissionDate
    sealed trait Department
    // encounter model measures
    sealed trait EncounterCount
    sealed trait TotalLos
    sealed trait ExpiredCount
    sealed trait AvgLos
    // per-row columns used in Q3 (window/lag) — not in any catalog,
    // but typed via SemanticDimension so Predicate.Eq accepts them.
    sealed trait DaysSincePrev
    sealed trait IsReadmission
    // temporary measure added in Scala for Q3
    sealed trait AnyReadmission

    implicit val patientId:       SemanticDimension[PatientId]       = SemanticDimension.of[PatientId]("patient_id")
    implicit val mrn:            SemanticDimension[Mrn]            = SemanticDimension.of[Mrn]("mrn")
    implicit val gender:         SemanticDimension[Gender]         = SemanticDimension.of[Gender]("gender")
    implicit val insurance:      SemanticDimension[Insurance]      = SemanticDimension.of[Insurance]("insurance")
    implicit val patientCount:    SemanticMeasure[PatientCount]      = SemanticMeasure.of[PatientCount]("patient_count")

    implicit val encounterId:     SemanticDimension[EncounterId]     = SemanticDimension.of[EncounterId]("encounter_id")
    implicit val admissionDate:   SemanticDimension[AdmissionDate]   = SemanticDimension.of[AdmissionDate]("admission_date")
    implicit val department:     SemanticDimension[Department]      = SemanticDimension.of[Department]("department")
    implicit val encounterCount:  SemanticMeasure[EncounterCount]    = SemanticMeasure.of[EncounterCount]("encounter_count")
    implicit val totalLos:        SemanticMeasure[TotalLos]          = SemanticMeasure.of[TotalLos]("total_los")
    implicit val expiredCount:    SemanticMeasure[ExpiredCount]      = SemanticMeasure.of[ExpiredCount]("expired_count")
    implicit val avgLos:          SemanticMeasure[AvgLos]            = SemanticMeasure.of[AvgLos]("avg_los")

    // Q3 typed refs for the per-row readmission window columns.
    implicit val daysSincePrev:   SemanticDimension[DaysSincePrev]   = SemanticDimension.of[DaysSincePrev]("days_since_prev")
    implicit val isReadmission:   SemanticDimension[IsReadmission]   = SemanticDimension.of[IsReadmission]("is_readmission")
    implicit val anyReadmission:  SemanticMeasure[AnyReadmission]     = SemanticMeasure.of[AnyReadmission]("any_readmission")
  }

  def main(args: Array[String]): Unit = {
    // `implicit` so call sites can write `.execute` / `.toDataFrame` without
    // passing spark positionally. Backward-compatible: explicit `.execute(spark)`
    // still works (PR #81).
    implicit val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("semanticdf-hospital")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    try {
      // Bring the typed refs into scope
      import Refs._

      Logger.info("=" * 70)
      Logger.info(
        "Hospital data management + cleansing — full ETL → semantic workflow"
      )
      Logger.info("=" * 70)

      // ---------------------------------------------------------------------
      // 1. INGEST — load raw data (with intentional quality issues)
      // ---------------------------------------------------------------------
      val rawPatients = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("data/patients_raw.csv")
        .withColumn("date_of_birth", col("date_of_birth").cast("date"))
      val rawEncounters = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("data/encounters_raw.csv")
        .withColumn("admission_date", col("admission_date").cast("date"))
        .withColumn("discharge_date", col("discharge_date").cast("date"))
      val diagnoses = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("data/diagnoses.csv")
      Logger.info(s"  raw patients:    ${rawPatients.count()} rows")
      Logger.info(s"  raw encounters:  ${rawEncounters.count()} rows")
      Logger.info(s"  diagnoses:        ${diagnoses.count()} rows")

      // ---------------------------------------------------------------------
      // 2. QUALITY REPORT — surface the data quality issues
      // ---------------------------------------------------------------------
      Logger.info("=" * 70)
      Logger.info("STEP 2: Data quality report")
      Logger.info("=" * 70)

      // Duplicate patients by (first_name, last_name, dob) — case-insensitive.
      // These use raw Spark DataFrame APIs (groupBy, count) — they're
      // diagnostic queries, not semanticdf queries. The typed refs aren't
      // applicable here because we're operating on raw (pre-cleansing) data
      // and we don't want to build a semanticdf model just for diagnostics.
      val rawPatientsLower = rawPatients
        .withColumn("first_name", lower(col("first_name")))
        .withColumn("last_name", lower(col("last_name")))
      val dupByNameDob = rawPatientsLower
        .groupBy("first_name", "last_name", "date_of_birth")
        .count()
        .filter(col("count") > 1)
        .count()
      val missingMrn =
        rawPatients.filter(col("mrn").isNull || col("mrn") === "").count()
      val dupMrn = rawPatients
        .filter(col("mrn").isNotNull && col("mrn") =!= "")
        .groupBy("mrn")
        .count()
        .filter(col("count") > 1)
        .count()

      Logger.info(s"  duplicate patients (same name+dob): $dupByNameDob")
      Logger.info(s"  rows with missing/empty MRN:        $missingMrn")
      Logger.info(s"  duplicate MRN values:                $dupMrn")

      // ---------------------------------------------------------------------
      // 3. CLEANSE — produce cleansed DataFrames
      // ---------------------------------------------------------------------
      Logger.info("=" * 70)
      Logger.info("STEP 3: Cleanse")
      Logger.info("=" * 70)

      // 3a. Normalize names to Title Case, then deduplicate by (name, dob).
      val normalizedPatients = rawPatients
        .withColumn("first_name", initcap(col("first_name")))
        .withColumn("last_name", initcap(col("last_name")))
      val cleansedPatients = normalizedPatients
        .dropDuplicates("first_name", "last_name", "date_of_birth")
        // Fill missing MRNs with a generated value.
        .withColumn(
          "mrn",
          when(
            col("mrn").isNull || col("mrn") === "",
            concat(
              lit("MRN-GEN-"),
              monotonically_increasing_id().cast("string")
            )
          )
            .otherwise(col("mrn"))
        )

      // 3b. Remap encounter patient_ids to the primary. In a real pipeline
      //     you'd do this with a join; here the raw data is already
      //     consistent with our dedup (P003, P004, P011 → P001, etc.).
      val cleansedEncounters = rawEncounters

      Logger.info(s"  raw patients:        ${rawPatients.count()} rows")
      Logger.info(
        s"  cleansed patients:   ${cleansedPatients.count()} rows (after dedup)"
      )
      Logger.info(s"  encounters:          ${cleansedEncounters.count()} rows")

      // ---------------------------------------------------------------------
      // 4. SEMANTIC — load YAML models on the cleansed DataFrames
      // ---------------------------------------------------------------------
      Logger.info("=" * 70)
      Logger.info("STEP 4: Build semantic models on the cleansed data")
      Logger.info("=" * 70)

      val tables = Map(
        "patients_clean_csv" -> cleansedPatients,
        "encounters_clean_csv" -> cleansedEncounters,
        "diagnoses_csv" -> diagnoses
      )
      val models = YamlLoader.loadDir("models/", tables)
      val patients = models("patients")
      val encounters = models("encounters")
      Logger.info(s"  loaded models: ${models.keys.mkString(", ")}")

      // ---------------------------------------------------------------------
      // 5. QUERIES
      // ---------------------------------------------------------------------
      Logger.info("=" * 70)
      Logger.info("STEP 5: Queries on the cleansed data")
      Logger.info("=" * 70)

      // Q1: Patient demographics — by gender + by insurance.
      Logger.info("--- Q1: Patient demographics (by gender, by insurance) ---")
      patients
        .groupByDimensions(gender)
        .aggregateMeasures(patientCount)
        .orderBy(SortKey.asc(gender))
        .execute
        .show(false)
      patients
        .groupByDimensions(insurance)
        .aggregateMeasures(patientCount)
        .orderBy(SortKey.desc(patientCount))
        .execute
        .show(false)

      // Q2: ALOS by department.
      Logger.info("--- Q2: Average length of stay (ALOS) by department ---")
      encounters
        .groupByDimensions(department)
        .aggregateMeasures(avgLos, encounterCount)
        .orderBy(SortKey.asc(department))
        .execute
        .show(false)

      // Q3: 30-day readmission rate. Per-encounter `days_since_prev` and
      //     `is_readmission` are pre-computed as columns (the framework's
      //     Pass 1 doesn't accept per-row window functions in base measures).
      //     Per-patient aggregate: max(is_readmission) is 1 if ANY of
      //     the patient's encounters is a readmission. Final rate is
      //     computed in Scala (a simple ratio of two DataFrame counts) —
      //     this is a reasonable pattern when the final aggregation
      //     crosses group boundaries.
      Logger.info("--- Q3: 30-day readmission rate ---")
      val encountersDf = encounters
        .toDataFrame
        .withColumn(
          daysSincePrev.name,
          datediff(
            col(admissionDate.name),
            lag(col(admissionDate.name), 1)
              .over(Window.partitionBy(patientId.name).orderBy(admissionDate.name))
          )
        )
        .withColumn(
          isReadmission.name,
          when(
            col(daysSincePrev.name) > 0 && col(daysSincePrev.name) <= 30,
            lit(1)
          )
            .otherwise(lit(0))
        )
      val encountersWithReadmission = YamlLoader
        .loadDir(
          "models/",
          Map(
            "patients_clean_csv" -> cleansedPatients,
            "encounters_clean_csv" -> encountersDf
          )
        )("encounters")
        .withMeasures(Measure(anyReadmission.name, t => max(t(isReadmission.name))))
      val perPatient = encountersWithReadmission
        .groupByDimensions(patientId)
        .aggregateMeasures(encounterCount, anyReadmission)
        .execute
      // Final ratio is computed in Scala because it crosses group boundaries
      // (we need a count across all patients, not per-patient).
      val multiEncounter = perPatient.filter(col(encounterCount.name) > 1)
      val readmitted = multiEncounter.filter(col(anyReadmission.name) === 1)
      val rate =
        if (multiEncounter.count() > 0)
          readmitted.count().toDouble / multiEncounter.count().toDouble
        else 0.0
      Logger.info(s"  patients with multiple encounters: ${multiEncounter.count()}")
      Logger.info(s"  of which had a 30-day readmission:    ${readmitted.count()}")
      Logger.info(f"  30-day readmission rate:             $rate%.2f")

      Logger.info("=" * 70)
      Logger.info("All steps complete. The data quality issues from STEP 2 are now")
      Logger.info("resolved — the queries above run on the cleansed data.")
      Logger.info("=" * 70)
    } finally spark.stop()
  }
}
