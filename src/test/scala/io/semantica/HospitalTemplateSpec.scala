package io.semantica

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Smoke test for the `examples/hospital/` consumer template.
  *
  * Loads the template's data + YAML, exercises the cleansing workflow,
  * and verifies the 3 queries produce sensible results.
  */
class HospitalTemplateSpec extends AnyFunSuite with Matchers with SparkSessionFixture {

  /** Resolve a path under `<project_root>/examples/...` from the working dir. */
  private def examplesPath(parts: String*): String = {
    val cwd = System.getProperty("user.dir")
    val candidates = Seq(cwd, new java.io.File(cwd).getParent).distinct
    candidates.iterator.map { c =>
      val p = new java.io.File(c, "examples/" + parts.mkString("/"))
      if (p.exists) p.getAbsolutePath else null
    }.find(_ != null).getOrElse {
      fail(s"Could not locate examples/${parts.mkString("/")} from cwd=$cwd")
      ""
    }
  }

  private def loadCleansedData() = {
    val dir = examplesPath("hospital")
    val cleansedPatients = spark.read.option("header", "true").option("inferSchema", "true")
      .csv(s"$dir/data/patients_clean.csv")
      .withColumn("date_of_birth", col("date_of_birth").cast("date"))
    val cleansedEncounters = spark.read.option("header", "true").option("inferSchema", "true")
      .csv(s"$dir/data/encounters_clean.csv")
      .withColumn("admission_date",  col("admission_date").cast("date"))
      .withColumn("discharge_date", col("discharge_date").cast("date"))
    val diagnoses = spark.read.option("header", "true").option("inferSchema", "true")
      .csv(s"$dir/data/diagnoses.csv")
    val tables = Map(
      "patients_clean_csv"   -> cleansedPatients,
      "encounters_clean_csv"  -> cleansedEncounters,
      "diagnoses_csv"         -> diagnoses,
    )
    val models = YamlLoader.loadDir(s"$dir/models/", tables)
    (models("patients"), models("encounters"))
  }

  test("hospital: Q1 patient demographics produces non-empty results") {
    val (patients, _) = loadCleansedData()
    val byGender = patients.groupBy("gender")
      .aggregate("patient_count")
      .execute(spark).collect()
    assert(byGender.length > 0, "demographics query should produce rows")
    val byInsurance = patients.groupBy("insurance")
      .aggregate("patient_count")
      .execute(spark).collect()
    assert(byInsurance.length > 0, "by-insurance query should produce rows")
    val totalPatients = byGender.map(_.getAs[Long]("patient_count")).sum
    assert(totalPatients >= 1, s"expected ≥1 cleansed patient, got $totalPatients")
  }

  test("hospital: Q2 ALOS by department produces non-empty results") {
    val (_, encounters) = loadCleansedData()
    val rows = encounters.groupBy("department")
      .aggregate("avg_los", "encounter_count")
      .execute(spark).collect()
    assert(rows.length > 0, "ALOS query should produce at least one row")
    val avgLoses = rows.map(_.getAs[Number]("avg_los").doubleValue())
    assert(avgLoses.forall(_ > 0), s"all ALOS values must be > 0, got $avgLoses")
  }

  test("hospital: Q3 30-day readmission rate executes and returns valid output") {
    val (patients, encounters) = loadCleansedData()
    // Per-row window functions (lag, datediff) can't be semantica base
    // measures — the framework's Pass 1 only accepts aggregate functions.
    // Pre-compute the per-row columns on a raw DataFrame, then reload
    // the YAML model with the augmented DataFrame.
    val encountersDf = encounters.toDataFrame(spark)
      .withColumn("days_since_prev",
        datediff(col("admission_date"),
          lag(col("admission_date"), 1).over(
            Window.partitionBy("patient_id").orderBy("admission_date"))))
      .withColumn("is_readmission",
        when(col("days_since_prev") > 0 && col("days_since_prev") <= 30, lit(1))
          .otherwise(lit(0)))
    // loadDir reads both patients.yml and encounters.yml, so we need
    // both tables in the map (even though we only use encounters here).
    val patientsDf = patients.toDataFrame(spark)
    val reloaded = YamlLoader.loadDir(
      examplesPath("hospital", "models"),
      Map(
        "patients_clean_csv"  -> patientsDf,
        "encounters_clean_csv" -> encountersDf,
      )
    )("encounters")
    val perPatient = reloaded
      .withMeasures(Measure("any_readmission", t => max(t("is_readmission"))))
      .groupBy("patient_id")
      .aggregate("encounter_count", "any_readmission")
      .toDataFrame(spark)
    val rows = perPatient.collect()
    assert(rows.length > 0, "per-patient aggregation should produce at least one row")
  }
}
