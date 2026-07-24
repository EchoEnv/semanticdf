package com.example.joinedmanifeste2e

import io.semanticdf._
import io.semanticdf.SemanticManifest
import io.semanticdf.adapters.SDFAdapter
import io.semanticdf.adapters.SemanticMetadataAdapter.loadSemanticTables

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/** Phase 2 — Query: load the joined-manifest artifact and run analytics.
  *
  * Represents the runtime app in the artifact workflow:
  *
  *   JSON on disk  →  loadSemanticTables (via SDFAdapter)  →  SemanticTable  →  queries
  *
  * Notice this phase does NOT load any YAML. The artifact carries
  * everything: dims, measures, joins, predicate shape, side metadata.
  * The app only needs the artifact + a resolve function for each
  * source table name.
  *
  * The `loadSemanticTables` entry point is the unified typeclass
  * interface (post-v0.1.16). For just the SDF (manifest) format,
  * `SDFAdapter` is the matching instance. Dbt and Apache Ossie use
  * `DbtAdapter` and `OssieReader` against the same entry point.
  *
  * Run:
  *   mvn -o scala:run -DmainClass=com.example.joinedmanifeste2e.Query
  *
  * Requires: target/clinical_encounters.joined-manifest.json (run Build first).
  */
object Query {

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession.builder()
      .master("local[2]")
      .appName("joined-manifest-e2e-query")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    Logger.info("=== Query: load joined manifest artifact and run analytics ===")

    // Load the artifact from disk. (In a real app this is the model
    // registry / S3 / git checkout — doesn't matter where it came from.)
    val artifact = new java.io.File("target/clinical_encounters.joined-manifest.json")
    if (!artifact.exists()) {
      Logger.error(s"artifact not found at ${artifact.getAbsolutePath}")
      Logger.error("Run `mvn scala:run -DmainClass=com.example.joinedmanifeste2e.Build` first.")
      sys.exit(1)
    }
    val json = scala.io.Source.fromFile(artifact, "UTF-8").mkString
    Logger.info(s"loaded artifact: ${artifact.getAbsolutePath} (${json.length} bytes)")

    // Source-free header — fast pre-flight check (no Spark required).
    val meta = SemanticManifest.parseJoinedMeta(json)
    Logger.info(s"artifact kind=${meta.kind} cardinality=${meta.cardinality} " +
                s"leftKeys=${meta.leftKeys} rightKeys=${meta.rightKeys}")
    Logger.info(s"artifact identity: ${meta.id.getOrElse("(none)")}")
    Logger.info(s"artifact joined predicateAst: ${meta.predicateAst}")

    // Reconstruct the joined model via the typeclass entry point
    // (post-v0.1.16). The `SDFAdapter._` import brings the matching
    // adapter into implicit scope; `loadSemanticTables` picks it up.
    // The resolve function maps each `sourceTable` string from the
    // manifest to a DataFrame — same pattern as dbt / Ossie.
    //
    // The join is asymmetric: encounters.primary_diagnosis (left side)
    // joined to diagnoses.icd_code (right side). Different column
    // names — supported as of v0.1.14.
    import SDFAdapter._
    val tables = loadSemanticTables(artifact.toPath, source => source match {
      case "encounters_clean_csv" => spark.read.option("header", "true").csv("data/encounters_clean.csv")
      case "diagnoses_csv"        => spark.read.option("header", "true").csv("data/diagnoses.csv")
      case other => throw new IllegalArgumentException(
        s"unexpected source in manifest: $other")
    })
    val restored = tables.values.head
    Logger.info(s"restored model joined=${restored.isJoined}, joins=${restored.joins.size}")

    // ── Analytics ─────────────────────────────────────────────────
    // Each query exercises a different surface of the restored joined
    // model. Base dims are addressed by their bare name. Joined-side
    // dims surface as extraDimensions on the wrapped SemanticTransformsOp
    // and can be discovered via the `joins` accessor; the demo focuses
    // on the base side, which is the typical case for a fact model.

    Logger.info("── Q1: encounters per department ──")
    restored
      .query(
        dimensions = Seq("department"),
        measures   = Seq("encounter_count", "avg_los"),
        orderBy    = Seq(SortKey.desc("encounter_count")),
      )
      .toDataFrame(spark).show(false)

    Logger.info("── Q2: encounters per discharge_status ──")
    restored
      .query(
        dimensions = Seq("discharge_status"),
        measures   = Seq("encounter_count", "expired_count", "avg_los"),
        orderBy    = Seq(SortKey.desc("encounter_count")),
      )
      .toDataFrame(spark).show(false)

    Logger.info("── Q3: top departments by total length-of-stay ──")
    restored
      .query(
        dimensions = Seq("department"),
        measures   = Seq("encounter_count", "total_los", "avg_los"),
        orderBy    = Seq(SortKey.desc("total_los")),
        limit      = Some(5),
      )
      .toDataFrame(spark).show(false)

    Logger.info("── Q4: alias-prefixed dims surfaced via the join ──")
    // The join exposes diagnoses.description and diagnoses.category
    // as extraDimensions on the joined side. They live on the
    // SemanticTransformsOp that wraps the base join — discover via
    // the public `joins` accessor.
    val joins = restored.joins
    Logger.info(s"joins.size=${joins.size}")
    joins.headOption.foreach { j =>
      Logger.info(s"join 0 keys=${j.keys} extraDimensions=${j.extraDimensions}")
    }

    Logger.info("=== query complete — joined model executed from JSON artifact ===")
    spark.stop()
  }
}
