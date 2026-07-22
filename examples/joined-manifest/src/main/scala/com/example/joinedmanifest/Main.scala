package com.example.joinedmanifest

import io.semanticdf._
import io.semanticdf.SemanticManifest
import io.semanticdf.SemanticManifest.Identity

import org.apache.spark.sql.SparkSession

/** Worked example: emit and consume a joined-manifest using the v0.1.11
  * library API. See README for context. */
object Main {

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession.builder()
      .master("local[2]")
      .appName("joined-manifest")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    println("=== joined-manifest demo ===")

    val dataDir = "data"
    spark.read.option("header", "true").option("inferSchema", "true")
      .csv(s"$dataDir/flights.csv").createOrReplaceTempView("flights_csv")
    spark.read.option("header", "true").option("inferSchema", "true")
      .csv(s"$dataDir/carriers.csv").createOrReplaceTempView("carriers_csv")

    // Step 1: load YAML, drive a programmatic join.
    // NOTE: starter/flights.yml already joins carriers, so the loaded
    // `flights` model is itself joined. To get a fresh single-table
    // LEFT side, rebuild flights from its source DataFrame + dimensions.
    val all = YamlLoader.loadDir("models", spark)
    val carriers = all("carriers")
    val flightsSource = spark.table("flights_csv")
    val flights = toSemanticTable(
      flightsSource, name = Some("flights-only")
    ).withDimensions(
      Dimension("carrier", _ => org.apache.spark.sql.functions.col("carrier")),
      Dimension("flight_date", _ => org.apache.spark.sql.functions.col("flight_date")),
    )

    val joined = flights.join_one(
      carriers,
      (l, r) => l("carrier") === r("carrier")
    )
    println(s"[demo] joined.isJoined = ${joined.isJoined}")

    // Step 2: emit a joined manifest.
    val identity = Identity(
      id              = "io.example.joinedmanifest.flights",
      manifestVersion = SemanticManifest.InitialManifestVersion,
      namespace       = "demo",
      metadata        = Map("owner" -> "data-platform", "demo" -> "joined-manifest"),
    )
    val json = SemanticManifest.toJoinedJson(joined, identity, prettyPrint = true)
    val outFile = new java.io.File("target/joined-manifest.json")
    outFile.getParentFile.mkdirs()
    val pw = new java.io.PrintWriter(outFile, "UTF-8")
    try pw.write(json) finally pw.close()
    println(s"[demo] wrote joined manifest to ${outFile.getAbsolutePath}")

    // Step 3: parse the meta header (no Spark needed).
    val meta = SemanticManifest.parseJoinedMeta(json)
    println("[demo] joined header:")
    println(s"  kind              : ${meta.kind}")
    println(s"  cardinality       : ${meta.cardinality}")
    println(s"  leftDimensions    : ${meta.leftDimensions}")
    println(s"  rightDimensions   : ${meta.rightDimensions}")
    println(s"  mergedDimensions  : ${meta.mergedDimensions}")
    println(s"  identity.id       : ${meta.id.getOrElse("(none)")}")
    println(s"  identity.namespace : ${meta.namespace.getOrElse("(none)")}")

    // Step 4: round-trip the manifest back into a joined SemanticTable.
    val flightsDf  = spark.read.option("header", "true").csv(s"$dataDir/flights.csv")
    val carriersDf = spark.read.option("header", "true").csv(s"$dataDir/carriers.csv")
    val restored = SemanticManifest.fromJoinedJson(json, flightsDf, carriersDf)
    println(s"[demo] restored.isJoined = ${restored.isJoined}")
    assert(restored.isJoined, "restored model must be joined-rooted")
    assert(restored.joins.length >= 1,
      "restored model should expose its joins via the public `joins` accessor")
    println(s"[demo] restored joins = ${restored.joins.length}")

    println("[demo] (skipping restored join execution; BLOCK §1 prevents this until re-loaded from YAML)")
    println("=== demo complete ===")
    spark.stop()
  }
}
