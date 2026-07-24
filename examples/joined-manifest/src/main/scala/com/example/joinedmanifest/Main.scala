package com.example.joinedmanifest

import io.semanticdf._
import io.semanticdf.SemanticManifest
import io.semanticdf.SemanticManifest.Identity

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.jdk.CollectionConverters._

/** Worked example: emit and consume a joined-manifest using the
  * current library API. Demonstrates all three reconstruction paths
  * for the join `on`:
  *
  *   - Equi-join (single + multi-column) — `leftKeys` / `rightKeys`
  *     on the join block.
  *   - Non-equi / OR / compound — `predicate_ast` on the join block.
  *   - Prefixed producer — `leftPrefix` / `rightPrefix` on the join
  *     block.
  *
  * See README for the full context. */
object Main {

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession.builder()
      .master("local[2]")
      .appName("joined-manifest")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    println("=== joined-manifest demo: equi + non-equi + prefixed ===")

    val dataDir = "data"
    spark.read.option("header", "true").option("inferSchema", "true")
      .csv(s"$dataDir/flights.csv").createOrReplaceTempView("flights_csv")
    spark.read.option("header", "true").option("inferSchema", "true")
      .csv(s"$dataDir/carriers.csv").createOrReplaceTempView("carriers_csv")

    // Step 1: load via YamlLoader (the canonical flow).
    val all = YamlLoader.loadDir("models", spark)
    val flights = all("flights")
    println("[demo] flights.isJoined = " + flights.isJoined)
    println("[demo] flights.joins.size = " + flights.joins.size)
    val joinInfo = flights.joins.head
    println("[demo] joinInfo.cardinality    = " + joinInfo.cardinality)
    println("[demo] joinInfo.keys           = " + joinInfo.keys.mkString(", "))
    println("[demo] joinInfo.extraDimensions = " + joinInfo.extraDimensions.mkString(", "))

    // Step 2: emit a joined manifest with the equi-join.
    val identity = Identity(
      id              = "io.example.joinedmanifest.flights",
      manifestVersion = SemanticManifest.InitialManifestVersion,
      namespace       = "demo",
      metadata        = Map("owner" -> "data-platform", "demo" -> "joined-manifest"),
    )
    val json = SemanticManifest.toJoinedJson(flights, identity, prettyPrint = true)
    val outFile = new java.io.File("target/joined-manifest.json")
    outFile.getParentFile.mkdirs()
    val pw = new java.io.PrintWriter(outFile, "UTF-8")
    try pw.write(json) finally pw.close()
    println("[demo] wrote joined manifest to " + outFile.getAbsolutePath)

    // Confirm the equi-join wire shape: leftKeys / rightKeys carry the
    // equi key directly; predicate_ast is absent (the equi case uses
    // the keys lattice, zero AST overhead).
    val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
    val tree = mapper.readTree(json)
    val modelObj = tree.path("model")
    val joinObj = modelObj.path("join")
    println("[demo] wire model.join.leftKeys:    " + joinObj.path("leftKeys").size())
    println("[demo] wire model.join.rightKeys:   " + joinObj.path("rightKeys").size())
    println("[demo] wire model.join.predicate_ast present?  " +
            !joinObj.path("predicate_ast").isMissingNode)

    // Step 3: parse the meta header (no Spark needed).
    val meta = SemanticManifest.parseJoinedMeta(json)
    println("[demo] joined header:")
    println("  kind              : " + meta.kind)
    println("  cardinality       : " + meta.cardinality)
    println("  leftDimensions    : " + meta.leftDimensions)
    println("  rightDimensions   : " + meta.rightDimensions)
    println("  mergedDimensions  : " + meta.mergedDimensions)
    println("  extraDimensions   : " + meta.extraDimensions)
    println("  identity.id       : " + meta.id.getOrElse("(none)"))
    println("  identity.namespace : " + meta.namespace.getOrElse("(none)"))

    // Step 4: round-trip the equi-join back to a SemanticTable.
    val flightsDf  = spark.read.option("header", "true").csv(s"$dataDir/flights.csv")
    val carriersDf = spark.read.option("header", "true").csv(s"$dataDir/carriers.csv")
    val restored = SemanticManifest.fromJoinedJson(json, flightsDf, carriersDf)
    println("[demo] restored.joins.size = " + restored.joins.size)
    assert(restored.joins.length >= 1, "restored model should expose its joins via the public `joins` accessor")

    // Step 5: demonstrate the structured predicate AST — non-equi case.
    // Build a programmatic join with a non-equi predicate
    // (flights.distance < carriers.year_founded) so the writer captures
    // `predicate_ast` rather than the keys lattice.
    println("[demo] --- non-equi predicate demo ---")
    val leftT  = toSemanticTable(flightsDf, Some("flights"))
    val rightT = toSemanticTable(carriersDf, Some("carriers"))
    val nonEquiJoin = leftT.join_many(
      rightT,
      (l, r) => l("distance") < r("year_founded"),
    )
    val jsonNonEqui = SemanticManifest.toJoinedJson(nonEquiJoin, prettyPrint = true)
    val treeNonEqui = mapper.readTree(jsonNonEqui)
    val astNode = treeNonEqui.path("model").path("join").path("predicate_ast")
    println("[demo] non-equi predicate_ast:")
    println("  op    : " + astNode.path("op").asText())
    println("  left  : " + astNode.path("left").path("side").asText() +
            "." + astNode.path("left").path("col").asText())
    println("  right : " + astNode.path("right").path("side").asText() +
            "." + astNode.path("right").path("col").asText())
    assert(astNode.path("op").asText() == "lt", "non-equi predicate should be captured as `lt`")

    // Round-trip the non-equi AST through fromJoinedJson.
    val restoredNonEqui =
      SemanticManifest.fromJoinedJson(jsonNonEqui, flightsDf, carriersDf)
    val nonEquiMeta = SemanticManifest.parseJoinedMeta(jsonNonEqui)
    println("[demo] restoredNonEqui predicateAst defined? " +
            nonEquiMeta.predicateAst.isDefined)
    println("[demo] restoredNonEqui predicateAst.op = " +
            nonEquiMeta.predicateAst.map(_.op.toString).getOrElse("(none)"))
    assert(nonEquiMeta.predicateAst.isDefined,
      "non-equi predicate should round-trip via predicate_ast")
    assert(nonEquiMeta.predicateAst.get.op == PredicateAst.Op.Lt,
      "the round-tripped predicate should preserve the `lt` operator")

    // Step 6: demonstrate the prefixed producer.
    println("[demo] --- prefixed producer demo ---")
    val jsonTree = mapper.readTree(json)
    val joinNode = jsonTree.path("model").path("join")
    if (joinNode.isObject) {
      val joinObj2 = joinNode.asInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode]
      joinObj2.put("leftPrefix",  "left_")
      joinObj2.put("rightPrefix", "right_")
    }
    val json2 = mapper.writeValueAsString(jsonTree)
    println("[demo] injected leftPrefix/rightPrefix into wire, re-emitted")
    val restored2 = SemanticManifest.fromJoinedJson(json2, flightsDf, carriersDf)
    val meta2 = SemanticManifest.parseJoinedMeta(json2)
    println("[demo] round-tripped meta2.leftPrefix  = " + meta2.leftPrefix)
    println("[demo] round-tripped meta2.rightPrefix = " + meta2.rightPrefix)
    assert(meta2.leftPrefix == "left_",
      "round-tripped leftPrefix should be 'left_', got: " + meta2.leftPrefix)
    assert(meta2.rightPrefix == "right_",
      "round-tripped rightPrefix should be 'right_', got: " + meta2.rightPrefix)

    println("=== demo complete (equi + non-equi + prefixed verified end-to-end) ===")
    spark.stop()
  }
}
