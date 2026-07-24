package io.semanticdf.adapters

import io.semanticdf.SemanticManifest
import io.semanticdf.SemanticTable

import com.fasterxml.jackson.databind.ObjectMapper

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.nio.file.{Files, Path => NioPath}
import scala.jdk.CollectionConverters._

/** semanticdf `manifest.json` adapter — implements
  * [[SemanticMetadataAdapter]] for the cross-process artifact format
  * produced by [[SemanticManifest.toJson]] /
  * [[SemanticManifest.toJoinedJson]].
  *
  * == Why a separate adapter ==
  *
  * The manifest is the OUTPUT of a build phase (`SemanticTable.toJson`),
  * not user-authored metadata. But from a reader's perspective, the
  * manifest is just a file with a semantic model in it — the same
  * shape as dbt or Ossie. Adding it to the typeclass means a unified
  * `loadSemanticTables(...)` call works for all three sources,
  * including the build/query workflow that uses manifests as the
  * interchange format.
  *
  * == Zero overhead ==
  *
  * The adapter is a thin wrapper. `parse` reads the file once and
  * extracts the kind + source fields (one Jackson `readTree` call —
  * the same cost as `SemanticManifest.fromJson` already pays). The
  * `toSemanticTables` step calls **the existing**
  * [[SemanticManifest.fromJson]] / [[SemanticManifest.fromJoinedJson]]
  * with `df` obtained from the `resolve` callback. No new parsing
  * logic, no new allocation, no behavioral change.
  *
  * == Backward compatibility ==
  *
  * The existing `SemanticManifest.fromJson(text, df)` and
  * `SemanticManifest.fromJoinedJson(text, left, right)` methods are
  * preserved and now carry an `@deprecated` annotation pointing at
  * the typeclass entry point. They will not be removed in 0.1.x.
  *
  * == Usage ==
  *
  * {{{
  *   import io.semanticdf.adapters.SDFAdapter
  *   import io.semanticdf.adapters.SemanticMetadataAdapter.loadSemanticTables
  *
  *   implicit val spark: SparkSession = ...
  *   val tables = loadSemanticTables(Paths.get("manifest.json"), resolve)
  * }}} */
object SDFAdapter extends SemanticMetadataAdapter[NioPath, SDFProject] {

  private val mapper: ObjectMapper = new ObjectMapper()

  /** Phase 1 — pure parse. Reads the file, extracts the `kind` and
    * the source-table name(s) so `toSemanticTables` doesn't re-parse.
    * The full JSON text is preserved verbatim and passed to the
    * existing reader. */
  def parse(source: NioPath): Seq[SDFProject] = {
    val text = Files.readString(source)
    val tree = mapper.readTree(text)
    val obj  = tree.asInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode]
    val kind = obj.path("kind").asText() match {
      case s if s.nonEmpty && s != "null" => s
      case _ => throw new IllegalArgumentException(
        s"manifest at $source missing required `kind` field")
    }
    kind match {
      case "semanticdf-model-manifest" =>
        Seq(SDFProject(
          text   = text,
          kind   = kind,
          source = Some(obj.path("model").path("sourceTable").asText()),
        ))
      case "semanticdf-joined-manifest" =>
        Seq(SDFProject(
          text        = text,
          kind        = kind,
          leftSource  = Some(obj.path("model").path("left").path("model").path("sourceTable").asText()),
          rightSource = Some(obj.path("model").path("right").path("model").path("sourceTable").asText()),
        ))
      case other => throw new IllegalArgumentException(
        s"manifest at $source has unknown kind '$other'. " +
        s"Expected 'semanticdf-model-manifest' or 'semanticdf-joined-manifest'.")
    }
  }

  /** Phase 2 — bind to Spark. Routes to the right reader based on
    * the `kind` field. Calls the existing `fromJson` /
    * `fromJoinedJson` with `df` from the `resolve` callback — no
    * behavior change vs. calling those methods directly. */
  def toSemanticTables(
      projects: Seq[SDFProject],
      resolve:  String => DataFrame,
  )(implicit spark: SparkSession): Map[String, SemanticTable] = {
    projects.map { p =>
      p.kind match {
        case "semanticdf-model-manifest" =>
          val source = p.source.getOrElse(
            throw new IllegalArgumentException(
              s"manifest is missing model.sourceTable — cannot resolve to a DataFrame"))
          SemanticManifest.fromJson(p.text, resolve(source)) -> Some(source)
        case "semanticdf-joined-manifest" =>
          val left  = p.leftSource.getOrElse(throw new IllegalArgumentException(
            s"joined manifest is missing model.left.model.sourceTable"))
          val right = p.rightSource.getOrElse(throw new IllegalArgumentException(
            s"joined manifest is missing model.right.model.sourceTable"))
          SemanticManifest.fromJoinedJson(p.text, resolve(left), resolve(right)) -> Some(s"joined($left,$right)")
        case other =>
          throw new IllegalArgumentException(
            s"unknown manifest kind '$other' — cannot build SemanticTable")
      }
    }.map { case (table, _) =>
      // Stable key for the Map: prefer the table's own name; if it's
      // None (common for joined manifests), fall back to a sentinel.
      // The caller can always look up the table by inspecting the
      // returned SemanticTable's name field directly.
      val key = table.name.getOrElse("<unnamed>")
      key -> table
    }.toMap
  }
}
