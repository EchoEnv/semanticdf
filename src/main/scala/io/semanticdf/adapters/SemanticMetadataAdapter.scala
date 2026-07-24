package io.semanticdf.adapters

import io.semanticdf.SemanticTable

import org.apache.spark.sql.{DataFrame, SparkSession}

/** Pluggable adapter for a semantic-metadata interchange format.
  *
  * Each format (dbt's `manifest.json`, Apache Ossie's YAML, future
  * Cube / Looker formats) is one `Source` type and one intermediate
  * `Project` type, with a `parse` step (pure) and a `toSemanticTables`
  * step (Spark-binding). The trait unifies them so consumers can
  * write a single entry point regardless of the source format.
  *
  * The dbt adapter lives in
  * [[io.semanticdf.DbtManifestReader]]; the Ossie adapter lives in
  * [[io.semanticdf.adapters.OssieReader]]. Both implement this trait.
  *
  * == Why a trait, not parallel objects ==
  *
  * Two formats share the same two-phase API:
  *
  *   1. `parse(source) → Project` — pure, no Spark
  *   2. `toSemanticTables(project, spark, resolve) → Map[Name, SemanticTable]`
  *
  * With a trait, callers can write a single `loadSemanticTables(...)`
  * that works for both. Future formats (Cube, Looker, Snowflake)
  * add an instance and inherit the entry point for free.
  *
  * == Design notes ==
  *
  * - `Source` is the format-specific input type (a path, a URI, a
  *   string of YAML — whatever the format requires).
  * - `Project` is the format-specific intermediate that the adapter
  *   produces from `parse`. Different formats have different shapes
  *   (dbt has `DbtProject` with `models` + `sources`; Ossie has
  *   `OssieProject` with `semantic_model` + `ontology_mappings`).
  * - `Seq[Project]` — one source file can contain multiple projects
  *   (Ossie's `semantic_model` is an array; dbt's manifest is one
  *   project but the trait returns `Seq` for uniformity).
  *
  * @tparam Source the format-specific input type
  * @tparam Project the format-specific intermediate produced by `parse` */
trait SemanticMetadataAdapter[Source, Project] {

  /** Phase 1 — pure parse. Reads the source, returns the format-specific
    * intermediate(s). No Spark involvement. */
  def parse(source: Source): Seq[Project]

  /** Phase 2 — bind to Spark. Builds a `Map[Name, SemanticTable]`
    * across all projects, calling `resolve(source)` for each
    * dataset's `source` string to get a `DataFrame`. */
  def toSemanticTables(
      projects:   Seq[Project],
      spark:      SparkSession,
      resolve:    String => DataFrame,
  ): Map[String, SemanticTable]
}

object SemanticMetadataAdapter {

  /** Format-agnostic entry point. Picks the right adapter via the
    * implicit, runs parse + bind, flattens the result.
    *
    * Usage:
    * {{{
    *   import io.semanticdf.adapters.DbtAdapter._
    *   val tables = loadSemanticTables(Paths.get("manifest.json"), spark, resolve)
    * }}} */
  def loadSemanticTables[S, P](
      source:  S,
      spark:   SparkSession,
      resolve: String => DataFrame,
  )(implicit adapter: SemanticMetadataAdapter[S, P]): Map[String, SemanticTable] =
    adapter.toSemanticTables(adapter.parse(source), spark, resolve)
}
