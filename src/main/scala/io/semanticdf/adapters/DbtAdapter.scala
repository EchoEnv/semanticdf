package io.semanticdf.adapters

import io.semanticdf.DbtManifestReader
import io.semanticdf.DbtManifestReader.{DbtModel, DbtProject, DbtSource, DbtField}

import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.jdk.CollectionConverters._

/** dbt `manifest.json` adapter — implements [[SemanticMetadataAdapter]]
  * for dbt's v12+ manifest shape.
  *
  * The adapter is a thin wrapper over the existing
  * [[io.semanticdf.DbtManifestReader]]: same parsing logic, same
  * two-phase API, same `DbtProject` intermediate. The wrapper exposes
  * dbt as a `SemanticMetadataAdapter` instance so the unified
  * `loadSemanticTables(...)` entry point works.
  *
  * == Why a wrapper, not a refactor ==
  *
  * The existing `DbtManifestReader` has its own public surface
  * (`read`, `toSemanticTables`, `DbtProject`, `DbtModel`, etc.).
  * Replacing it with a trait-based redesign would be a breaking
  * change for downstream users. The wrapper is the minimum-cost
  * path to unification: dbt users keep their existing API, and the
  * new typeclass entry point picks up dbt via this adapter.
  *
  * == Usage ==
  *
  * {{{
  *   import io.semanticdf.adapters.DbtAdapter._
  *   val tables = loadSemanticTables(Paths.get("manifest.json"), spark, resolve)
  * }}}
  *
  * The `_` import brings `DbtAdapter` into implicit scope. */
object DbtAdapter extends SemanticMetadataAdapter[java.nio.file.Path, DbtProject] {

  /** Phase 1 — pure parse. Wraps [[DbtManifestReader.read]]. */
  def parse(source: java.nio.file.Path): Seq[DbtProject] =
    Seq(DbtManifestReader.read(source))

  /** Phase 2 — bind to Spark. Wraps
    * [[DbtManifestReader.toSemanticTables]]; the project is already
    * parsed, so we just feed it through. */
  def toSemanticTables(
      projects: Seq[DbtProject],
      spark:    SparkSession,
      resolve:  String => DataFrame,
  ): Map[String, io.semanticdf.SemanticTable] = {
    if (projects.isEmpty) Map.empty
    else if (projects.size == 1) DbtManifestReader.toSemanticTables(projects.head, spark, resolve)
    else throw new IllegalArgumentException(
      s"dbt manifest: expected exactly 1 project, got ${projects.size}. " +
      s"Use DbtManifestReader.read + toSemanticTables for multi-manifest merging.")
  }
}
