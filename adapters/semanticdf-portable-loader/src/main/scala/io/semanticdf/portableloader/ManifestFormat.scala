package io.semanticdf.portableloader

/** Which manifest format a YAML file uses.
  *
  * Per the v0.3.2 design doc (PR #437): the dual reader (Step 3
  * PR #B) supports BOTH legacy and portable YAML formats side-by-
  * side. This enum identifies which one a given YAML uses.
  *
  * ==Why a sealed trait (vs. a String or Boolean)==
  *
  * Per scala-data-driven-refacer §3 ("default: sealed trait +
  * match"): 2 cases (Legacy, Portable). Sealed trait forces
  * exhaustive handling at the dispatch site. A String ("legacy"
  * vs. "portable") would let typos slip in silently.
  *
  * ==Why in the portable-loader module (not core)==
  *
  * The `ManifestFormat` is part of the dual-reader API. The legacy
  * format itself is owned by `semanticdf-spark` (via `YamlLoader`),
  * but the format-detection logic lives in the portable module
  * because the portable loader is the natural home for "anything
  * that loads manifests."
  *
  * ==Boundary contract==
  *
  * Zero Spark imports. Pure data.
  *
  * Verifiable by:
  * `grep -r 'org.apache.spark' adapters/semanticdf-portable-loader/src/main/scala/io/semanticdf/portableloader/ManifestFormat.scala` */
sealed trait ManifestFormat extends Product with Serializable {
  /** Wire-stable lowercase string. Useful for logs + error messages. */
  def asString: String = this match {
    case ManifestFormat.Legacy   => "legacy"
    case ManifestFormat.Portable => "portable"
  }
}

object ManifestFormat {
  /** The legacy YAML format (Spark-coupled `SemanticTable` shape).
    * Used by the 20 existing example YAML files (under `examples`/). */
  case object Legacy extends ManifestFormat

  /** The portable YAML format (engine-portable `core.Model` shape).
    * The new format introduced in v0.3.2 Step 3 PR #A.2. */
  case object Portable extends ManifestFormat
}
