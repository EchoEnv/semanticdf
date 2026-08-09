package io.semanticdf.portableloader

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory

/** Detects whether a YAML manifest uses the legacy or portable format.
  *
  * Per the v0.3.2 design doc (PR #437): the dual reader (Step 3
  * PR #B) auto-detects the format. This is the detector.
  *
  * ==Detection heuristic==
  *
  * Portable YAML uses `source.type: <ByName|ByPath|ByProvider>`
  * discriminator. Legacy YAML uses `table: <tableName>` (a string).
  *
  * Detection rules (in priority order):
  *   1. If the YAML has `source.type: <one of {ByName, ByPath, ByProvider}>`
  *      → **Portable**
  *   2. Otherwise (no `source.type` discriminator, or legacy `table:` field)
  *      → **Legacy**
  *
  * The "source.type" discriminator is the most reliable signal because
  * it's an EXPLICIT schema choice (the portable format requires it).
  * Legacy YAML never has `source.type: ByName|ByPath|ByProvider` because
  * it uses `table: <name>` instead.
  *
  * ==Why not detect from the wrapper key==
  *
  * Legacy YAML wraps the model in a top-level key (e.g., `flights:`).
  * Portable YAML does not (the file IS the model).
  *
  * But this signal is ambiguous: a portable YAML could ALSO have a
  * top-level wrapper key (e.g., `model:` or `manifest:`). The
  * `source.type` discriminator is unambiguous.
  *
  * ==Standard compliance==
  *
  * Per scala-error-handling: returns `ManifestFormat` (not `Option`
  * or `Either`). The detector cannot FAIL — at worst, it returns
  * the wrong format, and the actual reader (next step) will surface
  * the parse error.
  *
  * ==Boundary contract==
  *
  * Zero Spark imports. Verifiable by:
  * `grep -r 'org.apache.spark' adapters/semanticdf-portable-loader/src/main/scala/io/semanticdf/portableloader/ManifestFormatDetector.scala` */
object ManifestFormatDetector {

  /** The Jackson YAML ObjectMapper for parsing (used only to walk the
    * top-level structure; not for full deserialization). */
  private val parser: ObjectMapper = new ObjectMapper(new YAMLFactory())

  /** The set of valid portable source.type discriminators. */
  private val PortableSourceTypes: Set[String] =
    Set("ByName", "ByPath", "ByProvider")

  /** Detect the manifest format from a YAML string. */
  def detect(yaml: String): ManifestFormat = {
    try {
      val root = parser.readTree(yaml)
      // Portable YAML has `source.type: <ByName|ByPath|ByProvider>`
      val sourceNode = root.get("source")
      if (sourceNode != null && !sourceNode.isNull) {
        val typeNode = sourceNode.get("type")
        if (typeNode != null && !typeNode.isNull && typeNode.isTextual) {
          val typeValue = typeNode.asText()
          if (PortableSourceTypes.contains(typeValue)) {
            return ManifestFormat.Portable
          }
        }
      }
    } catch {
      // YAML parse failure → fall through to Legacy (the legacy YamlLoader
      // will surface the actual parse error).
      case _: com.fasterxml.jackson.core.JsonProcessingException => ()
    }
    ManifestFormat.Legacy
  }
}
