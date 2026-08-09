package io.semanticdf.spark

import java.nio.file.{Files, Path}

import scala.jdk.CollectionConverters._

import io.semanticdf.ModelBridge
import io.semanticdf.adapters.YamlLoader
import io.semanticdf.core.model.{Model, ModelValidationError, ModelValidator}
import io.semanticdf.core.manifest.ManifestError
import io.semanticdf.portableloader.{ManifestFormat, ManifestFormatDetector, PortableManifestLoader}

/** The dual manifest reader — auto-detects YAML format and dispatches
  * to the right reader.
  *
  * Per the v0.3.2 design doc (PR #437): Step 3 PR #B supports BOTH
  * legacy and portable YAML formats side-by-side. New manifests use
  * the portable format; existing 20 example YAMLs use the legacy
  * format. The dual reader handles both transparently.
  *
  * ==Pipeline==
  *
  * ```
  * YAML file (or string)
  *   |
  *   v
  * ManifestFormatDetector.detect(yaml)
  *   |--> Portable → PortableManifestLoader.loadString(yaml) → core.Model
  *   |
  *   `--> Legacy   → YamlLoader.loadDir(...) → SemanticTable
  *                  → ModelBridge.toModel(st) → core.Model
  * ```
  *
  * ==Why this lives in `semanticdf-spark` (not `semanticdf-portable-loader`)==
  *
  * The legacy path needs `YamlLoader` + `ModelBridge` (both in
  * `semanticdf-spark`). The dual reader is the natural home for
  * "BOTH paths together." Putting it in the portable loader would
  * require adding `semanticdf-spark` as a dep to the portable loader
  * (violates the "portable loader is Spark-free" intent).
  *
  * ==Why both paths produce `core.Model`==
  *
  * Per the v0.3.2 design doc §3: the goal is to make `core.Model`
  * the canonical model shape. Both legacy and portable paths produce
  * the same `core.Model` (with different conversion quality — see
  * the limitations in `ModelBridge.toModel`).
  *
  * ==Error handling==
  *
  * Per docs/design/error-handling-style.md: public API returns
  * `Either[ManifestError, Model]`. `ManifestError` is the portable
  * module's error ADT; legacy errors are wrapped in
  * `ManifestError.DomainValidation` for unification.
  *
  * ==Known limitations (inherited from ModelBridge.toModel)==
  *
  * See [[io.semanticdf.ModelBridge.toModel]] for the legacy-path
  * limitations. In summary: measure aggregate defaults to Sum,
  * filters / calculated measures / rollups are not converted, and
  * dimensions are simplified to FieldRef.
  *
  * ==Boundary contract==
  *
  * Zero new Spark imports (uses the existing spark dependencies
  * already declared in this module's pom).
  *
  * Verifiable by:
  * `grep -r 'org.apache.spark' adapters/semanticdf-spark/src/main/scala/io/semanticdf/spark/DualManifestReader.scala` */
object DualManifestReader {

  /** Load a YAML manifest, auto-detecting the format.
    *
    * @param path  the YAML file path
    * @param spark the Spark session (needed for the legacy path;
    *             ignored for the portable path)
    * @return     `Right(core.Model)` on success, `Left(ManifestError)`
    *             on any failure (parse error, shape mismatch,
    *             domain validation, or legacy conversion limitation)
    *
    * Example:
    * {{{
    *   val spark: SparkSession = ...
    *   val model = DualManifestReader.load(Paths.get("models/flights.yml"), spark) match {
    *     case Right(m) => m
    *     case Left(err) => throw new RuntimeException(s"manifest load failed: ${err.message}")
    *   }
    * }}} */
  def load(path: Path, spark: org.apache.spark.sql.SparkSession): Either[ManifestError, Model] = {
    if (!Files.exists(path)) {
      return Left(ManifestError.MissingField(field = "file", path = Nil))
    }
    val yaml = try {
      new String(Files.readAllBytes(path), "UTF-8")
    } catch {
      case e: java.io.IOException =>
        return Left(ManifestError.YamlSyntaxError(
          reason = s"could not read file: ${e.getMessage}"
        ))
    }
    loadString(yaml, spark)
  }

  /** Load a YAML manifest from a string, auto-detecting the format. */
  def loadString(
      yaml:  String,
      spark: org.apache.spark.sql.SparkSession,
  ): Either[ManifestError, Model] = {
    val format = ManifestFormatDetector.detect(yaml)
    format match {
      case ManifestFormat.Portable =>
        // Portable path: direct YAML → core.Model
        PortableManifestLoader.loadString(yaml)
      case ManifestFormat.Legacy =>
        // Legacy path: YAML → SemanticTable → core.Model
        loadLegacy(yaml, spark)
    }
  }

  /** Load the legacy YAML format via the existing YamlLoader +
    * ModelBridge pipeline.
    *
    * Per scala-impact-analysis: this is the smallest change to wire
    * the legacy path into the dual reader. We DON'T refactor
    * YamlLoader or ModelBridge — just compose them. */
  private def loadLegacy(
      yaml:  String,
      spark: org.apache.spark.sql.SparkSession,
  ): Either[ManifestError, Model] = {
    // The legacy YamlLoader takes a path (or dir), not a string.
    // For dual-reader purposes, write the YAML to a temp file and
    // pass it to YamlLoader.loadDir.
    val tmpPath = try {
      val tmp = Files.createTempFile("legacy-manifest-", ".yml")
      Files.write(tmp, yaml.getBytes("UTF-8"))
      tmp
    } catch {
      case e: java.io.IOException =>
        return Left(ManifestError.YamlSyntaxError(
          reason = s"could not write temp file: ${e.getMessage}"
        ))
    }

    try {
      // YamlLoader.load takes a single YAML FILE (not a directory).
      // YamlLoader.loadDir takes a directory — not what we want.
      val tables = try {
        YamlLoader.load(tmpPath.toString, spark)
      } catch {
        // Per docs/design/error-handling-style.md "IO boundary":
        // catch SPECIFIC exception types (not `case _: Exception`)
        // and convert to typed `ManifestError` at the boundary.
        case e: IllegalArgumentException =>
          return Left(ManifestError.YamlSyntaxError(
            reason = s"legacy YAML parse error: ${e.getMessage}"
          ))
        case e: java.io.IOException =>
          return Left(ManifestError.YamlSyntaxError(
            reason = s"legacy YAML read error: ${e.getMessage}"
          ))
      }
      // The legacy YAML may have multiple models; pick the FIRST
      // one (deterministic for now). Future work: support multiple
      // models per file (returns `Map[String, Model]`).
      tables.headOption match {
        case None =>
          Left(ManifestError.YamlSyntaxError(
            reason = "legacy YAML contains no models"
          ))
        case Some((name, st)) =>
          // Per SemanticTable: model name is the Map key (from the
          // YAML top-level key, e.g. `flights:`). SemanticTable itself
          // doesn't have a `.name` field.
          ModelBridge.toModel(st).left.map(toDomainValidationError(name))
      }
    } finally {
      // Per scala-jvm-safety §2: close resources even on failure.
      // The temp file is the resource we own; the Spark session
      // is owned by the caller.
      Files.deleteIfExists(tmpPath)
    }
  }

  /** Wrap a `ModelValidationError` in a `ManifestError.DomainValidation`
    * for unified error handling. */
  private def toDomainValidationError(modelName: String)(err: ModelValidationError): ManifestError = {
    val reason = err match {
      case ModelValidationError.InvalidName(reason)         => s"invalid name: $reason"
      case ModelValidationError.DuplicateMember(kind, name) => s"duplicate $kind: $name"
      case ModelValidationError.UnknownReference(referent, target) =>
        s"unknown reference: $referent -> $target"
      case ModelValidationError.CalcDepthExceeded(depth, max) =>
        s"calc depth exceeded: $depth > $max"
      case ModelValidationError.ExtensionEnvelopeExceeded(fieldCount, byteCount) =>
        s"extension envelope exceeded: $fieldCount fields, $byteCount bytes"
      case ModelValidationError.FilterConversionUnsupported(reason) =>
        s"filter conversion unsupported: $reason"
    }
    ManifestError.DomainValidation(reason = reason, modelName = modelName)
  }
}
