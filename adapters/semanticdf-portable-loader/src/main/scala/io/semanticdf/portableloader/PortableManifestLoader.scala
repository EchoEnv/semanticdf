package io.semanticdf.portableloader

import java.nio.file.Path

import io.semanticdf.core.manifest.ManifestError
import io.semanticdf.core.model.Model

/** The single public API for the portable manifest loader.
  *
  * Per the v0.3.2 design doc (PR #437): the loader is the user-
  * facing entry point for portable YAML manifests. Internally it
  * composes:
  *   1. [[YamlManifestLoader]] — Jackson YAML → `PortableModel`
  *   2. [[PortableManifestConverter]] — `PortableModel` → `core.Model`
  *
  * The user sees ONE method: `load(path)`. Internal pipeline is
  * an implementation detail.
  *
  * ==Why a wrapper (vs. calling loader + converter directly)==
  *
  * Two reasons:
  *   1. **API stability**: future changes (e.g., adding validation,
  *      caching, schema resolution) happen inside this wrapper
  *      without breaking callers.
  *   2. **Composition**: callers should never need to know that
  *      there's a two-stage pipeline. The intermediate `PortableModel`
  *      is internal to this module.
  *
  * ==Standard compliance==
  *
  * Per docs/design/error-handling-style.md: public API returns
  * `Either[ManifestError, Model]`. No `Either[String, _]`. All
  * errors are typed and surface the original error type.
  *
  * ==Boundary contract==
  *
  * Zero Spark imports. Engine-portable. Verifiable by:
  * `grep -r 'org.apache.spark' adapters/semanticdf-portable-loader/src/main/scala/` */
object PortableManifestLoader {

  /** Load a portable YAML manifest from a file and convert it to
    * a validated `core.Model`.
    *
    * @param path the YAML file path
    * @return `Right(Model)` on success, `Left(ManifestError)` on any
    *         failure (parse error, shape mismatch, domain validation,
    *         known conversion limitation)
    *
    * Example:
    * {{{
    *   import io.semanticdf.portableloader.PortableManifestLoader
    *   import java.nio.file.Paths
    *
    *   val result = PortableManifestLoader.load(Paths.get("models/flights.yml"))
    *   result match {
    *     case Right(model)      => println(s"loaded: ${model.name}")
    *     case Left(manifestErr) => println(s"failed: ${manifestErr.message}")
    *   }
    * }}} */
  def load(path: Path): Either[ManifestError, Model] = {
    for {
      portable <- YamlManifestLoader.load(path).right
      model    <- PortableManifestConverter.toModel(portable).right
    } yield model
  }

  /** Load a portable YAML manifest from a string and convert it.
    *
    * Same as `load(path)` but takes the YAML content directly.
    * Useful for tests, dynamic manifests, or in-memory construction. */
  def loadString(yaml: String): Either[ManifestError, Model] = {
    for {
      portable <- YamlManifestLoader.loadString(yaml).right
      model    <- PortableManifestConverter.toModel(portable).right
    } yield model
  }
}
