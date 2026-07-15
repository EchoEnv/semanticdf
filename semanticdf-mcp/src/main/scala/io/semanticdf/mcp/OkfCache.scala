package io.semanticdf.mcp

import io.semanticdf.tools.OkfGen

import java.io.File
import java.nio.file.{Files, Paths}

/** Pre-built OKF (Open Knowledge Format) markdown cache, populated at server
  * startup.
  *
  * Per `mcp-contract.md` v2 §"Per-model `okf_markdown` field":
  *
  *   "Source-of-truth: [[OkfGen.generate(modelDir, bundleDir)]] — the same tool the
  *    CLI exposes. The server runs it once at startup, then caches each
  *    model's Markdown string in memory keyed by model name. Live
  *    regeneration per request is not required (YAML changes are a server
  *    restart)."
  *
  * The construction:
  *   1. Calls `OkfGen.generate(<modelsDir>, <bundleDir>)` to write all
  *      per-model markdowns under `<bundleDir>/<model>.md`.
  *   2. Reads each `<bundleDir>/<model>.md` into memory as a `String`.
  *
  * Lookup at request time is O(1) `Map.get`. */
final class OkfCache private[mcp] (private val cache: Map[String, String]) {
  def apply(modelName: String): Option[String] = cache.get(modelName)
  def names: Iterable[String] = cache.keys
  def size: Int = cache.size
}

object OkfCache {

  /** Build the cache by running `OkfGen.generate` on `modelsDir` and reading
    * the resulting files into memory.
    *
    * @param modelsDir   directory of `*.yml` model files (the same directory
    *                    passed to `YamlLoader.loadDir`)
    * @param bundleDir   directory where OkfGen writes its per-model markdowns;
    *                    the server creates it if absent (OkfGen does too)
    * @return a cache mapping YamlLoader's model-key → okf-markdown-string
    */
  def build(modelsDir: String, bundleDir: String): OkfCache = {
    require(new File(modelsDir).isDirectory,
      s"OkfCache.build: modelsDir is not a directory: $modelsDir")

    // 1. Run OkfGen — produces <bundleDir>/<model>.md for each model.
    val count = new OkfGen().generate(modelsDir, bundleDir)
    if (count == 0) {
      // OkfGen throws require(...) for an empty dir, but we defend explicitly
      // in case a future version returns 0 silently.
      throw new IllegalStateException(
        s"OkfGen produced 0 markdowns for $modelsDir — check the YAML files"
      )
    }

    // 2. Read every <bundleDir>/*.md into a Map[basename -> content].
    //    The YamlLoader keys models by the YAML's top-level key (e.g.
    //    `flights:`); OkfGen's filenames are also the model name, so they
    //    line up directly.
    val dir = new File(bundleDir)
    require(dir.isDirectory, s"OkfCache.build: bundleDir missing after generate: $bundleDir")
    val mdFiles = Option(dir.listFiles())
      .getOrElse(Array.empty[File])
      .filter(f => f.isFile && f.getName.endsWith(".md"))

    val cache = mdFiles.map { f =>
      // Strip the ".md" extension to get the model name. This matches both
      // YamlLoader's keys and any other consumer of the bundle.
      val modelName = f.getName.stripSuffix(".md")
      val content   = Files.readString(Paths.get(f.getAbsolutePath))
      modelName -> content
    }.toMap

    new OkfCache(cache)
  }
}
