package io.semanticdf.tools

import scala.jdk.CollectionConverters._
import java.io.File
import org.yaml.snakeyaml.Yaml


/** Generate an Open Knowledge Format (OKF) bundle from semanticdf YAML model files.
  *
  * One concept document (.md) is emitted per YAML model. The bundle is conformant
  * with OKF v0.1 (https://github.com/GoogleCloudPlatform/knowledge-catalog/blob/main/okf/SPEC.md).
  *
  * Source-of-truth: the YAML files. This generator reads them through snakeyaml
  * (no Spark, no `SemanticTable`) — the OKF doc is a knowledge-layer render of
  * the declared schema, not a re-execution of the engine. Round-trip equivalence
  * (load YAML → emit OKF) is the contract; see `OkfRoundTripSpec`.
  *
  * Spec: docs/agents/okf-mapping.md. That doc is the source of truth for what
  * each frontmatter field and body section contains; this code is the
  * implementation. If they disagree, the doc wins.
  *
  * Run with:
  *   mvn scala:run -DmainClass=io.semanticdf.tools.Main \
  *     -Dexec.args="okfgen --path models/ --out agents/"
  */
class OkfGen {

  /** Generate the OKF bundle for the given input under `outDir`.
    *
    * @param inPath  file or directory of YAMLs
    * @param outDir  directory under which the bundle is written (created if absent)
    * @return        the count of .md concept files written
    */
  def generate(inPath: String, outDir: String): Int = {
    val inFile = new File(inPath)
    require(inFile.exists(), s"Input path does not exist: $inPath")
    val out = new File(outDir)
    out.mkdirs()

    val models: Seq[(String, ModelEntry)] =
      if (inFile.isDirectory) {
        // Walk input dir — every YAML becomes one or more concept docs (one per model in file)
        walkDir(inFile, inFile).flatMap { case (relDir, file) =>
          loadFile(file.getPath).map(m => (relDir, m))
        }
      } else {
        // Single-file input → emit one concept doc at the bundle root
        Seq(("", loadFile(inFile.getPath))).flatMap { case (relDir, ms) =>
          ms.map(m => (relDir, m))
        }
      }

    require(models.nonEmpty, s"No models found in $inPath")

    // 1. Emit per-model concept documents
    models.foreach { case (relDir, m) =>
      writeConcept(m, relDir, out)
    }

    // 2. Emit per-directory `index.md` (alphabetic by default, by-owner if multi-owner)
    val byDir = models.groupBy(_._1).toSeq.sortBy(_._1)
    byDir.foreach { case (relDir, dirModels) =>
      writeIndex(relDir, dirModels.map(_._2).sortBy(_.name), out)
    }

    // 3. Emit bundle-root `index.md` (with `okf_version: "0.1"`) and `log.md`
    val sourceFiles = models.map(_._2.sourcePath).distinct
    writeBundleRoot(out, models.map(_._2), sourceFiles)

    models.size
  }

  // -------------------------------------------------------------------------
  // YAML parsing — mirrors DocsGen for consistency, no Spark dependency
  // -------------------------------------------------------------------------

  /** Walk `root` recursively returning (relativeDir, file) tuples for every .yml / .yaml file.
    * relativeDir is "" for files directly in `root`, "subdir" for files one level deep, etc. */
  private def walkDir(root: File, current: File, prefix: String = ""): Seq[(String, File)] = {
    val ymlFilter = new java.io.FilenameFilter {
      override def accept(d: File, n: String): Boolean =
        n.endsWith(".yml") || n.endsWith(".yaml")
    }
    val subdirs = current.listFiles((f: File) => f.isDirectory)
      .filter(_.getName != "okf")         // don't recurse into our own output
      .filter(_.getName != "node_modules") // generic hygiene
    val dirResults: Seq[(String, File)] = subdirs.toSeq.flatMap { sd =>
      walkDir(root, sd, if (prefix.isEmpty) sd.getName else s"$prefix/${sd.getName}")
    }
    val fileResults: Seq[(String, File)] = current.listFiles(ymlFilter).toSeq.map(f => (prefix, f))
    dirResults ++ fileResults
  }

  private def loadFile(path: String): Seq[ModelEntry] = {
    val yaml = new Yaml()
    val raw = try {
      yaml.load[java.util.Map[String, java.util.Map[String, Any]]](
        scala.io.Source.fromFile(path).mkString)
    } catch {
      case e: Exception =>
        System.err.println(s"Skipping $path: ${e.getMessage}")
        return Nil
    }
    if (raw == null) return Nil
    raw.asScala.toSeq.sortBy(_._1).map { case (modelName, modelMap) =>
      ModelEntry(
        name        = modelName,
        table       = strOpt(modelMap, "table"),
        description = strOpt(modelMap, "description"),
        dimensions  = parseDims(modelMap),
        measures    = parseMeasures(modelMap, base = true),
        calcMeasures = parseMeasures(modelMap, base = false),
        joins       = parseJoins(modelMap),
        filters     = parseFilters(modelMap),
        metadata    = parseMetaOf(modelMap.get("metadata")),
        sourcePath  = path,
        version     = parseVersion(modelName, modelMap),
      )
    }
  }

  private def parseDims(m: java.util.Map[String, Any]): Seq[FieldEntry] = {
    val dims = m.get("dimensions")
    if (dims == null) return Nil
    javaMapEntries(dims).sortBy(_._1).map { case (name, v) =>
      val map = v match {
        case x: java.util.Map[_, _] => asScalaMap(x)
        case _ => Map.empty[String, Any]
      }
      FieldEntry(name = name, expr = strOpt(map, "expr"), description = strOpt(map, "description"),
                 metadata = parseMetaOf(map.get("metadata")))
    }
  }

  private def parseMeasures(m: java.util.Map[String, Any], base: Boolean): Seq[FieldEntry] = {
    val key = if (base) "measures" else "calculated_measures"
    val meas = m.get(key)
    if (meas == null) return Nil
    javaMapEntries(meas).sortBy(_._1).map { case (name, v) =>
      val map = v match {
        case s: String => Map("expr" -> s)
        case x: java.util.Map[_, _] => asScalaMap(x)
        case _ => Map.empty[String, Any]
      }
      FieldEntry(name = name, expr = strOpt(map, "expr"), description = strOpt(map, "description"),
                 metadata = parseMetaOf(map.get("metadata")))
    }
  }

  private def parseJoins(m: java.util.Map[String, Any]): Seq[JoinEntry] = {
    val joins = m.get("joins")
    if (joins == null) return Nil
    javaMapEntries(joins).sortBy(_._1).map { case (alias, v) =>
      val map = v match {
        case x: java.util.Map[_, _] => asScalaMap(x)
        case _ => Map.empty[String, Any]
      }
      JoinEntry(alias = alias,
                model = strOpt(map, "model"),
                joinType = strOpt(map, "type"),
                leftOn = strOpt(map, "left_on"),
                rightOn = strOpt(map, "right_on"))
    }
  }

  /** Dispatch to [[parseMeta]] for both Scala `Option[Any]` (from Scala Maps)
    * and `Any` (from Java Maps, where null means missing). */
  private def parseMetaOf(v: Any): Map[String, String] = v match {
    case null     => Map.empty
    case None     => Map.empty
    case Some(x)  => parseMeta(x)
    case other    => parseMeta(other)
  }

  /** Iterate the entries of a Java Map value, returning Seq[(String, Any)].
    * We can't pattern-match on `java.util.Map[String, Any]` at runtime (erasure),
    * so we cast more loosely and rely on the caller to filter. */
  private def javaMapEntries(v: Any): Seq[(String, Any)] = v match {
    case null => Nil
    case jm: java.util.Map[_, _] =>
      jm.asScala.toSeq.map { case (k, vv) => (k.toString, vv.asInstanceOf[Any]) }
    case _ => Nil
  }

  private def asScalaMap(jm: java.util.Map[_, _]): Map[String, Any] =
    jm.asScala.toMap.map { case (k, v) => (k.toString, v.asInstanceOf[Any]) }

  /** Read the optional `version:` field from the YAML model map. Defaults to 0.
    * Negative values raise — same policy as SemanticTable.version. */
  private def parseVersion(modelName: String, modelMap: java.util.Map[String, Any]): Int = {
    val v = modelMap.get("version")
    if (v == null) 0
    else v match {
      case n: java.lang.Number =>
        val intVal = n.intValue()
        require(intVal >= 0, s"Model '$modelName': 'version' must be non-negative, got $intVal")
        intVal
      case other =>
        throw new IllegalArgumentException(
          s"Model '$modelName': 'version' must be an integer, got ${other.getClass.getSimpleName}")
    }
  }

  private def parseMeta(v: Any): Map[String, String] =
    if (v == null) Map.empty
    else v match {
      case m: java.util.Map[_, _] => m.asScala.toMap.map { case (k, vv) => (k.toString, vv.toString) }
      case _ => Map.empty
    }

  private def strOpt(m: java.util.Map[String, Any], key: String): Option[String] =
    Option(m.get(key)).map(_.toString.trim).filter(_.nonEmpty)

  private def strOpt(m: scala.collection.Map[String, Any], key: String): Option[String] =
    m.get(key).map(_.toString.trim).filter(_.nonEmpty)

  // -------------------------------------------------------------------------
  // Frontmatter rendering
  // -------------------------------------------------------------------------

  private def renderFrontmatter(m: ModelEntry, resourcePath: String): String = {
    val parts = scala.collection.mutable.ListBuffer.empty[(String, String)]
    parts += "type"        -> "SemanticTable"
    parts += "title"       -> titleCase(m.name)
    if (m.version > 0) parts += "version" -> m.version.toString
    parts += "description" -> truncate(m.description.getOrElse(""), max = 200)._1
    parts += "resource"    -> resourcePath
    timestamp(m).foreach(t => parts += "timestamp" -> t)
    val tags = computeTags(m)
    if (tags.nonEmpty) parts += "tags" -> renderTagsList(tags)

    val rendered = parts.map { case (k, v) => s"$k: ${renderYamlScalar(k, v)}" }.mkString("\n")
    s"---\n$rendered\n---\n"
  }

  /** Render a value as YAML for frontmatter. Strings get quoted if they contain
    * special chars; lists use flow syntax. Everything else is `toString`. */
  private def renderYamlScalar(key: String, value: String): String = {
    if (key == "tags") {
      // tags is a list — render flow form: [a, b, c]
      val xs = value.stripPrefix("[").stripSuffix("]").split(",").map(_.trim).filter(_.nonEmpty)
      s"[${xs.map(quoteYamlString).mkString(", ")}]"
    } else if (value.startsWith("[") && value.endsWith("]")) {
      value  // already list-shaped
    } else {
      quoteYamlString(value)
    }
  }

  private def quoteYamlString(s: String): String = {
    if (s.isEmpty) {
      "\"\""
    } else if (s.matches("""[A-Za-z0-9_\-./: ]+""") && !s.contains(": ") && !s.startsWith(" ")) {
      s
    } else {
      "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", " ") + "\""
    }
  }

  /** Compute the tag list — see okf-mapping.md "Tag derivation":
    *   owner:<x>, all dimensions' tags, all measures' tags, structural "semantic-table".
    *   Deduped, sorted alphabetically. */
  private def computeTags(m: ModelEntry): Seq[String] = {
    val srcs = scala.collection.mutable.ListBuffer.empty[String]
    m.metadata.get("owner").foreach(o => srcs += s"owner:$o")
    m.metadata.get("tags").foreach(t => srcs ++= splitTags(t))
    m.dimensions.foreach(d => d.metadata.get("tags").foreach(t => srcs ++= splitTags(t)))
    m.measures.foreach(m => m.metadata.get("tags").foreach(t => srcs ++= splitTags(t)))
    m.calcMeasures.foreach(m => m.metadata.get("tags").foreach(t => srcs ++= splitTags(t)))
    srcs += "semantic-table"
    srcs.toList.distinct.sorted
  }

  private def splitTags(s: String): Seq[String] =
    s.split("[,\\[\\]\\s]+").map(_.trim).filter(_.nonEmpty)

  private def renderTagsList(tags: Seq[String]): String =
    tags.map(quoteYamlString).mkString("[", ", ", "]")

  // -------------------------------------------------------------------------
  // Body rendering
  // -------------------------------------------------------------------------

  private def renderBody(m: ModelEntry): String = {
    val sb = new StringBuilder
    sb.append(renderSchemaSection(m)).append("\n")
    if (m.filters.nonEmpty) sb.append(renderFiltersSection(m)).append("\n")
    if (m.joins.nonEmpty) sb.append(renderJoinsSection(m)).append("\n")
    if (m.calcMeasures.nonEmpty) sb.append(renderCalcSection(m)).append("\n")
    sb.append(renderExamplesSection(m)).append("\n")
    sb.append(renderCitationsSection(m)).append("\n")
    sb.toString.trim.stripSuffix("\n") + "\n"
  }

  private def renderSchemaSection(m: ModelEntry): String = {
    val sb = new StringBuilder("# Schema\n\n")
    if (m.dimensions.isEmpty && m.measures.isEmpty) {
      sb.append("| (none) |\n|---------|\n")
    } else {
      sb.append("| name | kind | expr | description | metadata |\n")
      sb.append("|------|------|------|-------------|----------|\n")
      m.dimensions.foreach { d =>
        sb.append(renderSchemaRow(d.name, "dimension", d))
      }
      m.measures.foreach { meas =>
        sb.append(renderSchemaRow(meas.name, "measure", meas))
      }
    }
    sb.toString
  }

  private def renderSchemaRow(name: String, kind: String, f: FieldEntry): String = {
    val rawExpr = f.expr.getOrElse("—").replace("|", "\\|")
    val expr =
      if (rawExpr.nonEmpty && (rawExpr.contains(' ') || !rawExpr.startsWith("`"))) s"`$rawExpr`"
      else rawExpr
    val desc = f.description.getOrElse("—").replace("|", "\\|").replace("\n", " ")
    val meta = if (f.metadata.isEmpty) "—" else renderMetadata(f.metadata)
    s"| $name | $kind | $expr | $desc | $meta |\n"
  }

  private def renderMetadata(meta: Map[String, String]): String =
    meta.toSeq.sortBy(_._1).map { case (k, v) => s"$k=$v" }.mkString("; ")

  private def renderFiltersSection(m: ModelEntry): String = {
    val sb = new StringBuilder("# Filters\n\n")
    sb.append("Pre-join row-level predicates on this model's source table. Applied automatically before joins.\n\n")
    sb.append("| name | expr | description | metadata |\n")
    sb.append("|------|------|-------------|----------|\n")
    m.filters.foreach { f =>
      val exprCell   = "`" + f.expr.replace("|", "\\|") + "`"
      val descCell   = f.description.getOrElse("—")
      val metaCell   = if (f.metadata.isEmpty) "—" else f.metadata.toSeq.sortBy(_._1).map { case (k, v) => s"$k=$v" }.mkString("; ")
      sb.append(s"| ${f.name} | $exprCell | $descCell | $metaCell |\n")
    }
    sb.toString
  }

  private def renderJoinsSection(m: ModelEntry): String = {
    val sb = new StringBuilder("# Joins\n\n")
    m.joins.foreach { j =>
      val rt  = j.joinType.getOrElse("?")
      val lo  = j.leftOn.getOrElse("?")
      val ro  = j.rightOn.getOrElse("?")
      val tgt = j.model.getOrElse("?")
      sb.append(s"- **[${escMd(tgt)}]($tgt.md)** — ${escMd(rt)}, on \\`$lo = $ro\\`\n")
    }
    sb.toString
  }

  private def renderCalcSection(m: ModelEntry): String = {
    val sb = new StringBuilder("# Calculated measures\n\n")
    sb.append("| name | kind | expr | description | metadata |\n")
    sb.append("|------|------|------|-------------|----------|\n")
    m.calcMeasures.foreach { c =>
      sb.append(renderSchemaRow(c.name, "calc", c))
    }
    sb.toString
  }

  private def renderExamplesSection(m: ModelEntry): String = {
    val sb = new StringBuilder("# Examples\n\n")
    sb.append("A consumer pointed at this catalog can run any of the following MCP `query` payloads:\n\n")
    sb.append("```json\n")
    sb.append(s"""{"model": "${m.name}", "dimensions": ["carrier"], "measures": ["flight_count"]}""").append("\n")
    sb.append("```\n\n")
    sb.append("```json\n")
    sb.append(s"""{"model": "${m.name}", "dimensions": ["carrier"], "measures": ["total_passengers"], "where": [{"type": "ge", "field": "distance", "value": 500}]}""").append("\n")
    sb.append("```\n\n")
    if (m.joins.nonEmpty) {
      // Use a known join target for the third example
      val joinAlias = m.joins.head.model.getOrElse("lookup")
      sb.append("```json\n")
      sb.append(s"""{"model": "${m.name}", "dimensions": ["$joinAlias.name"], "measures": ["total_passengers"], "limit": 10}""").append("\n")
      sb.append("```\n\n")
    } else {
      sb.append("```json\n")
      sb.append(s"""{"model": "${m.name}", "dimensions": ["carrier"], "measures": ["flight_count"], "order_by": [{"field": "flight_count", "direction": "desc"}], "limit": 10}""").append("\n")
      sb.append("```\n\n")
    }
    sb.append("> Run via an MCP client pointed at this catalog. See [mcp-contract.md](./mcp-contract.md) for the full schema.\n")
    sb.toString
  }

  private def renderCitationsSection(m: ModelEntry): String = {
    val sb = new StringBuilder("# Citations\n\n")
    sb.append(s"[1] [${escMd(m.sourcePath)}](file://${escMd(m.sourcePath)}) — the source schema this document references.\n")
    sb.toString
  }

  // -------------------------------------------------------------------------
  // Bundle writers
  // -------------------------------------------------------------------------

  private def writeConcept(m: ModelEntry, relDir: String, out: File): Unit = {
    val slug = fileSlug(m.name) + ".md"
    val targetDir = if (relDir.isEmpty) out else new File(out, relDir)
    targetDir.mkdirs()
    val outFile = new File(targetDir, slug)

    val resourcePath = if (relDir.isEmpty) {
      // Source path is relative to project root; if absolute, keep absolute
      absOrRel(m.sourcePath)
    } else {
      absOrRel(m.sourcePath)
    }
    val frontmatter = renderFrontmatter(m, resourcePath)
    val body        = renderBody(m)
    writeFile(outFile, frontmatter + "\n" + body)
  }

  private def writeIndex(relDir: String, models: Seq[ModelEntry], out: File): Unit = {
    val targetDir = if (relDir.isEmpty) out else new File(out, relDir)
    targetDir.mkdirs()
    val indexFile = new File(targetDir, "index.md")

    val owners = models.flatMap(_.metadata.get("owner")).distinct
    val grouping =
      if (owners.size > 1) Grouping.ByOwner(owners)
      else Grouping.Alphabetic

    val body = grouping match {
      case Grouping.Alphabetic =>
        val sb = new StringBuilder("# Models\n\n")
        models.foreach { m =>
          val desc = m.description.map(truncate(_, 120)._1).getOrElse("")
          sb.append(s"* [${escMd(titleCase(m.name))}](${fileSlug(m.name)}.md) — ${escMd(desc)}\n")
        }
        sb.toString
      case Grouping.ByOwner(owners) =>
        val sb = new StringBuilder()
        owners.sorted.foreach { owner =>
          val owned = models.filter(_.metadata.get("owner").contains(owner))
          sb.append(s"# Owner: ${escMd(owner)}\n\n")
          owned.foreach { m =>
            val desc = m.description.map(truncate(_, 120)._1).getOrElse("")
            sb.append(s"* [${escMd(titleCase(m.name))}](${fileSlug(m.name)}.md) — ${escMd(desc)}\n")
          }
          sb.append("\n")
        }
        sb.toString.stripSuffix("\n") + "\n"
    }
    writeFile(indexFile, body)
  }

  private def writeBundleRoot(out: File, models: Seq[ModelEntry], sourceFiles: Seq[String]): Unit = {
    // Root index.md — the only index.md that has frontmatter (okf_version per spec §11)
    val rootIndex = new File(out, "index.md")
    val subdirs = out.listFiles((f: File) => f.isDirectory).toSeq
      .filter(_.getName != "index.md")
      .map(_.getName)
      .sorted
    val sb = new StringBuilder()
    sb.append("---\nokf_version: \"0.1\"\n---\n\n")
    sb.append("# Bundle Index\n\n")
    subdirs.foreach { sd =>
      val idxFile = new File(new File(out, sd), "index.md")
      val desc = if (idxFile.exists()) "(see subdirectory index)" else "(no index)"
      sb.append(s"* [${escMd(sd)}/](${escMd(sd)}/index.md) — $desc\n")
    }
    models.sortBy(_.name).foreach { m =>
      val relLink = s"${fileSlug(m.name)}.md"
      val desc = m.description.map(truncate(_, 120)._1).getOrElse("")
      sb.append(s"* [${escMd(titleCase(m.name))}]($relLink) — ${escMd(desc)}\n")
    }
    writeFile(rootIndex, sb.toString)

    // Root log.md — over the SOURCE files (in git), not the OUTPUT .md files
    val rootLog = new File(out, "log.md")
    writeFile(rootLog, renderLogContent(sourceFiles))
  }

  /** Build the bundle-root log. Lists each source YAML with its last-modified date
    * (from git if available, else mtime). For v1 we don't parse git commit messages;
    * consumers wanting commit history should consult the source repo directly. */
  private def renderLogContent(sourceFiles: Seq[String]): String = {
    val entries = sourceFiles.distinct.sortBy(_.toLowerCase).map { sf =>
      val f = new File(sf)
      val name = f.getName
      val date = gitTimestamp(f).orElse(mtimeTimestamp(f)).getOrElse("unknown")
      (name, date)
    }
    if (entries.isEmpty) {
      "# Directory Update Log\n\n> No source files tracked.\n"
    } else {
      val sb = new StringBuilder("# Directory Update Log\n\n")
      entries.foreach { case (name, date) =>
        sb.append(s"## $date\n\n")
        sb.append(s"* **Update**: $name\n\n")
      }
      sb.toString
    }
  }

  /** Try to get the last-modified ISO timestamp for a file from git.
    *
    * Uses `git log --format=%ct` (epoch seconds) and converts to a canonical
    * ISO 8601 UTC string via `Instant.toString`. The naive `%aI` format depends
    * on the user's git `date.format-local` config and produces `…+00:00` or
    * `…Z` inconsistently — so byte-stable output across machines requires
    * canonicalizing. Returns None if git is unavailable, the file isn't tracked,
    * or there's any error.
    */
  private def gitTimestamp(f: File): Option[String] = try {
    val pb = new ProcessBuilder("git", "log", "-1", "--format=%ct", "--", f.getPath)
    pb.directory(new File("."))
    pb.redirectErrorStream(true)
    val proc = pb.start()
    val out = scala.io.Source.fromInputStream(proc.getInputStream).mkString.trim
    proc.waitFor()
    if (proc.exitValue() == 0 && out.nonEmpty) {
      // Convert epoch seconds to ISO 8601 UTC, second precision.
      // Instant.toString at second precision always emits the `Z` suffix.
      Some(java.time.Instant.ofEpochSecond(out.toLong).toString)
    } else None
  } catch { case _: Exception => None }

  // -------------------------------------------------------------------------
  // Helpers — naming, escaping, git
  // -------------------------------------------------------------------------

  /** Title-case a model name. `flights_carriers` → `Flights Carriers`. */
  private def titleCase(name: String): String =
    name.split("_").filter(_.nonEmpty).map(_.capitalize).mkString(" ")

  /** File-system-safe slug for a model name. `foo.bar` → `foo_bar`. */
  private def fileSlug(name: String): String =
    name.replace('.', '_').replaceAll("[^A-Za-z0-9_-]", "_")

  /** Truncate a string at `max` chars, on a word boundary if possible.
    * Returns (truncatedText, wasTruncated). */
  private def truncate(s: String, max: Int): (String, Boolean) = {
    if (s.length <= max) (s, false)
    else {
      val cut = s.take(max)
      val lastSpace = cut.lastIndexOf(' ')
      val end = if (lastSpace > max / 2) lastSpace else max
      (s.take(end).stripSuffix(",") + "…", true)
    }
  }

  /** Try git for last-commit ISO timestamp of the file; else fall back to mtime ISO; else None. */
  private def timestamp(m: ModelEntry): Option[String] = {
    val f = new File(m.sourcePath)
    if (!f.exists()) return None
    gitTimestamp(f).orElse(mtimeTimestamp(f))
  }

  private def mtimeTimestamp(f: File): Option[String] = try {
    Some(java.time.Instant.ofEpochMilli(f.lastModified()).toString)
  } catch { case _: Exception => None }

  /** Escape a string for inclusion in plain markdown text. */
  private def escMd(s: String): String =
    s.replace("|", "\\|").replace("\n", " ").replace("[", "\\[").replace("]", "\\]")

  /** Compute the path to write in `resource:` — absolute under project cwd,
    * Earlier versions emitted an absolute path, but the absolute path depends on
    * the runner's cwd (e.g. `/home/runner/work/...` on GitHub Actions vs
    * `/home/emilio/...` locally) — so the byte-stable bundle check fails whenever
    * the cwd differs. A repo-relative path avoids that drift.
    */
  private def absOrRel(p: String): String = {
    val abs = new File(p).getAbsoluteFile.toPath.normalize
    val cwd = new File(".").getAbsoluteFile.toPath.normalize
    val rel = if (abs.startsWith(cwd)) cwd.relativize(abs).toString
              else abs.toString  // fallback: outside cwd, use absolute
    "file://" + rel
  }

  /** Compute the subdirectory of the bundle root that contains this model's concept doc,
    * relative to the input directory. Used for cross-links in the root index.
    * Currently returns "" since the spec has us mirroring the source tree flat. */
  private def fileParent(sourcePath: String, baseDir: File): String = {
    val sf = new File(sourcePath)
    if (sf.isAbsolute && baseDir != null) {
      val rel = baseDir.toPath.relativize(sf.toPath).toString
      // Strip the filename — return parent dir only
      val sep = rel.lastIndexOf('/')
      if (sep < 0) "" else rel.take(sep)
    } else ""
  }

  /** Write `content` to `f`, overwriting if present. Wraps java.nio.file.Files
    * since java.io.File has no write method directly. */
  private def writeFile(f: File, content: String): Unit = {
    java.nio.file.Files.writeString(
      f.toPath, content,
      java.nio.file.StandardOpenOption.CREATE,
      java.nio.file.StandardOpenOption.TRUNCATE_EXISTING,
      java.nio.file.StandardOpenOption.WRITE)
  }

  private sealed trait Grouping
  private object Grouping {
    case object Alphabetic extends Grouping
    case class ByOwner(owners: Seq[String]) extends Grouping
  }

  // -------------------------------------------------------------------------
  // Data models
  // -------------------------------------------------------------------------

  private case class ModelEntry(
    name: String,
    table: Option[String],
    description: Option[String],
    dimensions: Seq[FieldEntry],
    measures: Seq[FieldEntry],
    calcMeasures: Seq[FieldEntry],
    joins: Seq[JoinEntry],
    filters: Seq[FilterEntry] = Seq.empty,
    metadata: Map[String, String],
    sourcePath: String,
    version: Int = 0,
  )

  /** Read the optional `filters:` block from the YAML model map. */
  private def parseFilters(modelMap: java.util.Map[String, Any]): Seq[FilterEntry] = {
    val raw = modelMap.get("filters")
    if (raw == null) return Nil
    javaMapEntries(raw).sortBy(_._1).map { case (fName, fCfg) =>
      val fMap = fCfg match {
        case jm: java.util.Map[_, _] => jm.asScala.toMap.map { case (k, v) => (k.toString, v.toString) }
        case _ => Map.empty[String, String]
      }
      val expr = fMap.getOrElse("expr",
        throw new IllegalArgumentException(
          s"Filter '$fName' must specify 'expr' (a Spark SQL filter expression)."))
      val description = fMap.get("description").filter(_.nonEmpty)
      FilterEntry(
        name = fName,
        description = description,
        expr = expr,
        metadata = parseMetaOf(fMap.get("metadata")),
      )
    }
  }

  private case class FieldEntry(
    name: String,
    expr: Option[String],
    description: Option[String],
    metadata: Map[String, String],
  )

  private case class JoinEntry(
    alias: String,
    model: Option[String],
    joinType: Option[String],
    leftOn: Option[String],
    rightOn: Option[String],
  )

  private case class FilterEntry(
    name: String,
    description: Option[String],
    expr: String,
    metadata: Map[String, String],
  )
}
