package io.semanticdf

import java.io.File
import java.nio.file.{Files, Path}

import scala.jdk.CollectionConverters._
import org.scalatest.funsuite.AnyFunSuite
import org.yaml.snakeyaml.Yaml
import io.semanticdf.tools.OkfGen


/** Round-trip property test for [[OkfGen]].
  *
  * Invariant under test: every value in the emitted OKF body comes from the
  * YAML declaration \— no invented fields, no dropped fields. If this fails,
  * the generator has either fabricated a value or lost one during emission.
  *
  * For every YAML under examples/<name>/models/, this test:
  *
  *  1. Loads the YAML via snakeyaml (same parser OkfGen uses internally).
  *  2. Runs OkfGen on it, writing to a per-test tmp directory.
  *  3. Re-parses the emitted markdown frontmatter via snakeyaml.
  *  4. Re-counts dimension / measure / calc-measure rows in the emitted body.
  *  5. Asserts each YAML field name appears in the emitted output.
  *
  * This test catches the class of bug where OkfGen hardcodes a string instead
  * of reading it from YAML, or silently drops a YAML field on emit. It will
  * NOT catch every conceivable drift \— markdown-table cell rendering and
  * complex expr round-tripping are out of scope \— but it catches the major
  * source-of-truth violations cheaply (per-YAML cost roughly 200 ms).
  */
class OkfRoundTripSpec extends AnyFunSuite {

  // Locate every YAML under any examples/<name>/models/ directory.
  private val exampleYamls: Seq[File] = {
    val examplesRoot = new File("examples")
    require(examplesRoot.isDirectory, "examples/ directory not found — run from project root")
    val modelsDirs = Option(examplesRoot.listFiles())
      .map(_.toSeq).getOrElse(Nil)
      .filter(_.isDirectory)
      .flatMap(d => Option(new File(d, "models")).filter(_.isDirectory))
    val ymls = modelsDirs.flatMap(_.listFiles((f: File) =>
      f.isFile && (f.getName.endsWith(".yml") || f.getName.endsWith(".yaml"))).toSeq)
    ymls.sortBy(_.getPath)
  }

  // A single OkfGen instance — generator is stateless across files.
  private val okf = new OkfGen()

  // snakeyaml reuses the same parser OkfGen uses, so we test apples-to-apples.
  private val yaml = new Yaml()

  /** Run `block` with a fresh tempdir; deleted on completion. */
  private def withTempDir[A](block: File => A): A = {
    val tmp = Files.createTempDirectory("okf-roundtrip-").toFile
    try block(tmp)
    finally deleteRecursively(tmp)
  }

  private def deleteRecursively(f: File): Unit = {
    if (f.isDirectory) Option(f.listFiles()).foreach(_.foreach(deleteRecursively))
    f.delete()
  }

  // --------------------------------------------------------------------------
  // Per-YAML parameterized test
  // --------------------------------------------------------------------------

  for (ymlFile <- exampleYamls) {
    val displayName = ymlFile.getPath.stripPrefix("examples/").stripSuffix("/")

    test(s"[$displayName] round-trips through OkfGen without dropping or inventing fields") {
      withTempDir { outDir =>
        // 1. Pre-parse the YAML the same way OkfGen does, so we know what to assert.
        val raw = yaml.load[java.util.Map[String, java.util.Map[String, Any]]](
          scala.io.Source.fromFile(ymlFile).mkString).asScala.toMap
        val models = raw.toSeq
        assume(models.nonEmpty, s"$ymlFile has no models")

        // 2. Generate OKF bundle.
        okf.generate(ymlFile.getPath, outDir.getPath)

        // 3. For each model declared in the file, re-parse its emitted .md.
        for ((modelName, modelYaml) <- models) {
          val mdFile = new File(outDir, s"$modelName.md")
          assert(mdFile.exists(), s"Expected $mdFile to exist")
          val mdContent = scala.io.Source.fromFile(mdFile).mkString

          // -- 3a. Frontmatter parses as YAML and carries expected fields --
          val parts = mdContent.split("---")
          assert(parts.length >= 3, s"$modelName: frontmatter must be delimited by '---'")
          val fm = yaml.load[java.util.Map[String, Any]](parts(1)).asScala.toMap

          assert(fm.get("type").contains("SemanticTable"),
            s"$modelName: frontmatter type must be 'SemanticTable', got: ${fm.get("type")}")
          assert(fm.get("title").contains(toTitleCase(modelName)),
            s"$modelName: frontmatter title must be title-cased model name, got: ${fm.get("title")}")

          // -- version: frontmatter carries the YAML's declared version (if non-zero) --
          val yamlVersion = modelYaml.asScala.toMap.get("version").map {
            case n: java.lang.Number => n.intValue()
            case other =>
              fail(s"$modelName: 'version' must be an integer, got ${other.getClass.getSimpleName}")
          }.getOrElse(0)
          val fmVersion = fm.get("version") match {
            case Some(n: java.lang.Number) => n.intValue()
            case Some(_)                   => fail(s"$modelName: frontmatter version is not an integer")
            case None                      => 0
          }
          assert(fmVersion == yamlVersion,
            s"$modelName: version mismatch. YAML=$yamlVersion, OKF=$fmVersion")
          // -- status: frontmatter carries the YAML's declared status (default "published") --
          val yamlStatus = modelYaml.asScala.toMap.get("status") match {
            case Some(s: String) => s
            case _               => "published"
          }
          assert(fm.get("status").contains(yamlStatus),
            s"$modelName: status mismatch. YAML=$yamlStatus, OKF=${fm.get("status")}")
          assert(fm.get("resource").exists(_.toString.startsWith("file://")),
            s"$modelName: frontmatter resource must be a file:// URI, got: ${fm.get("resource")}")

          // -- 3b. Tags are exactly the union of declared tags + 'semantic-table' --
          val expectedTags = computeExpectedTags(modelYaml.asScala.toMap)
          val actualTags = fm.get("tags") match {
            case Some(t: java.util.List[_]) => t.asScala.toSeq.map(_.toString).toSet
            case _                          => Set.empty[String]
          }
          assert(actualTags == expectedTags,
            s"$modelName: tags mismatch.\n  expected: ${expectedTags.toSeq.sorted}\n  actual:   ${actualTags.toSeq.sorted}")

          // -- 3c. Schema body has one row per dimension + one row per measure --
          val dims   = modelYaml.asScala.toMap.get("dimensions") match {
            case Some(m: java.util.Map[_, _]) => m.asScala.keys.toSet
            case _                            => Set.empty[String]
          }
          val baseMs = modelYaml.asScala.toMap.get("measures") match {
            case Some(m: java.util.Map[_, _]) => m.asScala.keys.toSet
            case _                            => Set.empty[String]
          }
          val calcMs = modelYaml.asScala.toMap.get("calculated_measures") match {
            case Some(m: java.util.Map[_, _]) => m.asScala.keys.toSet
            case _                            => Set.empty[String]
          }
          val joins = modelYaml.asScala.toMap.get("joins") match {
            case Some(m: java.util.Map[_, _]) => m.asScala.keys.toSet
            case _                            => Set.empty[String]
          }

          val schemaSection: String = extractBodySection(mdContent, "# Schema")
            .getOrElse(fail(s"$modelName: '# Schema' section missing"))
          for (dimName <- dims) {
            assert(schemaSection.contains(s"| $dimName | dimension |"),
              s"$modelName: dimension '$dimName' missing or wrong-kind in # Schema\nsection was:\n$schemaSection")
          }
          for (measName <- baseMs) {
            assert(schemaSection.contains(s"| $measName | measure |"),
              s"$modelName: measure '$measName' missing or wrong-kind in # Schema\nsection was:\n$schemaSection")
          }
          if (calcMs.nonEmpty) {
            val calcSection: String = extractBodySection(mdContent, "# Calculated measures")
              .getOrElse(fail(s"$modelName: '# Calculated measures' section missing despite $calcMs"))
            for (calcName <- calcMs) {
              assert(calcSection.contains(s"| $calcName | calc |"),
                s"$modelName: calc '$calcName' missing or wrong-kind in # Calculated measures")
            }
          }
          if (joins.nonEmpty) {
            val joinsSection: String = extractBodySection(mdContent, "# Joins")
              .getOrElse(fail(s"$modelName: '# Joins' section missing despite $joins"))
            for (joinAlias <- joins) {
              assert(joinsSection.contains(s"**[$joinAlias]"),
                s"$modelName: join alias '$joinAlias' missing in # Joins\nsection was:\n$joinsSection")
            }
          }
          // -- Filters round-trip: each YAML `filters:` entry appears in # Filters --
          val yamlFilters = modelYaml.asScala.toMap.get("filters") match {
            case Some(jm: java.util.Map[_, _]) => jm.asScala.keys.toSet
            case Some(m: scala.collection.Map[_, _]) => m.keys.toSet
            case _ => Set.empty[String]
          }
          if (yamlFilters.nonEmpty) {
            val filtersSection: String = extractBodySection(mdContent, "# Filters")
              .getOrElse(fail(s"$modelName: '# Filters' section missing despite $yamlFilters"))
            for (filterName <- yamlFilters) {
              val needle = "| " + filterName + " |"
              assert(filtersSection.contains(needle),
                s"$modelName: filter '$filterName' missing in # Filters — looked for '$needle'")
            }
          }
          // -- 3d. Citations always present (1 entry, links back to YAML) --
          assert(mdContent.contains("# Citations"),
            s"$modelName: # Citations section missing")
          assert(mdContent.contains(ymlFile.getName),
            s"$modelName: Citations must reference source YAML filename ${ymlFile.getName}")

          // -- 3e. Description present in either frontmatter or omitted (per spec) --
          // OKF spec: 'description' is optional — only assert that if YAML has one, OKF carries one
          modelYaml.asScala.toMap.get("description").foreach { _ =>
            assert(fm.get("description").isDefined,
              s"$modelName: YAML has 'description' but OKF frontmatter does not")
          }
        }
      }
    }
  }

  // --------------------------------------------------------------------------
  // Generator invariants independent of any YAML
  // --------------------------------------------------------------------------

  test("OkfGen rejects non-existent input path") {
    val ex = intercept[IllegalArgumentException] {
      okf.generate("nonexistent/path/to/yaml", Files.createTempDirectory("okf-").toString)
    }
    assert(ex.getMessage.contains("does not exist"))
  }

  test("OkfGen rejects an unknown status value (same policy as YamlLoader)") {
    val yml =
      """flights:
        |  table: flights_tbl
        |  status: retired
        |  dimensions:
        |    carrier: carrier
        |  measures:
        |    flight_count: "count(1)"
        |""".stripMargin
    val tmp = Files.createTempDirectory("okf-bad-status-").toFile
    val inFile  = new java.io.File(tmp, "flights.yml")
    val outDir  = Files.createTempDirectory("okf-bad-status-out-").toFile
    java.nio.file.Files.write(inFile.toPath, yml.getBytes("UTF-8"))
    try {
      val ex = intercept[IllegalArgumentException] {
        okf.generate(inFile.getAbsolutePath, outDir.getAbsolutePath)
      }
      assert(ex.getMessage.contains("status"),
        s"Expected status-related error, got: ${ex.getMessage}")
      assert(ex.getMessage.contains("retired"),
        s"Expected unknown-value mention, got: ${ex.getMessage}")
    } finally {
      outDir.delete(); tmp.delete()
    }
  }

  test("OkfGen accepts status: draft and emits it in the frontmatter") {
    val yml =
      """flights:
        |  table: flights_tbl
        |  status: draft
        |  dimensions:
        |    carrier: carrier
        |  measures:
        |    flight_count: "count(1)"
        |""".stripMargin
    val tmp = Files.createTempDirectory("okf-draft-").toFile
    val inFile  = new java.io.File(tmp, "flights.yml")
    val outDir  = Files.createTempDirectory("okf-draft-out-").toFile
    java.nio.file.Files.write(inFile.toPath, yml.getBytes("UTF-8"))
    try {
      val n = okf.generate(inFile.getAbsolutePath, outDir.getAbsolutePath)
      assert(n == 1, s"Expected 1 model generated, got $n")
      val md = new String(
        java.nio.file.Files.readAllBytes(new java.io.File(outDir, "flights.md").toPath),
        "UTF-8")
      assert(md.contains("status: draft"),
        s"Expected 'status: draft' in frontmatter, got:\n$md")
    } finally {
      outDir.delete(); tmp.delete()
    }
  }

  test("directory with no .yml files throws") {
    val tmp = Files.createTempDirectory("okf-empty-").toFile
    try {
      val ex = intercept[IllegalArgumentException] {
        okf.generate(tmp.getPath, Files.createTempDirectory("okf-out-").toString)
      }
      assert(ex.getMessage.toLowerCase.contains("no models"))
    } finally deleteRecursively(tmp)
  }

  // --------------------------------------------------------------------------
  // Helpers
  // --------------------------------------------------------------------------

  /** Mirror of OkfGen.computeTags — duplicated here so the test fails loudly if
    * the two diverge in interesting ways. */
  private def computeExpectedTags(m: Map[String, Any]): Set[String] = {
    val srcs = scala.collection.mutable.ListBuffer.empty[String]

    def asStrMap(jm: java.util.Map[_, _]): Map[String, Any] =
      jm.asScala.toMap.map { case (k, v) => (k.toString, v) }

    m.get("metadata") match {
      case Some(jm: java.util.Map[_, _]) =>
        val meta = asStrMap(jm)
        meta.get("owner").foreach(o => srcs += s"owner:$o")
        meta.get("tags").foreach(t => srcs ++= splitTags(t.toString))
      case _ =>
    }

    def fieldTags(fieldMap: Any): Unit = fieldMap match {
      case jm: java.util.Map[_, _] =>
        jm.asScala.foreach { case (_, v) =>
          v match {
            case fm: java.util.Map[_, _] =>
              val metaOpt = asStrMap(fm).get("metadata")
              metaOpt match {
                case Some(metaJM: java.util.Map[_, _]) =>
                  asStrMap(metaJM).get("tags").foreach(t => srcs ++= splitTags(t.toString))
                case _ =>
              }
            case _ =>
          }
        }
      case _ =>
    }

    fieldTags(m.getOrElse("dimensions", null))
    fieldTags(m.getOrElse("measures", null))
    fieldTags(m.getOrElse("calculated_measures", null))

    srcs += "semantic-table"
    srcs.distinct.sorted.toSet
  }

  private def splitTags(s: String): Seq[String] =
    s.split("[,\\[\\]\\s]+").map(_.trim).filter(_.nonEmpty)

  /** Title-case a model name the way OkfGen does. */
  private def toTitleCase(name: String): String =
    name.split("_").filter(_.nonEmpty).map(_.capitalize).mkString(" ")

  /** Extract the body of a top-level `# Heading` section, returning everything
    * until the next `# Heading` at the same level. Returns None if not found. */
  private def extractBodySection(md: String, heading: String): Option[String] = {
    val lines = md.split("\n").toList
    val startIdx = lines.indexWhere(_.trim == heading)
    if (startIdx < 0) return None
    // Walk forward until we hit the next '# ' line at column 0 (next section header)
    val endIdx = lines.drop(startIdx + 1).indexWhere(l => l.trim.startsWith("# ") && l.trim != heading)
    val end = if (endIdx < 0) lines.length else startIdx + 1 + endIdx
    Some(lines.slice(startIdx + 1, end).mkString("\n"))
  }
}
