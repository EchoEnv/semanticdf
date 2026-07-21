package io.semanticdf.tools

import org.apache.spark.sql.SparkSession

import io.semanticdf.{ManifestParsingException, SemanticManifest, YamlLoader}


/** CLI entry point for semanticdf tooling.
  *
  * Run with:
  *   mvn scala:run -DmainClass=io.semanticdf.tools.Main -Dexec.args="<subcommand> ..."
  *
  * Subcommands:
  *   introspect  — read a data file, infer a starter YAML model
  *   docsgen     — read YAML model files, produce browsable HTML docs
  *   okfgen      — read YAML model files, emit OKF knowledge bundle (.md)
  */
object Main {

  def main(args: Array[String]): Unit = {
    args.headOption match {
      case Some("docsgen") =>
        runDocsGen(args.tail)
      case Some("okfgen") =>
        runOkfGen(args.tail)
      case Some("introspect") =>
        runIntrospect(args.tail)
      case Some("manifest") =>
        runManifest(args.tail)
      case Some("validate-manifest") =>
        runValidateManifest(args.tail)
      case Some(cmd) =>
        System.err.println(s"Unknown subcommand: $cmd")
        printUsage()
        sys.exit(1)
      case None =>
        printUsage()
        sys.exit(1)
    }
  }

  /** docsgen — no Spark needed, runs pure YAML → HTML. */
  private def runDocsGen(args: Array[String]): Unit = {
    val parser = new CliParser(args)
    val path    = parser.require("--path", "Usage: docsgen --path <file-or-dir> [--out FILE]")
    val outFile = parser.option("--out", "")
    val html = new DocsGen().fromFile(path)
    if (outFile.nonEmpty) {
      new DocsGen().write(outFile, html)
      println(s"Wrote HTML docs to $outFile")
    } else {
      println(html)
    }
  }

  /** okfgen — no Spark needed, runs pure YAML → OKF .md bundle. */
  private def runOkfGen(args: Array[String]): Unit = {
    val parser = new CliParser(args)
    val path   = parser.require("--path", "Usage: okfgen --path <file-or-dir> --out <bundle-dir>")
    val outDir = parser.require("--out",  "Usage: okfgen --path <file-or-dir> --out <bundle-dir>")
    try {
      val n = new OkfGen().generate(path, outDir)
      println(s"Wrote $n OKF concept document(s) under $outDir")
    } catch {
      case e: Exception =>
        System.err.println(s"okfgen failed: ${e.getMessage}")
        sys.exit(1)
    }
  }

  /** introspect — needs Spark session for DataFrame operations. */
  private def runIntrospect(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("semanticdf-introspect")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    try {
      val parser = new CliParser(args)
      val path        = parser.require("--path", "Usage: introspect --path <path> [--format parquet|csv|json] ...")
      val format      = parser.option("--format", "parquet")
      val modelName  = parser.option("--model", "model")
      val maxMeasures = parser.option("--max-measures", "8").toInt
      val sampleSize = parser.option("--sample-size", "100").toInt
      val outFile    = parser.option("--out", "")
      val readOpts   = parser.collectOptions("--option")

      val yaml = try {
        new Introspector(Introspector.Config(maxMeasures = maxMeasures, sampleSize = sampleSize))
          .fromFile(spark, path, format, modelName, readOpts)
      } catch {
        case e: Exception =>
          System.err.println(s"Failed to introspect $path: ${e.getMessage}")
          sys.exit(1)
      }

      if (outFile.nonEmpty) {
        val pw = new java.io.PrintWriter(new java.io.File(outFile))
        try pw.write(yaml) finally pw.close()
        println(s"Wrote YAML model to $outFile")
      } else {
        println(yaml)
      }
    } finally spark.stop()
  }

  /** manifest — emit a JSON manifest artifact for a YAML model. Needs
    * Spark (used to compile the source DataFrame shape). */
  private def runManifest(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("semanticdf-manifest")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    try {
      val parser = new CliParser(args)
      val yamlPath = parser.require("--yaml", "Usage: manifest --yaml <file> [--out FILE]")
      val outFile  = parser.option("--out", "")

      val models = YamlLoader.load(yamlPath, spark)
      val pretty = new StringBuilder
      models.values.foreach { m =>
        pretty.append(SemanticManifest.toJson(m, prettyPrint = true))
        pretty.append('\n')
      }

      val payload = pretty.toString
      if (outFile.nonEmpty) {
        val pw = new java.io.PrintWriter(new java.io.File(outFile))
        try pw.write(payload) finally pw.close()
        println(s"Wrote ${models.values.size} manifest(s) to $outFile")
      } else {
        println(payload)
      }
    } finally spark.stop()
  }

  /** validate-manifest — read a JSON manifest, surface identity + digest.
    * Source-free (uses parseMeta). No Spark needed. */
  private def runValidateManifest(args: Array[String]): Unit = {
    val parser = new CliParser(args)
    val file   = parser.require("--file", "Usage: validate-manifest --file <manifest.json>")
    val src    = scala.io.Source.fromFile(file)
    val text   = try src.getLines.mkString("\n") finally src.close()

    try {
      val meta = SemanticManifest.parseMeta(text)
      println(s"OK manifest")
      println(s"  schemaVersion : ${meta.schemaVersion}")
      println(s"  kind          : ${meta.kind}")
      println(s"  modelName     : ${meta.modelName.getOrElse("(none)")}")
      println(s"  version       : ${meta.version}")
      println(s"  description   : ${meta.description.getOrElse("(none)")}")
      println(s"  sourceTable   : ${meta.sourceTable.getOrElse("(none)")}")
      println(s"  dimensions    : ${meta.dimensions}")
      println(s"  measures      : ${meta.measures}  (calc: ${meta.calcMeasures})")
      println(s"  joins         : ${meta.joins}")
      println(s"  filters       : ${meta.filters}")
      println(s"  isStreaming   : ${meta.isStreaming}")
      println(s"  usesTAll      : ${meta.usesTAll}")
    } catch {
      case e: ManifestParsingException =>
        System.err.println(s"Invalid manifest: ${e.getMessage}")
        sys.exit(1)
    }
  }

  private def printUsage(): Unit = {
    println(
      """semanticdf-tools — CLI utilities for the semanticdf semantic layer
        |
        |Run with:
        |  mvn exec:java -Dexec.mainClass=io.semanticdf.tools.Main -Dexec.args="..."
        |
        |Subcommands:
        |  docsgen    --path <file-or-dir> [--out FILE]
        |              Read YAML model files and produce browsable HTML documentation.
        |
        |  okfgen     --path <file-or-dir> --out <bundle-dir>
        |              Read YAML model files and emit an OKF v0.1 knowledge bundle
        |              (.md files with YAML frontmatter). Source: docs/agents/okf-mapping.md.
        |
        |  introspect --path <path> [--format parquet|csv|json] [--model NAME]
        |            [--max-measures N] [--sample-size N] [--out FILE]
        |            Read a data file and infer a starter YAML model.
        |
        |  manifest --yaml <file> [--out FILE]
        |            Read a YAML model and emit a JSON manifest artifact (one
        |            per model, separated by blank lines). The manifest is
        |            portable metadata only — it does NOT carry computed
        |            results. Operator owns data lifecycle.
        |
        |  validate-manifest --file <manifest.json>
        |            Read a JSON manifest and print its identity + digest
        |            header. Source-free (no Spark session required).
        |
        |Examples:
        |  mvn exec:java -Dexec.args="docsgen --path models/ --out docs/index.html"
        |  mvn exec:java -Dexec.args="okfgen --path models/ --out agents/"
        |  mvn exec:java -Dexec.args="introspect --path data/orders.csv --format csv --model orders"
        |""".stripMargin)
  }

  /** Tiny CLI argument parser — avoids pulling in a dependency. */
  private class CliParser(args: Array[String]) {
    private val map        = scala.collection.mutable.LinkedHashMap[String, String]()
    private val optionArgs = scala.collection.mutable.Map[String, String]()

    parse()

    private def parse(): Unit = {
      var i = 0
      while (i < args.length) {
        args(i) match {
          case "--option" =>
            Predef.require(i + 1 < args.length, "--option requires key=value")
            val kv = args(i + 1).split("=", 2)
            Predef.require(kv.length == 2, s"--option expects key=value, got: ${args(i + 1)}")
            optionArgs(kv(0)) = kv(1)
            i += 2
          case flag if flag.startsWith("--") =>
            val key   = flag  // store full "--path" so require("--path", ..) finds it
            val value = if (i + 1 < args.length && !args(i + 1).startsWith("--")) {
              i += 1; args(i)
            } else "true"
            map(key) = value
            i += 1
          case _ =>
            i += 1
        }
      }
    }

    def option(name: String, default: String): String = map.getOrElse(name, default)
    def require(name: String, usageHint: String): String =
      map.getOrElse(name, {
        System.err.println(s"Missing required --$name")
        System.err.println(usageHint)
        sys.exit(1)
      })
    def collectOptions(prefix: String): Map[String, String] = optionArgs.toMap
  }
}
