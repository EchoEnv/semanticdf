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
  *   query       — run a SQL string against a YAML model (no Scala needed)
  */
object Main {

  def main(args: Array[String]): Unit = {
    try {
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
        case Some("validate-joined-manifest") =>
          runValidateJoinedManifest(args.tail)
        case Some("query") =>
          runQuery(args.tail)
        case Some(cmd) =>
          System.err.println(s"Unknown subcommand: $cmd")
          printUsage()
          sys.exit(1)
        case None =>
          printUsage()
          sys.exit(1)
      }
    } catch {
      case e: IllegalArgumentException =>
        System.err.println(e.getMessage)
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
    * Spark (used to compile the source DataFrame shape).
    *
    * Required: `--yaml <file>` and `--id <FQN>` (the manifest's reverse-DNS
    * FQN identity). Optional: `--namespace <ns>` (default: `default`),
    * `--metadata-author k=v --metadata-license k=v ...` (repeated inline
    * flags; no separate file convention). */
  private def runManifest(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("semanticdf-manifest")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    try {
      val parser = new CliParser(args)
      val yamlPath  = parser.require("--yaml", "Usage: manifest --yaml <file> --id <FQN> [--out FILE] [--namespace NS] [--metadata-K V ...]")
      val outFile   = parser.option("--out", "")
      val idArg     = parser.require("--id", "manifest --yaml <file> --id <FQN> is required (recipe identity-bump §11 Q1)")
      val namespace = parser.option("--namespace", "default")
      val metadata  = parser.collectOptions("--metadata")   // repeated key=value flags

      val models = YamlLoader.load(yamlPath, spark)
      val identity = SemanticManifest.Identity(
        id              = idArg,
        manifestVersion = SemanticManifest.InitialManifestVersion,
        namespace       = namespace,
        metadata        = metadata,
      )
      val pretty = new StringBuilder
      models.values.foreach { m =>
        pretty.append(SemanticManifest.toJson(m, identity, prettyPrint = true))
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

  /** validate-joined-manifest — read a joined manifest, surface identity + digest.
    * Source-free (no Spark session required). Mirrors validate-manifest but
    * for the joined wire shape introduced in PR #151. */
  private def runValidateJoinedManifest(args: Array[String]): Unit = {
    val parser = new CliParser(args)
    val file   = parser.require("--file", "Usage: validate-joined-manifest --file <manifest.json>")
    val src    = scala.io.Source.fromFile(file)
    val text   = try src.getLines.mkString("\n") finally src.close()

    try {
      val meta = SemanticManifest.parseJoinedMeta(text)
      println(s"OK joined manifest")
      println(s"  schemaVersion    : ${meta.schemaVersion}")
      println(s"  kind             : ${meta.kind}")
      println(s"  modelName        : ${meta.modelName.getOrElse("(none)")}")
      println(s"  version          : ${meta.version}")
      println(s"  description      : ${meta.description.getOrElse("(none)")}")
      println(s"  identity.id      : ${meta.id.getOrElse("(none)")}")
      println(s"  identity.namespace : ${meta.namespace.getOrElse("(none)")}")
      println(s"  cardinality      : ${meta.cardinality}")
      println(s"  leftKeys         : ${meta.leftKeys.mkString(", ")}")
      println(s"  rightKeys        : ${meta.rightKeys.mkString(", ")}")
      println(s"  leftDimensions   : ${meta.leftDimensions}")
      println(s"  rightDimensions  : ${meta.rightDimensions}")
      println(s"  mergedDimensions : ${meta.mergedDimensions}")
      println(s"  leftMeasures     : ${meta.leftMeasures}")
      println(s"  rightMeasures    : ${meta.rightMeasures}")
      println(s"  mergedMeasures   : ${meta.mergedMeasures}")
      println(s"  isStreaming      : ${meta.isStreaming}")
      println(s"  warnings:")
      println(s"    - joined-manifest wire shape BLOCK §1 (recipe §10):")
      println(s"      semanticdfModel.join.on cannot be reconstructed from the wire;")
      println(s"      the restored SemanticTable won't execute its join without")
      println(s"      re-loading from YAML or supplying explicit join keys.")
    } catch {
      case e: ManifestParsingException =>
        System.err.println(s"Invalid joined manifest: ${e.getMessage}")
        sys.exit(1)
    }
  }

  /** query — run a SQL string against a YAML model. Ad-hoc exploration
    * without writing Scala. Uses [[SqlCli]] to map the SQL string to
    * [[SemanticTable.query]] parameters; the existing query() API handles
    * aggregation, filtering, ordering, and limits.
    *
    * Required: `--models <dir-or-file>` and `--sql '<sql>'`.
    * Optional: `--model <name>` (when --models points at a dir of multiple
    * models, the FROM clause in the SQL can also pick the model).
    *
    * Output: prints the result rows as `col1\tcol2\t...` to stdout. */
  private def runQuery(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("semanticdf-query")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    try {
      val parser = new CliParser(args)
      val modelsPath = parser.require("--models", "Usage: query --models <dir-or-file> --sql '<SQL>'")
      val sql        = parser.require("--sql",     "Usage: query --models <dir-or-file> --sql '<SQL>'")

      val models = YamlLoader.loadDir(modelsPath, spark)
      val parsed = SqlCli.parse(sql)
      val model  = models.getOrElse(
        parsed.model,
        throw new IllegalArgumentException(
          s"SQL references model '${parsed.model}' but models dir doesn't contain it. " +
          s"Available: ${models.keys.toSeq.sorted.mkString(", ")}."))
      val result = SqlCli(parsed, model).toDataFrame(spark)
      result.show(truncate = false)
    } finally spark.stop()
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
        |  query --models <dir-or-file> --sql '<sql>'
        |          Run a SQL string against a YAML model. Ad-hoc exploration
        |          without writing Scala. Supports SELECT, FROM, WHERE (AND/OR),
        |          ORDER BY (ASC/DESC), LIMIT, GROUP BY (ignored), aliases.
        |          The model decides which fields are dims vs measures.
        |
        |Examples:
        |  mvn exec:java -Dexec.args="docsgen --path models/ --out docs/index.html"
        |  mvn exec:java -Dexec.args="okfgen --path models/ --out agents/"
        |  mvn exec:java -Dexec.args="introspect --path data/orders.csv --format csv --model orders"
        |  mvn exec:java -Dexec.args="query --models examples/starter/models/ --sql 'SELECT carrier, total_passengers FROM flights GROUP BY carrier ORDER BY total_passengers DESC LIMIT 10'"
        |""".stripMargin)
  }

  /** Tiny CLI argument parser — avoids pulling in a dependency. */
  private[tools] class CliParser(args: Array[String]) {
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
      map.getOrElse(name, throw new IllegalArgumentException(
        s"Missing required --$name\n$usageHint"
      ))
    /** Return all parsed flags whose name starts with `prefix`, with the
      * prefix and a leading separator (`-`) stripped. Supports repeated
      * `<prefix>-KEY VALUE` pairs as a uniform inline convention (used
      * for `--metadata-author X --metadata-license Y` etc.). */
    def collectOptions(prefix: String): Map[String, String] =
      map.toMap.iterator.flatMap { case (k, v) =>
        if (k == prefix)              None      // bare --metadata, ignored
        else if (k.startsWith(prefix)) {
          val key = k.stripPrefix(prefix).stripPrefix("-")
          if (key.isEmpty) None else Some(key -> v)
        } else None
      }.toMap
  }

  /** Test-only helper exposed for `ManifestWriteSpec`. Wraps the `CliParser`
    * instance method `collectOptions` so tests can construct a metadata
    * map from repeated `--metadata-K V` flags without spinning up a
    * full CLI invocation. Placed AFTER the `CliParser` class (it depends
    * on it); it's still inside `object Main`. */
  private[tools] def collectOptionsForTest(args: Array[String]): Map[String, String] =
    new CliParser(args).collectOptions("--metadata")
}
