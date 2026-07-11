package io.semantica.tools

import org.apache.spark.sql.SparkSession


/** CLI entry point for semantica tooling.
  *
  * Run with:
  *   mvn scala:run -DmainClass=io.semantica.tools.Main -Dexec.args="<subcommand> ..."
  *
  * Subcommands:
  *   introspect  — read a data file, infer a starter YAML model
  *   docsgen     — read YAML model files, produce browsable HTML docs
  */
object Main {

  def main(args: Array[String]): Unit = {
    args.headOption match {
      case Some("docsgen") =>
        runDocsGen(args.tail)
      case Some("introspect") =>
        runIntrospect(args.tail)
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

  /** introspect — needs Spark session for DataFrame operations. */
  private def runIntrospect(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("semantica-introspect")
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

  private def printUsage(): Unit = {
    println(
      """semantica-tools — CLI utilities for the semantica semantic layer
        |
        |Run with:
        |  mvn exec:java -Dexec.mainClass=io.semantica.tools.Main -Dexec.args="..."
        |
        |Subcommands:
        |  docsgen    --path <file-or-dir> [--out FILE]
        |              Read YAML model files and produce browsable HTML documentation.
        |
        |  introspect --path <path> [--format parquet|csv|json] [--model NAME]
        |            [--max-measures N] [--sample-size N] [--out FILE]
        |            Read a data file and infer a starter YAML model.
        |
        |Examples:
        |  mvn exec:java -Dexec.args="docsgen --path models/ --out docs/index.html"
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
