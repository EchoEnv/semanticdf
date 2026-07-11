package io.semantica.tools

import io.semantica.{Dimension, Measure, SemanticTable, toSemanticTable}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


/** CLI entry point for semantica tooling.
  *
  * Currently supports one subcommand:
  *   introspect <path> [--format parquet|csv|json] [--max-measures N] [--model NAME]
  *   [--sample-size N] [--out FILE]
  *
  * Reads a data file, infers a starter YAML model, prints it (or writes to file).
  *
  * Run with:
  *   mvn scala:run -DmainClass=io.semantica.tools.Main \
  *       -Dexec.args="introspect /path/to/data --format parquet --model orders"
  */
object Main {

  /** Public so Maven exec args parsing is trivial. */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("semantica-introspect")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    try {
      args.headOption match {
        case Some("introspect") => runIntrospect(spark, args.tail)
        case Some(cmd) =>
          System.err.println(s"Unknown subcommand: $cmd")
          printUsage()
          sys.exit(1)
        case None =>
          printUsage()
          sys.exit(1)
      }
    } finally spark.stop()
  }

  private def runIntrospect(spark: SparkSession, args: Array[String]): Unit = {
    val parser = new CliParser(args)
    val path        = parser.require("--path", "Usage: introspect --path <path> [--format parquet|csv|json] ...")
    val format      = parser.option("--format", "parquet")
    val modelName   = parser.option("--model", "model")
    val maxMeasures = parser.option("--max-measures", "8").toInt
    val sampleSize  = parser.option("--sample-size", "100").toInt
    val outFile     = parser.option("--out", "")
    val readOpts    = parser.collectOptions("--option")

    val yaml = try {
      val introspector = new Introspector(
        Introspector.Config(maxMeasures = maxMeasures, sampleSize = sampleSize)
      )
      introspector.fromFile(spark, path, format, modelName, readOpts)
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
  }

  private def printUsage(): Unit = {
    println(
      """semantica-tools — CLI utilities for the semantica semantic layer
        |
        |Usage:
        |  introspect --path <path> [--format parquet|csv|json] [--model NAME]
        |            [--max-measures N] [--sample-size N] [--out FILE]
        |            [--option key=value]...
        |
        |Examples:
        |  introspect --path s3://bucket/orders/ --format parquet --model orders
        |  introspect --path ./data/orders.csv --format csv --model orders --max-measures 5
        |""".stripMargin)
  }

  /** Tiny CLI argument parser — avoids pulling in a dependency. */
  private class CliParser(args: Array[String]) {
    private val map = scala.collection.mutable.LinkedHashMap[String, String]()
    private val extras = scala.collection.mutable.LinkedHashMap[String, String]()
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
            val key = flag.drop(2)
            val value = if (i + 1 < args.length && !args(i + 1).startsWith("--")) {
              i += 1
              args(i)
            } else {
              "true"
            }
            map(key) = value
            i += 1
          case positional =>
            extras(positional) = ""
            i += 1
        }
      }
    }

    def option(name: String, default: String): String = map.getOrElse(name, default)
    def require(name: String, usageHint: String): String =
      map.getOrElse(name, {
        System.err.println(s"Missing required --${name.drop(2)}")
        System.err.println(usageHint)
        sys.exit(1)
      })
    def collectOptions(prefix: String): Map[String, String] = optionArgs.toMap
  }
}
