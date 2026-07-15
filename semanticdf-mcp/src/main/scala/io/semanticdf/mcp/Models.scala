package io.semanticdf.mcp

import io.semanticdf.SemanticTable
import io.semanticdf.YamlLoader
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.yaml.snakeyaml.Yaml

import java.io.FileInputStream
import java.util.{Map => JMap}
import scala.jdk.CollectionConverters._

/** The loaded-model registry — a thin wrapper around `YamlLoader.loadDir` that
  * also pre-resolves the `data:` block of the server config into `DataFrame`s
  * (per `mcp-contract.md` v2 §"Server lifecycle").
  *
  * Construction is eager: the data-config is read and every entry is loaded
  * through Spark before this class is returned. The SparkSession is taken as
  * a parameter (caller owns it — typically the singleton owned by [[Server]]).
  *
  * Tools read this via `models(name)` or `models.all` — no per-call Spark
  * initialization, no per-call config parsing. */
final class Models private[semanticdf] (
    val registry: Map[String, SemanticTable],
    val dataConfig: DataConfig,
) {
  def apply(name: String): SemanticTable =
    registry.get(name).getOrElse {
      val available = registry.keys.toList.sorted.mkString(", ")
      throw ModelNotFound(name, available)
    }
  def all: List[(String, SemanticTable)] = registry.toList.sortBy(_._1)
}

object Models {

  /** Load the model registry from a YAML model directory + a data-config file.
    *
    * @param modelsDir   directory of `*.yml` model files (passed to `YamlLoader.loadDir`)
    * @param dataConfig  parsed data sources; one entry per `table:` reference in the models
    * @param spark       the shared `SparkSession` (caller-owned)
    * @throws IllegalArgumentException if a model's `table:` reference has no matching
    *                                  data-config entry
    */
  def load(modelsDir: String, dataConfig: DataConfig, spark: SparkSession): Models = {
    val tables: Map[String, DataFrame] = dataConfig.entries.map { case (name, entry) =>
      name -> loadDataFrame(spark, entry)
    }
    val registry = YamlLoader.loadDir(modelsDir, tables)
    new Models(registry, dataConfig)
  }

  private def loadDataFrame(spark: SparkSession, entry: DataConfig.Entry): DataFrame = {
    val reader = entry.format.toLowerCase match {
      case "parquet" => spark.read
      case "csv"     => spark.read.format("csv")
      case "json"    => spark.read.format("json")
      case "delta"   => spark.read.format("delta")
      case other     => throw new IllegalArgumentException(s"Unsupported data-config format: '$other' (allowed: parquet, csv, json, delta)")
    }
    entry.readOptions.foldLeft(reader)((r, kv) => r.option(kv._1, kv._2))
      .load(entry.path)
  }
}

/** One entry in the server's data-config YAML — points at a file the server
  * reads once at startup and exposes to the model layer under `name`.
  *
  * Mirrors the `data:` block in `mcp-contract.md` v2 §"Server lifecycle":
  *
  *     data:
  *       flights_csv:
  *         path: /data/raw/flights
  *         format: parquet
  *         readOptions:
  *           mergeSchema: "true"
  */
final case class DataConfig(entries: Map[String, DataConfig.Entry]) {

  /** Names that should be available to models. Used as a pre-check before
    * `YamlLoader.loadDir` runs (catches typos in `table:` references). */
  def names: Set[String] = entries.keySet
}

object DataConfig {
  final case class Entry(
      path: String,
      format: String = "parquet",      // parquet | csv | json | delta
      readOptions: Map[String, String] = Map.empty,
  )

  /** Parse the data-config YAML file. Throws on unknown fields, missing
    * `path`, etc. The agent's config is treated as a hard contract — silent
    * acceptance of typos leads to confusing runtime errors later. */
  def fromFile(path: String): DataConfig = {
    val yaml = new Yaml()
    val rootRaw: JMap[String, AnyRef] = yaml.load(new FileInputStream(path))
    val dataBlock = Option(rootRaw.get("data"))
      .getOrElse(throw new IllegalArgumentException(s"data-config '$path' missing top-level 'data:' block"))
      .asInstanceOf[JMap[String, AnyRef]]

    val entries = dataBlock.asScala.toMap.map { case (name, raw) =>
      val m = raw.asInstanceOf[JMap[String, AnyRef]]
      val format = Option(m.get("format")).map(_.toString).getOrElse("parquet")
      val roRaw = Option(m.get("readOptions")).map(_.asInstanceOf[JMap[String, AnyRef]]).getOrElse(java.util.Collections.emptyMap())
      val readOptions = roRaw.asScala.toMap.map { case (k, v) => k -> v.toString }
      name -> Entry(path = m.get("path").toString, format = format, readOptions = readOptions)
    }
    DataConfig(entries)
  }
}

/** Thrown by `Models(name)` when a model is not in the loaded registry.
  * The MCP server maps this to the `MODEL_NOT_FOUND` error envelope. */
final case class ModelNotFound(name: String, available: String)
    extends RuntimeException(s"No model named '$name' is loaded. Available: $available")


