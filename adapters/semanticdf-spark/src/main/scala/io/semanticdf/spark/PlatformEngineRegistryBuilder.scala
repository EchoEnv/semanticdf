package io.semanticdf.spark

import io.semanticdf.core.engine.{MCPEngineRegistry, MCPEngineProvider}
import org.apache.spark.sql.SparkSession

/** Builder helper for the platform's engine registry.
  *
  * Per the v0.3.1 Platform migration design doc (PR #443): the
  * platform wires an `MCPEngineRegistry` so the QueryService can
  * route queries through the engine-portable path. For v0.3.1, only
  * Spark is registered (no other engine providers).
  *
  * ==Why a Scala-side helper==
  *
  * Constructing Scala collections from Java is messy
  * (Map.empty, Tuple2.apply, Map.canBuildFrom). This helper
  * encapsulates the Scala-side construction so the Java platform
  * only needs one static call.
  *
  * ==Java interop==
  *
  * Scala companion objects expose their `apply` / `buildX` methods
  * as static forwarders via the `PlatformEngineRegistryBuilder$.MODULE$`
  * syntax. For convenience, we also expose a Java-friendly static
  * method `buildSparkDefaultStatic` that delegates to `buildSparkDefault`.
  * Java callers should use the static method (not the Scala one).
  *
  * ==Spark-only for v0.3.1==
  *
  * Future work: register Trino / DuckDB / PG / Hera / UC / HMS
  * providers as well, each with their own availability checks.
  * For now, the registry has only `spark` and the default is `spark`.
  */
object PlatformEngineRegistryBuilder {

  /** Build the platform's engine registry with Spark as the default
    * engine.
    *
    * @param spark the shared SparkSession (for the SparkEngineProvider)
    * @return the registry; throws IllegalArgumentException if the
    *         default engine is unavailable at startup (per
    *         MCPEngineRegistry doc: "misconfigured boots must fail loud")
    */
  def buildSparkDefault(spark: SparkSession): MCPEngineRegistry = {
    val sparkProvider: MCPEngineProvider = new SparkEngineProvider(
      spark,
      Map.empty[String, io.semanticdf.SemanticTable])
    MCPEngineRegistry(
      engines = Map[String, MCPEngineProvider]("spark" -> sparkProvider),
      default = "spark",
    )
  }

  /** Java-friendly static entry point. Java callers should use this
    * method (not the Scala one) — the Scala-side `buildSparkDefault`
    * is accessible but requires `MODULE$.buildSparkDefault(...)`. */
  def buildSparkDefaultStatic(spark: SparkSession): MCPEngineRegistry =
    buildSparkDefault(spark)
}
