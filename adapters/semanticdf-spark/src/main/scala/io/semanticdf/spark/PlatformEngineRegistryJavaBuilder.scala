package io.semanticdf.spark

import io.semanticdf.core.engine.{MCPEngineRegistry, MCPEngineProvider}
import io.semanticdf.core.model.Model
import org.apache.spark.sql.SparkSession
import scala.jdk.CollectionConverters._

/** Java-friendly builder for the platform's engine registry.
 *
 *  Per the v0.3.1 Platform migration design doc (PR #443): the
 *  platform wires an `MCPEngineRegistry` so the QueryService can
 *  route queries through the engine-portable path.
 *
 *  This helper accepts Java types (java.util.Optional,
 *  java.util.Map) and converts them to Scala internally.
 *
 *  DuckDB/PostgreSQL providers are loaded via reflection (the
 *  platform pom doesn't depend on those adapters by default;
 *  when those jars are on the classpath, the providers are
 *  loaded automatically).
 */
object PlatformEngineRegistryJavaBuilder {

  /** Build the registry with optional DuckDB and PostgreSQL providers. */
  def build(
      spark:        SparkSession,
      duckdb:       java.util.Optional[AnyRef],
      postgres:     java.util.Optional[AnyRef],
      duckModelRegistry: java.util.Map[String, Model],
      pgModelRegistry:   java.util.Map[String, Model],
  ): MCPEngineRegistry = {
    val sparkProvider: MCPEngineProvider = new SparkEngineProvider(
      spark,
      Map.empty[String, io.semanticdf.SemanticTable])

    val providers = scala.collection.mutable.Map[String, MCPEngineProvider](
      "spark" -> sparkProvider
    )

    // DuckDB provider via reflection (semanticdf-duckdb may or may not be on classpath)
    if (duckdb.isPresent) {
      val engine = duckdb.get().asInstanceOf[AnyRef]
      val duckProvider = tryLoadDuckDBProvider(engine, duckModelRegistry.asScala.toMap)
      if (duckProvider != null) {
        providers += "duckdb" -> duckProvider
      }
    }

    // PostgreSQL provider via reflection
    if (postgres.isPresent) {
      val engine = postgres.get().asInstanceOf[AnyRef]
      val pgProvider = tryLoadPostgreSQLProvider(engine, pgModelRegistry.asScala.toMap)
      if (pgProvider != null) {
        val dbName = tryGetDatabaseName(engine)
        providers += s"postgresql:$dbName" -> pgProvider
      }
    }

    MCPEngineRegistry(
      engines = providers.toMap,
      default = "spark",
    )
  }

  /** Load DuckDBEngineProvider via reflection (avoids hard
    * dependency on the duckdb adapter). Returns null if the class
    * isn't on the classpath (graceful degradation). */
  private def tryLoadDuckDBProvider(
      engine: AnyRef,
      models: Map[String, Model]
  ): MCPEngineProvider = {
    try {
      val cls = Class.forName("io.semanticdf.duckdb.DuckDBEngineProvider")
      val ctor = cls.getConstructors.head
      ctor.newInstance(engine, models).asInstanceOf[MCPEngineProvider]
    } catch {
      case _: ClassNotFoundException => null
      case _: NoSuchMethodException => null
      case _: Exception => null
    }
  }

  /** Load PostgreSqlEngineProvider via reflection. */
  private def tryLoadPostgreSQLProvider(
      engine: AnyRef,
      models: Map[String, Model]
  ): MCPEngineProvider = {
    try {
      val cls = Class.forName("io.semanticdf.postgresql.PostgreSqlEngineProvider")
      val ctor = cls.getConstructors.head
      val dbField = engine.getClass.getMethod("database").invoke(engine).asInstanceOf[String]
      ctor.newInstance(engine, dbField, models).asInstanceOf[MCPEngineProvider]
    } catch {
      case _: ClassNotFoundException => null
      case _: Exception => null
    }
  }

  /** Reflectively read the `database` field from PostgreSqlEngine. */
  private def tryGetDatabaseName(engine: AnyRef): String = {
    try {
      engine.getClass.getMethod("database").invoke(engine).asInstanceOf[String]
    } catch {
      case _: Exception => "postgresql"
    }
  }

  /** Spark-only convenience overload. */
  def buildSparkOnly(spark: SparkSession): MCPEngineRegistry = {
    build(
      spark,
      java.util.Optional.empty[AnyRef](),
      java.util.Optional.empty[AnyRef](),
      java.util.Collections.emptyMap[String, Model](),
      java.util.Collections.emptyMap[String, Model]()
    )
  }
}
