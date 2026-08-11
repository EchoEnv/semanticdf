package io.semanticdf.spark

import io.semanticdf.core.engine.{MCPEngineRegistry, MCPEngineProvider}
import io.semanticdf.core.model.Model
import org.apache.spark.sql.SparkSession
import scala.jdk.CollectionConverters._

/** Java-friendly builder for the platform's engine registry.
 *
 *  v0.3.1: an alternative entry point for the engine registry that
 *  accepts Java types (java.util.Optional, java.util.Map) and optionally
 *  includes DuckDB and PostgreSQL providers. The DuckDB/PostgreSQL
 *  providers are loaded via reflection (semanticdf-spark does NOT
 *  depend on those adapters at compile time; the jars are loaded
 *  at runtime only when the providers are requested).
 */
object PlatformEngineRegistryJavaBuilder {

  /** Build the registry with optional DuckDB and PostgreSQL providers.
    *
    * @param spark             the shared SparkSession
    * @param duckdbEngine      optional DuckDB engine (java.util.Optional.empty() = skip)
    * @param postgresEngine    optional PostgreSQL engine (java.util.Optional.empty() = skip)
    * @param duckModelRegistry name → core.Model map for DuckDB queries
    * @param pgModelRegistry   name → core.Model map for PostgreSQL queries
    * @return the registry
    */
  def build(
      spark:           SparkSession,
      duckdbEngine:     java.util.Optional[AnyRef],
      postgresEngine:  java.util.Optional[AnyRef],
      duckModelRegistry: java.util.Map[String, Model],
      pgModelRegistry:   java.util.Map[String, Model],
  ): MCPEngineRegistry = {
    val sparkProvider: MCPEngineProvider = new SparkEngineProvider(
      spark,
      Map.empty[String, io.semanticdf.SemanticTable])

    val providers = scala.collection.mutable.Map[String, MCPEngineProvider](
      "spark" -> sparkProvider
    )

    duckdbEngine.ifPresent { engineObj =>
      tryLoadDuckDBProvider(engineObj, duckModelRegistry.asScala.toMap).foreach {
        duckProvider => providers += "duckdb" -> duckProvider
      }
    }

    postgresEngine.ifPresent { engineObj =>
      tryLoadPostgreSQLProvider(engineObj, pgModelRegistry.asScala.toMap).foreach {
        pgProvider =>
          val dbName = tryGetDatabaseName(engineObj)
          providers += s"postgresql:$dbName" -> pgProvider
      }
    }

    MCPEngineRegistry(
      engines = providers.toMap,
      default = "spark",
    )
  }

  /** Load DuckDBEngineProvider via reflection. Returns None if the
    * class isn't on the classpath or the reflection fails. */
  private def tryLoadDuckDBProvider(
      engine: AnyRef,
      models: Map[String, Model]
  ): Option[MCPEngineProvider] = {
    try {
      val cls = Class.forName("io.semanticdf.duckdb.DuckDBEngineProvider")
      val ctor = cls.getConstructors.head
      Some(ctor.newInstance(engine, models).asInstanceOf[MCPEngineProvider])
    } catch {
      case _: ClassNotFoundException => None
      case _: NoSuchMethodException => None
      case _: Exception => None
    }
  }

  /** Load PostgreSqlEngineProvider via reflection. */
  private def tryLoadPostgreSQLProvider(
      engine: AnyRef,
      models: Map[String, Model]
  ): Option[MCPEngineProvider] = {
    try {
      val cls = Class.forName("io.semanticdf.postgresql.PostgreSqlEngineProvider")
      // Per scala-error-handling: find the right constructor explicitly
      // (PostgreSqlClient, String, Map) by parameter types. The default
      // getConstructors().head may pick a synthetic Scala bridge.
      val pgClientCls = Class.forName("io.semanticdf.postgresql.PostgreSqlClient")
      val mapCls = classOf[java.util.Map[_, _]]
      val ctor = cls.getConstructors.find { c =>
        val ps = c.getParameterTypes
        ps.length == 3 &&
          ps(0).isAssignableFrom(pgClientCls) &&
          ps(1) == classOf[String] &&
          ps(2).isAssignableFrom(mapCls)
      }.getOrElse(cls.getConstructors.head)
      val dbName = tryGetDatabaseName(engine)
      Some(ctor.newInstance(engine, dbName, models).asInstanceOf[MCPEngineProvider])
    } catch {
      case _: ClassNotFoundException => None
      case _: Exception => None
    }
  }

  /** Reflectively read the `database` field from PostgreSqlEngine.
    *
    * Per scala-jvm-safety §1: PostgreSqlEngine's `database` is a
    * private val with NO public/private getter method (Scala doesn't
    * generate one for a constructor parameter without `val`).
    * `getMethod`/`getDeclaredMethod` both fail. The correct path
    * is `getDeclaredField` + `setAccessible(true)`.
    * Per scala-jvm-safety §1: wrap the `setAccessible(true)` in a
    * try-catch since SecurityException can be thrown when the JVM
    * has a SecurityManager (rare in modern apps but defensive).
    */
  private def tryGetDatabaseName(engine: AnyRef): String = {
    try {
      val field = engine.getClass.getDeclaredField("database")
      try field.setAccessible(true) catch { case _: SecurityException => }
      field.get(engine).asInstanceOf[String]
    } catch {
      case _: Exception => "postgresql"
    }
  }

  /** Spark-only convenience overload. Equivalent to calling
    * [[PlatformEngineRegistryBuilder.buildSparkOnly]] directly. */
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
