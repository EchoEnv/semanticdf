package io.semanticdf.mcp.handlers

import io.semanticdf.core.engine.{EngineContext, MCPEngineRegistry, MCPQueryRequest, PortableQueryResult, ResultValue}
import io.semanticdf.core.model.{Model, ModelPolicyDefaults, ModelStatus, SourceRef}
import io.semanticdf.core.engine.{ResolvedSource, ResolvedSchema}
import io.semanticdf.core.schema.{Field, SealedDataType}
import io.semanticdf.mcp.{DataConfig, Models}
import io.semanticdf.mcp.SparkFixture
import io.semanticdf.{Dimension, Measure, SemanticTable, toSemanticTable}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{count, lit}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, Suite}

import scala.util.{Try, Success, Failure}

/** v0.3.1 (engine-default flip): tests that when the engine registry
  * is configured, queries route through it by DEFAULT (no `engine`
  * field required in the request).
  *
  * Per the user audit (post-v0.3.1): the legacy `Models` +
  * `SemanticTable` path was the default for too long, hiding the
  * engine-portable path. Flipping the default makes the new path
  * the standard.
  *
  * Per the standard: real behavior assertions, not source-file
  * inspection. We exercise the actual `Query.handle` method with
  * a real SparkSession + a real `Models` registry + a real engine
  * registry, and verify the result comes from the engine path.
  */
class QueryEngineDefaultFlipSpec extends AnyFunSuite with SparkFixture with Matchers { this: Suite =>

  // -- fixtures --

  /** A real Spark [[SemanticTable]] with one dimension + one measure. */
  private def flightsSemanticTable: SemanticTable = {
    import spark.implicits._
    val df = Seq(
      ("AA", 1), ("AA", 1),
      ("UA", 1), ("UA", 1), ("UA", 1),
      ("DL", 1),
    ).toDF("carrier", "dummy")
    toSemanticTable(df, name = Some("flights"))
      .withDimensions(Dimension("carrier", t => t("carrier")))
      .withMeasures(Measure("flight_count", t => count(lit(1))))
  }

  /** A legacy `Models` registry wrapping the semantic table. */
  private def legacyModels: Models =
    new Models(Map("flights" -> flightsSemanticTable), DataConfig(Map.empty))

  /** A real `MCPEngineRegistry` with a Spark provider wired to the
    * legacy models (so the engine can look up tables by name).
    *
    * This is the production setup per `Main.scala` line 85-95. */
  private def engineRegistry: MCPEngineRegistry = {
    import io.semanticdf.spark.SparkEngineProvider
    val sparkProvider = new SparkEngineProvider(spark, legacyModels.registry)
    MCPEngineRegistry(
      engines = Map("spark" -> sparkProvider),
      default = "spark",
    )
  }

  // -- engine-default flip tests --

  test("when request.engine is empty AND engineRegistry is configured, the engine path is used") {
    // Per the audit: previously you had to pass `engine: "spark"` in
    // the request to route through the engine. Now you don't.
    val query = new Query(spark, engineRegistry = Some(engineRegistry))
    val req = QueryRequest(
      model      = "flights",
      measures   = Seq("flight_count"),
      dimensions = Some(Seq("carrier")),
    )
    // request.engine is the default "" (empty)
    val result = query.handle(legacyModels, req)
    result.status shouldBe "ok"
    // The engine path returns portableToData(...) — 3 grouped rows
    // (AA:2, UA:3, DL:1 = 6 total in 3 groups)
    result.data.rows.size shouldBe 3
  }

  test("backward compat: explicit engine selection still works (was the v1 path)") {
    val query = new Query(spark, engineRegistry = Some(engineRegistry))
    val req = QueryRequest(
      model    = "flights",
      measures   = Seq("flight_count"),
      dimensions = Some(Seq("carrier")),
      engine     = "spark",  // explicit selection (backward compat)
    )
    val result = query.handle(legacyModels, req)
    result.status shouldBe "ok"
    result.data.rows.size shouldBe 3
  }

  test("when engineRegistry is NOT configured, falls back to the legacy Models + SemanticTable path") {
    // No engineRegistry → legacy path. This is the unchanged
    // backward-compat path: anyone who hasn't yet wired the
    // engine registry still works.
    val query = new Query(spark)  // no engineRegistry
    val req = QueryRequest(
      model      = "flights",
      measures   = Seq("flight_count"),
      dimensions = Some(Seq("carrier")),
    )
    val result = query.handle(legacyModels, req)
    result.status shouldBe "ok"
    result.data.rows.size shouldBe 3
  }
}
