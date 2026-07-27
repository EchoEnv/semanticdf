package io.semanticdf.mcp.handlers

import io.modelcontextprotocol.json.McpJsonMapper
import io.semanticdf.adapters.YamlLoader
import io.semanticdf.mcp.{DataConfig, Handlers, Models, SparkFixture}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Tests for the `lineage` MCP handler.
  *
  * The handler is a thin adapter over the `io.semanticdf.lineage.Lineage`
  * library; the library's own tests cover the lineage build. These tests
  * cover the handler's wire shape (the JSON DTOs), the field-name
  * filter, the error path, and the parseRequest helpers. */
class LineageSpec extends AnyFunSuite with Matchers with SparkFixture {

  // The MCP SDK's McpJsonMapper is a Jackson-with-Scala-module instance.
  // We use the project's own JsonSupport to keep the test consistent
  // with the rest of the MCP module.
  private lazy val mapper: McpJsonMapper = io.semanticdf.mcp.JsonSupport.scalaMapper()

  // Build a Models registry from the existing sql-cli-fixtures (shared
  // with the existing SqlCliEndToEndSpec). One model: `flights`.
  // We have to also register the temp view because YamlLoader resolves
  // `table:` references against the catalog.
  private def setupModels(): Models = {
    val s = spark
    val rows = s.sparkContext.parallelize(Seq(
      org.apache.spark.sql.Row("AA", 100L, 50L),
      org.apache.spark.sql.Row("UA", 200L, 75L),
    ))
    val schema = org.apache.spark.sql.types.StructType(Seq(
      org.apache.spark.sql.types.StructField("carrier",   org.apache.spark.sql.types.StringType),
      org.apache.spark.sql.types.StructField("distance",  org.apache.spark.sql.types.LongType),
      org.apache.spark.sql.types.StructField("passengers", org.apache.spark.sql.types.LongType),
    ))
    s.createDataFrame(rows, schema).createOrReplaceTempView("flights_csv")

    val tables  = Map("flights_csv" -> s.table("flights_csv"))
    val loaded = YamlLoader.load("src/test/resources/sql-cli-fixtures/flights.yml", tables)
    // No data-config block in the fixture; an empty DataConfig works
    // for tests that don't use data-config-aware features.
    new Models(loaded, DataConfig(Map.empty))
  }

  test("getFieldLineage: returns the model's full lineage (no filter)") {
    val models = setupModels()
    val handler = new Lineage(models)
    val out = handler.getFieldLineage(GetFieldLineageRequest("flights", field_name = None))
    assert(out.status == "ok")
    val data = out.data
    assert(data.model == "flights")
    // fixture's flights.yml has 1 dimension + 1 measure
    assert(data.dimensions.length == 1, s"expected 1 dimension, got ${data.dimensions}")
    assert(data.dimensions.head.name == "carrier")
    assert(data.measures.length == 1, s"expected 1 measure, got ${data.measures}")
    assert(data.measures.head.name == "total_passengers")
    // No transforms, no joins, no upstream
    assert(data.transforms.isEmpty)
    assert(data.kind == "batch")
  }

  test("getFieldLineage: filter to a single field returns one entry") {
    val models = setupModels()
    val handler = new Lineage(models)
    val out = handler.getFieldLineage(GetFieldLineageRequest("flights", field_name = Some("carrier")))
    val data = out.data
    assert(data.dimensions.length == 1)
    assert(data.dimensions.head.name == "carrier")
    assert(data.measures.isEmpty, s"expected no measures (filtered out), got ${data.measures}")
    assert(data.transforms.isEmpty)
  }

  test("getFieldLineage: unknown field returns empty lists (not an error)") {
    val models = setupModels()
    val handler = new Lineage(models)
    val out = handler.getFieldLineage(GetFieldLineageRequest("flights", field_name = Some("no_such_field")))
    // The handler treats "field not found" as a successful empty
    // result, not a hard error. This is friendlier for an LLM
    // that typo-ed the field name.
    assert(out.status == "ok")
    val data = out.data
    assert(data.dimensions.isEmpty)
    assert(data.measures.isEmpty)
    assert(data.transforms.isEmpty)
  }

  test("getFieldLineage: unknown model raises ModelNotFound") {
    val models = setupModels()
    val handler = new Lineage(models)
    val ex = intercept[Exception] {
      handler.getFieldLineage(GetFieldLineageRequest("nope", field_name = None))
    }
    // Models.apply throws ModelNotFound, which the SDK adapter
    // catches and turns into an error envelope. We just verify the
    // exception class is what the Models layer raises.
    assert(ex.getClass.getSimpleName.contains("ModelNotFound"),
      s"expected ModelNotFound, got: ${ex.getClass.getName}: ${ex.getMessage}")
  }

  test("getDependencies: single-model workspace has empty upstream/downstream") {
    val models = setupModels()
    val handler = new Lineage(models)
    val out = handler.getDependencies(GetDependenciesRequest("flights"))
    assert(out.status == "ok")
    val data = out.data
    assert(data.model == "flights")
    assert(data.upstream.isEmpty,   s"expected no upstream, got ${data.upstream}")
    assert(data.downstream.isEmpty, s"expected no downstream, got ${data.downstream}")
  }

  test("JSON wire shape: getFieldLineageData round-trips through the McpJsonMapper") {
    val models = setupModels()
    val handler = new Lineage(models)
    val env = handler.getFieldLineage(GetFieldLineageRequest("flights", field_name = Some("carrier")))
    // The MCP server returns the Envelope as a text-result JSON; the
    // SDK adapter is responsible for the actual transport. For the
    // test we just verify that the data serialises to a sensible
    // shape via the McpJsonMapper.
    val text = Handlers.textResult(env, mapper)
    val json = text.toString
    assert(json.contains("\"status\":\"ok\""), s"expected 'ok' status, got: $json")
    assert(json.contains("\"model\":\"flights\""), s"expected model in JSON, got: $json")
    assert(json.contains("\"carrier\""), s"expected field name in JSON, got: $json")
  }

  test("JSON wire shape: getDependenciesData round-trips through the McpJsonMapper") {
    val models = setupModels()
    val handler = new Lineage(models)
    val env = handler.getDependencies(GetDependenciesRequest("flights"))
    val text = Handlers.textResult(env, mapper)
    val json = text.toString
    assert(json.contains("\"status\":\"ok\""), s"expected 'ok' status, got: $json")
    assert(json.contains("\"model\":\"flights\""), s"expected model in JSON, got: $json")
    assert(json.contains("\"upstream\":[]"), s"expected empty upstream, got: $json")
    assert(json.contains("\"downstream\":[]"), s"expected empty downstream, got: $json")
  }

  // ---------------------------------------------------------------------
  // Argument parser tests — these exercise the MCP request parsing.
  // The parse methods are package-private in the Lineage object so
  // the test can call them directly.
  // ---------------------------------------------------------------------

  test("parseGetFieldLineageRequest: model_name required") {
    val ex = intercept[IllegalArgumentException] {
      Lineage.parseGetFieldLineageRequest(java.util.Collections.emptyMap[String, Object]())
    }
    assert(ex.getMessage.contains("model_name"))
  }

  test("parseGetFieldLineageRequest: field_name optional") {
    val args = new java.util.HashMap[String, Object]()
    args.put("model_name", "flights")
    val req = Lineage.parseGetFieldLineageRequest(args)
    assert(req.model_name == "flights")
    assert(req.field_name.isEmpty)
  }

  test("parseGetDependenciesRequest: model_name required") {
    val ex = intercept[IllegalArgumentException] {
      Lineage.parseGetDependenciesRequest(java.util.Collections.emptyMap[String, Object]())
    }
    assert(ex.getMessage.contains("model_name"))
  }
}
