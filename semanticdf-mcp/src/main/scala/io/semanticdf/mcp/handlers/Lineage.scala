package io.semanticdf.mcp.handlers

import io.modelcontextprotocol.json.McpJsonMapper
import io.modelcontextprotocol.server.McpServerFeatures.SyncToolSpecification
import io.modelcontextprotocol.spec.McpSchema.Tool
import io.semanticdf.lineage.{Lineage, ModelLineage}
import io.semanticdf.mcp.{Envelope, ErrorEnvelope, Handlers, Models}

import java.util.{List => JList}
import scala.jdk.CollectionConverters._

// =====================================================================
// Wire DTOs — file-level (matches the Query.scala pattern: case classes
// are visible to both the class body and the companion object).
// =====================================================================

/** `get_field_lineage` request. */
final case class GetFieldLineageRequest(
    model_name: String,
    field_name: Option[String] = None,
)

/** `get_field_lineage` response. */
final case class GetFieldLineageData(
    model:      String,
    dimensions: List[FieldJson],
    measures:   List[FieldJson],
    transforms: List[FieldJson],
    /** The `sourceTable` for the model (None ⇒ no source table set). */
    sources:    List[String],
    kind:       String,            // batch / streaming
)

/** `get_dependencies` request. */
final case class GetDependenciesRequest(model_name: String)

/** `get_dependencies` response. */
final case class GetDependenciesData(
    model:      String,
    upstream:   List[String],
    downstream: List[String],
)

/** Per-field JSON shape. Used by `get_field_lineage`. */
final case class FieldJson(
    name:        String,
    kind:        String,           // dimension | measure | transform
    baseColumns: List[String],     // the source columns this field reads
    dependsOn:   List[String],     // other FIELD names (for calc measures)
    exprString:  String,           // the SQL form (empty if not set)
    status:      String,           // complete | partial | opaque
)

// =====================================================================
// Handler class
// =====================================================================

/** `lineage` handler — exposes the static-analysis lineage (PRs #207/#208)
  * to LLM agents.
  *
  * Two tools:
  *
  *   - `get_field_lineage` — returns the per-model lineage
  *     (dimensions / measures / transforms), optionally filtered to
  *     a single field.
  *
  *   - `get_dependencies` — returns the upstream and downstream
  *     model graph for one model.
  *
  * The lineage is built from a single `SemanticTable` (no Spark
  * action — pure static analysis on the op tree).
  */
final class Lineage(models: Models) {

  def getFieldLineage(req: GetFieldLineageRequest): Envelope[GetFieldLineageData] = {
    val st = models(req.model_name)  // throws ModelNotFound on bad name
    val lineage = io.semanticdf.lineage.Lineage.of(st)
    val filtered = req.field_name match {
      case None    => lineage
      case Some(fn) => filterToField(lineage, fn)
    }
    Envelope.ok(GetFieldLineageData(
      model      = filtered.modelId,
      dimensions = filtered.dimensions.map(toFieldJson).toList,
      measures   = filtered.measures.map(toFieldJson).toList,
      transforms = filtered.transforms.map(toFieldJson).toList,
      sources    = filtered.sourceTable.toList,
      kind       = filtered.sourceKind.toString.toLowerCase,
    ))
  }

  def getDependencies(req: GetDependenciesRequest): Envelope[GetDependenciesData] = {
    val st = models(req.model_name)  // throws ModelNotFound on bad name
    // workspaceOf is a single-model workspace; the upstreamOf /
    // downstreamOf indexes are still computed.
    val wl = io.semanticdf.lineage.Lineage.workspaceOf(Map(req.model_name -> st))
    val upstream   = wl.upstreamOf.getOrElse(req.model_name, Set.empty).toList.sorted
    val downstream = wl.downstreamOf.getOrElse(req.model_name, Set.empty).toList.sorted
    Envelope.ok(GetDependenciesData(
      model      = req.model_name,
      upstream   = upstream,
      downstream = downstream,
    ))
  }

  // ---------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------

  /** Filter a `ModelLineage` to a single field. If the field isn't
    * found in dimensions / measures / transforms, the lists are all
    * empty (an empty filter result is more useful than a hard error
    * for an LLM that's typo-ing). */
  private def filterToField(ml: ModelLineage, fieldName: String): ModelLineage =
    ml.copy(
      dimensions = ml.dimensions.filter(_.name == fieldName),
      measures   = ml.measures.filter(_.name == fieldName),
      transforms = ml.transforms.filter(_.name == fieldName),
    )

  private def toFieldJson(c: io.semanticdf.lineage.ColumnLineage): FieldJson =
    FieldJson(
      name        = c.name,
      kind        = c.kind.toString.toLowerCase,
      baseColumns = c.baseColumns.toList,
      dependsOn   = c.dependsOn.toList,
      exprString  = c.exprString.getOrElse(""),
      status      = c.status.toString.toLowerCase,
    )
}

// =====================================================================
// Companion object — tool spec registration
// =====================================================================

object Lineage {

  // ---------------------------------------------------------------------
  // JSON Schemas
  // ---------------------------------------------------------------------

  private def stringProp(description: String): java.util.Map[String, Object] = {
    val m = new java.util.LinkedHashMap[String, Object]()
    m.put("type", "string")
    m.put("description", description)
    m
  }

  private val getFieldLineageSchema: io.modelcontextprotocol.spec.McpSchema.JsonSchema = {
    val props = new java.util.LinkedHashMap[String, Object]()
    props.put("model_name", stringProp(
      "The model name (must be loaded in the MCP server's model registry)"))
    props.put("field_name", stringProp(
      "Optional. If given, return only this field's lineage (a single entry in dimensions/measures/transforms)."))
    new io.modelcontextprotocol.spec.McpSchema.JsonSchema(
      "object", props, JList.of(),
      java.lang.Boolean.TRUE, java.util.Map.of(), java.util.Map.of(),
    )
  }

  private val getDependenciesSchema: io.modelcontextprotocol.spec.McpSchema.JsonSchema = {
    val props = new java.util.LinkedHashMap[String, Object]()
    props.put("model_name", stringProp(
      "The model name (must be loaded in the MCP server's model registry)"))
    new io.modelcontextprotocol.spec.McpSchema.JsonSchema(
      "object", props, JList.of(),
      java.lang.Boolean.TRUE, java.util.Map.of(), java.util.Map.of(),
    )
  }

  // ---------------------------------------------------------------------
  // Tool spec registration — one per tool, matching the existing pattern
  // (AuditLog.registerSpec, Query.registerQuerySpec, etc.)
  // ---------------------------------------------------------------------

  def registerGetFieldLineageSpec(models: Models, mapper: McpJsonMapper): SyncToolSpecification = {
    val handler = new Lineage(models)
    val tool = new Tool.Builder()
      .name("get_field_lineage")
      .description("Return the static-analysis lineage for a model (column-level base columns + " +
        "calc-measure dependencies + join keys). Optional `field_name` filter narrows the " +
        "result to a single field. No Spark action — pure static analysis.")
      .inputSchema(getFieldLineageSchema)
      .build()
    new SyncToolSpecification(
      tool,
      (_exchange: io.modelcontextprotocol.server.McpSyncServerExchange, args: java.util.Map[String, Object]) => {
        try Handlers.textResult(handler.getFieldLineage(parseGetFieldLineageRequest(args)), mapper)
        catch { case e: IllegalArgumentException => Handlers.textResult(ErrorEnvelope.of("INVALID_REQUEST", e.getMessage), mapper) }
      },
    )
  }

  def registerGetDependenciesSpec(models: Models, mapper: McpJsonMapper): SyncToolSpecification = {
    val handler = new Lineage(models)
    val tool = new Tool.Builder()
      .name("get_dependencies")
      .description("Return the upstream and downstream model graph for one model. Useful for " +
        "impact analysis: 'if I change this model, which downstream models are affected?' " +
        "and 'which upstream models does this depend on?'. No Spark action — pure static analysis.")
      .inputSchema(getDependenciesSchema)
      .build()
    new SyncToolSpecification(
      tool,
      (_exchange: io.modelcontextprotocol.server.McpSyncServerExchange, args: java.util.Map[String, Object]) => {
        try Handlers.textResult(handler.getDependencies(parseGetDependenciesRequest(args)), mapper)
        catch { case e: IllegalArgumentException => Handlers.textResult(ErrorEnvelope.of("INVALID_REQUEST", e.getMessage), mapper) }
      },
    )
  }

  // ---------------------------------------------------------------------
  // Argument parsers
  // ---------------------------------------------------------------------

  private[mcp] def parseGetFieldLineageRequest(args: java.util.Map[String, Object]): GetFieldLineageRequest = {
    val m = args.asScala.toMap.asInstanceOf[Map[String, Any]]
    val modelName = m.get("model_name") match {
      case Some(s: String) => s
      case _ => throw new IllegalArgumentException("`model_name` is required")
    }
    GetFieldLineageRequest(
      model_name = modelName,
      field_name = m.get("field_name").collect { case s: String => s },
    )
  }

  private[mcp] def parseGetDependenciesRequest(args: java.util.Map[String, Object]): GetDependenciesRequest = {
    val m = args.asScala.toMap.asInstanceOf[Map[String, Any]]
    val modelName = m.get("model_name") match {
      case Some(s: String) => s
      case _ => throw new IllegalArgumentException("`model_name` is required")
    }
    GetDependenciesRequest(model_name = modelName)
  }
}
