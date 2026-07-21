package io.semanticdf.mcp.handlers

import io.modelcontextprotocol.json.McpJsonMapper
import io.modelcontextprotocol.server.McpServerFeatures.SyncToolSpecification
import io.modelcontextprotocol.spec.McpSchema.{CallToolRequest, Tool}
import io.modelcontextprotocol.server.McpSyncServerExchange
import io.semanticdf.mcp.{Envelope, Handlers, Models}

/** `list_models` handler — the bootstrap tool. Reports which models the
  * server has loaded, with their human-readable descriptions.
  *
  * Per `mcp-contract.md` v2 §"Tool 1: list_models":
  *
  *   Request:  `{}`
  *   Response: `{"models": [{"name": "...", "description": "..."}, ...]}`
  *
  * Models are returned sorted alphabetically by name so the agent gets a
  * deterministic order (and so the response can be diffed across server
  * restarts without false positives from `Map` ordering).
  *
  * The handler takes no arguments — `request` is a `Unit`-shaped call. The
  * `name` parameter on the carrier resolves to the top-level model name
  * (not the YAML file's key) when set; falls back to the file key for
  * anonymous models.
  */
final class ListModels {

  /** Type the handler returns — mirrors the contract's response shape. */
  final case class Data(
      models: List[ModelSummary],
  )

  final case class ModelSummary(
      name: String,
      description: String,
      /** Lifecycle status — `"draft"` / `"published"` / `"deprecated"`.
        * Wire-stable lowercase string mirroring `SemanticTable.status`. */
      status: String,
  )

  def handle(registry: io.semanticdf.mcp.Models): Envelope[Data] = {
    // Alphabetical order on the registry key for deterministic response
    // shape — both the `models` list and the warnings list are sorted,
    // so snapshot tests can rely on stable ordering.
    val sorted = registry.all.toList.sortBy(_._1)
    val data = Data(
      models = sorted.map { case (modelName, t) =>
        // The YamlLoader map key is the YAML top-level key — that's the
        // canonical model name. Use it directly; fall back to the
        // SemanticTable's own name field only if the loader key is empty
        // (shouldn't happen, but defensive).
        val name = if (modelName.nonEmpty) modelName
                   else t.name.getOrElse("<unnamed>")
        ModelSummary(
          name        = name,
          description = t.description.getOrElse(""),
          status      = t.status.asString,
        )
      },
    )
    val warnings = sorted.flatMap { case (modelName, t) =>
      Handlers.lifecycleWarnings(modelName, t.status)
    }.toList
    Envelope.ok(data, warnings = warnings)
  }
}

/** Companion: registers the `list_models` tool in the MCP server. Kept at
  * companion scope so `Server.build` can stay compact. */
object ListModels {
  def registerSpec(registry: Models, mapper: McpJsonMapper): SyncToolSpecification = {
    val handler = new ListModels()
    val tool = new Tool.Builder()
      .name("list_models")
      .description("List all loaded semantic models. Returns each model's name and human-readable description.")
      .inputSchema(Handlers.emptySchema)
      .build()
    new SyncToolSpecification(
      tool,
      (_exchange: McpSyncServerExchange, _arguments: java.util.Map[String, Object]) => {
        Handlers.textResult(handler.handle(registry), mapper)
      },
    )
  }
}
