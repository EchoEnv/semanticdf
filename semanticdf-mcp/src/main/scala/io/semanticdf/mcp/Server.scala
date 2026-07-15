package io.semanticdf.mcp

import io.modelcontextprotocol.json.McpJsonMapper
import io.modelcontextprotocol.server.McpServer
import io.modelcontextprotocol.server.McpServerFeatures.SyncToolSpecification
import io.modelcontextprotocol.server.McpSyncServer
import io.modelcontextprotocol.server.transport.StdioServerTransportProvider
import io.modelcontextprotocol.spec.McpSchema
import io.modelcontextprotocol.spec.McpSchema.CallToolResult
import io.modelcontextprotocol.spec.McpSchema.JsonSchema
import io.modelcontextprotocol.spec.McpSchema.ServerCapabilities
import io.modelcontextprotocol.spec.McpSchema.TextContent
import io.modelcontextprotocol.spec.McpSchema.Tool
import io.semanticdf.mcp.handlers.ListModels
import org.slf4j.LoggerFactory

import java.util.{List => JList}

/** Server construction — wires the stdio transport, the SDK's McpServer, and
  * the registered tools. For MCP-1a we only register `list_models`; subsequent
  * PRs add the remaining 4 tools (`describe_model`, `query`, `explain`, `introspect`).
  *
  * The handler output is JSON-serialised into the MCP `TextContent` payload.
  * Per `mcp-contract.md` v2 §"Result envelope", the JSON is always wrapped in
  * the [[Envelope]] shape — agents parse one shape, not two. */
object Server {

  private val log = LoggerFactory.getLogger(getClass)

  /** Build the McpServer. Caller runs `server.close()` when shutting down. */
  def build(models: Models, mapper: McpJsonMapper): McpSyncServer = {
    val transport = new StdioServerTransportProvider(mapper)

    McpServer.sync(transport)
      .serverInfo("semanticdf-mcp", "0.1.0")
      .capabilities(
        ServerCapabilities.builder()
          .tools(true)            // tools list may change as we add them in subsequent PRs
          .logging()
          .build(),
      )
      .tools(java.util.List.of(registerListModels(models, mapper)))
      .build()
  }

  /** One [[SyncToolSpecification]] for `list_models`. The MCP SDK calls our
    * handler with a `CallToolRequest` whose `arguments` map is the JSON object
    * the agent sent — empty for `list_models`. The handler returns a
    * `CallToolResult` whose `content` is the JSON envelope as `TextContent`. */
  private def registerListModels(models: Models, mapper: McpJsonMapper): SyncToolSpecification = {
    val tool = new Tool.Builder()
      .name("list_models")
      .description("List all loaded semantic models. Returns each model's name and human-readable description.")
      .inputSchema(emptyObjectSchema())
      .build()

    val handler = new ListModels()
    new SyncToolSpecification(
      tool,
      (exchange: io.modelcontextprotocol.server.McpSyncServerExchange, arguments: java.util.Map[String, Object]) => {
        // `list_models` takes no arguments — ignore the map. The MCP SDK's
        // built-in JSON-Schema validator has already enforced this (we set
        // the schema to require no properties above).
        val env = handler.handle(models)
        textResult(env, mapper)
      },
    )
  }

  /** JSON Schema for "this tool takes no arguments".
    *
    * `{"type": "object", "properties": {}, "required": [], "additionalProperties": false}`
    * — the canonical shape per JSON Schema 2020-12. Required by the MCP SDK's
    * tool input validation. */
  private def emptyObjectSchema(): JsonSchema = new JsonSchema(
    "object",
    java.util.Map.of(),
    JList.of(),
    java.lang.Boolean.FALSE,
    java.util.Map.of(),
    java.util.Map.of(),
  )

  /** Wrap any value into a `CallToolResult` whose single `TextContent` is
    * the JSON-serialised form. Errors travel the same way (with `isError = true`). */
  private[mcp] def textResult[T](value: T, mapper: McpJsonMapper): CallToolResult = {
    val json = mapper.writeValueAsString(value)
    CallToolResult.builder()
      .content(JList.of(new TextContent(json)))
      .build()
  }
}
